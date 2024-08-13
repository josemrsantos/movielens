import logging
from pyspark.sql import SparkSession
from contextlib import contextmanager


@contextmanager
def get_spark_session(app_name="MovieLensPipeline", shuffle_partitions="200", executor_memory="4g"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    # Set Spark logging level to WARN to reduce verbosity
    spark_logger = logging.getLogger('py4j')
    spark_logger.setLevel(logging.WARN)
    # Initialize Spark session
    with get_spark_session() as spark:
        # Set Spark context logging level to WARN to reduce verbosity
        spark.sparkContext.setLogLevel("OFF")
        # Read the outputted Parquet files into new DataFrames for analysis
        movies_with_stats_df_new = spark.read.parquet(f"./output/movies_with_stats/")
        top_movies_per_user_df_new = spark.read.parquet(f"./output/top_movies_per_user/")
        ratings_df_new = spark.read.parquet(f"./output/ratings/")
        movies_df_new = spark.read.parquet(f"./output/movies/")
        # Register the DataFrames as temporary views
        movies_with_stats_df_new.createOrReplaceTempView("movies_with_stats")
        top_movies_per_user_df_new.createOrReplaceTempView("top_movies_per_user")
        ratings_df_new.createOrReplaceTempView("ratings")
        movies_df_new.createOrReplaceTempView("movies")
        # Check that we have added 3 new columns to the movies_with_stats
        movies_base_fields = "movieId, title, genres"
        spark.sql(f"SELECT {movies_base_fields}, max_rating, min_rating, avg_rating FROM movies_with_stats ORDER BY movieId LIMIT 10").show()
        spark.sql( f"SELECT {movies_base_fields} FROM movies ORDER BY movieId LIMIT 10").show()
        # Number of unique users
        spark.sql("SELECT count(DISTINCT userid) FROM ratings").show()
        # 6040
        spark.sql("SELECT count(distinct userid) FROM top_movies_per_user").show()
        # 6040 - We expect this to be 6040 as we have 6040 unique users in the ratings dataset
        spark.sql("SELECT count(*) FROM top_movies_per_user").show()
        # 18120 - We expect this to be <= 18120 as 6040*3=18120
        # Number of movies
        spark.sql("SELECT count(*) FROM movies").show()
        # 3883
        spark.sql("SELECT count(*) FROM movies_with_stats").show()
        # 3883 - We expect this to be 3883 as we have 3883 unique movies


if __name__ == "__main__":
    main()