import argparse
import logging
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set Spark logging level to WARN to reduce verbosity
spark_logger = logging.getLogger('py4j')
spark_logger.setLevel(logging.FATAL)


@contextmanager
def get_spark_session(app_name="MovieLensPipeline"):
    """
    Initializes a SparkSession with the provided configuration.
    :param app_name: The name of the Spark application.
    :return: A SparkSession instance.
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        yield spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise
    else:
        spark.stop()


def load_data(spark, base_path, file_name, schema):
    """
    Load the data from the specified "CSV" file into a DataFrame with the specified schema.
    In this case, the separated values are separated by '::'.

    :param spark: The SparkSession instance.
    :param base_path: The base path for the data files.
    :param file_name: The name of the data file.
    :param schema: The schema for the DataFrame.
    :return: A DataFrame containing the loaded data. (lazy evaluation, meaning the data is not loaded until it's needed)
    """
    try:
        file_path = f"{base_path}/{file_name}.dat"
        return spark.read.csv(file_path, sep='::', schema=schema)
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_name}.dat - {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading data from {file_name}: {e}")
        raise


def calculate_movie_ratings_statistics(ratings_df, movies_df):
    """
    Calculate the maximum, minimum, and average rating for each movie.
    Join the movie data with the ratings statistics.
    Also, add a rank column based on the user's ratings for each movie, considering the highest rating.

    :param ratings_df: DataFrame of user ratings
    :param movies_df: DataFrame of movies
    :return: DataFrame of movies with ratings statistics and top movies per user
    """
    try:
        ratings_stats_df = ratings_df.groupBy("movieId").agg(
            F.max("rating").alias("max_rating"),
            F.min("rating").alias("min_rating"),
            F.avg("rating").alias("avg_rating")
        )
        movies_with_stats_df = movies_df.join(ratings_stats_df, on="movieId", how="left")
        window_spec = Window.partitionBy("userId").orderBy(F.col("rating").desc())
        top_movies_per_user_df = ratings_df.withColumn(
            "rank",
            F.row_number().over(window_spec)
        ).filter(F.col("rank") <= 3)
        return movies_with_stats_df, top_movies_per_user_df
    except Exception as e:
        logger.error(f"Error calculating movie ratings statistics: {e}")
        raise


def output_result(ratings_df,
                  movies_df,
                  movies_with_stats_df,
                  top_movies_per_user_df,
                  output_base_path):
    """
    Write the input DataFrames to Parquet files at the specified output path.

    :param ratings_df: DataFrame of user ratings
    :param movies_df: DataFrame of movies
    :param movies_with_stats_df: DataFrame of movie ratings statistics
    :param top_movies_per_user_df: DataFrame of top movies per user
    :param output_base_path: Output base path for the Parquet files
    :return: None
    """
    # Output the DataFrames to Parquet files
    try:
        movies_with_stats_df.coalesce(2).write.mode("overwrite").parquet(f"{output_base_path}/movies_with_stats/")
        top_movies_per_user_df.coalesce(2).write.mode("overwrite").parquet(f"{output_base_path}/top_movies_per_user/")
        ratings_df.coalesce(2).write.mode("overwrite").parquet(f"{output_base_path}/ratings/")
        movies_df.coalesce(2).write.mode("overwrite").parquet(f"{output_base_path}/movies/")
        logger.info("Dataframes have been written to parquet files.")
    except Exception as e:
        logger.error(f"Error writing results to Parquet files: {e}")
        raise
    # stdout log some stats for logging and debugging purposes
    # Log the number of records in each dataframe
    logger.info(f"Number of records in ratings dataframe: {ratings_df.count()}")
    logger.info(f"Number of records in movies dataframe: {movies_df.count()}")
    logger.info(f"Number of records in movies_with_stats_df dataframe: {movies_with_stats_df.count()}")
    logger.info(f"Number of records in top_movies_per_user dataframe: {top_movies_per_user_df.count()}")
    # Log 10 random records in each dataframe
    logger.info("\n10 random records in ratings dataframe:")
    ratings_df.orderBy(F.rand()).show(10)
    logger.info("\n10 random records in movies dataframe:")
    movies_df.orderBy(F.rand()).show(10)
    logger.info("\n10 random records in movies_with_stats dataframe:")
    movies_with_stats_df.orderBy(F.rand()).show(10)
    logger.info("\n10 random records in top_movies_per_user dataframe:")
    top_movies_per_user_df.orderBy(F.rand()).show(10)


def parse_args():
    parser = argparse.ArgumentParser(description="MovieLens Data Processing Pipeline")
    parser.add_argument("--input-path", type=str, required=True, help="Input path for the data")
    parser.add_argument("--output-path", type=str, required=True, help="Output path for the results")
    return parser.parse_args()


def main():
    args = parse_args()
    # Capture command-line arguments
    input_path = args.input_path
    output_base_path = args.output_path
    # Initialize Spark session
    with get_spark_session(app_name="MovieLensPipeline") as spark:
        # Set Spark context logging level to WARN to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        # Define schemas for the MovieLens datasets
        ratings_schema = "userId INT, movieId INT, rating FLOAT, timestamp LONG"
        movies_schema = "movieId INT, title STRING, genres STRING"
        # Load datasets
        ratings_df = load_data(spark, input_path, "ratings", ratings_schema)
        movies_df = load_data(spark, input_path, "movies", movies_schema)
        ratings_df.cache()  # Cache this dataframe for efficient query execution
        movies_df.cache()  # Cache this dataframe for efficient query execution
        # Calculate statistics and top movies per user
        movies_with_stats_df, top_movies_per_user_df = calculate_movie_ratings_statistics(ratings_df, movies_df)
        # Cache the dataframes for efficient query execution
        movies_with_stats_df.cache()  # Cache this dataframe for efficient query execution
        top_movies_per_user_df.cache()  # Cache this dataframe for efficient query execution
        # Write the resulting dataframes to the output path
        output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, output_base_path)


if __name__ == "__main__":
    main()
