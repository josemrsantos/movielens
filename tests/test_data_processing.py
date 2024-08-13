import pytest
from pyspark.sql import SparkSession
from movielens_pipeline import calculate_movie_ratings_statistics


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_when_calculate_movie_ratings_statistics_then_correct_columns(spark):
    ratings_data = [
        (1, 1, 4.0),
        (1, 2, 5.0),
        (2, 1, 3.0),
        (2, 2, 4.0)
    ]
    movies_data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy"),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")
    ]
    ratings_schema = "userId INT, movieId INT, rating FLOAT"
    movies_schema = "movieId INT, title STRING, genres STRING"
    ratings_df = spark.createDataFrame(ratings_data, schema=ratings_schema)
    movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
    movies_with_stats_df, top_movies_per_user_df = calculate_movie_ratings_statistics(ratings_df, movies_df)
    assert "max_rating" in movies_with_stats_df.columns
    assert "min_rating" in movies_with_stats_df.columns
    assert "avg_rating" in movies_with_stats_df.columns


def test_when_calculate_movie_ratings_statistics_with_empty_data_then_empty_result(spark):
    ratings_data = []
    movies_data = []
    ratings_schema = "userId INT, movieId INT, rating FLOAT"
    movies_schema = "movieId INT, title STRING, genres STRING"
    ratings_df = spark.createDataFrame(ratings_data, schema=ratings_schema)
    movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
    movies_with_stats_df, top_movies_per_user_df = calculate_movie_ratings_statistics(ratings_df, movies_df)
    assert movies_with_stats_df.count() == 0
    assert top_movies_per_user_df.count() == 0
