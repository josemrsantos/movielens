from unittest.mock import patch, MagicMock
import pytest
from movielens_pipeline import load_data, calculate_movie_ratings_statistics, output_result, get_spark_session


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_when_load_data_file_not_found_then_raise_exception(spark):
    with patch("pyspark.sql.DataFrameReader.csv", side_effect=FileNotFoundError("File not found")):
        with pytest.raises(FileNotFoundError, match="File not found"):
            load_data(spark, "/invalid_path", "movies", "movieId INT, title STRING, genres STRING")


def test_when_load_data_general_exception_then_raise_exception(spark):
    with patch("pyspark.sql.DataFrameReader.csv", side_effect=Exception("General error")):
        with pytest.raises(Exception, match="General error"):
            load_data(spark, "/invalid_path", "movies", "movieId INT, title STRING, genres STRING")


def test_when_calculate_movie_ratings_statistics_then_raise_exception(spark):
    ratings_df = spark.createDataFrame([], "userId INT, movieId INT, rating FLOAT, timestamp LONG")
    movies_df = spark.createDataFrame([], "movieId INT, title STRING, genres STRING")
    with patch("pyspark.sql.DataFrame.groupBy", side_effect=Exception("General error")):
        with pytest.raises(Exception, match="General error"):
            calculate_movie_ratings_statistics(ratings_df, movies_df)


def test_when_output_result_then_raise_exception(spark):
    ratings_df = spark.createDataFrame([], "userId INT, movieId INT, rating FLOAT, timestamp LONG")
    movies_df = spark.createDataFrame([], "movieId INT, title STRING, genres STRING")
    movies_with_stats_df = spark.createDataFrame([], "movieId INT, title STRING, genres STRING, max_rating FLOAT, min_rating FLOAT, avg_rating FLOAT")
    top_movies_per_user_df = spark.createDataFrame([], "userId INT, movieId INT, rating FLOAT, timestamp LONG, rank INT")
    with patch("pyspark.sql.DataFrameWriter.parquet", side_effect=Exception("General error")):
        with pytest.raises(Exception, match="General error"):
            output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, "/invalid_path")


def test_when_exception_from_code_in_get_spark_session_then_throw_exception(spark):
    with patch("pyspark.sql.SparkSession.builder"):
        with pytest.raises(Exception, match="General error"):
            with get_spark_session(app_name="test"):
                raise Exception("General error")
