import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock
from movielens_pipeline import output_result


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_when_output_result_then_parquet_called(spark):
    ratings_data = [
        (1, 1, 4.0, 964982703),
        (1, 2, 4.0, 964982224),
        (2, 1, 5.0, 964982931)
    ]
    ratings_schema = "userId INT, movieId INT, rating FLOAT, timestamp LONG"
    ratings_df = spark.createDataFrame(ratings_data, schema=ratings_schema)
    movies_data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy"),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")
    ]
    movies_schema = "movieId INT, title STRING, genres STRING"
    movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
    movies_with_stats_data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy", 5.0, 4.0, 4.5),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy", 4.0, 4.0, 4.0)
    ]
    movies_with_stats_schema = "movieId INT, title STRING, genres STRING, max_rating FLOAT, min_rating FLOAT, avg_rating FLOAT"
    movies_with_stats_df = spark.createDataFrame(movies_with_stats_data, schema=movies_with_stats_schema)
    top_movies_per_user_data = [
        (1, 1, 4.0, 964982703, 1),
        (1, 2, 4.0, 964982224, 2),
        (2, 1, 5.0, 964982931, 1)
    ]
    top_movies_per_user_schema = "userId INT, movieId INT, rating FLOAT, timestamp LONG, rank INT"
    top_movies_per_user_df = spark.createDataFrame(top_movies_per_user_data, schema=top_movies_per_user_schema)
    output_base_path = "/tmp"
    with patch("pyspark.sql.DataFrameWriter.parquet") as mock_write_parquet:
        output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, output_base_path)
        mock_write_parquet.assert_called()
        assert mock_write_parquet.call_count == 4


def test_when_output_result_with_invalid_path_then_raise_exception(spark):
    ratings_data = [
        (1, 1, 4.0, 964982703),
        (1, 2, 4.0, 964982224),
        (2, 1, 5.0, 964982931)
    ]
    ratings_schema = "userId INT, movieId INT, rating FLOAT, timestamp LONG"
    ratings_df = spark.createDataFrame(ratings_data, schema=ratings_schema)

    movies_data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy"),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")
    ]
    movies_schema = "movieId INT, title STRING, genres STRING"
    movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
    movies_with_stats_data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy", 5.0, 4.0, 4.5),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy", 4.0, 4.0, 4.0)
    ]
    movies_with_stats_schema = "movieId INT, title STRING, genres STRING, max_rating FLOAT, min_rating FLOAT, avg_rating FLOAT"
    movies_with_stats_df = spark.createDataFrame(movies_with_stats_data, schema=movies_with_stats_schema)
    top_movies_per_user_data = [
        (1, 1, 4.0, 964982703, 1),
        (1, 2, 4.0, 964982224, 2),
        (2, 1, 5.0, 964982931, 1)
    ]
    top_movies_per_user_schema = "userId INT, movieId INT, rating FLOAT, timestamp LONG, rank INT"
    top_movies_per_user_df = spark.createDataFrame(top_movies_per_user_data, schema=top_movies_per_user_schema)
    invalid_output_base_path = "/invalid_path"
    with patch("pyspark.sql.DataFrameWriter.parquet", side_effect=Exception("General error")):
        with pytest.raises(Exception, match="General error"):
            output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, invalid_output_base_path)
