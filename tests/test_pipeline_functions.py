from unittest.mock import patch, MagicMock
import pytest
from movielens_pipeline import load_data, calculate_movie_ratings_statistics, output_result, get_spark_session


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_when_load_data_then_correct_columns_and_count(spark):
    data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy"),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")
    ]
    schema = "movieId INT, title STRING, genres STRING"
    df = spark.createDataFrame(data, schema=schema)
    with patch("pyspark.sql.DataFrameWriter.csv") as mock_write_csv:
        df.write.csv("/tmp/movies.dat", sep='::', mode="overwrite", header=False)
        mock_write_csv.assert_called_once()
    with patch("pyspark.sql.DataFrameReader.csv", return_value=df) as mock_read_csv:
        loaded_df = load_data(spark, "/tmp", "movies", schema)
        mock_read_csv.assert_called_once()
    assert loaded_df.count() == 2
    assert loaded_df.columns == ["movieId", "title", "genres"]


def test_when_calculate_stats_then_correct_columns(spark):
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
    movies_with_stats_df, top_movies_per_user_df = calculate_movie_ratings_statistics(ratings_df, movies_df)
    assert "max_rating" in movies_with_stats_df.columns
    assert "min_rating" in movies_with_stats_df.columns
    assert "avg_rating" in movies_with_stats_df.columns
    assert "rank" in top_movies_per_user_df.columns


@patch("pyspark.sql.DataFrameWriter.parquet")
def test_when_output_result_then_parquet_called(mock_write_parquet, spark):
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
    movies_with_stats_df, top_movies_per_user_df = calculate_movie_ratings_statistics(ratings_df, movies_df)
    output_base_path = "/tmp"
    output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, output_base_path)
    mock_write_parquet.assert_called()
    assert mock_write_parquet.call_count == 4
