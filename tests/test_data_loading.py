import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from movielens_pipeline import load_data


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_when_load_data_with_valid_path_then_correct_columns_and_count(spark):
    data = [
        (1, "Toy Story (1995)", "Animation|Children's|Comedy"),
        (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")
    ]
    schema = "movieId INT, title STRING, genres STRING"
    df = spark.createDataFrame(data, schema=schema)
    with patch("pyspark.sql.DataFrameReader.csv", return_value=df) as mock_read_csv:
        loaded_df = load_data(spark, "/valid_path", "movies", schema)
        mock_read_csv.assert_called_once()
    assert loaded_df.count() == 2
    assert loaded_df.columns == ["movieId", "title", "genres"]


def test_when_load_data_with_invalid_path_then_raise_exception(spark):
    with patch("pyspark.sql.DataFrameReader.csv", side_effect=FileNotFoundError("File not found")):
        with pytest.raises(FileNotFoundError, match="File not found"):
            load_data(spark, "/invalid_path", "movies", "movieId INT, title STRING, genres STRING")


def test_when_load_data_with_general_exception_then_raise_exception(spark):
    with patch("pyspark.sql.DataFrameReader.csv", side_effect=Exception("General error")):
        with pytest.raises(Exception, match="General error"):
            load_data(spark, "/invalid_path", "movies", "movieId INT, title STRING, genres STRING")
