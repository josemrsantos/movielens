import pytest
from unittest.mock import patch, MagicMock
from movielens_pipeline import main


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_when_main_function_called_then_no_exceptions(spark):
    test_args = ["movielens_pipeline.py", "--input-path", "/path/to/input", "--output-path", "/path/to/output"]
    with patch("movielens_pipeline.load_data") as mock_load_data, \
         patch("movielens_pipeline.calculate_movie_ratings_statistics") as mock_calculate_stats, \
         patch("movielens_pipeline.output_result") as mock_output_result, \
         patch("movielens_pipeline.get_spark_session", return_value=spark), \
         patch("sys.argv", test_args):
        mock_load_data.return_value = MagicMock()
        mock_calculate_stats.return_value = (MagicMock(), MagicMock())
        try:
            main()
        except Exception as e:
            pytest.fail(f"main() raised {e} unexpectedly!")
