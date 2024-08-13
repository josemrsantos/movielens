import pytest
from movielens_pipeline import get_spark_session


def test_when_spark_session_created_then_not_none():
    with get_spark_session(app_name="TestApp") as spark:
        assert spark is not None
        assert spark.sparkContext.appName == "TestApp"


def test_when_spark_session_exception_then_logged_and_raised():
    with pytest.raises(Exception):
        with get_spark_session(app_name="InvalidApp") as spark:
            raise Exception("Forced exception for testing")
