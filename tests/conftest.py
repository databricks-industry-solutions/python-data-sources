"""
Shared pytest fixtures for python-data-sources tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a SparkSession for testing.
    The session is reused across all tests in the session.
    """
    spark_session = SparkSession.builder.master("local[2]").appName("python-data-sources-tests").getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    yield spark_session
    spark_session.stop()
