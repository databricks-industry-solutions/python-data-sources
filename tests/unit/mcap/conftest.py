"""
MCAP-specific pytest fixtures.
"""

import pytest
from pathlib import Path
from pyspark.sql import SparkSession

from python_data_sources.mcap.mcap_datasource import MCAPDataSource


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a SparkSession with MCAP data source registered.
    """
    spark_session = SparkSession.builder.master("local[2]").appName("mcap-tests").getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    spark_session.dataSource.register(MCAPDataSource)
    yield spark_session
    spark_session.stop()


@pytest.fixture
def test_mcap_file() -> Path:
    """Path to the test MCAP file."""
    return Path(__file__).parent / "test.mcap"
