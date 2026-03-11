"""
ZipDCM-specific pytest fixtures.
"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from python_data_sources.zipdcm import ZipDCMDataSource


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a SparkSession with ZipDCM data source registered."""
    spark_session = SparkSession.Builder().getOrCreate()
    spark_session.dataSource.register(ZipDCMDataSource)
    return spark_session


@pytest.fixture
def resources_dir() -> Path:
    """Path to the test resources directory (common with examples)."""
    return Path(__file__).parent.parent.parent.parent / "examples" / "zipdcm" / "resources"


@pytest.fixture(scope="session", autouse=True)
def setup_teardown_database(spark: SparkSession) -> None:
    """Session-scoped fixture for database setup/teardown."""


@pytest.fixture(autouse=True)
def cleanup_after_test(spark: SparkSession) -> None:
    """Function-scoped fixture for cleanup after each test."""
