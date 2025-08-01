import pytest
#from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from zip_dcm_ds import ZipDCMDataSource


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a SparkSession (the entry point to Spark functionality) on
    the cluster in the remote Databricks workspace. Unit tests do not
    have access to this SparkSession by default.
    """
    #sparkSession = DatabricksSession.builder.serverless(True).getOrCreate()
    sparkSession = (SparkSession.builder.getOrCreate())
    sparkSession.dataSource.register(ZipDCMDataSource)
    return sparkSession


@pytest.fixture(scope="session", autouse=True)
def setup_teardown_database(spark: SparkSession):
    """
    Session-scoped fixture that creates the database and volume at the start of the test session
    and drops them after all tests are completed.
    """


@pytest.fixture(autouse=True)
def cleanup_after_test(spark: SparkSession):
    """
    Function-scoped fixture that cleans up tables and folders before each test.
    """
