"""
Shared pytest fixtures for python-data-sources tests.
"""

import logging
import os
import pytest
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, TerminationTypeType

logger = logging.getLogger(__name__)
logging.getLogger("tests").setLevel(logging.DEBUG)
logging.getLogger("databricks_industry_solutions.python_data_sources").setLevel(logging.DEBUG)

TEST_CATALOG = "python_data_sources_tests"


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


@pytest.fixture
def library_ref() -> str:
    test_library_ref = "git+https://github.com/databricks-industry-solutions/python-data-sources"
    if os.getenv("REF_NAME"):
        test_library_ref = f"{test_library_ref}.git@refs/pull/{os.getenv('REF_NAME')}"
    return test_library_ref


@pytest.fixture
def test_compute_cluster(cluster_type: str) -> str | None:
    if cluster_type == "classic":
        return os.getenv("TEST_CLASSIC_CLUSTER_ID")
    if cluster_type == "serverless":
        return None
    raise ValueError(f"Invalid cluster type: {cluster_type}")


def validate_run_status(run: Run, client: WorkspaceClient) -> None:
    """
    Validates that a job task run completed successfully.

    Args:
        run: `Run` object returned from a `WorkspaceClient.jobs.submit(...)` command
        client: `WorkspaceClient` object for getting task output
    """
    task = run.tasks[0]
    termination_details = run.status.termination_details

    run_output = client.jobs.get_run_output(task.run_id)
    logger.info("Run output:")
    logger.info(run_output.as_dict())
    assert termination_details.type == TerminationTypeType.SUCCESS, (
        f"Run of '{task.task_key}' "
        f"failed with message: {run_output.error}, "
        f"error trace: {run_output.error_trace}"
    )
