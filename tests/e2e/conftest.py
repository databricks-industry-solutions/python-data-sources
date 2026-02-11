import logging
import os

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, TerminationTypeType

logger = logging.getLogger(__name__)
logging.getLogger("tests").setLevel(logging.DEBUG)
logging.getLogger("databricks_industry_solutions.python_data_sources").setLevel(logging.DEBUG)

TEST_CATALOG = "gh_catalog_001"


@pytest.fixture
def debug_env_name():
    # This tells pytester which debug env file to look for (e.g., ~/.databricks/debug/ws.env)
    # BUT if DATABRICKS_HOST and auth env vars are set, pytester uses those instead.
    # The file is only a fallback for local development.
    return "ws"


@pytest.fixture
def library_ref() -> str:
    test_library_ref = "git+https://github.com/databricks-industry-solutions/python-data-sources"
    if os.getenv("REF_NAME"):
        test_library_ref = f"{test_library_ref}.git@refs/pull/{os.getenv('REF_NAME')}"
    return test_library_ref


@pytest.fixture
def test_compute_cluster() -> str:
    return os.getenv("DATABRICKS_CLUSTER_ID")


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
