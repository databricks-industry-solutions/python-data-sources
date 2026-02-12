import logging
import os
from pathlib import Path

import pytest
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, TerminationTypeType

install_logger()
logger = logging.getLogger(__name__)
logging.getLogger("tests").setLevel(logging.DEBUG)
logging.getLogger("databricks_industry_solutions.python_data_sources").setLevel(logging.DEBUG)
TEST_CATALOG = "main"


@pytest.fixture
def is_in_debug() -> bool:
    return False


@pytest.fixture
def library_ref() -> str:
    test_library_ref = "git+https://github.com/databricks-industry-solutions/python-data-sources"
    if os.getenv("REF_NAME"):
        test_library_ref = f"{test_library_ref}.git@refs/pull/{os.getenv('REF_NAME')}"
    return test_library_ref


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


def upload_directory_recursive(ws: WorkspaceClient, local_path: Path, target_path: Path) -> None:
    """
    Recursively uploads a local directory and its contents to a Databricks workspace directory.

    Args:
        ws: `WorkspaceClient` instance
        local_path: `Path` object pointing to the directory to upload
        target_path: `Path` object pointing to the destination in the workspace or a UC volume
    """
    for item in local_path.iterdir():
        dest_path = target_path / item.name

        if item.is_dir():
            upload_directory_recursive(ws, item, dest_path)
            continue

        with open(item, "rb") as f:
            content = f.read()

        ws.workspace.upload(str(dest_path), content, overwrite=True)
        logger.info(f"Uploaded file: {item.name} -> {dest_path}")
