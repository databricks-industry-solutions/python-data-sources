import logging
import os
from pathlib import Path

import pytest
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, TerminationTypeType
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Kind

install_logger()
logger = logging.getLogger(__name__)
logging.getLogger("tests").setLevel(logging.DEBUG)
logging.getLogger("python_data_sources").setLevel(logging.DEBUG)
TEST_CATALOG = "gh_catalog_001"


@pytest.fixture
def is_in_debug() -> bool:
    return False


@pytest.fixture
def library_ref() -> str:
    test_library_ref = "git+https://github.com/databricks-industry-solutions/python-data-sources"
    if os.getenv("REF_NAME"):
        test_library_ref = f"{test_library_ref}.git@refs/pull/{os.getenv('REF_NAME')}"
    return f"{test_library_ref}.git@{os.getenv('REF_NAME')}"


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
        else:
            ws.files.upload_from(dest_path.as_posix(), item.as_posix())
            logger.info(f"Uploaded file: {item.name} -> {dest_path}")


def new_classic_job_cluster(ws: WorkspaceClient | None = None) -> ClusterSpec:
    _DEFAULT_SPARK_VERSION = "17.3.x-scala2.13"
    if ws is None:
        ws = WorkspaceClient()
    node_type = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
    return ClusterSpec(
        is_single_node=True,
        node_type_id=node_type,
        spark_version=_DEFAULT_SPARK_VERSION,
        kind=Kind.CLASSIC_PREVIEW,
        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        single_user_name=ws.current_user.me().user_name,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
    )
