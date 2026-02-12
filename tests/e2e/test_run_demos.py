import logging
from datetime import timedelta
from pathlib import Path

from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask
from tests.e2e.conftest import validate_run_status, upload_directory_recursive, TEST_CATALOG

logger = logging.getLogger(__name__)


def test_run_zipdcm_demo_notebook(
    ws,
    library_ref,
    make_notebook,
    make_directory,
    make_schema,
    make_job,
):
    local_example_dir = Path(__file__).parent.parent.parent / "examples" / "zipdcm"
    local_notebook_path = local_example_dir / "zip-dicom-demo.ipynb"
    local_resources_path = local_example_dir / "resources"

    workspace_dir = make_directory()
    logger.info(f"Created workspace directory: {workspace_dir}")

    with open(local_notebook_path, "rb") as f:
        notebook = make_notebook(path=workspace_dir / "zip-dicom-demo", content=f, format=ImportFormat.JUPYTER)
    logger.info(f"Uploaded notebook: {notebook}")

    resources_workspace_path = WorkspacePath(ws, workspace_dir / "resources")
    upload_directory_recursive(ws, local_resources_path, resources_workspace_path)
    logger.info(f"Uploaded resources to: {resources_workspace_path}")

    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name

    notebook_task = NotebookTask(
        notebook_path=notebook.as_fuse().as_posix(),
        base_parameters={
            "catalog": catalog,
            "schema": schema,
            "test_library_ref": library_ref,
        },
    )
    job = make_job(tasks=[notebook_task])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=20),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run '{run.run_id}' completed with status '{run.state.life_cycle_state}'")
