import logging
from datetime import timedelta
from pathlib import Path

from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask
from tests.e2e.conftest import validate_run_status, TEST_CATALOG

logger = logging.getLogger(__name__)


def test_run_zipdcm_demo_notebook(
    ws,
    library_ref,
    make_notebook,
    make_directory,
    make_schema,
    make_job,
    test_compute_cluster,
):
    directory = make_directory(path=Path(__file__).parent.parent.parent / "examples" / "zipdcm").as_fuse().as_posix()
    path = directory / "zip-dicom-demo.ipynb"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.JUPYTER)

    catalog = TEST_CATALOG
    schema = make_schema(catalog=catalog).name
    notebook_path = notebook.as_fuse().as_posix()

    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "catalog": catalog,
            "schema": schema,
            "demo_file_directory": directory,
            "test_library_ref": library_ref,
        },
    )
    job = make_job(tasks=[notebook_task], existing_cluster_id=test_compute_cluster)

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=20),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run '{run.run_id}' completed with status '{run.state.life_cycle_state}'")
