from hymir.executor import WorkflowState, JobState
from hymir.job import Retry
from hymir.executors.celery import CeleryExecutor
from hymir.workflow import (
    Workflow,
    job,
    Chain,
)


@job()
def job_that_retries():
    return Retry(wait_min=1, wait_max=2, max_retries=3)


@job()
def job_never_reached():
    pass


def test_retry_failure(celery_session_worker):
    """
    Ensures a workflow fails properly when a job retries too many times.
    """
    workflow = Workflow(Chain(job_that_retries(), job_never_reached()))

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.FAILURE

    states = executor.job_states(workflow_id)
    assert states["1"].status == JobState.Status.FAILURE
    assert states["1"].retries == 3
