import time

from hymir.executor import WorkflowState, JobState
from hymir.job import Retry, Failure, CheckLater, Success
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


@job()
def job_that_fails():
    return Failure("This job failed.")


@job()
def job_that_checks_later(started_at: float):
    now = time.time()
    if now - started_at < 10.0:
        return CheckLater()

    return Success("This job completed.")


@job()
def job_invalid_return():
    return "This is not a valid return type."


@job()
def job_unhandled_exception():
    raise Exception("This is an unhandled exception.")


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


def test_failure(celery_session_worker):
    """
    Ensures a workflow fails properly when a job fails.
    """
    workflow = Workflow(Chain(job_that_fails(), job_never_reached()))

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.FAILURE

    states = executor.job_states(workflow_id)
    assert states["1"].status == JobState.Status.FAILURE


def test_check_later(celery_session_worker):
    """
    Ensures a job that checks later can be retried.
    """
    workflow = Workflow(Chain(job_that_checks_later(time.time())))

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.SUCCESS

    states = executor.job_states(workflow_id)
    assert states["1"].status == JobState.Status.SUCCESS


def test_invalid_return(celery_session_worker):
    """
    Ensures a job that returns an invalid value fails properly.
    """
    workflow = Workflow(Chain(job_invalid_return()))

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.FAILURE

    states = executor.job_states(workflow_id)
    assert states["1"].status == JobState.Status.FAILURE


def test_unhandled_exception(celery_session_worker):
    """
    Ensures a job that raises an unhandled exception fails properly.
    """
    workflow = Workflow(Chain(job_unhandled_exception()))

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.FAILURE

    states = executor.job_states(workflow_id)
    assert states["1"].status == JobState.Status.FAILURE
