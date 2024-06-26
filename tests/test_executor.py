import time

import pytest

from hymir.errors import WorkflowDoesNotExist
from hymir.executor import WorkflowState, JobState
from hymir.job import Retry, Failure, CheckLater, Success, job
from hymir.executors.celery import CeleryExecutor
from hymir.workflow import (
    Workflow,
    Chain,
)


@job()
def job_that_succeeds():
    return Success("This job completed.")


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


@job(inputs=["job_state"])
def job_that_checks_later_with_context(job_state: JobState):
    if job_state.context.get("retries", 0) < 3:
        return CheckLater(
            context={"retries": job_state.context.get("retries", 0) + 1}
        )

    return Success("This job completed.")


@job()
def job_invalid_return():
    return "This is not a valid return type."


@job()
def job_unhandled_exception():
    raise Exception("This is an unhandled exception.")


@job(inputs=["job_id", "workflow_id", "workflow"], output="the_ids")
def job_that_needs_input(job_id: str, workflow_id: str, workflow: Workflow):
    assert isinstance(workflow, Workflow)
    return Success(
        {
            "job_id": job_id,
            "workflow_id": workflow_id,
        }
    )


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


def test_check_later_with_context(celery_session_worker):
    """
    Ensures a job that checks later can be retried with context.
    """
    workflow = Workflow(Chain(job_that_checks_later_with_context()))

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


def test_job_needs_input(celery_session_worker):
    """
    Ensures a job that needs input can be run properly.
    """
    workflow = Workflow(Chain(job_that_needs_input()))

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    ws = executor.wait(workflow_id)
    assert ws.status == WorkflowState.Status.SUCCESS

    states = executor.job_states(workflow_id)
    assert states["1"].status == JobState.Status.SUCCESS

    outputs = executor.outputs(workflow_id)
    assert outputs == {"the_ids": [{"job_id": "1", "workflow_id": workflow_id}]}


def test_workflow_progress(celery_session_worker):
    """
    Ensures that the progress of a workflow can be monitored.
    """
    workflow = Workflow(
        Chain(
            job_that_succeeds(),
        )
    )

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    ws = executor.wait(workflow_id)
    assert ws.status == WorkflowState.Status.SUCCESS
    assert executor.progress(workflow_id) == (1, 1)

    workflow = Workflow(
        Chain(
            job_that_fails(),
            job_that_succeeds(),
        )
    )

    workflow_id = executor.run(workflow)
    ws = executor.wait(workflow_id)
    assert ws.status == WorkflowState.Status.FAILURE
    assert executor.progress(workflow_id) == (1, 2)


def test_invalid_workflow(celery_session_worker):
    """
    Ensure we raise an exception when we try to fetch workflow's that do not
    exist.
    """
    executor = CeleryExecutor()
    with pytest.raises(WorkflowDoesNotExist):
        executor.workflow("does_not_exist")


def test_job_states(celery_session_worker):
    """
    Ensure we can fetch the states of all jobs or specific jobs in a workflow.
    """
    workflow = Workflow(
        Chain(
            job_that_succeeds(),
            job_that_succeeds(),
            job_that_succeeds(),
        )
    )

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    ws = executor.wait(workflow_id)
    assert ws.status == WorkflowState.Status.SUCCESS

    states = executor.job_states(workflow_id)
    assert len(states) == 3

    states = executor.job_states(workflow_id, job_ids=["1", "2"])
    assert len(states) == 2


def test_non_blocking_wait(celery_session_worker):
    """
    Ensure we can wait for a workflow to finish without blocking.
    """
    workflow = Workflow(
        Chain(
            job_that_checks_later(time.time()),
        )
    )

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    ws = executor.wait(workflow_id, block=False)
    assert ws.status in [
        WorkflowState.Status.RUNNING,
        WorkflowState.Status.PENDING,
    ]

    ws = executor.wait(workflow_id)
    assert ws.status == WorkflowState.Status.SUCCESS


def test_clear(celery_session_worker):
    """
    Ensure that we can clear an existing workflow.
    """
    workflow = Workflow(
        Chain(
            job_that_succeeds(),
        )
    )

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    ws = executor.wait(workflow_id)
    assert ws.status == WorkflowState.Status.SUCCESS

    executor.clear(workflow_id)

    with pytest.raises(WorkflowDoesNotExist):
        executor.workflow(workflow_id)
