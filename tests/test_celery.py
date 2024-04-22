from hymir.executor import WorkflowState, JobState
from hymir.job import Success, job
from hymir.executors.celery import CeleryExecutor
from hymir.workflow import (
    Workflow,
    Group,
    Chain,
)


@job(output="reports")
def start_report(report_key: str, options: dict):
    return Success(
        {
            "plan_id": "123",
            "report_key": report_key,
        }
    )


@job(inputs=["reports"])
def process_reports(reports: list[dict]):
    for report in reports:
        pass

    return


@job()
def run_rollups():
    return


@job(inputs=["workflow_id"])
def save_results(workflow_id: str):
    return


@job()
def start_onboarding():
    return


@job()
def task_that_fails():
    raise Exception("This task failed")


def test_celery(celery_session_worker):
    """
    Ensure that a workflow can be executed successfully using the Celery
    executor.
    """
    workflow = Workflow(
        Chain(
            start_onboarding(),
            Group(
                start_report("inventory", {}),
                start_report("sales", {}),
            ),
            process_reports(),
            run_rollups(),
        )
    )
    workflow.on(Workflow.Callbacks.ON_FINISHED, save_results())

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.SUCCESS


def test_exception_capture(celery_session_worker):
    """
    Ensure that on the failure of a job within the workflow, the exception
    is captured and the workflow is marked as failed.
    """
    workflow = Workflow(
        Chain(
            task_that_fails(),
        )
    )
    workflow.on(Workflow.Callbacks.ON_FINISHED, save_results())

    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.FAILURE

    job_states = executor.job_states(workflow_id)
    j = job_states["1"]
    assert j.status == JobState.Status.FAILURE
    assert j.exception.startswith("Traceback")
