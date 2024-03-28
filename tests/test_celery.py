from hymir.executor import WorkflowState
from hymir.job import Success, Job
from hymir.executors.celery import CeleryExecutor
from hymir.workflow import (
    Workflow,
    job,
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


def test_celery(celery_session_worker):
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
