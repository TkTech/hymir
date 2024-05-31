import random
import traceback
from typing import Optional

from celery import shared_task, Task
from celery.utils import gen_unique_id
from celery.utils.log import get_task_logger

from hymir.job import Retry, CheckLater

from hymir.errors import WorkflowDoesNotExist
from hymir.executor import Executor, JobState, WorkflowState
from hymir.workflow import Workflow

logger = get_task_logger("hymir")


class CeleryExecutor(Executor):
    """
    An executor that uses the celery task queue to run jobs in a workflow.

    .. note::

        You must ensure Celery has been configured to load the tasks from
        this file, or the tasks will not be available to the Celery worker.
        You can do this by adding the following to your Celery configuration:

        .. code-block:: python

            celery_app.autodiscover_tasks(["hymir.executors.celery"])

    :param queue: The name of the queue to use for the workflow's main monitor
                  task. [default: None]
    :param priority: The priority of the workflow's main monitor task.
                     [default: None]
    """

    def __init__(
        self, *, queue: Optional[str] = None, priority: Optional[int] = None
    ):
        self.queue = queue
        self.priority = priority

    def run(self, workflow: Workflow) -> str:
        workflow_id = gen_unique_id()
        self.store_workflow(workflow_id, workflow)

        state = self.workflow_state(workflow_id)
        state.status = WorkflowState.Status.RUNNING
        self.store_workflow_state(workflow_id, state)

        monitor_workflow.apply_async(
            kwargs={"workflow_id": workflow_id},
            queue=self.queue,
            priority=self.priority,
        )
        return workflow_id


@shared_task(bind=True)
def monitor_workflow(self: Task, *, workflow_id: str, iterations: int = 1):
    """
    Monitor the progress of a workflow, identified by the `workflow_id`.

    Once started, this task will re-run itself until the workflow has
    completed. As dependencies in the workflow are resolved, this task will
    start tasks that were previously blocked by the dependency until the entire
    workflow is complete.
    """
    try:
        workflow = CeleryExecutor.workflow(workflow_id)
    except WorkflowDoesNotExist:
        # This is not necessarily an error, as the workflow may have been
        # deleted after the monitor task was scheduled, such as by calling
        # `clear()`
        logger.error(
            "The workflow with ID %r does not exist. The monitor task will"
            " terminate.",
            workflow_id,
        )
        return

    ws = CeleryExecutor.workflow_state(workflow_id)

    on_finished = workflow.callbacks.get(Workflow.Callbacks.ON_FINISHED)

    jobs = CeleryExecutor.job_states(workflow_id)
    jobs_to_start = []

    for job_id, dependencies in workflow.dependencies:
        state = jobs[job_id]
        match state.status:
            case JobState.Status.FAILURE:
                ws.status = WorkflowState.Status.FAILURE
                CeleryExecutor.store_workflow_state(workflow_id, ws)
                if on_finished:
                    on_finished(
                        **CeleryExecutor.populate_kwargs(
                            workflow_id,
                            on_finished,
                            {
                                "workflow": workflow,
                                "workflow_id": workflow_id,
                                "workflow_state": ws,
                                "executor": CeleryExecutor,
                            },
                        )
                    )
                return
            case JobState.Status.SUCCESS:
                continue
            case JobState.Status.STARTING:
                continue

        # If all the dependencies are not in a terminal state, we can't
        # proceed with this job.
        if not all(jobs[dep].is_finished for dep in dependencies):
            continue

        state.status = JobState.Status.STARTING
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
        jobs_to_start.append(job_id)

    for job_id in jobs_to_start:
        job_wrapper.apply_async(
            kwargs={
                "workflow_id": workflow_id,
                "job_id": job_id,
            },
            **(workflow[job_id].meta or {}).get("celery", {}),
        )

    if all(job.is_finished for job in jobs.values()):
        ws.status = WorkflowState.Status.SUCCESS
        if on_finished:
            on_finished(
                **CeleryExecutor.populate_kwargs(
                    workflow_id,
                    on_finished,
                    {
                        "workflow_id": workflow_id,
                        "workflow_state": ws,
                        "executor": CeleryExecutor,
                    },
                )
            )
    else:
        ws.status = WorkflowState.Status.RUNNING

    CeleryExecutor.store_workflow_state(workflow_id, ws)
    if not ws.is_finished:
        # If running an excessive number of workflows, the usage of countdown
        # here may lead to memory issues on workers due to Celery prefetching
        # tasks that need scheduling.
        monitor_workflow.apply_async(
            kwargs={"workflow_id": workflow_id, "iterations": iterations + 1},
            countdown=min(iterations * 2, 60),
            queue=self.request.delivery_info["routing_key"],
            priority=self.request.delivery_info["priority"],
        )


@shared_task()
def job_wrapper(*, workflow_id: str, job_id: str):
    """
    Wrapper around a job to handle the state transitions and dependencies.
    """
    workflow = CeleryExecutor.workflow(workflow_id)

    job = workflow[job_id]
    state = CeleryExecutor.job_state(workflow_id, job_id)

    # If the job is in a terminal state, we've been erroneously called after
    # the job has already completed.
    if state.is_finished:
        return

    logger.debug(
        f"{job!r} running for workflow {workflow_id} with state {state!r}."
    )

    try:
        state, result = CeleryExecutor.process_job(workflow_id, job_id)
    except Exception as exc:
        state.status = JobState.Status.FAILURE
        state.exception = traceback.format_exception(exc)
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
        return

    CeleryExecutor.store_job_state(workflow_id, job_id, state)

    if isinstance(result, (Retry, CheckLater)):
        if isinstance(result, Retry):
            countdown = random.randint(result.wait_min, result.wait_max)
        else:
            countdown = result.wait_for or random.randint(1, 15)

        job_wrapper.apply_async(
            kwargs={"workflow_id": workflow_id, "job_id": job_id},
            # Because of the way Celery implements scheduling for future tasks,
            # this may lead to issues on workers if running an excessive
            # number of workflows. This is due to Celery prefetching tasks that
            # need scheduling and storing them in-memory of a worker with a
            # maximum of 65536 tasks.
            countdown=countdown,
        )
