import json
import random

from celery import shared_task
from celery.utils import gen_unique_id
from celery.utils.log import get_task_logger

from hymir.config import get_configuration
from hymir.job import Success, Failure, Retry, CheckLater, Job

from hymir.errors import InvalidJobReturn, InvalidWorkflow
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

    """

    def run(self, workflow: Workflow) -> str:
        workflow_id = gen_unique_id()
        self.store_workflow(workflow_id, workflow)

        state = self.workflow_state(workflow_id)
        state.status = WorkflowState.Status.RUNNING
        self.store_workflow_state(workflow_id, state)

        monitor_workflow.delay(workflow_id=workflow_id)
        return workflow_id


@shared_task()
def monitor_workflow(*, workflow_id: str):
    """
    Monitor the progress of a workflow, identified by the `workflow_id`.

    Once started, this task will re-run itself until the workflow has
    completed. As dependencies in the workflow are resolved, this task will
    start tasks that were previously blocked by the dependency until the entire
    workflow is complete.
    """
    workflow = CeleryExecutor.workflow(workflow_id)
    ws = CeleryExecutor.workflow_state(workflow_id)

    jobs = CeleryExecutor.job_states(workflow_id)
    jobs_to_start = []

    for job_id, dependencies in workflow.dependencies:
        state = jobs[job_id]
        match state.status:
            case JobState.Status.FAILURE:
                ws.status = WorkflowState.Status.FAILURE
                CeleryExecutor.store_workflow_state(workflow_id, ws)
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
        job_wrapper.delay(workflow_id, job_id)

    if all(job.is_finished for job in jobs.values()):
        ws.status = WorkflowState.Status.SUCCESS
    else:
        ws.status = WorkflowState.Status.RUNNING

    CeleryExecutor.store_workflow_state(workflow_id, ws)
    if not ws.is_finished:
        monitor_workflow.delay(workflow_id=workflow_id)


@shared_task()
def job_wrapper(workflow_id: str, job_id: str):
    """
    Wrapper around a job to handle the state transitions and dependencies.
    """
    config = get_configuration()
    workflow = CeleryExecutor.workflow(workflow_id)

    job = workflow[job_id]
    state = CeleryExecutor.job_state(workflow_id, job_id)

    # If the job is in a terminal state, we've been erroneously called after
    # the job has already completed.
    if state.status in (JobState.Status.SUCCESS, JobState.Status.FAILURE):
        return

    def crumb_getter(key: str):
        # Provides the job with a way to retrieve crumbs from the workflow when
        # it has one specified.
        match key:
            case "workflow_id":
                return [workflow_id]
            case "job_id":
                return [job_id]
            case "workflow":
                return [workflow]
            case "job_state":
                return [state]
            case "workflow_state":
                ws = CeleryExecutor.workflow_state(workflow_id)
                return [ws]

        try:
            crumbs = config.redis.lrange(f"{workflow_id}:crumb:{key}", 0, -1)
        except KeyError:
            raise InvalidWorkflow(
                f"Output with key {key!r} not found in workflow {workflow_id},"
                f" requested as an input for the job {job.name!r}."
            )

        return [json.loads(crumb) for crumb in crumbs]

    try:
        ret = job(crumb_getter=crumb_getter)
    except Exception as e:
        state.status = JobState.Status.FAILURE
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
        raise e

    if ret is None:
        ret = Success()

    if isinstance(ret, Success):
        if job.output:
            # If the task succeeded, and it's setup to capture its output,
            # store the output in the crumb for future use.
            config.redis.rpush(
                f"{workflow_id}:crumb:{job.output}", json.dumps(ret.result)
            )

        state.status = JobState.Status.SUCCESS
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
    elif isinstance(ret, Failure):
        state.status = JobState.Status.FAILURE
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
    elif isinstance(ret, Retry):
        if ret.max_retries and state.retries >= ret.max_retries:
            state.status = JobState.Status.FAILURE
            CeleryExecutor.store_job_state(workflow_id, job_id, state)
            return

        state.status = JobState.Status.STARTING
        state.retries += 1
        CeleryExecutor.store_job_state(workflow_id, job_id, state)

        job_wrapper.apply_async(
            (workflow_id, job_id),
            countdown=random.randint(
                max(0, ret.wait_min), max(1, ret.wait_max)
            ),
        )
    elif isinstance(ret, CheckLater):
        # The job is not ready to run yet, so we'll check back later.
        # This is useful for implementing workflows that are waiting for
        # external events to occur before they can proceed. We don't consider
        # this a retry, so we don't increment the retry count.
        state.status = JobState.Status.PENDING
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
    else:
        state.status = JobState.Status.FAILURE
        CeleryExecutor.store_job_state(workflow_id, job_id, state)
        raise InvalidJobReturn(
            f"The job {job.name!r} returned an invalid value: {ret!r}. Jobs"
            " must return a value of type Success, Failure, Retry, or"
            " CheckLater."
        )
