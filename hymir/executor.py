import dataclasses
import enum
import inspect
import json
import time
import traceback
from abc import ABC, abstractmethod
from typing import Any, get_origin, Optional

from hymir.config import get_configuration
from hymir.errors import WorkflowDoesNotExist, InvalidJobReturn
from hymir.job import Success, Failure, Retry, CheckLater, JobResult, Job
from hymir.workflow import Workflow


@dataclasses.dataclass
class JobState:
    """
    Holds the state for a specific job, within a specific run of a workflow.
    """

    class Status(enum.IntEnum):
        """
        The status of a job in a workflow.
        """

        PENDING = 0
        STARTING = 1
        SUCCESS = 2
        FAILURE = 3

    #: The last known status of the job.
    status: Status = Status.PENDING
    #: The total number of times this job has been retried.
    retries: int = 0
    #: Arbitrary data that can be stored with the job for subsequent runs.
    context: dict[str, Any] = dataclasses.field(default_factory=dict)
    #: Exception information if the job failed.
    exception: str | None = None

    @classmethod
    def deserialize(cls, data: str) -> "JobState":
        """
        Deserialize a JobState from a JSON string.
        """
        v = json.loads(data)
        # Convert the status back to an enum.
        v["status"] = cls.Status(v["status"])
        return cls(**v)

    def serialize(self) -> str:
        """
        Serialize the JobState to a JSON string.
        """
        return json.dumps(dataclasses.asdict(self))

    @property
    def is_finished(self) -> bool:
        """
        Check if the job has finished executing.
        """
        return self.status in (self.Status.SUCCESS, self.Status.FAILURE)


@dataclasses.dataclass
class WorkflowState:
    """
    Holds the state for a specific run of a workflow.
    """

    class Status(enum.IntEnum):
        """
        The status of a workflow.
        """

        PENDING = 0
        RUNNING = 1
        SUCCESS = 3
        FAILURE = 4

    #: The last known status of the workflow.
    status: Status = Status.PENDING

    @classmethod
    def deserialize(cls, data: str) -> "WorkflowState":
        """
        Deserialize a WorkflowState from a JSON string.
        """
        v = json.loads(data)
        # Convert the status back to an enum.
        v["status"] = cls.Status(v["status"])
        return cls(**v)

    def serialize(self) -> str:
        """
        Serialize the WorkflowState to a JSON string.
        """
        return json.dumps(dataclasses.asdict(self))

    @property
    def is_finished(self) -> bool:
        """
        Check if the workflow has finished executing.
        """
        return self.status in (self.Status.SUCCESS, self.Status.FAILURE)


class Executor(ABC):
    """
    An executor is responsible for executing workflows and managing the state
    of the jobs in the workflow.
    """

    @staticmethod
    def workflow(workflow_id: str) -> Workflow:
        """
        Retrieve the workflow from the executor.

        :param workflow_id: The unique identifier for the workflow.
        :return: The workflow.
        """
        config = get_configuration()
        w = config.redis.get(f"{workflow_id}:def")
        if w is None:
            raise WorkflowDoesNotExist()
        return Workflow.deserialize(w)

    @staticmethod
    def store_workflow(workflow_id: str, workflow: Workflow):
        """
        Store the workflow for the executor to retrieve later.

        :param workflow_id: The unique identifier for the workflow.
        :param workflow: The workflow to be stored.
        """
        config = get_configuration()
        config.redis.set(f"{workflow_id}:def", workflow.serialize())

    @staticmethod
    def workflow_state(workflow_id: str) -> WorkflowState:
        """
        Get the state of a workflow.

        :param workflow_id: The unique identifier for the workflow.
        """
        config = get_configuration()
        ws = config.redis.get(f"{workflow_id}:state")
        if ws is None:
            return WorkflowState()
        return WorkflowState.deserialize(ws)

    @staticmethod
    def store_workflow_state(workflow_id: str, state: WorkflowState):
        """
        Store the state of a workflow.

        :param workflow_id: The unique identifier for the workflow.
        :param state: The WorkflowState to store.
        """
        config = get_configuration()
        config.redis.set(f"{workflow_id}:state", state.serialize())

    @staticmethod
    def job_state(workflow_id: str, job_id: str) -> JobState:
        """
        Get the state of a specific job within a workflow.

        :param workflow_id: The unique identifier for the workflow.
        :param job_id: The unique identifier for the job.
        """
        config = get_configuration()
        js = config.redis.hget(f"{workflow_id}:jobs", job_id)
        if js is None:
            return JobState()
        return JobState.deserialize(js)

    @staticmethod
    def store_job_state(workflow_id: str, job_id: str, state: JobState):
        """
        Store the state of a specific job within a specific run of a workflow.

        :param workflow_id: The unique identifier for the workflow.
        :param job_id: The unique identifier for the job.
        :param state: The JobState to store.
        """
        config = get_configuration()
        config.redis.hset(f"{workflow_id}:jobs", job_id, state.serialize())

    @staticmethod
    def store_output(workflow_id: str, output: str, value: Any):
        """
        Store an output value for a workflow.

        :param workflow_id: The unique identifier for the workflow.
        :param output: The name of the output variable.
        :param value: The value to store.
        """
        config = get_configuration()
        config.redis.rpush(f"{workflow_id}:crumb:{output}", json.dumps(value))

    @classmethod
    def job_states(
        cls, workflow_id: str, job_ids: list[str] = None
    ) -> dict[str, JobState]:
        """
        Fetch multiple JobStates at once, returning a mapping of
        {job_id: job_state}.

        If no job_ids are provided, all jobs in the workflow will be returned.

        This function should be used whenever more than a single job needs to
        be checked, as it's typically much more efficient.
        """
        config = get_configuration()

        js = {
            k.decode("utf-8"): JobState.deserialize(v)
            for k, v in config.redis.hgetall(f"{workflow_id}:jobs").items()
        }

        if job_ids:
            return {job_id: js.get(job_id, JobState()) for job_id in job_ids}

        return {
            job_id: js.get(job_id, JobState())
            for job_id in cls.workflow(workflow_id).graph.nodes
        }

    @abstractmethod
    def run(self, workflow: Workflow) -> str:
        """
        Run a workflow and return a unique identifier which can be used to
        track the progress of the workflow.

        :param workflow: The workflow to run.
        :return: The unique identifier for the workflow.
        """

    def progress(self, workflow_id: str) -> tuple[int, int]:
        """
        Gets the progress of a workflow, returning the number of jobs that have
        finished and the total number of jobs. It does not matter if a job
        failed or succeeded, only that it has finished executing.

        :param workflow_id: The unique identifier for the workflow.
        """
        total = 0
        finished = 0
        for job in self.job_states(workflow_id).values():
            total += 1
            if job.is_finished:
                finished += 1

        return finished, total

    def wait(self, workflow_id: str, *, block: bool = True, sleep: int = 5):
        """
        Wait until the given workflow has completed.

        By default, this function blocks until the workflow has completed,
        sleeping `sleep` seconds between checks.

        If `block` is False, the function will return immediately after
        checking the state of the workflow.

        :param workflow_id: The unique identifier for the workflow.
        :param block: Should the call block or not.
        :param sleep: The number of seconds to sleep for between checks.
        :return:
        """
        while True:
            state = self.workflow_state(workflow_id)
            if state.is_finished:
                break

            if not block:
                return state

            time.sleep(sleep)

        return state

    def clear(self, workflow_id: str):
        """
        Erase all information associated with the given workflow_id, only
        if the workflow has finished.
        """
        state = self.workflow_state(workflow_id)
        if not state.is_finished:
            return

        config = get_configuration()
        config.redis.delete(
            f"{workflow_id}:def",
            f"{workflow_id}:state",
            f"{workflow_id}:jobs",
        )

    @classmethod
    def outputs(
        cls, workflow_id: str, *, only: Optional[list[str]] = None
    ) -> dict[str, list[Any]]:
        """
        Get all outputs from all jobs that ran as part of the given workflow.

        If outputs are missing - for example because the job that provided them
        failed - they will be ignored.

        Outputs are always lists of values.

        :param workflow_id: The unique identifier for the workflow.
        :param only: If provided, only return the named outputs.
        """
        config = get_configuration()
        workflow = cls.workflow(workflow_id)

        outputs = {}
        for output in workflow.outputs:
            if only and output not in only:
                continue

            try:
                crumbs = config.redis.lrange(
                    f"{workflow_id}:crumb:{output}", 0, -1
                )
            except KeyError:
                continue

            outputs[output] = [json.loads(crumb) for crumb in crumbs]

        return outputs

    @classmethod
    def process_job(
        cls, workflow_id: str, job_id: str
    ) -> (JobState, JobResult):
        """
        Process a single job in a workflow.

        .. note:

            This method is intended to be called by the executor's `run` method
            and should not be called directly.

        :param workflow_id: The unique identifier for the workflow.
        :param job_id: The unique identifier for the job.
        """
        workflow = cls.workflow(workflow_id)
        state = cls.job_state(workflow_id, job_id)

        job = workflow[job_id]

        kwargs = cls.populate_kwargs(
            workflow_id,
            job,
            {
                "job_id": job_id,
                "workflow_id": workflow_id,
                "workflow": workflow,
                "workflow_state": cls.workflow_state(workflow_id),
                "job_state": state,
                "executor": cls,
            },
        )

        # Actually run the function wrapped by the job.
        f = job.get_function()
        try:
            return_value = f(*job.args, **kwargs)
        except Exception:  # noqa
            state.status = JobState.Status.FAILURE
            state.exception = traceback.format_exc()
            return state, Failure()

        if return_value is None:
            return_value = Success()

        if isinstance(return_value, Success):
            if job.output:
                cls.store_output(workflow_id, job.output, return_value.result)

            state.status = JobState.Status.SUCCESS
            return state, return_value
        elif isinstance(return_value, Failure):
            state.status = JobState.Status.FAILURE
            return state, return_value
        elif isinstance(return_value, Retry):
            if (
                return_value.max_retries
                and state.retries >= return_value.max_retries
            ):
                state.status = JobState.Status.FAILURE
                return state, Failure()

            state.status = JobState.Status.PENDING
            state.retries += 1
            return state, return_value
        elif isinstance(return_value, CheckLater):
            # The job is not ready to run yet, so we'll check back later.
            # This is useful for implementing workflows that are waiting for
            # external events to occur before they can proceed. We don't
            # consider this a retry, so we don't increment the retry count.
            state.status = JobState.Status.PENDING
            state.context = return_value.context
            return state, return_value
        else:
            state.status = JobState.Status.FAILURE
            raise InvalidJobReturn(
                f"The job {job.name!r} returned an invalid value:"
                f" {return_value!r}. Jobs must return a value of type Success,"
                f" Failure, Retry, or CheckLater."
            )

    @classmethod
    def populate_kwargs(
        cls, workflow_id: str, job: Job, extra_kwargs: dict[str, Any]
    ):
        """
        Populate the keyword arguments for a job, including any inputs that
        the job needs.

        .. note::

            This method is intended to be called by the executor's `run` method
            and should not be called directly.
        """
        kwargs = dict(job.kwargs) or {}
        f = job.get_function()

        # If the job is expecting inputs, we need to provide them. We look
        # at the function signature to determine if the input is a list or
        # a single value, defaulting to a list if we can't figure it out.
        if job.inputs:
            signature = inspect.signature(f)

            outputs = cls.outputs(
                workflow_id, only=list(job.needed_inputs.values())
            )

            for input_name, output_name in job.needed_inputs.items():
                parameter = signature.parameters.get(input_name)
                if parameter is None:
                    raise ValueError(
                        f"Input {input_name!r} not found in the signature of"
                        f" {f.__name__!r}."
                    )

                if input_name in extra_kwargs:
                    kwargs[input_name] = extra_kwargs[input_name]
                    continue

                try:
                    v = outputs[output_name]
                except KeyError:
                    raise ValueError(
                        f"Output {output_name!r} not found in the"
                        f" outputs of the workflow and is required for"
                        f" the job {job.name!r}."
                    )

                is_list = get_origin(parameter.annotation) == list
                if is_list:
                    kwargs[input_name] = v
                else:
                    if len(outputs[output_name]) > 1:
                        raise ValueError(
                            f"Expected a single value for input {input_name},"
                            f" but got multiple values."
                        )

                    kwargs[input_name] = outputs[output_name][0]

        return kwargs

    def __getitem__(self, workflow_id: str) -> WorkflowState:
        return self.workflow_state(workflow_id)

    def __delitem__(self, workflow_id: str):
        self.clear(workflow_id)
