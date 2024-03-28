import dataclasses
import enum
import json
import time
from abc import ABC, abstractmethod
from typing import Any

from hymir.config import get_configuration
from hymir.errors import WorkflowDoesNotExits
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

    @classmethod
    def deserialize(cls, data: str) -> "JobState":
        """
        Deserialize a JobState from a JSON string.
        """
        return cls(**json.loads(data))

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
        return cls(**json.loads(data))

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
            raise WorkflowDoesNotExits()
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

    @classmethod
    def job_states(cls, workflow_id: str, job_ids: list[str] = None):
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
            for job_id in cls.workflow(workflow_id).graph.nodes.keys()
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
        checking the state of the workflow, returning None if it's not yet
        finished.

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
                return None

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

    def outputs(self, workflow_id: str) -> dict[str, list[Any]]:
        """
        Get all outputs from all jobs that ran as part of the given workflow.

        If outputs are missing - for example because the job that provided them
        failed - they will be ignored.

        Outputs are always lists of values.

        :param workflow_id: The unique identifier for the workflow.
        """
        config = get_configuration()
        workflow = self.workflow(workflow_id)

        outputs = {}
        for output in workflow.outputs:
            try:
                crumbs = config.redis.lrange(
                    f"{workflow_id}:crumb:{output}", 0, -1
                )
            except KeyError:
                continue

            outputs[output] = [json.loads(crumb) for crumb in crumbs]

        return outputs

    def __getitem__(self, workflow_id: str) -> WorkflowState:
        return self.workflow_state(workflow_id)

    def __delitem__(self, workflow_id: str):
        self.clear(workflow_id)
