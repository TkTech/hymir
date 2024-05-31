import pprint
import traceback

from hymir.executor import Executor, WorkflowState, JobState
from hymir.utils import gen_unique_id
from hymir.workflow import Workflow
from hymir.logger import logger


class DebugExecutor(Executor):
    """
    An executor that runs the workflow in the current process, useful for
    debugging and testing.

    This makes it trivial to attach debuggers to the workflow and inspect the
    state of the workflow as it runs when it would otherwise run using a
    distributed or asynchronous executor.
    """

    def run(self, workflow: Workflow) -> str:
        workflow_id = gen_unique_id()
        self.store_workflow(workflow_id, workflow)

        self.store_workflow_state(
            workflow_id, WorkflowState(status=WorkflowState.Status.RUNNING)
        )

        while not self.workflow_state(workflow_id).is_finished:
            self._tick(workflow_id, workflow)

        logger.debug(f"Workflow {workflow_id} finished.")
        logger.debug(
            pprint.pformat(
                self.job_states(workflow_id),
            )
        )

        return workflow_id

    def _tick(self, workflow_id: str, workflow: Workflow):
        """
        Run a single iteration of the workflow.
        """
        jobs = self.job_states(workflow_id)

        # Find an unblocked job and progress it.
        for job_id, deps in workflow.dependencies:
            state = jobs[job_id]
            match state.status:
                case JobState.Status.FAILURE:
                    ws = self.workflow_state(workflow_id)
                    ws.status = WorkflowState.Status.FAILURE
                    self.store_workflow_state(workflow_id, ws)
                    return
                case JobState.Status.PENDING:
                    # Check if the dependencies are satisfied.
                    is_unblocked = all(jobs[dep].is_finished for dep in deps)
                    if not is_unblocked:
                        continue

                    try:
                        jobs[job_id], _ = self.process_job(workflow_id, job_id)
                    except Exception as exc:
                        jobs[job_id].status = JobState.Status.FAILURE
                        jobs[job_id].exception = traceback.format_exception(exc)

                    self.store_job_state(workflow_id, job_id, jobs[job_id])
                    return
        else:
            # If we didn't find any unblocked jobs, we're done.
            ws = self.workflow_state(workflow_id)
            ws.status = WorkflowState.Status.SUCCESS
            self.store_workflow_state(workflow_id, ws)
            return
