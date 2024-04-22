import dataclasses
import inspect
from functools import wraps
from typing import Any, Callable, get_origin

from hymir.utils import importable_name


class JobResult:
    """
    Base class for the return value of a job.
    """


class Success(JobResult):
    """
    The job completed successfully, potentially with a value to be returned.
    """

    def __init__(self, result: Any = None):
        self.result = result

    def __repr__(self):
        return f"{self.__class__.__name__}({self.result!r})"


class Failure(JobResult):
    """
    The job completed with a failure, potentially with a value which can be
    used to return information about the error.
    """

    def __init__(self, result: Any = None):
        self.result = result

    def __repr__(self):
        return f"{self.__class__.__name__}({self.result!r})"


class Retry(JobResult):
    """
    The job failed, but the workflow should be retried.

    This result can be used to indicate that the job failed due to a transient
    error, and the job should be retried.

    :param wait_min: The lower bounds of random jitter to apply to the retry
                     interval.
    :param wait_max: The upper bounds of random jitter to apply to the retry
                     interval.
    :param max_retries: The maximum number of times to retry the job.
    """

    def __init__(
        self,
        *,
        wait_min: int = None,
        wait_max: int = None,
        max_retries: int = None,
    ):
        self.wait_min = wait_min
        self.wait_max = wait_max
        self.max_retries = max_retries

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(wait_min={self.wait_min}, wait_max={self.wait_max},"
            f" max_retries={self.max_retries})"
        )


class CheckLater(JobResult):
    """
    The job has not yet completed, and should be checked later.

    This result can be used to indicate that the job is waiting for an external
    event to occur before it can be completed. The workflow engine should
    periodically check the job to see if it has completed, and will not
    continue until the job has completed.

    .. note::

        This result is _not_ a retry, and does not count towards the maximum
        number of retries for a job.

    :param wait_for: The number of seconds to wait before checking again.
    :param context: A dictionary of context to pass back to the job when it is
                    checked again.
    """

    def __init__(self, *, wait_for: int = None, context: dict[str, Any] = None):
        self.wait_for = wait_for
        self.context = context

    def __repr__(self):
        return f"{self.__class__.__name__}(wait_for={self.wait_for})"


@dataclasses.dataclass
class Job:
    """
    A job is a single unit of work in the workflow.
    """

    # The name of the function to import and run.
    name: str
    # The unique identity of the node in the graph. This is automatically
    # assigned by the workflow when the graph is built.
    identity: int = None
    # The positional arguments to pass to the job.
    args: tuple = dataclasses.field(default_factory=tuple)
    # The keyword arguments to pass to the job.
    kwargs: dict = dataclasses.field(default_factory=dict)
    # If provided, the output of the job will be stored in this variable,
    # which can be used as the input to other jobs.
    output: str = None
    # If provided, the keyword arguments that match these strings will be
    # replaced with matching outputs from other jobs.
    inputs: list[str] = None

    def __call__(self, crumb_getter: Callable[[str], Any] = None):
        mod_name, func_name = self.name.rsplit(".", 1)
        mod = __import__(mod_name, fromlist=[func_name])
        func = getattr(mod, func_name)

        kwargs = self.kwargs
        if self.inputs:
            if not crumb_getter:
                raise ValueError(
                    f"Job {self.name} requires input crumb getter, but none"
                    f" was provided."
                )

            # Replace the inputs with the output values of other tasks.
            # We look at the call signature of the function we wrap to see
            # if it's expecting a list or a single value.
            signature = inspect.signature(func.__wrapped__)

            for input_name in self.inputs:
                annotation = signature.parameters[input_name].annotation
                # The caller explicitly provided a value for this input,
                # overriding the inputs.
                if input_name in kwargs:
                    continue

                v = crumb_getter(input_name)

                if get_origin(annotation) == list:
                    kwargs[input_name] = v
                else:
                    if len(v) > 1:
                        raise ValueError(
                            f"Expected a single value for input {input_name},"
                            f" but got multiple values."
                        )

                    kwargs[input_name] = v[0]

        return func.__wrapped__(*self.args, **kwargs)

    @classmethod
    def deserialize(cls, data: dict):
        """
        Deserialize a job from a dictionary.
        """
        return cls(
            name=data["n"],
            identity=data["i"],
            args=data["a"],
            kwargs=data["k"],
            output=data[">"],
            inputs=data["<"],
        )

    def serialize(self):
        """
        Serialize the job to a dictionary.
        """
        return {
            "n": self.name,
            "i": self.identity,
            "a": self.args,
            "k": self.kwargs,
            ">": self.output,
            "<": self.inputs,
        }

    def with_output(self, name: str) -> "Job":
        """
        Capture the output of this job in a variable, replacing any existing
        output variable if one is set.

        :param name: The name of the variable to capture the output in.
        """
        return dataclasses.replace(self, output=name)

    def with_inputs(self, *inputs: str) -> "Job":
        """
        Bind the inputs of this job to the outputs of other jobs.

        :param inputs: The names of the outputs to bind to the inputs.
        """
        return dataclasses.replace(self, inputs=list(inputs))

    def set(self, *args, **kwargs: Any) -> "Job":
        """
        Set the arguments for the job.
        """
        return dataclasses.replace(self, args=args, kwargs=kwargs)

    @classmethod
    def from_function(cls, f: Callable[..., Any]) -> "Job":
        """
        Create a job from a function.
        """
        return cls(name=importable_name(f))


def job(*, inputs: list[str] = None, output: str = None):
    """
    Convenience decorator to create a job from a function.

    The wrapped function should return a `Success`, `Failure`, `CheckLater`, or
    `Retry` object to explicitly indicate the result of the job.

    Some inputs are reserved. The inputs "workflow_id", "job_id", "workflow",
    "job_state", and "workflow_state" can be requested to get information about
    the currently running job.

    .. note::

        PyCharm will not recognize the return type of this decorator, and will
        not provide autocompletion for the Job. This is a limitation of
        PyCharm's type system and is bug PY-40071.
    """

    def _f(f: Callable[..., Any]) -> Callable[..., Job]:
        @wraps(f)
        def _wrapper(*args, **kwargs) -> "Job":
            return (
                Job.from_function(f)
                .set(*args, **kwargs)
                .with_inputs(*(inputs or []))
                .with_output(output)
            )

        return _wrapper

    return _f
