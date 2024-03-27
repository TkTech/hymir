import dataclasses
import enum
import inspect
from typing import Any, Callable, get_origin


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
    """

    def __init__(self, *, wait_for: int = None):
        self.wait_for = wait_for

    def __repr__(self):
        return f"{self.__class__.__name__}(wait_for={self.wait_for})"


@dataclasses.dataclass
class Job:
    """
    A job is a single unit of work in the workflow.
    """

    class Flags(enum.IntFlag):
        # If this stage fails, the workflow will stop and fail.
        FAIL_ON_ERROR = 1
        # If this stage fails, the workflow will continue.
        CONTINUE_ON_ERROR = 2
        # If this stage fails, the entire workflow will terminate and
        # be retried from the beginning.
        RETRY_ON_ERROR = 4

    # The name of the function to import and run.
    name: str
    # The flags for the job, controlling how the workflow should behave if
    # the job fails.
    flags: Flags = Flags.FAIL_ON_ERROR
    # The unique identity of the node in the graph. This is automatically
    # assigned by the workflow when the graph is built.
    identity: int = None
    # The arguments to pass to the job.
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

        if not hasattr(func, "_wrapped_as_job"):
            return func(*self.args, **self.kwargs)

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
        return cls(
            name=data["n"],
            flags=cls.Flags(data["f"]),
            identity=int(data["i"]),
            args=data["a"],
            kwargs=data["k"],
            output=data[">"],
            inputs=data["<"],
        )

    def serialize(self):
        return {
            "n": self.name,
            "f": int(self.flags),
            "i": self.identity,
            "a": self.args,
            "k": self.kwargs,
            ">": self.output,
            "<": self.inputs,
        }
