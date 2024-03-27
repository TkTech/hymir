from typing import Union, Callable

JobResultT = Union["JobResult", None]
JobT = Callable[..., "JobResultT"]
ContainerT = Union["Chain", "Group"]
