class WorkflowDoesNotExits(ValueError):
    pass


class JobDoesNotExits(ValueError):
    pass


class InvalidJobReturn(ValueError):
    """
    Raised if a job returns a value that can't be handled. Only a JobResult
    or None (which represents a Success(None)) can be returned by a Job.
    """


class InvalidWorkflow(ValueError):
    pass
