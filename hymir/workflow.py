import functools
import json
from typing import Union, Callable, Iterator

import networkx as nx
from networkx.readwrite import json_graph

from hymir.config import get_configuration
from hymir.job import Job
from hymir.types import JobResultT, ContainerT
from hymir.utils import importable_name


class Group:
    """
    A group of jobs which can be executed in parallel.

    May contain other chains or groups.
    """

    def __init__(self, *jobs: Union[JobResultT, "Chain", "Group"]):
        self.jobs = list(jobs)


class Chain:
    """
    A chain of jobs which must be executed in sequence.

    May contain other chains or groups.
    """

    def __init__(self, *jobs: Union[JobResultT, "Chain", "Group"]):
        self.jobs = list(jobs)


class Workflow:
    """
    A Workflow class encapsulates a graph representing groups and chains
    of jobs to run, along with settings that may control its execution.
    """

    def __init__(self, workflow: Union[Chain, Group, nx.DiGraph]):
        if isinstance(workflow, (Chain, Group)):
            self.graph = build_graph_from_workflow(workflow)
        else:
            self.graph = workflow

    def serialize(self) -> str:
        """
        Serialize the workflow to a JSON string.

        :return: The JSON-serialized workflow.
        """
        copy = self.graph.copy()

        # Iterate over the graph, replacing the "job" attribute on each node
        # with the serialized form.
        for node in copy.nodes:
            copy.nodes[node]["job"] = copy.nodes[node]["job"].serialize()

        return json.dumps(json_graph.node_link_data(copy), sort_keys=True)

    @classmethod
    def deserialize(cls, data: str) -> "Workflow":
        """
        Deserialize a JSON-serialized workflow back into a Workflow
        object.

        :param data: The JSON-serialized workflow.
        """
        graph = json_graph.node_link_graph(json.loads(data))

        # Replace the serialized job objects with the actual job objects.
        for node in graph.nodes:
            graph.nodes[node]["job"] = Job.deserialize(graph.nodes[node]["job"])

        return Workflow(graph)

    @property
    def dependencies(self) -> list[tuple[str, list[str]]]:
        """
        Get the dependencies for each node in the graph.

        Returns a list of tuples, where the first element is the node, and the
        second element is a list of the nodes that are dependencies of the
        first node.

        The nodes are returned in topological order.

        Example:

            >>> workflow = Workflow(
            ...     Chain(
            ...         dummy_job(),
            ...         Group(
            ...             dummy_job(),
            ...             dummy_job(),
            ...         ),
            ...     )
            ... )
            >>> list(workflow.dependencies)
            [
                ('1', []),
                ('2', ['1']),
                ('3', ['1']),
                ('4', ['2', '3']),
            ]
        """
        return [
            (node, list(self.graph.predecessors(node)))
            for node in nx.topological_sort(self.graph)
        ]

    @property
    def outputs(self) -> set[str]:
        """
        Get all the outputs that are provided by the jobs in the workflow.
        """
        return set(
            self.graph.nodes[node]["job"].output
            for node in nx.topological_sort(self.graph)
            if self.graph.nodes[node]["job"].output
        )

    @property
    def inputs(self) -> set[str]:
        """
        Get all the inputs that are requested by jobs in the workflow.
        """
        return set(
            input_
            for node in self.graph.nodes
            for input_ in self.graph.nodes[node]["job"].inputs or []
        )

    def __getitem__(self, job_id: str) -> Job:
        return self.graph.nodes[job_id]["job"]


def job(
    *,
    output: str = None,
    inputs: list[str] = None,
):
    """
    Decorator to mark a function as a job in the workflow.

    This decorator is used to mark a function as a job in the workflow. The
    function should return a `Success`, `Failure`, `CheckLater`, or `Retry`
    object to indicate the result of the job.

    The function can accept any number of arguments and keyword arguments,
    which will be passed to the function when it is executed. If you specify
    inputs, the keyword arguments that match these strings will be replaced
    with matching outputs from other jobs.

    Some inputs are reserved. The inputs "workflow_id", "job_id", "workflow",
    "job_state", and "workflow_state" can be requested to get information about
    the currently running job.

    Example:

        .. code-block:: python

            @job()
            def my_job():
                pass

    :param output: If provided, the output of the job will be stored in this
                   variable, which can be used as the input to other jobs.
    :param inputs: If provided, the keyword arguments that match these strings
                   will be replaced with matching outputs from other jobs.
    """

    def _f(f) -> Callable[..., Job]:
        f._wrapped_as_job = True

        @functools.wraps(f)
        def _wrapper(*args, **kwargs) -> Job:
            return Job(
                name=importable_name(f),
                args=args,
                kwargs=kwargs,
                output=output,
                inputs=inputs,
            )

        return _wrapper

    return _f


def _terminal_nodes(node: ContainerT) -> Iterator["Job"]:
    """
    Given a node, return all terminal nodes that are descendants of that node.

    These terminal nodes are the very last nodes in a chain, group, or any
    combination of the two.

    :param node: The node to get the terminal nodes for.
    :return: Iterator[Job]
    """
    if isinstance(node, Group):
        for j in node.jobs:
            yield from _terminal_nodes(j)
    elif isinstance(node, Chain):
        yield from _terminal_nodes(node.jobs[-1])
    elif isinstance(node, Job):
        yield node
    else:
        raise ValueError(f"Unknown node type: {node!r}")


def build_graph_from_workflow(workflow: Union[Group, Chain]) -> nx.DiGraph:
    """
    Build a directed graph representing the workflow, with each node
    representing a job in the workflow. The edges represent the order in
    which the jobs should be executed.
    """
    _last_used_identity = 0
    graph = nx.DiGraph()

    def get_node_identity(node: Job) -> str:
        """
        Get the unique identity for a node.

        Functionally identical nodes can be added to the workflow, and this
        function ensures that each node has a unique identity that can be used
        to reference the node in the graph.

        :param node: The node to get the identity for.
        :return: int
        """
        if node.identity is None:
            nonlocal _last_used_identity
            _last_used_identity += 1
            node.identity = _last_used_identity
        return str(node.identity)

    def add_node(node, parent=None):
        if isinstance(node, Group):
            for j in node.jobs:
                add_node(j, parent=parent)
        elif isinstance(node, Chain):
            previous_in_chain = parent
            for j in node.jobs:
                add_node(j, parent=previous_in_chain)
                previous_in_chain = j
        elif isinstance(node, Job):
            graph.add_node(get_node_identity(node), job=node)
            if parent:
                for terminal_job in _terminal_nodes(parent):
                    graph.add_edge(
                        get_node_identity(terminal_job),
                        get_node_identity(node),
                    )
        else:
            raise ValueError(f"Unknown node type: {node!r}")

    add_node(workflow)

    return graph
