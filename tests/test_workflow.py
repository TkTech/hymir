"""
General workflow tests that do not involve an Executor.
"""

import pytest

from hymir.job import job
from hymir.workflow import Workflow, Group, Chain


@job()
def dummy_job():
    pass


@job()
def job_with_input(sample_input: str):
    pass


@job()
def job_with_output():
    pass


def test_workflow_serialize():
    """
    Ensure we can round-trip serialize and deserialize a workflow and end
    up with the same result.
    """
    workflow = Workflow(Chain(dummy_job(), Group(dummy_job(), dummy_job())))

    serialized = workflow.serialize()
    deserialized = Workflow.deserialize(serialized)

    assert deserialized.serialize() == serialized


def test_workflow_dependencies():
    """
    Ensure we can get the dependencies for each node in the graph.
    """
    workflow = Workflow(
        Chain(
            dummy_job(),
            Group(dummy_job(), dummy_job()),
        )
    )

    assert list(workflow.dependencies) == [
        ("1", []),
        ("2", ["1"]),
        ("3", ["1"]),
    ]


def test_workflow_chain_before_group():
    """
    Ensure we can chain a job before a group.
    """
    workflow = Workflow(
        Chain(
            Chain(
                dummy_job(),
                dummy_job(),
            ),
            Group(
                dummy_job(),
                dummy_job(),
            ),
        )
    )

    assert list(workflow.dependencies) == [
        ("1", []),
        ("2", ["1"]),
        ("3", ["2"]),
        ("4", ["2"]),
    ]


def test_workflow_group_before_chain():
    """
    Ensure we can group jobs before a chain.
    """
    workflow = Workflow(
        Chain(
            Group(
                dummy_job(),
                dummy_job(),
            ),
            Chain(
                dummy_job(),
                dummy_job(),
            ),
        )
    )

    assert list(workflow.dependencies) == [
        ("1", []),
        ("2", []),
        ("3", ["1", "2"]),
        ("4", ["3"]),
    ]


def test_workflow_invalid_node():
    """
    Ensure trying to add an unknown node type to the graph fails properly.
    """
    with pytest.raises(ValueError):
        Workflow(
            Chain(
                "invalid_node",
            )
        )


def test_inputs_and_outputs():
    """
    Ensure we can correctly get all the inputs and outputs in the workflow.
    """
    workflow = Workflow(
        Chain(
            job_with_output().with_output("sample_output"),
            job_with_input().with_input("sample_input"),
        )
    )

    assert workflow.outputs == {"sample_output"}
    assert workflow.inputs == {"sample_input"}
