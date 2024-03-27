"""
General workflow tests that do not involve an Executor.
"""

from hymir.workflow import Workflow, job, Group, Chain


@job()
def dummy_job():
    pass


def test_workflow_serialize():
    """
    Ensure we can round-trip serialize and deserialize a workflow and end
    up with the same result.
    """
    workflow = Workflow(
        Chain(
            dummy_job(),
            Group(
                dummy_job(),
                dummy_job(),
            ),
        )
    )

    serialized = workflow.serialize()
    deserialized = Workflow.deserialize(serialized)

    assert deserialized.serialize() == serialized
