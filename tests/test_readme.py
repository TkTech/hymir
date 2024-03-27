from hymir.executor import WorkflowState
from hymir.job import Success
from hymir.executors.celery import CeleryExecutor
from hymir.workflow import (
    Workflow,
    job,
    Group,
    Chain,
)


@job(output="words")
def uppercase_word(word: str):
    return Success(word.upper())


@job(inputs=["words"], output="count")
def count_uppercase_words(words: list[str]):
    count = sum(1 for word in words if word.isupper())
    return Success(count)


def test_readme(celery_session_worker):
    """
    Tests the same example from the README to ensure it always works.
    """
    workflow = Workflow(
        Chain(
            Group(
                uppercase_word("hello"),
                uppercase_word("world"),
            ),
            count_uppercase_words(),
        )
    )

    # This assumes you've already setup & configured a celery app before you
    # get here.
    executor = CeleryExecutor()
    workflow_id = executor.run(workflow)

    assert executor.wait(workflow_id).status == WorkflowState.Status.SUCCESS
