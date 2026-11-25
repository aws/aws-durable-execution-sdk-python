"""Cloud test for the AgentCore session example.

The local runner cannot start an AgentCore session. So the test is
cloud-only.

The cloud run needs a deployed agent runtime that runs agentcore-agent/app.py.
The stack deploys the example only when AgentRuntimeArn is set. Without it,
the function-name map leaves the example out, and the test is skipped.
"""

import hashlib

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)


# The examples package does not depend on the extensions. So an environment
# without the extensions skips this module instead of failing to import the
# example.
pytest.importorskip("aws_durable_execution_sdk_python_extensions")

from src.agentcore import agentcore_session  # noqa: E402
from test.conftest import deserialize_operation_payload  # noqa: E402


FIRST = "the quick brown fox"
SECOND = "jumps over the lazy dog"
SUCCEEDED = OperationStatus.SUCCEEDED


def tree(operations):
    """Return (name, type, status, subtype, children) for each operation."""
    return [
        (
            operation.name,
            operation.operation_type,
            operation.status,
            operation.sub_type,
            tree(getattr(operation, "child_operations", None) or []),
        )
        for operation in operations
    ]


def job(name):
    return (
        name,
        OperationType.CONTEXT,
        SUCCEEDED,
        "AgentcoreSessionJob",
        [
            (
                f"{name}.callback",
                OperationType.CALLBACK,
                SUCCEEDED,
                "AgentcoreCallback",
                [],
            ),
            (
                f"{name}.request",
                OperationType.STEP,
                SUCCEEDED,
                "AgentcoreRequest",
                [],
            ),
        ],
    )


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=agentcore_session.handler,
    lambda_function_name="AgentCore Session",
    cloud_only=True,
)
def test_runs_both_jobs_in_the_same_agentcore_session(durable_runner):
    with durable_runner:
        # A synchronous Invoke returns only when the execution ends. The cloud
        # runner's boto3 client reads for at most 60 seconds, and it sends the
        # Invoke again after a read timeout. Each resend starts a new
        # execution. So the test starts the execution asynchronously and
        # polls for its result.
        execution_arn = durable_runner.run_async(
            input={"first": FIRST, "second": SECOND}, timeout=300
        )
        result = durable_runner.wait_for_result(
            execution_arn=execution_arn, timeout=300
        )

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    output = deserialize_operation_payload(result.result)
    assert output["first"]["wordCount"] == 4
    assert output["first"]["sha256"] == hashlib.sha256(FIRST.encode()).hexdigest()
    assert output["second"]["wordCount"] == 5
    assert output["second"]["sha256"] == hashlib.sha256(SECOND.encode()).hexdigest()

    # The session runs its durable operations in one child context. Each job
    # runs in its own child context.
    assert tree(result.operations) == [
        (
            "count-session",
            OperationType.CONTEXT,
            SUCCEEDED,
            "AgentcoreSession",
            [
                (
                    "count-session.session",
                    OperationType.STEP,
                    SUCCEEDED,
                    "AgentcoreSessionId",
                    [],
                ),
                job("count-first"),
                ("pause", OperationType.WAIT, SUCCEEDED, "Wait", []),
                job("count-second"),
                (
                    "count-session.stop",
                    OperationType.STEP,
                    SUCCEEDED,
                    "AgentcoreStop",
                    [],
                ),
            ],
        )
    ]

    # AgentCore passed the session ID to the worker in a request header. So
    # both jobs ran in the session that the session step checkpointed.
    session = result.get_context("count-session")
    session_id = deserialize_operation_payload(
        session.get_step("count-session.session").result
    )
    assert output["runtimeSessionId"] == session_id
    assert output["first"]["sessionId"] == session_id
    assert output["second"]["sessionId"] == session_id
    # A failed StopRuntimeSession is logged, not raised. So the check reads
    # the stop step's own outcome.
    assert session.get_step("count-session.stop").error is None
