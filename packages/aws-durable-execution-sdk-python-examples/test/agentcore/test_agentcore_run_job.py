"""Cloud test for the AgentCore run-job example.

The test runs the real path: the durable function, InvokeAgentRuntime, the
AgentCore session, the worker, the callback, and StopRuntimeSession. The
local runner cannot start an AgentCore session. So the test is cloud-only.

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


# The examples package does not depend on the extras. So an environment
# without the extras skips this module instead of failing to import the
# example.
pytest.importorskip("aws_durable_execution_sdk_python_extras")

from src.agentcore import agentcore_run_job  # noqa: E402
from test.conftest import deserialize_operation_payload  # noqa: E402


TEXT = "the quick brown fox jumps over the lazy dog"


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=agentcore_run_job.handler,
    lambda_function_name="AgentCore Run Job",
    cloud_only=True,
)
def test_runs_the_job_in_an_agentcore_session_and_returns_its_result(
    durable_runner,
):
    with durable_runner:
        # A synchronous Invoke returns only when the execution ends. The cloud
        # runner's boto3 client reads for at most 60 seconds, and it sends the
        # Invoke again after a read timeout. Each resend starts a new
        # execution. So the test starts the execution asynchronously and
        # polls for its result.
        execution_arn = durable_runner.run_async(input={"text": TEXT}, timeout=300)
        result = durable_runner.wait_for_result(
            execution_arn=execution_arn, timeout=300
        )

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    output = deserialize_operation_payload(result.result)
    assert output["wordCount"] == 9
    assert output["sha256"] == hashlib.sha256(TEXT.encode()).hexdigest()

    # The operation runs its durable operations in one child context named
    # after the call.
    context = result.get_context("count-words")
    assert context.operation_type is OperationType.CONTEXT
    assert context.status is OperationStatus.SUCCEEDED
    assert context.sub_type == "Agentcore"
    assert [
        (child.name, child.operation_type, child.status, child.sub_type)
        for child in context.child_operations
    ] == [
        (
            "count-words.session",
            OperationType.STEP,
            OperationStatus.SUCCEEDED,
            "AgentcoreSessionId",
        ),
        (
            "count-words.callback",
            OperationType.CALLBACK,
            OperationStatus.SUCCEEDED,
            "AgentcoreCallback",
        ),
        (
            "count-words.request",
            OperationType.STEP,
            OperationStatus.SUCCEEDED,
            "AgentcoreRequest",
        ),
        (
            "count-words.stop",
            OperationType.STEP,
            OperationStatus.SUCCEEDED,
            "AgentcoreStop",
        ),
    ]

    # AgentCore passed the session ID to the worker in a request header. So
    # the job's session is the one that the session step checkpointed.
    session_id = deserialize_operation_payload(
        context.get_step("count-words.session").result
    )
    assert output["sessionId"] == session_id
    # A failed StopRuntimeSession is logged, not raised. So the check reads
    # the stop step's own outcome.
    assert context.get_step("count-words.stop").error is None
