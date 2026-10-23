"""Cloud test for the MicroVM run-job example.

The local runner cannot launch a MicroVM. So the test is cloud-only.
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

from src.microvm import microvm_run_job  # noqa: E402
from test.conftest import deserialize_operation_payload  # noqa: E402
from test.microvm.microvm_helpers import expect_microvm_terminated  # noqa: E402


TEXT = "the quick brown fox jumps over the lazy dog"


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=microvm_run_job.handler,
    lambda_function_name="MicroVM Run Job",
    cloud_only=True,
)
def test_runs_the_job_in_a_microvm_and_returns_its_result(durable_runner):
    with durable_runner:
        # A synchronous Invoke returns only when the execution ends. The cloud
        # runner's boto3 client reads for at most 60 seconds, and it sends the
        # Invoke again after a read timeout. Each resend starts a new
        # execution, which launches its own MicroVM. So the test starts the
        # execution asynchronously and polls for its result.
        execution_arn = durable_runner.run_async(input={"text": TEXT}, timeout=300)
        result = durable_runner.wait_for_result(
            execution_arn=execution_arn, timeout=300
        )

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    output = deserialize_operation_payload(result.result)
    assert output["wordCount"] == 9
    assert output["sha256"] == hashlib.sha256(TEXT.encode()).hexdigest()
    assert isinstance(output["microvmId"], str)
    assert output["microvmId"]

    expect_microvm_terminated(output["microvmId"])

    # The operation runs its durable operations in one child context named
    # after the call. The input is small, so the launch step delivers the job
    # in the run hook, and no request step runs.
    context = result.get_context("count-words")
    assert context.operation_type is OperationType.CONTEXT
    assert context.status is OperationStatus.SUCCEEDED
    assert [
        (child.name, child.operation_type, child.status)
        for child in context.child_operations
    ] == [
        ("count-words.callback", OperationType.CALLBACK, OperationStatus.SUCCEEDED),
        ("count-words.launch", OperationType.STEP, OperationStatus.SUCCEEDED),
        ("count-words.terminate", OperationType.STEP, OperationStatus.SUCCEEDED),
    ]

    launch = deserialize_operation_payload(
        context.get_step("count-words.launch").result
    )
    assert launch["delivery"] == "run-hook"
    assert launch["microvmId"] == output["microvmId"]
