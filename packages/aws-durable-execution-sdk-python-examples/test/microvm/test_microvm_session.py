"""Cloud test for the MicroVM session example.

The local runner cannot launch a MicroVM. So the test is cloud-only.
"""

import uuid

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

from src.microvm import microvm_session  # noqa: E402
from test.conftest import deserialize_operation_payload  # noqa: E402
from test.microvm.microvm_helpers import expect_microvm_terminated  # noqa: E402


def tree(operations):
    """Return (name, type, status, children) for each operation, in order."""
    return [
        (
            operation.name,
            operation.operation_type,
            operation.status,
            tree(getattr(operation, "child_operations", None) or []),
        )
        for operation in operations
    ]


SUCCEEDED = OperationStatus.SUCCEEDED


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=microvm_session.handler,
    lambda_function_name="MicroVM Session",
    cloud_only=True,
)
def test_runs_both_jobs_in_the_same_microvm(durable_runner):
    key = str(uuid.uuid4())

    with durable_runner:
        # A synchronous Invoke returns only when the execution ends. The cloud
        # runner's boto3 client reads for at most 60 seconds, and it sends the
        # Invoke again after a read timeout. Each resend starts a new
        # execution, which launches its own MicroVM. So the test starts the
        # execution asynchronously and polls for its result.
        execution_arn = durable_runner.run_async(input={"key": key}, timeout=300)
        result = durable_runner.wait_for_result(
            execution_arn=execution_arn, timeout=300
        )

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    output = deserialize_operation_payload(result.result)
    # The second job read the file that the first job wrote. So both jobs ran
    # in the session's MicroVM.
    assert output["value"] == f"written-by-{key}"
    assert output["writtenBy"] == output["sessionMicrovmId"]
    assert output["readBy"] == output["sessionMicrovmId"]

    expect_microvm_terminated(output["sessionMicrovmId"])

    # The session runs its durable operations in one child context. Each job
    # runs in its own child context. Every session job goes over HTTP. So
    # each job has a request step, and the launch result records "http".
    assert tree(result.operations) == [
        (
            "file-session",
            OperationType.CONTEXT,
            SUCCEEDED,
            [
                ("file-session.session", OperationType.STEP, SUCCEEDED, []),
                ("file-session.launch", OperationType.STEP, SUCCEEDED, []),
                (
                    "write",
                    OperationType.CONTEXT,
                    SUCCEEDED,
                    [
                        ("write.callback", OperationType.CALLBACK, SUCCEEDED, []),
                        ("write.request", OperationType.STEP, SUCCEEDED, []),
                    ],
                ),
                ("pause", OperationType.WAIT, SUCCEEDED, []),
                (
                    "read",
                    OperationType.CONTEXT,
                    SUCCEEDED,
                    [
                        ("read.callback", OperationType.CALLBACK, SUCCEEDED, []),
                        ("read.request", OperationType.STEP, SUCCEEDED, []),
                    ],
                ),
                ("file-session.terminate", OperationType.STEP, SUCCEEDED, []),
            ],
        )
    ]

    session = result.get_context("file-session")
    launch = deserialize_operation_payload(
        session.get_step("file-session.launch").result
    )
    assert launch["delivery"] == "http"
    assert launch["microvmId"] == output["sessionMicrovmId"]
