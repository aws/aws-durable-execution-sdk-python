"""Cloud test for the MicroVM session example that suspends its MicroVM.

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

from src.microvm import microvm_session_suspend  # noqa: E402
from test.conftest import deserialize_operation_payload  # noqa: E402
from test.microvm.microvm_helpers import (  # noqa: E402
    expect_microvm_terminated,
    find_launched_microvm_id,
    wait_for_history_event,
    wait_for_microvm_state,
)


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
    handler=microvm_session_suspend.handler,
    lambda_function_name="MicroVM Session Suspend",
    cloud_only=True,
)
def test_suspends_the_idle_microvm_and_resumes_it_for_the_next_job(durable_runner):
    key = str(uuid.uuid4())

    with durable_runner:
        # A synchronous Invoke returns only when the execution ends. The cloud
        # runner's boto3 client reads for at most 60 seconds, and it sends the
        # Invoke again after a read timeout. Each resend starts a new
        # execution, which launches its own MicroVM. So the test starts the
        # execution asynchronously and polls for its result.
        execution_arn = durable_runner.run_async(input={"key": key}, timeout=300)

        # The test reads the MicroVM state while the execution waits. So it
        # reads the MicroVM ID from the launch step in the execution history.
        microvm_id = find_launched_microvm_id(execution_arn, "suspend-session.launch")

        # 1. The worker starts its idle time at the run hook, and again after
        #    each job.
        # 2. If the first job arrives more than 10 seconds after the run hook,
        #    the worker suspends the MicroVM before the first job.
        # 3. The wait named pause starts only after the first job ends.
        # 4. So a SUSPENDED state read after the wait has started lies between
        #    the two jobs. So the test reads the state only after the history
        #    shows that the wait has started.
        wait_for_history_event(execution_arn, "WaitStarted", "pause")

        # 1. The worker suspends the MicroVM 10 seconds after the first job
        #    ends.
        # 2. The freeze takes up to about 15 seconds more.
        # 3. The second job resumes it about 60 seconds after the first job
        #    ends.
        # 4. So GetMicrovm reports SUSPENDED for about 35 seconds or more.
        # The state can also pass through SUSPENDING. The service controls
        # that timing. So the test requires only that SUSPENDED is seen once.
        #
        # The helper returns only after it reads SUSPENDED. The test does not
        # also require a RUNNING state before SUSPENDED:
        # 1. The idle time starts when the first job ends.
        # 2. The wait starts later. The service first completes the callback,
        #    and then invokes the function again. That time has no upper
        #    bound.
        # 3. So the wait can start more than 10 seconds after the idle time
        #    starts. The first read can then already be SUSPENDING or
        #    SUSPENDED.
        wait_for_microvm_state(microvm_id, "SUSPENDED")

        result = durable_runner.wait_for_result(
            execution_arn=execution_arn, timeout=300
        )

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    output = deserialize_operation_payload(result.result)
    # 1. GetMicrovm reported the MicroVM SUSPENDED after the wait started.
    # 2. The second job read the file that the first job wrote.
    # 3. A suspended MicroVM keeps its files.
    # So both jobs ran in the same MicroVM, before and after the suspend.
    assert output["sessionMicrovmId"] == microvm_id
    assert output["writtenBy"] == microvm_id
    assert output["readBy"] == microvm_id
    assert output["value"] == f"written-by-{key}"

    expect_microvm_terminated(microvm_id)

    # The tree is the same as in the session example without suspending. The
    # state check and the resume run inside each request step. So the tree
    # shows them only as a request step that succeeded after the suspend.
    assert tree(result.operations) == [
        (
            "suspend-session",
            OperationType.CONTEXT,
            SUCCEEDED,
            [
                ("suspend-session.session", OperationType.STEP, SUCCEEDED, []),
                ("suspend-session.launch", OperationType.STEP, SUCCEEDED, []),
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
                ("suspend-session.terminate", OperationType.STEP, SUCCEEDED, []),
            ],
        )
    ]

    # With auto_suspend_on_idle, the launch records the resume mode. Each
    # request step then calls GetMicrovm before it sends the job, and calls
    # ResumeMicrovm for a suspended MicroVM. No idle policy is set. So only
    # the request step can have called ResumeMicrovm.
    session = result.get_context("suspend-session")
    launch = deserialize_operation_payload(
        session.get_step("suspend-session.launch").result
    )
    assert launch["delivery"] == "http"
    assert launch["microvmId"] == microvm_id
    assert launch["resumeBeforeDelivery"] is True
