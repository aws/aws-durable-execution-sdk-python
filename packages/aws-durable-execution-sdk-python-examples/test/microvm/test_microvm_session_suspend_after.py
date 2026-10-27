"""Cloud test for the MicroVM session example that suspends after a job.

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

from src.microvm import microvm_session_suspend_after  # noqa: E402
from test.conftest import deserialize_operation_payload  # noqa: E402
from test.microvm.microvm_helpers import (  # noqa: E402
    expect_microvm_terminated,
    fail_callback_after_test_failure,
    find_launched_microvm_id,
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
    handler=microvm_session_suspend_after.handler,
    lambda_function_name="MicroVM Session Suspend After",
    cloud_only=True,
)
def test_suspends_the_microvm_after_the_first_job_and_resumes_it_for_the_next(
    durable_runner,
):
    key = str(uuid.uuid4())

    with durable_runner:
        # A synchronous Invoke returns only when the execution ends. The cloud
        # runner's boto3 client reads for at most 60 seconds, and it sends the
        # Invoke again after a read timeout. Each resend starts a new
        # execution, which launches its own MicroVM. So the test starts the
        # execution asynchronously and polls for its result.
        execution_arn = durable_runner.run_async(input={"key": key}, timeout=300)

        # The test reads the MicroVM state while the gate is pending. So it
        # reads the MicroVM ID from the launch step in the execution history.
        microvm_id = find_launched_microvm_id(
            execution_arn, "suspend-after-session.launch"
        )

        callback_id = None
        gate_completed = False
        try:
            # The cloud runner reads the callback ID from the CallbackStarted
            # event named gate. It returns the ID only after the invocation
            # that created the callback has completed.
            callback_id = durable_runner.wait_for_callback(
                execution_arn=execution_arn, name="gate", timeout=120
            )

            # 1. The gate starts after the first job ends.
            # 2. The second job starts only after the test completes the gate.
            # 3. The session does not suspend when idle. So only the first
            #    job's suspend_after can suspend the MicroVM.
            # 4. So a SUSPENDED state read now lies between the two jobs, and
            #    comes from suspend_after.
            # The poll lasts at most 150 seconds. The gate times out after 5
            # minutes. So the gate stays pending for the whole poll. The
            # state can also pass through SUSPENDING. So the test requires
            # only that SUSPENDED is seen once.
            wait_for_microvm_state(microvm_id, "SUSPENDED")

            durable_runner.send_callback_success(callback_id=callback_id)
            gate_completed = True
        finally:
            # A failure before the gate completes leaves the execution waiting
            # for the gate, with the MicroVM still there. A callback failure
            # makes the session terminate the MicroVM now, instead of after
            # the gate's timeout. If the test fails before it has the callback
            # ID, the gate times out after 5 minutes. The session then
            # terminates the MicroVM.
            if callback_id is not None and not gate_completed:
                fail_callback_after_test_failure(durable_runner, callback_id)

        result = durable_runner.wait_for_result(
            execution_arn=execution_arn, timeout=300
        )

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    output = deserialize_operation_payload(result.result)
    # 1. GetMicrovm reported the MicroVM SUSPENDED while the gate was pending.
    # 2. The second job read the file that the first job wrote.
    # 3. A suspended MicroVM keeps its files.
    # So both jobs ran in the same MicroVM, before and after the suspend.
    assert output["sessionMicrovmId"] == microvm_id
    assert output["writtenBy"] == microvm_id
    assert output["readBy"] == microvm_id
    assert output["value"] == f"written-by-{key}"

    expect_microvm_terminated(microvm_id)

    # The tree is the tree of the suspend example. The state check and the
    # resume run inside the second request step. So the tree shows them only
    # as a request step that succeeded after the suspend.
    assert tree(result.operations) == [
        (
            "suspend-after-session",
            OperationType.CONTEXT,
            SUCCEEDED,
            [
                ("suspend-after-session.session", OperationType.STEP, SUCCEEDED, []),
                ("suspend-after-session.launch", OperationType.STEP, SUCCEEDED, []),
                (
                    "write",
                    OperationType.CONTEXT,
                    SUCCEEDED,
                    [
                        ("write.callback", OperationType.CALLBACK, SUCCEEDED, []),
                        ("write.request", OperationType.STEP, SUCCEEDED, []),
                    ],
                ),
                ("gate", OperationType.CALLBACK, SUCCEEDED, []),
                (
                    "read",
                    OperationType.CONTEXT,
                    SUCCEEDED,
                    [
                        ("read.callback", OperationType.CALLBACK, SUCCEEDED, []),
                        ("read.request", OperationType.STEP, SUCCEEDED, []),
                    ],
                ),
                (
                    "suspend-after-session.terminate",
                    OperationType.STEP,
                    SUCCEEDED,
                    [],
                ),
            ],
        )
    ]

    # The session sets neither auto_suspend_on_idle nor an idle policy. So the
    # launch records no resume mode. suspend_after is an option of one job,
    # and the launch checkpoint does not record it.
    session = result.get_context("suspend-after-session")
    launch = deserialize_operation_payload(
        session.get_step("suspend-after-session.launch").result
    )
    assert launch["delivery"] == "http"
    assert launch["microvmId"] == microvm_id
    assert launch["resumeBeforeDelivery"] is False
