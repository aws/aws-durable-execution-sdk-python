"""Tests for the replay_logging example.

These tests do not assert on emitted log lines (the replay-aware
de-duplication is best observed in CloudWatch after deploying). They verify the
workflow runs end-to-end across the wait/replay boundary and produces the
expected operations and result.
"""

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.logger_example import replay_logging
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=replay_logging.handler,
    lambda_function_name="Replay Logging",
)
def test_replay_logging(durable_runner):
    """Test the replay-aware logging example runs across the wait boundary."""
    with durable_runner:
        result = durable_runner.run(input={"item": "widget"}, timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == {
        "result": "done:audited:prepared:widget",
        "item": "widget",
    }

    # Two wait operations force suspend/replay cycles: one in the parent context
    # and one inside the child (audit) context. This exercises per-context replay
    # status in different contexts.
    wait_ops = [
        op for op in result.operations if op.operation_type == OperationType.WAIT
    ]
    assert len(wait_ops) >= 1

    # Steps before (prepare) and after (finalize) the wait both ran. The child
    # context's record_audit step is nested inside the CONTEXT operation.
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) >= 2

    # The audit child context produces a CONTEXT operation.
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
