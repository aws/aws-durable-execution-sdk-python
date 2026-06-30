"""Tests for the replay_logging example.

Most assertions here verify the workflow runs end-to-end across the
wait/replay boundary and produces the expected operations and result. One test
additionally asserts on the replay-aware logger: messages emitted before the
wait are de-duplicated on replay, so each appears exactly once despite the
handler replaying from the top after the wait resumes.
"""

import logging

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


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=replay_logging.handler,
    lambda_function_name="Replay Logging",
)
def test_replay_logging_dedupes_logs_across_wait(durable_runner, caplog):
    """Verify the replay-aware logger de-duplicates messages across the wait.

    The handler logs before the wait, suspends, then replays from the top when
    the wait resumes. The replay-aware logger suppresses logs while the context
    is replaying, so a message emitted before the wait must appear exactly once
    even though the code that produces it runs again on replay. Messages emitted
    after the wait are new work and also appear exactly once.

    This only holds in local mode, where every invocation runs in-process and is
    captured by a single ``caplog``; cloud mode spreads invocations across
    separate Lambda executions, so the test is skipped there.
    """
    if durable_runner.mode != "local":
        pytest.skip("Log capture is only available in local (in-process) mode")

    with caplog.at_level(logging.INFO):
        with durable_runner:
            result = durable_runner.run(input={"item": "widget"}, timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    messages = [record.getMessage() for record in caplog.records]

    # Emitted before the wait: the handler replays past this line, but the
    # replay-aware logger suppresses the duplicate, so it appears exactly once.
    assert messages.count("Workflow started (before wait)") == 1
    assert messages.count("Prepared, about to wait") == 1

    # Emitted after the wait as new work: also exactly once.
    assert messages.count("Resumed after wait") == 1
    assert messages.count("Workflow completed") == 1
