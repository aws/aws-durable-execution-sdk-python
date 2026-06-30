"""Tests for the replay_logging_concurrent example.

These tests verify the workflow runs end-to-end across the per-branch
wait/replay boundaries and produces the expected result. One test additionally
asserts on the replay-aware logger: each branch logs around its own wait, and
those lines are de-duplicated per branch so each appears exactly once across all
replay invocations.
"""

import logging

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.logger_example import replay_logging_concurrent
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=replay_logging_concurrent.handler,
    lambda_function_name="Replay Logging Concurrent",
)
def test_replay_logging_concurrent(durable_runner):
    """Test the concurrent replay-aware logging example runs across branch waits."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == {
        "results": ["alpha-done", "bravo-done", "charlie-done"]
    }

    # The parallel container holds one child branch per function.
    parallel_op = result.get_context("concurrent_branches")
    assert parallel_op is not None
    assert len(parallel_op.child_operations) == 3

    # Each branch has its own wait (forcing an independent suspend/replay cycle)
    # and its own finalize step, both nested inside the branch's child context.
    for branch in parallel_op.child_operations:
        wait_ops = [
            op
            for op in branch.child_operations
            if op.operation_type == OperationType.WAIT
        ]
        step_ops = [
            op
            for op in branch.child_operations
            if op.operation_type == OperationType.STEP
        ]
        assert len(wait_ops) == 1
        assert len(step_ops) == 1


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=replay_logging_concurrent.handler,
    lambda_function_name="Replay Logging Concurrent",
)
def test_replay_logging_concurrent_dedupes_logs_per_branch(durable_runner, caplog):
    """Verify the replay-aware logger de-duplicates per branch.

    Each branch runs in its own child context with its own replay status and its
    own wait boundary, so the branches suspend and replay independently. The
    replay-aware logger must de-duplicate each branch's "before wait" line
    (suppressed while that branch is replaying) and emit each "after wait" line
    once as new work. We assert exactly one record per branch per message via the
    ``branch`` extra the example attaches.

    Only valid in local mode, where all invocations run in-process under a single
    ``caplog``; cloud mode is skipped.
    """
    if durable_runner.mode != "local":
        pytest.skip("Log capture is only available in local (in-process) mode")

    with caplog.at_level(logging.INFO):
        with durable_runner:
            result = durable_runner.run(input={}, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED

    branches = ("alpha", "bravo", "charlie")
    for branch in branches:
        before = [
            record
            for record in caplog.records
            if record.getMessage() == "branch start (before wait)"
            and getattr(record, "branch", None) == branch
        ]
        after = [
            record
            for record in caplog.records
            if record.getMessage() == "branch resumed (after wait)"
            and getattr(record, "branch", None) == branch
        ]
        # Each branch logs its before/after-wait lines exactly once despite the
        # per-branch replay cycle.
        assert len(before) == 1, f"branch {branch} 'before wait' not deduped"
        assert len(after) == 1, f"branch {branch} 'after wait' not deduped"
