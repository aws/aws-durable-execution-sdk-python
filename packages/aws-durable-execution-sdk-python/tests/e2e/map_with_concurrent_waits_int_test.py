"""Integration tests for coalescing the refreshes of concurrent resume waves.

A coordinator that resumes timed waits in-process needs a refresh, an empty
checkpoint whose response shows the waits complete. It requests one refresh
per distinct resume time through ExecutionState.schedule_refresh(resume_at).
Independent coordinators, such as nested maps, each request their own.

The batcher holds a refresh until resume_at, then sends every refresh due at
that time in one request. So the number of requests depends on the resume
times, not on how far apart the requests were made or how the threads were
scheduled.

The batch operation limit still applies. The first empty checkpoint counts
toward the 250-operation limit and the rest do not, so 300 refreshes fit in
one batch. These tests verify both rules.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor


from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    LambdaClient,
    OperationAction,
    OperationUpdate,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.state import (
    CheckpointBatcherConfig,
    ExecutionState,
    QueuedOperation,
)
from aws_durable_execution_sdk_python.threading import CompletionEvent

from unittest.mock import Mock


def _make_state(
    mock_client: Mock,
    batch_time: float = 5.0,
    max_ops: int = 250,
) -> ExecutionState:
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=batch_time,
        max_batch_operations=max_ops,
    )
    return ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="token-0",  # noqa: S106
        operations={},
        service_client=mock_client,
        batcher_config=config,
        plugin_executor=PluginExecutor([]),
    )


def _make_tracking_client() -> tuple[Mock, list]:
    """Return a (mock LambdaClient, checkpoint_calls list) pair."""
    calls: list[list] = []
    mock_client = Mock(spec=LambdaClient)

    def _checkpoint(
        durable_execution_arn, checkpoint_token, updates, client_token=None
    ):
        calls.append(list(updates))
        return CheckpointOutput(
            checkpoint_token=f"token_{len(calls)}",
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    mock_client.checkpoint = _checkpoint
    return mock_client, calls


def test_map_with_concurrent_waits_coalesces_empty_checkpoints():
    """300 due refreshes from 300 independent callers must make one API call.

    All 300 threads request their refreshes before the batcher starts, so the
    result does not depend on how fast the scheduler runs them. The check time
    is in the past: deferral of future refreshes is covered by unit tests, and
    here every refresh is due when the batcher first looks. Without the
    batch-limit optimization the 250-op limit would split them into 2 requests.
    """
    mock_client, calls = _make_tracking_client()
    state = _make_state(mock_client, batch_time=5.0, max_ops=250)

    branch_count = 300
    check_time = time.time() - 1.0
    errors: list[Exception] = []
    handles = []
    handles_lock = threading.Lock()

    def branch_work():
        try:
            handle = state.schedule_refresh(check_time)
            with handles_lock:
                handles.append(handle)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=branch_work) for _ in range(branch_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, f"Branch errors: {errors}"
    assert len(handles) == branch_count, "every caller must have enqueued first"

    batcher = ThreadPoolExecutor(max_workers=1)
    batcher.submit(state.checkpoint_batches_forever)
    try:
        for handle in handles:
            assert handle.wait(timeout=30)
        assert len(calls) == 1, (
            f"Expected 1 coalesced API call for {branch_count} due refreshes, "
            f"got {len(calls)}."
        )
        assert calls[0] == [], "Refreshes should produce an empty updates list"
    finally:
        state.stop_checkpointing()
        batcher.shutdown(wait=True)


def test_map_with_concurrent_waits_api_call_count_scales_with_real_ops_not_empties():
    """400 empty checkpoints + 10 real ops → 1 API call with limit=11.

    Demonstrates that the effective batch count is driven by real operations
    (and only the *first* empty), not the total number of empties.

    With limit=11: the first empty counts as effective_op 1, and each of the
    10 real ops increments the count (effective_ops 2–11). The limit is hit
    exactly when the last real op is collected. All 399 remaining empties are
    coalesced in without incrementing the count.

    Result: 1 batch (410 operations, 10 real) → 1 API call.
    """
    mock_client, calls = _make_tracking_client()
    # limit = 1 (first empty) + 10 (real ops) = 11, so all fit in one batch
    state = _make_state(mock_client, batch_time=5.0, max_ops=11)

    completion_events: list[CompletionEvent] = []

    try:
        # 400 empty checkpoints (simulating concurrent branch resumes)
        for _ in range(400):
            ev = CompletionEvent()
            completion_events.append(ev)
            state._checkpoint_queue.put(QueuedOperation(None, ev))  # noqa: SLF001

        # 10 real operations alongside the empties
        for i in range(10):
            op = OperationUpdate(
                operation_id=f"op_{i}",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            )

            ev = CompletionEvent()
            completion_events.append(ev)
            state._checkpoint_queue.put(QueuedOperation(op, ev))  # noqa: SLF001

        batcher = ThreadPoolExecutor(max_workers=1)
        batcher.submit(state.checkpoint_batches_forever)

        # Wait for all 410 to be processed
        for ev in completion_events:
            ev.wait()

        # 1 empty (effective=1) + 10 real ops (effective=11) exhaust the batch
        # limit exactly. The 399 remaining empties coalesce in → still 1 API call.
        assert len(calls) == 1, (
            f"Expected 1 API call with 400 empty + 10 real ops (limit=11), "
            f"got {len(calls)}."
        )
        # Only the 10 real ops appear in the updates list; empties are excluded.
        real_op_ids = {u.operation_id for batch in calls for u in batch}
        assert real_op_ids == {f"op_{i}" for i in range(10)}
    finally:
        state.stop_checkpointing()
        batcher.shutdown(wait=True)
