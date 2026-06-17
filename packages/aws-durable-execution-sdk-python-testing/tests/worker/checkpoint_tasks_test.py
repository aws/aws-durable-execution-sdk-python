"""Tests for checkpoint/get-state worker tasks and serialized client calls."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient
from aws_durable_execution_sdk_python_testing.worker.registry import ExecutionRegistry

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
        CheckpointProcessor,
    )
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore

ARN = "arn:aws:lambda:us-west-2:123456789012:function:fn/durable-execution/exec/inv"


class _FakeExecution:
    def __init__(self, *, is_complete: bool = False) -> None:
        self.is_complete = is_complete


class _FakeStore:
    def __init__(self, execution: _FakeExecution) -> None:
        self._execution = execution

    def load(self, execution_arn: str) -> _FakeExecution:  # noqa: ARG002
        return self._execution


def _client(processor: Mock, execution: _FakeExecution) -> InMemoryServiceClient:
    registry = ExecutionRegistry(
        cast("ExecutionStore", _FakeStore(execution)), cast("Scheduler", object())
    )
    return InMemoryServiceClient(cast("CheckpointProcessor", processor), registry)


def test_checkpoint_delegates_and_returns_output():
    execution = _FakeExecution(is_complete=False)
    processor = Mock()
    output = CheckpointOutput(checkpoint_token="t2", new_execution_state=Mock())  # noqa: S106
    processor.process_checkpoint.return_value = output

    client = _client(processor, execution)
    result = client.checkpoint(ARN, "t1", [], "ct")

    assert result is output
    processor.process_checkpoint.assert_called_once_with("t1", [], "ct")


def test_get_state_delegates_and_returns_output():
    execution = _FakeExecution(is_complete=False)
    processor = Mock()
    output = StateOutput(operations=[], next_marker=None)
    processor.get_execution_state.return_value = output

    client = _client(processor, execution)
    result = client.get_execution_state(ARN, "t1", "marker", 50)

    assert result is output
    processor.get_execution_state.assert_called_once_with("t1", "marker", 50)


def test_completing_execution_tears_down_its_worker():
    execution = _FakeExecution(is_complete=True)
    processor = Mock()
    processor.process_checkpoint.return_value = CheckpointOutput(
        checkpoint_token="t2",  # noqa: S106
        new_execution_state=Mock(),
    )
    registry = ExecutionRegistry(
        cast("ExecutionStore", _FakeStore(execution)), cast("Scheduler", object())
    )
    client = InMemoryServiceClient(cast("CheckpointProcessor", processor), registry)

    client.checkpoint(ARN, "t1", [], None)

    assert registry.get(ARN) is None


def test_concurrent_checkpoints_same_arn_are_serialized():
    execution = _FakeExecution(is_complete=False)
    active = 0
    max_active = 0
    guard = threading.Lock()

    def process_checkpoint(token, updates, client_token):  # noqa: ANN001, ARG001
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.01)
        with guard:
            active -= 1
        return CheckpointOutput(checkpoint_token="t", new_execution_state=Mock())  # noqa: S106

    processor = Mock()
    processor.process_checkpoint.side_effect = process_checkpoint

    client = _client(processor, execution)

    def call() -> None:
        client.checkpoint(ARN, "t1", [], None)

    threads = [threading.Thread(target=call) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert max_active == 1
    assert processor.process_checkpoint.call_count == 8
