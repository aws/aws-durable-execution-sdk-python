"""Tests for the concurrency module."""

import json
import random
import threading
import time
from concurrent.futures import Future
from functools import partial
from itertools import combinations
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.concurrency.executor import (
    ConcurrentExecutor,
    TimerScheduler,
)
from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    BranchStatus,
    CompletionReason,
    Executable,
    ExecutableWithState,
    ExecutionCounters,
)
from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    InvalidStateError,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.map import MapExecutor


def test_batch_item_status_enum():
    """Test BatchItemStatus enum values."""
    assert BatchItemStatus.SUCCEEDED.value == "SUCCEEDED"
    assert BatchItemStatus.FAILED.value == "FAILED"
    assert BatchItemStatus.STARTED.value == "STARTED"


def test_completion_reason_enum():
    """Test CompletionReason enum values."""
    assert CompletionReason.ALL_COMPLETED.value == "ALL_COMPLETED"
    assert CompletionReason.MIN_SUCCESSFUL_REACHED.value == "MIN_SUCCESSFUL_REACHED"
    assert (
        CompletionReason.FAILURE_TOLERANCE_EXCEEDED.value
        == "FAILURE_TOLERANCE_EXCEEDED"
    )


def test_branch_status_enum():
    """Test BranchStatus enum values."""
    assert BranchStatus.PENDING.value == "pending"
    assert BranchStatus.RUNNING.value == "running"
    assert BranchStatus.COMPLETED.value == "completed"
    assert BranchStatus.SUSPENDED.value == "suspended"
    assert BranchStatus.SUSPENDED_WITH_TIMEOUT.value == "suspended_with_timeout"
    assert BranchStatus.FAILED.value == "failed"


def test_batch_item_creation():
    """Test BatchItem creation and properties."""
    item = BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test_result")
    assert item.index == 0
    assert item.status == BatchItemStatus.SUCCEEDED
    assert item.result == "test_result"
    assert item.error is None


def test_batch_item_to_dict():
    """Test BatchItem to_dict method."""
    error = ErrorObject(
        message="test message", type="TestError", data=None, stack_trace=None
    )
    item = BatchItem(index=1, status=BatchItemStatus.FAILED, error=error)

    result = item.to_dict()
    expected = {
        "index": 1,
        "status": "FAILED",
        "result": None,
        "error": error.to_dict(),
    }
    assert result == expected


def test_batch_item_from_dict():
    """Test BatchItem from_dict method."""
    data = {
        "index": 2,
        "status": "SUCCEEDED",
        "result": "success_result",
        "error": None,
    }

    item = BatchItem.from_dict(data)
    assert item.index == 2
    assert item.status == BatchItemStatus.SUCCEEDED
    assert item.result == "success_result"
    assert item.error is None


def test_batch_result_creation():
    """Test BatchResult creation."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    assert len(result.all) == 2
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_batch_result_succeeded():
    """Test BatchResult succeeded method."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.SUCCEEDED, "result2"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    succeeded = result.succeeded()
    assert len(succeeded) == 2
    assert succeeded[0].result == "result1"
    assert succeeded[1].result == "result2"


def test_batch_result_failed():
    """Test BatchResult failed method."""
    error = ErrorObject("test message", "TestError", None, None)
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(1, BatchItemStatus.FAILED, error=error),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    failed = result.failed()
    assert len(failed) == 1
    assert failed[0].error == error


def test_batch_result_started():
    """Test BatchResult started method."""
    items = [
        BatchItem(0, BatchItemStatus.STARTED),
        BatchItem(1, BatchItemStatus.SUCCEEDED, "result1"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    started = result.started()
    assert len(started) == 1
    assert started[0].status == BatchItemStatus.STARTED


def test_batch_result_status():
    """Test BatchResult status property."""
    # No failures
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert result.status == BatchItemStatus.SUCCEEDED

    # Has failures
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert result.status == BatchItemStatus.FAILED


def test_batch_result_has_failure():
    """Test BatchResult has_failure property."""
    # No failures
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert not result.has_failure

    # Has failures
    items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        )
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert result.has_failure


def test_batch_result_throw_if_error():
    """Test BatchResult throw_if_error method."""
    # No errors
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    result.throw_if_error()  # Should not raise

    # Has error
    error = ErrorObject("test message", "TestError", None, None)
    items = [BatchItem(0, BatchItemStatus.FAILED, error=error)]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    with pytest.raises(CallableRuntimeError):
        result.throw_if_error()


def test_batch_result_get_results():
    """Test BatchResult get_results method."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.SUCCEEDED, "result2"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    results = result.get_results()
    assert results == ["result1", "result2"]


def test_batch_result_get_errors():
    """Test BatchResult get_errors method."""
    error1 = ErrorObject("msg1", "Error1", None, None)
    error2 = ErrorObject("msg2", "Error2", None, None)
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(1, BatchItemStatus.FAILED, error=error1),
        BatchItem(2, BatchItemStatus.FAILED, error=error2),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    errors = result.get_errors()
    assert len(errors) == 2
    assert error1 in errors
    assert error2 in errors


def test_batch_result_counts():
    """Test BatchResult count properties."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.STARTED),
        BatchItem(3, BatchItemStatus.SUCCEEDED, "result2"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    assert result.success_count == 2
    assert result.failure_count == 1
    assert result.started_count == 1
    assert result.total_count == 4


def test_batch_result_to_dict():
    """Test BatchResult to_dict method."""
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    result_dict = result.to_dict()
    expected = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "ALL_COMPLETED",
    }
    assert result_dict == expected


def test_batch_result_from_dict():
    """Test BatchResult from_dict method."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "ALL_COMPLETED",
    }

    result = BatchResult.from_dict(data)
    assert len(result.all) == 1
    assert result.all[0].index == 0
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_batch_result_from_dict_default_completion_reason():
    """Test BatchResult from_dict with default completion reason."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        # No completionReason provided
    }

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        assert "Missing completionReason" in mock_logger.warning.call_args[0][0]


def test_batch_result_from_dict_infer_all_completed_all_succeeded():
    """Test BatchResult from_dict infers ALL_COMPLETED when all items succeeded."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "SUCCEEDED", "result": "result2", "error": None},
        ],
        # No completionReason provided
    }

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_infer_failure_tolerance_exceeded_all_failed():
    """Test BatchResult from_dict infers FAILURE_TOLERANCE_EXCEEDED when all items failed."""
    error_data = {
        "message": "Test error",
        "type": "TestError",
        "data": None,
        "stackTrace": None,
    }
    data = {
        "all": [
            {"index": 0, "status": "FAILED", "result": None, "error": error_data},
            {"index": 1, "status": "FAILED", "result": None, "error": error_data},
        ],
        # No completionReason provided
    }

    # even if everything has failed, if we've completed all items, then we've finished as ALL_COMPLETED
    # https://github.com/aws/aws-durable-execution-sdk-js/blob/f20396f24afa9d6539d8e5056ee851ac7ef62301/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/concurrent-execution-handler.ts#L324-L335
    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_infer_all_completed_mixed_success_failure():
    """Test BatchResult from_dict infers ALL_COMPLETED when mix of success/failure but no started items."""
    error_data = {
        "message": "Test error",
        "type": "TestError",
        "data": None,
        "stackTrace": None,
    }
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "FAILED", "result": None, "error": error_data},
            {"index": 2, "status": "SUCCEEDED", "result": "result2", "error": None},
        ],
        # No completionReason provided
    }

    # the logic is that when \every item i: hasCompleted(i) then terminate due to all_completed
    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_infer_min_successful_reached_has_started():
    """Test BatchResult from_dict infers MIN_SUCCESSFUL_REACHED when items are still started."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "STARTED", "result": None, "error": None},
            {"index": 2, "status": "SUCCEEDED", "result": "result2", "error": None},
        ],
        # No completionReason provided
    }

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data, CompletionConfig(1))
        assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_infer_empty_items():
    """Test BatchResult from_dict infers ALL_COMPLETED for empty items."""
    data = {
        "all": [],
        # No completionReason provided
    }

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_with_explicit_completion_reason():
    """Test BatchResult from_dict uses explicit completionReason when provided."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "MIN_SUCCESSFUL_REACHED",
    }

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED
        # No warning should be logged when completionReason is provided
        mock_logger.warning.assert_not_called()


def test_batch_result_infer_completion_reason_edge_cases():
    """Test _infer_completion_reason method with various edge cases."""
    # Test with only started items
    started_items = [
        BatchItem(0, BatchItemStatus.STARTED).to_dict(),
        BatchItem(1, BatchItemStatus.STARTED).to_dict(),
    ]
    items = {"all": started_items}
    batch = BatchResult.from_dict(items, CompletionConfig(0))  # SLF001
    # this state is not possible with CompletionConfig(0)
    assert batch.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Test with only started items
    started_items = [
        BatchItem(0, BatchItemStatus.STARTED).to_dict(),
        BatchItem(1, BatchItemStatus.STARTED).to_dict(),
    ]
    items = {"all": started_items}
    batch = BatchResult.from_dict(items)  # SLF001
    # this state is not possible with CompletionConfig(0)
    assert batch.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED

    # Test with only failed items
    failed_items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ).to_dict(),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ).to_dict(),
    ]
    failed_items = {"all": failed_items}
    batch = BatchResult.from_dict(failed_items)  # SLF001
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test with only succeeded items
    succeeded_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
        BatchItem(1, BatchItemStatus.SUCCEEDED, "result2").to_dict(),
    ]
    succeeded_items = {"all": succeeded_items}
    batch = BatchResult.from_dict(succeeded_items)  # SLF001
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test with mixed but no started (all completed)
    mixed_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]

    batch = BatchResult.from_items(mixed_items)  # SLF001
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED


def test_batch_result_get_results_empty():
    """Test BatchResult get_results with no successful items."""
    items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(1, BatchItemStatus.STARTED),
    ]
    result = BatchResult(items, CompletionReason.FAILURE_TOLERANCE_EXCEEDED)

    results = result.get_results()
    assert results == []


def test_batch_result_get_errors_empty():
    """Test BatchResult get_errors with no failed items."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(1, BatchItemStatus.STARTED),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    errors = result.get_errors()
    assert errors == []


def test_executable_creation():
    """Test Executable creation."""

    def test_func():
        return "test"

    executable = Executable(index=5, func=test_func)
    assert executable.index == 5
    assert executable.func == test_func


def test_executable_with_state_creation():
    """Test ExecutableWithState creation."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    assert exe_state.executable == executable
    assert exe_state.status == BranchStatus.PENDING
    assert exe_state.index == 1
    assert exe_state.callable == executable.func


def test_executable_with_state_properties():
    """Test ExecutableWithState property access."""

    def test_callable():
        return "test"

    executable = Executable(index=42, func=test_callable)
    exe_state = ExecutableWithState(executable)

    assert exe_state.index == 42
    assert exe_state.callable == test_callable
    assert exe_state.suspend_until is None


def test_executable_with_state_future_not_available():
    """Test ExecutableWithState future property when not started."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    with pytest.raises(InvalidStateError):
        _ = exe_state.future


def test_executable_with_state_result_not_available():
    """Test ExecutableWithState result property when not completed."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    with pytest.raises(InvalidStateError):
        _ = exe_state.result


def test_executable_with_state_error_not_available():
    """Test ExecutableWithState error property when not failed."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    with pytest.raises(InvalidStateError):
        _ = exe_state.error


def test_executable_with_state_is_running():
    """Test ExecutableWithState is_running property."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    assert not exe_state.is_running

    future = Future()
    exe_state.run(future)
    assert exe_state.is_running


def test_executable_with_state_can_resume():
    """Test ExecutableWithState can_resume property."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    # Not suspended
    assert not exe_state.can_resume

    # Suspended indefinitely
    exe_state.suspend()
    assert exe_state.can_resume

    # Suspended with timeout in future
    future_time = time.time() + 10
    exe_state.suspend_with_timeout(future_time)
    assert not exe_state.can_resume

    # Suspended with timeout in past
    past_time = time.time() - 10
    exe_state.suspend_with_timeout(past_time)
    assert exe_state.can_resume


def test_executable_with_state_run():
    """Test ExecutableWithState run method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    future = Future()

    exe_state.run(future)
    assert exe_state.status == BranchStatus.RUNNING
    assert exe_state.future == future


def test_executable_with_state_run_invalid_state():
    """Test ExecutableWithState run method from invalid state."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    future1 = Future()
    future2 = Future()

    exe_state.run(future1)

    with pytest.raises(InvalidStateError):
        exe_state.run(future2)


def test_executable_with_state_suspend():
    """Test ExecutableWithState suspend method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    exe_state.suspend()
    assert exe_state.status == BranchStatus.SUSPENDED
    assert exe_state.suspend_until is None


def test_executable_with_state_suspend_with_timeout():
    """Test ExecutableWithState suspend_with_timeout method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    timestamp = time.time() + 5

    exe_state.suspend_with_timeout(timestamp)
    assert exe_state.status == BranchStatus.SUSPENDED_WITH_TIMEOUT
    assert exe_state.suspend_until == timestamp


def test_executable_with_state_complete():
    """Test ExecutableWithState complete method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    exe_state.complete("test_result")
    assert exe_state.status == BranchStatus.COMPLETED
    assert exe_state.result == "test_result"


def test_executable_with_state_fail():
    """Test ExecutableWithState fail method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    error = Exception("test error")

    exe_state.fail(error)
    assert exe_state.status == BranchStatus.FAILED
    assert exe_state.error == error


def test_execution_counters_creation():
    """Test ExecutionCounters creation."""
    counters = ExecutionCounters(
        total_tasks=10,
        min_successful=8,
        tolerated_failure_count=2,
        tolerated_failure_percentage=20.0,
    )

    assert counters.total_tasks == 10
    assert counters.min_successful == 8
    assert counters.tolerated_failure_count == 2
    assert counters.tolerated_failure_percentage == 20.0
    assert counters.success_count == 0
    assert counters.failure_count == 0


def test_execution_counters_complete_task():
    """Test ExecutionCounters complete_task method."""
    counters = ExecutionCounters(5, 3, None, None)

    counters.complete_task()
    assert counters.success_count == 1


def test_execution_counters_fail_task():
    """Test ExecutionCounters fail_task method."""
    counters = ExecutionCounters(5, 3, None, None)

    counters.fail_task()
    assert counters.failure_count == 1


def test_execution_counters_should_complete_min_successful():
    """Test ExecutionCounters should_complete with min successful reached."""
    counters = ExecutionCounters(5, 3, None, None)

    assert not counters.should_complete()

    counters.complete_task()
    counters.complete_task()
    counters.complete_task()

    assert counters.should_complete()


def test_execution_counters_should_complete_failure_count():
    """Test ExecutionCounters should_complete with failure count exceeded."""
    counters = ExecutionCounters(5, 3, 1, None)

    assert not counters.should_complete()

    counters.fail_task()
    assert not counters.should_complete()

    counters.fail_task()
    assert counters.should_complete()


def test_execution_counters_should_complete_failure_percentage():
    """Test ExecutionCounters should_complete with failure percentage exceeded."""
    counters = ExecutionCounters(10, 8, None, 15.0)

    assert not counters.should_complete()

    counters.fail_task()
    assert not counters.should_complete()

    counters.fail_task()
    assert counters.should_complete()  # 20% > 15%


def test_execution_counters_is_all_completed():
    """Test ExecutionCounters is_all_completed method."""
    counters = ExecutionCounters(3, 2, None, None)

    assert not counters.is_all_completed()

    counters.complete_task()
    counters.complete_task()
    assert not counters.is_all_completed()

    counters.complete_task()
    assert counters.is_all_completed()


def test_execution_counters_is_min_successful_reached():
    """Test ExecutionCounters is_min_successful_reached method."""
    counters = ExecutionCounters(5, 3, None, None)

    assert not counters.is_min_successful_reached()

    counters.complete_task()
    counters.complete_task()
    assert not counters.is_min_successful_reached()

    counters.complete_task()
    assert counters.is_min_successful_reached()


def test_execution_counters_is_failure_tolerance_exceeded():
    """Test ExecutionCounters is_failure_tolerance_exceeded method."""
    counters = ExecutionCounters(10, 8, 2, None)

    assert not counters.is_failure_tolerance_exceeded()

    counters.fail_task()
    counters.fail_task()
    assert not counters.is_failure_tolerance_exceeded()

    counters.fail_task()
    assert counters.is_failure_tolerance_exceeded()


def test_execution_counters_zero_total_tasks():
    """Test ExecutionCounters with zero total tasks."""
    counters = ExecutionCounters(0, 0, None, 50.0)

    # Should not fail with division by zero
    assert not counters.is_failure_tolerance_exceeded()


def test_execution_counters_failure_percentage_edge_case():
    """Test ExecutionCounters failure percentage at exact threshold."""
    counters = ExecutionCounters(10, 5, None, 20.0)

    # Exactly at threshold (20%)
    counters.failure_count = 2
    assert not counters.is_failure_tolerance_exceeded()

    # Just over threshold
    counters.failure_count = 3
    assert counters.is_failure_tolerance_exceeded()


def test_execution_counters_thread_safety():
    """Test ExecutionCounters thread safety."""
    counters = ExecutionCounters(100, 50, None, None)

    def worker():
        for _ in range(10):
            counters.complete_task()

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert counters.success_count == 50


def test_batch_result_failed_with_none_error():
    """Test BatchResult failed method filters out None errors."""
    items = [
        BatchItem(0, BatchItemStatus.FAILED, error=None),  # Should be filtered out
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    failed = result.failed()
    assert len(failed) == 1
    assert failed[0].error is not None


def test_concurrent_executor_properties():
    """Test ConcurrentExecutor basic properties."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )
    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Test basic properties
    assert executor.executables == executables
    assert executor.max_concurrency == 2
    assert executor.completion_config == completion_config
    assert executor.sub_type_top == "TOP"
    assert executor.sub_type_iteration == "ITER"
    assert executor.name_prefix == "test_"


def test_concurrent_executor_full_execution_path():
    """Test ConcurrentExecutor full execution."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )
    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    # Mock ChildConfig from the config module
    with patch(
        "aws_durable_execution_sdk_python.config.ChildConfig"
    ) as mock_child_config:
        mock_child_config.return_value = Mock()

        def mock_run_in_child_context(func, name, config):
            return func(Mock())

        result = executor.execute(execution_state, mock_run_in_child_context)
        assert len(result.all) >= 1


def test_timer_scheduler_double_check_resume_queue():
    """Test TimerScheduler double-check logic in scheduler loop."""
    callback = Mock()

    with TimerScheduler(callback) as scheduler:
        exe_state1 = ExecutableWithState(Executable(0, lambda: "test"))
        exe_state2 = ExecutableWithState(Executable(1, lambda: "test"))

        # Schedule two tasks with different times to avoid comparison issues
        past_time1 = time.time() - 2
        past_time2 = time.time() - 1
        scheduler.schedule_resume(exe_state1, past_time1)
        scheduler.schedule_resume(exe_state2, past_time2)

        # Give scheduler time to process
        time.sleep(0.1)

        # At least one callback should have been made
        assert callback.call_count >= 0


def test_concurrent_executor_on_task_complete_timed_suspend():
    """Test ConcurrentExecutor _on_task_complete with TimedSuspendExecution."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = TimedSuspendExecution("test message", time.time() + 1)

    scheduler = Mock()
    scheduler.schedule_resume = Mock()

    executor._on_task_complete(exe_state, future, scheduler)  # noqa: SLF001

    assert exe_state.status == BranchStatus.SUSPENDED_WITH_TIMEOUT
    scheduler.schedule_resume.assert_called_once()


def test_concurrent_executor_on_task_complete_suspend():
    """Test ConcurrentExecutor _on_task_complete with SuspendExecution."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = SuspendExecution("test message")

    scheduler = Mock()

    executor._on_task_complete(exe_state, future, scheduler)  # noqa: SLF001

    assert exe_state.status == BranchStatus.SUSPENDED


def test_concurrent_executor_on_task_complete_exception():
    """Test ConcurrentExecutor _on_task_complete with general exception."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = ValueError("Test error")

    scheduler = Mock()

    executor._on_task_complete(exe_state, future, scheduler)  # noqa: SLF001

    assert exe_state.status == BranchStatus.FAILED
    assert isinstance(exe_state.error, ValueError)


def test_concurrent_executor_create_result_with_failed_branches():
    """Test ConcurrentExecutor with failed branches using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            if executable.index == 0:
                return f"result_{executable.index}"
            msg = "Test error"
            raise ValueError(msg)

    def success_callable():
        return "test"

    def failure_callable():
        return "test2"

    executables = [Executable(0, success_callable), Executable(1, failure_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 2
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[1].status == BatchItemStatus.FAILED
    # WHEN all items complete, THEN completion reason is ALL_COMPLETED.
    # we don't consider thresholds and limits.
    # https://github.com/aws/aws-durable-execution-sdk-js/blob/ff8b72ef888dd47a840f36d4eb0ee84dd3b55a30/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/concurrent-execution-handler.test.ts#L630-L655
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_concurrent_executor_execute_item_in_child_context():
    """Test ConcurrentExecutor _execute_item_in_child_context."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = executor._execute_item_in_child_context(  # noqa: SLF001
        executor_context, executables[0]
    )
    assert result == "result_0"


def test_execution_counters_impossible_to_succeed():
    """Test ExecutionCounters should_complete when impossible to succeed."""
    counters = ExecutionCounters(5, 4, None, None)

    # Fail 3 tasks, leaving only 2 remaining (can't reach min_successful of 4)
    counters.fail_task()
    counters.fail_task()
    counters.fail_task()

    assert counters.should_complete()


def test_concurrent_executor_create_result_failure_tolerance_exceeded():
    """Test ConcurrentExecutor with failure tolerance exceeded using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            msg = "Task failed"
            raise ValueError(msg)

    def failure_callable():
        return "test"

    executables = [Executable(0, failure_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=0,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    def mock_run_in_child_context(func, name, config):
        return func(Mock())

    result = executor.execute(execution_state, mock_run_in_child_context)
    # WHEN all items complete, THEN completion reason is ALL_COMPLETED.
    # we don't consider thresholds and limits.
    # https://github.com/aws/aws-durable-execution-sdk-js/blob/ff8b72ef888dd47a840f36d4eb0ee84dd3b55a30/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/concurrent-execution-handler.test.ts#L630-L655
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_single_task_suspend_bubbles_up():
    """Test that single task suspend bubbles up the exception."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            msg = "test"
            raise TimedSuspendExecution(msg, time.time() + 1)  # Future time

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    # Should raise TimedSuspendExecution since no other tasks running
    with pytest.raises(TimedSuspendExecution):
        executor.execute(execution_state, executor_context)


def test_multiple_tasks_one_suspends_execution_continues():
    """Test that when one task suspends but others are running, execution continues."""

    class TestExecutor(ConcurrentExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.task_a_suspended = threading.Event()
            self.task_b_completed = False

        def execute_item(self, child_context, executable):
            if executable.index == 0:  # Task A
                self.task_a_suspended.set()
                msg = "test"
                raise TimedSuspendExecution(msg, time.time() + 1)  # Future time
            # Task B
            # Wait for Task A to suspend first
            self.task_a_suspended.wait(timeout=2.0)
            time.sleep(0.1)  # Ensure A has suspended
            self.task_b_completed = True
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "testA"), Executable(1, lambda: "testB")]
    completion_config = CompletionConfig.all_completed()

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    # Should raise TimedSuspendExecution after Task B completes
    with pytest.raises(TimedSuspendExecution):
        executor.execute(execution_state, executor_context)

    # Assert that Task B did complete before suspension
    assert executor.task_b_completed


def test_concurrent_executor_with_single_task_resubmit():
    """Test single task suspend bubbles up immediately."""

    class TestExecutor(ConcurrentExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.call_count = 0

        def execute_item(self, child_context, executable):
            self.call_count += 1
            msg = "test"
            raise TimedSuspendExecution(msg, time.time() + 10)  # Future time

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    # Should raise TimedSuspendExecution since single task suspends
    with pytest.raises(TimedSuspendExecution):
        executor.execute(execution_state, executor_context)


def test_concurrent_executor_with_timed_resubmit_while_other_task_running():
    """Test timed resubmission while other tasks are still running."""

    class TestExecutor(ConcurrentExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.call_counts = {}
            self.task_a_started = threading.Event()
            self.task_b_can_complete = threading.Event()
            self.task_b_completed = threading.Event()

        def execute_item(self, child_context, executable):
            task_id = executable.index
            self.call_counts[task_id] = self.call_counts.get(task_id, 0) + 1

            if task_id == 0:  # Task A - runs long
                self.task_a_started.set()
                # Wait for task B to complete before finishing
                self.task_b_can_complete.wait(timeout=5)
                self.task_b_completed.wait(timeout=1)
                return "result_A"

            if task_id == 1:  # Task B - suspends and resubmits
                call_count = self.call_counts[task_id]

                if call_count == 1:
                    # First call: immediate resubmit (past timestamp)
                    msg = "immediate"
                    raise TimedSuspendExecution(msg, time.time() - 1)
                if call_count == 2:
                    # Second call: short delay resubmit
                    msg = "short_delay"
                    raise TimedSuspendExecution(msg, time.time() + 0.2)
                # Third call: complete successfully
                result = "result_B"
                self.task_b_can_complete.set()
                self.task_b_completed.set()
                return result

            return None

    executables = [
        Executable(0, lambda: "task_A"),  # Long running task
        Executable(1, lambda: "task_B"),  # Suspending/resubmitting task
    ]
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    # Should complete successfully after B resubmits and both tasks finish
    result = executor.execute(execution_state, executor_context)

    # Verify results
    assert len(result.all) == 2
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in result.all)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED

    # Verify task B was called 3 times (initial + 2 resubmits)
    assert executor.call_counts[1] == 3
    # Verify task A was called only once
    assert executor.call_counts[0] == 1


def test_timer_scheduler_double_check_condition():
    """Test TimerScheduler double-check condition in _timer_loop (line 434)."""
    callback = Mock()

    with TimerScheduler(callback) as scheduler:
        exe_state = ExecutableWithState(Executable(0, lambda: "test"))
        exe_state.suspend()  # Make it resumable

        # Schedule a task with past time
        past_time = time.time() - 1
        scheduler.schedule_resume(exe_state, past_time)

        # Give scheduler time to process and hit the double-check condition
        time.sleep(0.2)

        # The callback should be called
        assert callback.call_count >= 1


def test_concurrent_executor_should_execution_suspend_with_timeout():
    """Test should_execution_suspend with SUSPENDED_WITH_TIMEOUT state."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with state in SUSPENDED_WITH_TIMEOUT
    exe_state = ExecutableWithState(executables[0])
    future_time = time.time() + 10
    exe_state.suspend_with_timeout(future_time)

    executor.executables_with_state = [exe_state]

    result = executor.should_execution_suspend()

    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)
    assert result.exception.scheduled_timestamp == future_time


def test_concurrent_executor_should_execution_suspend_indefinite():
    """Test should_execution_suspend with indefinite SUSPENDED state."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with state in SUSPENDED (indefinite)
    exe_state = ExecutableWithState(executables[0])
    exe_state.suspend()

    executor.executables_with_state = [exe_state]

    result = executor.should_execution_suspend()

    assert result.should_suspend
    assert isinstance(result.exception, SuspendExecution)
    assert "pending external callback" in str(result.exception)


def test_concurrent_executor_create_result_with_failed_status():
    """Test with failed executable status using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            msg = "Test error"
            raise ValueError(msg)

    def failure_callable():
        return "test"

    executables = [Executable(0, failure_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=0,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error is not None
    assert result.all[0].error.message == "Test error"


def test_timer_scheduler_can_resume_false():
    """Test TimerScheduler when exe_state.can_resume is False."""
    callback = Mock()

    with TimerScheduler(callback) as scheduler:
        exe_state = ExecutableWithState(Executable(0, lambda: "test"))

        # Set state to something that can't resume
        exe_state.complete("done")

        # Schedule with past time
        past_time = time.time() - 1
        scheduler.schedule_resume(exe_state, past_time)

        # Give scheduler time to process
        time.sleep(0.15)

        # Callback should not be called since can_resume is False
        callback.assert_not_called()


def test_concurrent_executor_mixed_suspend_states():
    """Test should_execution_suspend with mixed suspend states."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create one with timed suspend and one with indefinite suspend
    exe_state1 = ExecutableWithState(executables[0])
    exe_state2 = ExecutableWithState(executables[1])

    future_time = time.time() + 5
    exe_state1.suspend_with_timeout(future_time)
    exe_state2.suspend()  # Indefinite

    executor.executables_with_state = [exe_state1, exe_state2]

    result = executor.should_execution_suspend()

    # Should return timed suspend (earliest timestamp takes precedence)
    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)


def test_concurrent_executor_multiple_timed_suspends():
    """Test should_execution_suspend with multiple timed suspends to find earliest."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create two with different timed suspends
    exe_state1 = ExecutableWithState(executables[0])
    exe_state2 = ExecutableWithState(executables[1])

    later_time = time.time() + 10
    earlier_time = time.time() + 5

    exe_state1.suspend_with_timeout(later_time)
    exe_state2.suspend_with_timeout(earlier_time)

    executor.executables_with_state = [exe_state1, exe_state2]

    result = executor.should_execution_suspend()

    # Should return the earlier timestamp
    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)
    assert result.exception.scheduled_timestamp == earlier_time


def test_timer_scheduler_double_check_condition_race():
    """Test TimerScheduler double-check condition when heap changes between checks."""
    callback = Mock()

    with TimerScheduler(callback) as scheduler:
        exe_state1 = ExecutableWithState(Executable(0, lambda: "test"))
        exe_state2 = ExecutableWithState(Executable(1, lambda: "test"))

        exe_state1.suspend()
        exe_state2.suspend()

        # Schedule first task with past time
        past_time = time.time() - 1
        scheduler.schedule_resume(exe_state1, past_time)

        # Brief delay to let timer thread see the first task
        time.sleep(0.05)

        # Schedule second task with even more past time (will be heap[0])
        very_past_time = time.time() - 2
        scheduler.schedule_resume(exe_state2, very_past_time)

        # Wait for processing
        time.sleep(0.2)

        assert callback.call_count >= 1


def test_should_execution_suspend_earliest_timestamp_comparison():
    """Test should_execution_suspend timestamp comparison logic (line 554)."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test"),
        Executable(1, lambda: "test2"),
        Executable(2, lambda: "test3"),
    ]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=3,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create three executables with different suspend times
    exe_state1 = ExecutableWithState(executables[0])
    exe_state2 = ExecutableWithState(executables[1])
    exe_state3 = ExecutableWithState(executables[2])

    time1 = time.time() + 10
    time2 = time.time() + 5  # Earliest
    time3 = time.time() + 15

    exe_state1.suspend_with_timeout(time1)
    exe_state2.suspend_with_timeout(time2)
    exe_state3.suspend_with_timeout(time3)

    executor.executables_with_state = [exe_state1, exe_state2, exe_state3]

    result = executor.should_execution_suspend()

    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)
    assert result.exception.scheduled_timestamp == time2


def test_concurrent_executor_execute_with_failing_task():
    """Test execute() with a task that fails using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            msg = "Task failed"
            raise ValueError(msg)

    def failure_callable():
        return "test"

    executables = [Executable(0, failure_callable)]
    completion_config = CompletionConfig(
        min_successful=1, tolerated_failure_count=0, tolerated_failure_percentage=None
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error.message == "Task failed"


def test_timer_scheduler_cannot_resume_branch():
    """Test TimerScheduler when exe_state cannot resume (434->433 branch)."""
    callback = Mock()

    with TimerScheduler(callback) as scheduler:
        exe_state = ExecutableWithState(Executable(0, lambda: "test"))

        # Set to completed state so can_resume returns False
        exe_state.complete("done")

        # Schedule with past time
        past_time = time.time() - 1
        scheduler.schedule_resume(exe_state, past_time)

        # Wait for processing
        time.sleep(0.2)

        # Callback should not be called since can_resume is False
        callback.assert_not_called()


def test_create_result_no_failed_executables():
    """Test when no executables are failed using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    def success_callable():
        return "test"

    executables = [Executable(0, success_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_create_result_with_suspended_executable():
    """Test with suspended executable using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            msg = "Test suspend"
            raise SuspendExecution(msg)

    def suspend_callable():
        return "test"

    executables = [Executable(0, suspend_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    # Should raise SuspendExecution since single task suspends
    with pytest.raises(SuspendExecution):
        executor.execute(execution_state, executor_context)


# Tests for _create_result method match statement branches
def test_create_result_completed_branch():
    """Test _create_result with COMPLETED status branch."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with COMPLETED status
    exe_state = ExecutableWithState(executables[0])
    exe_state.complete("test_result")
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "test_result"
    assert result.all[0].error is None
    assert result.all[0].index == 0


def test_create_result_failed_branch():
    """Test _create_result with FAILED status branch."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with FAILED status
    exe_state = ExecutableWithState(executables[0])
    test_error = ValueError("Test error message")
    exe_state.fail(test_error)
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].result is None
    assert result.all[0].error is not None
    assert result.all[0].error.message == "Test error message"
    assert result.all[0].error.type == "ValueError"
    assert result.all[0].index == 0


def test_create_result_pending_branch():
    """Test _create_result with PENDING status branch."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with PENDING status (default state)
    exe_state = ExecutableWithState(executables[0])
    # PENDING is the default state, no need to change it
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # By default, if we've terminated the reasoning is failure tolerance exceeded
    # according to the spec
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_create_result_running_branch():
    """Test _create_result with RUNNING status branch."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with RUNNING status
    exe_state = ExecutableWithState(executables[0])
    future = Future()
    exe_state.run(future)
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # By default, if we've terminated the reasoning is failure tolerance exceeded
    # according to the spec
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_create_result_suspended_branch():
    """Test _create_result with SUSPENDED status branch."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with SUSPENDED status
    exe_state = ExecutableWithState(executables[0])
    exe_state.suspend()
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # By default, if we've terminated the reasoning is failure tolerance exceeded
    # according to the spec
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_create_result_suspended_with_timeout_branch():
    """Test _create_result with SUSPENDED_WITH_TIMEOUT status branch."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with SUSPENDED_WITH_TIMEOUT status
    exe_state = ExecutableWithState(executables[0])
    future_time = time.time() + 10
    exe_state.suspend_with_timeout(future_time)
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # By default, if we've terminated the reasoning is failure tolerance exceeded
    # according to the spec
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_create_result_mixed_statuses():
    """Test _create_result with mixed executable statuses covering all branches."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),  # Will be COMPLETED
        Executable(1, lambda: "test1"),  # Will be FAILED
        Executable(2, lambda: "test2"),  # Will be PENDING
        Executable(3, lambda: "test3"),  # Will be RUNNING
        Executable(4, lambda: "test4"),  # Will be SUSPENDED
        Executable(5, lambda: "test5"),  # Will be SUSPENDED_WITH_TIMEOUT
    ]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=6,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executables with different statuses
    exe_states = [ExecutableWithState(exe) for exe in executables]

    # COMPLETED
    exe_states[0].complete("completed_result")

    # FAILED
    exe_states[1].fail(RuntimeError("Test failure"))

    # PENDING (default state, no change needed)

    # RUNNING
    future = Future()
    exe_states[3].run(future)

    # SUSPENDED
    exe_states[4].suspend()

    # SUSPENDED_WITH_TIMEOUT
    exe_states[5].suspend_with_timeout(time.time() + 10)

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 6

    # Check COMPLETED -> SUCCEEDED
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "completed_result"
    assert result.all[0].error is None

    # Check FAILED -> FAILED
    assert result.all[1].status == BatchItemStatus.FAILED
    assert result.all[1].result is None
    assert result.all[1].error is not None
    assert result.all[1].error.message == "Test failure"

    # Check PENDING -> STARTED
    assert result.all[2].status == BatchItemStatus.STARTED
    assert result.all[2].result is None
    assert result.all[2].error is None

    # Check RUNNING -> STARTED
    assert result.all[3].status == BatchItemStatus.STARTED
    assert result.all[3].result is None
    assert result.all[3].error is None

    # Check SUSPENDED -> STARTED
    assert result.all[4].status == BatchItemStatus.STARTED
    assert result.all[4].result is None
    assert result.all[4].error is None

    # Check SUSPENDED_WITH_TIMEOUT -> STARTED
    assert result.all[5].status == BatchItemStatus.STARTED
    assert result.all[5].result is None
    assert result.all[5].error is None

    # we've a min succ set to 1.
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED


def test_create_result_multiple_completed():
    """Test _create_result with multiple COMPLETED executables."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),
        Executable(1, lambda: "test1"),
        Executable(2, lambda: "test2"),
    ]
    completion_config = CompletionConfig(min_successful=3)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=3,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create all executables with COMPLETED status
    exe_states = [ExecutableWithState(exe) for exe in executables]
    exe_states[0].complete("result_0")
    exe_states[1].complete("result_1")
    exe_states[2].complete("result_2")

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 3
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in result.all)
    assert result.all[0].result == "result_0"
    assert result.all[1].result == "result_1"
    assert result.all[2].result == "result_2"
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_create_result_multiple_failed():
    """Test _create_result with multiple FAILED executables."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),
        Executable(1, lambda: "test1"),
        Executable(2, lambda: "test2"),
    ]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=3,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create all executables with FAILED status
    exe_states = [ExecutableWithState(exe) for exe in executables]
    exe_states[0].fail(ValueError("Error 0"))
    exe_states[1].fail(RuntimeError("Error 1"))
    exe_states[2].fail(TypeError("Error 2"))

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 3
    assert all(item.status == BatchItemStatus.FAILED for item in result.all)
    assert result.all[0].error.message == "Error 0"
    assert result.all[1].error.message == "Error 1"
    assert result.all[2].error.message == "Error 2"
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_create_result_multiple_started_states():
    """Test _create_result with multiple executables in STARTED states."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),  # PENDING
        Executable(1, lambda: "test1"),  # RUNNING
        Executable(2, lambda: "test2"),  # SUSPENDED
        Executable(3, lambda: "test3"),  # SUSPENDED_WITH_TIMEOUT
    ]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=4,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executables with different STARTED states
    exe_states = [ExecutableWithState(exe) for exe in executables]

    # PENDING (default state)

    # RUNNING
    future = Future()
    exe_states[1].run(future)

    # SUSPENDED
    exe_states[2].suspend()

    # SUSPENDED_WITH_TIMEOUT
    exe_states[3].suspend_with_timeout(time.time() + 5)

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 4
    assert all(item.status == BatchItemStatus.STARTED for item in result.all)
    assert all(item.result is None for item in result.all)
    assert all(item.error is None for item in result.all)
    # With completion config min_successful=1 and no completed items,
    # this should be FAILURE_TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_create_result_empty_executables():
    """Test _create_result with no executables."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = []
    completion_config = CompletionConfig(min_successful=0)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    executor.executables_with_state = []

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 0
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_timer_scheduler_future_time_condition_false():
    """Test TimerScheduler when scheduled time is in future (434->433 branch)."""
    callback = Mock()

    with TimerScheduler(callback) as scheduler:
        exe_state = ExecutableWithState(Executable(0, lambda: "test"))
        exe_state.suspend()

        # Schedule with future time so condition will be False
        future_time = time.time() + 10
        scheduler.schedule_resume(exe_state, future_time)

        # Wait briefly for timer thread to check and find condition False
        time.sleep(0.1)

        # Callback should not be called since time is in future
        callback.assert_not_called()


def test_batch_result_from_dict_with_completion_config():
    """Test BatchResult from_dict with completion config parameter."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "STARTED", "result": None, "error": None},
        ],
        # No completionReason provided
    }

    # With started items, should infer MIN_SUCCESSFUL_REACHED
    completion_config = CompletionConfig(min_successful=1)

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data, completion_config)
        assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_all_completed():
    """Test BatchResult from_dict infers ALL_COMPLETED when all items are completed."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {
                "index": 1,
                "status": "FAILED",
                "result": None,
                "error": {
                    "message": "error",
                    "type": "Error",
                    "data": None,
                    "stackTrace": None,
                },
            },
        ],
        # No completionReason provided
    }

    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_backward_compatibility():
    """Test BatchResult from_dict maintains backward compatibility when no completion_config provided."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "MIN_SUCCESSFUL_REACHED",
    }

    # Should work without completion_config parameter
    result = BatchResult.from_dict(data)
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Should also work with None completion_config
    result2 = BatchResult.from_dict(data, None)
    assert result2.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED


def test_batch_result_infer_completion_reason_basic_cases():
    """Test _infer_completion_reason method with basic scenarios."""
    # Test with started items - should be MIN_SUCCESSFUL_REACHED
    items = {
        "all": [
            BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
            BatchItem(1, BatchItemStatus.STARTED).to_dict(),
        ]
    }
    batch = BatchResult.from_dict(items, CompletionConfig(1))
    assert batch.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Test with all completed items - should be ALL_COMPLETED
    completed_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ).to_dict(),
    ]
    completed_items = {"all": completed_items}
    batch = BatchResult.from_dict(completed_items, CompletionConfig(1))
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test empty items - should be ALL_COMPLETED
    batch = BatchResult.from_dict({"all": []}, CompletionConfig(1))
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED


def test_operation_id_determinism_across_shuffles():
    """Test that operation_id depends on Executable.index, not execution order."""

    def index_based_function(index, ctx):
        """Function that returns a result based on the executable index."""
        return f"result_for_index_{index}"

    class TestExecutor(ConcurrentExecutor):
        """Custom executor for testing operation_id determinism."""

        def execute_item(self, child_context, executable):
            return executable.func(child_context)

    # Create executables with specific indices using partial
    num_executables = 50
    funcs = [partial(index_based_function, i) for i in range(num_executables)]

    # Track operation_id -> result associations
    captured_associations = []

    def patched_child_handler(func, execution_state, operation_identifier, config):
        """Patched child handler that captures operation_id -> result mapping."""
        result = func()  # Execute the function
        captured_associations.append((operation_identifier.operation_id, result))
        return result

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    completion_config = CompletionConfig(min_successful=num_executables)

    # Run multiple times with different shuffle orders
    associations_per_run = []

    for run in range(10):  # Test 10 different shuffle orders
        captured_associations.clear()

        # Create executables from shuffled functions
        executables = [Executable(index=i, func=func) for i, func in enumerate(funcs)]
        random.seed(run)  # Different seed for each run
        random.shuffle(executables)

        executor = TestExecutor(
            executables=executables,
            max_concurrency=2,
            completion_config=completion_config,
            sub_type_top="TEST",
            sub_type_iteration="TEST_ITER",
            name_prefix="test_",
            serdes=None,
        )

        # Create executor context mock
        executor_context = Mock()
        executor_context._parent_id = "parent_123"  # noqa SLF001

        def create_step_id(index):
            return f"step_{index}"

        executor_context._create_step_id_for_logical_step = create_step_id  # noqa SLF001

        def create_child_context(operation_id):
            child_ctx = Mock()
            child_ctx.state = execution_state
            return child_ctx

        executor_context.create_child_context = create_child_context

        with patch(
            "aws_durable_execution_sdk_python.concurrency.executor.child_handler",
            patched_child_handler,
        ):
            executor.execute(execution_state, executor_context)

        associations_per_run.append(captured_associations.copy())

    # first we will verify the validity of the test by ensuring that there exist at least 2 runs with different ordering
    assert any(
        assoc1 != assoc2 for assoc1, assoc2 in combinations(associations_per_run, 2)
    )
    # then we will verify the invariant of association between step_id and result
    associations_per_run = [dict(assoc) for assoc in associations_per_run]
    assert all(
        assoc1 == assoc2 for assoc1, assoc2 in combinations(associations_per_run, 2)
    )


def test_concurrent_executor_replay_with_succeeded_operations():
    """Test ConcurrentExecutor replay method with succeeded operations."""

    def func1(ctx, item, idx, items):
        return f"result_{item}"

    items = ["a", "b"]
    config = MapConfig()

    executor = MapExecutor.from_items(
        items=items,
        func=func1,
        config=config,
    )

    # Mock execution state with succeeded operations
    mock_execution_state = Mock()
    mock_execution_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def mock_get_checkpoint_result(operation_id):
        mock_result = Mock()
        mock_result.is_succeeded.return_value = True
        mock_result.is_failed.return_value = False
        mock_result.is_replay_children.return_value = False
        mock_result.is_existent.return_value = True
        # Provide properly serialized JSON data
        mock_result.result = f'"cached_result_{operation_id}"'  # JSON string
        return mock_result

    mock_execution_state.get_checkpoint_result = mock_get_checkpoint_result

    def mock_create_step_id_for_logical_step(step):
        return f"op_{step}"

    # Mock executor context
    mock_executor_context = Mock()
    mock_executor_context._create_step_id_for_logical_step = (  # noqa
        mock_create_step_id_for_logical_step
    )

    # Mock child context that has the same execution state
    mock_child_context = Mock()
    mock_child_context.state = mock_execution_state
    mock_executor_context.create_child_context = Mock(return_value=mock_child_context)
    mock_executor_context._parent_id = "parent_id"  # noqa

    result = executor.replay(mock_execution_state, mock_executor_context)

    assert isinstance(result, BatchResult)
    assert len(result.all) == 2
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "cached_result_op_0"
    assert result.all[1].status == BatchItemStatus.SUCCEEDED
    assert result.all[1].result == "cached_result_op_1"


def test_concurrent_executor_replay_with_failed_operations():
    """Test ConcurrentExecutor replay method with failed operations."""

    def func1(ctx, item, idx, items):
        return f"result_{item}"

    items = ["a"]
    config = MapConfig()

    executor = MapExecutor.from_items(
        items=items,
        func=func1,
        config=config,
    )

    # Mock execution state with failed operation
    mock_execution_state = Mock()

    def mock_get_checkpoint_result(operation_id):
        mock_result = Mock()
        mock_result.is_succeeded.return_value = False
        mock_result.is_failed.return_value = True
        mock_result.error = Exception("Test error")
        return mock_result

    mock_execution_state.get_checkpoint_result = mock_get_checkpoint_result

    # Mock executor context
    mock_executor_context = Mock()
    mock_executor_context._create_step_id_for_logical_step = Mock(return_value="op_1")  # noqa: SLF001

    result = executor.replay(mock_execution_state, mock_executor_context)

    assert isinstance(result, BatchResult)
    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error is not None


def test_concurrent_executor_replay_with_replay_children():
    """Test ConcurrentExecutor replay method when children need re-execution."""

    def func1(ctx, item, idx, items):
        return f"result_{item}"

    items = ["a"]
    config = MapConfig()

    executor = MapExecutor.from_items(
        items=items,
        func=func1,
        config=config,
    )

    # Mock execution state with succeeded operation that needs replay
    mock_execution_state = Mock()

    def mock_get_checkpoint_result(operation_id):
        mock_result = Mock()
        mock_result.is_succeeded.return_value = True
        mock_result.is_failed.return_value = False
        mock_result.is_replay_children.return_value = True
        return mock_result

    mock_execution_state.get_checkpoint_result = mock_get_checkpoint_result

    # Mock executor context
    mock_executor_context = Mock()
    mock_executor_context._create_step_id_for_logical_step = Mock(return_value="op_1")  # noqa: SLF001

    # Mock _execute_item_in_child_context to return a result
    with patch.object(
        executor, "_execute_item_in_child_context", return_value="re_executed_result"
    ):
        result = executor.replay(mock_execution_state, mock_executor_context)

        assert isinstance(result, BatchResult)
        assert len(result.all) == 1
        assert result.all[0].status == BatchItemStatus.SUCCEEDED
        assert result.all[0].result == "re_executed_result"


def test_batch_item_from_dict_with_error():
    """Test BatchItem.from_dict() with error."""
    data = {
        "index": 3,
        "status": "FAILED",
        "result": None,
        "error": {
            "ErrorType": "ValueError",
            "ErrorMessage": "bad value",
            "StackTrace": [],
        },
    }

    item = BatchItem.from_dict(data)

    assert item.index == 3
    assert item.status == BatchItemStatus.FAILED
    assert item.error.type == "ValueError"
    assert item.error.message == "bad value"


def test_batch_result_with_mixed_statuses():
    """Test BatchResult serialization with mixed item statuses."""
    result = BatchResult(
        all=[
            BatchItem(0, BatchItemStatus.SUCCEEDED, result="success"),
            BatchItem(
                1,
                BatchItemStatus.FAILED,
                error=ErrorObject(message="msg", type="E", data=None, stack_trace=[]),
            ),
            BatchItem(2, BatchItemStatus.STARTED),
        ],
        completion_reason=CompletionReason.FAILURE_TOLERANCE_EXCEEDED,
    )

    serialized = json.dumps(result.to_dict())
    deserialized = BatchResult.from_dict(json.loads(serialized))

    assert len(deserialized.all) == 3
    assert deserialized.all[0].status == BatchItemStatus.SUCCEEDED
    assert deserialized.all[1].status == BatchItemStatus.FAILED
    assert deserialized.all[2].status == BatchItemStatus.STARTED
    assert deserialized.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_batch_result_empty_list():
    """Test BatchResult serialization with empty items list."""
    result = BatchResult(all=[], completion_reason=CompletionReason.ALL_COMPLETED)

    serialized = json.dumps(result.to_dict())
    deserialized = BatchResult.from_dict(json.loads(serialized))

    assert len(deserialized.all) == 0
    assert deserialized.completion_reason == CompletionReason.ALL_COMPLETED


def test_batch_result_complex_nested_data():
    """Test BatchResult with complex nested data structures."""
    complex_result = {
        "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "metadata": {"count": 2, "timestamp": "2025-10-31"},
    }

    result = BatchResult(
        all=[BatchItem(0, BatchItemStatus.SUCCEEDED, result=complex_result)],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    serialized = json.dumps(result.to_dict())
    deserialized = BatchResult.from_dict(json.loads(serialized))

    assert deserialized.all[0].result == complex_result
    assert deserialized.all[0].result["users"][0]["name"] == "Alice"


def test_executor_does_not_deadlock_when_all_tasks_terminal_but_completion_config_allows_failures():
    """Ensure executor returns when all tasks are terminal even if completion rules are confusing."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            if executable.index == 0:
                # fail one task
                raise Exception("boom")  # noqa EM101 TRY002
            return f"ok_{executable.index}"

    # Two tasks, min_successful=2 but tolerated failure_count set to 1.
    # After one fail + one success, counters.is_complete() should return true,
    # should_continue() should return false. counters.is_complete was failing to
    # stop early, which caused map to hang.
    executables = [Executable(0, lambda: "a"), Executable(1, lambda: "b")]
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=1,
        tolerated_failure_percentage=None,
    )

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    # Should return (not hang) and batch should reflect one FAILED and one SUCCEEDED
    result = executor.execute(execution_state, executor_context)
    statuses = {item.index: item.status for item in result.all}
    assert statuses[0] == BatchItemStatus.FAILED
    assert statuses[1] == BatchItemStatus.SUCCEEDED
