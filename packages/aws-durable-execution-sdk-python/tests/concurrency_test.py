"""Tests for the concurrency module."""

import hashlib
import json
import queue
import random
import threading
import time
from collections.abc import Callable
from functools import partial
from itertools import combinations
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.concurrency.executor import (
    ConcurrentExecutor,
)
from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    Branch,
    BranchEventKind,
    BranchStatus,
    CompletionPolicy,
    CompletionRecord,
    CompletionReason,
    Executable,
    envelope_summary_generator,
)
from aws_durable_execution_sdk_python.config import (
    ChildConfig,
    CompletionConfig,
    MapConfig,
    ParallelBranch,
    ParallelConfig,
    NestingType,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    ExecutionContext,
)
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    ChildContextError,
    ValidationError,
    InvalidStateError,
    OrphanedChildException,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.identifier import OperationIdNamespace


from aws_durable_execution_sdk_python.operation.map import MapExecutor
from aws_durable_execution_sdk_python.operation.parallel import (
    ParallelExecutor,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult


class _StubNamespace(OperationIdNamespace):
    """Test namespace producing readable ids matching checkpoint fixtures."""

    def create_id_for_step(self, step: int) -> str:
        return f"op_{step}"


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

    # Has error - reconstructs the typed operation error from its discriminator
    error = ErrorObject("test message", "ChildContextError", None, None)
    items = [BatchItem(0, BatchItemStatus.FAILED, error=error)]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    with pytest.raises(ChildContextError, match="test message"):
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
    """Test BatchResult from_dict infers completion reason when all items failed."""
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

    # With no completion config and failures, should fail-fast
    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        mock_logger.warning.assert_called_once()


def test_batch_result_from_dict_infer_all_completed_mixed_success_failure():
    """Test BatchResult from_dict infers completion reason with mix of success/failure."""
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

    # With no config and with failures, fail-fast
    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED
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
    # Test with min_successful reached while other items are still started
    started_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok").to_dict(),
        BatchItem(1, BatchItemStatus.STARTED).to_dict(),
    ]
    items = {"all": started_items}
    batch = BatchResult.from_dict(items, CompletionConfig(1))  # SLF001
    # With min_successful=1 and one success, should be MIN_SUCCESSFUL_REACHED
    assert batch.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Test with only started items and no config
    started_items = [
        BatchItem(0, BatchItemStatus.STARTED).to_dict(),
        BatchItem(1, BatchItemStatus.STARTED).to_dict(),
    ]
    items = {"all": started_items}
    batch = BatchResult.from_dict(items)  # SLF001
    # With no config and no completed items, defaults to ALL_COMPLETED
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

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
    # With no config and failures, should fail-fast
    assert batch.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED

    # Test with only succeeded items
    succeeded_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
        BatchItem(1, BatchItemStatus.SUCCEEDED, "result2").to_dict(),
    ]
    succeeded_items = {"all": succeeded_items}
    batch = BatchResult.from_dict(succeeded_items)  # SLF001
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test with mixed but no started (all completed) with tolerance
    mixed_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]

    batch = BatchResult.from_items(
        mixed_items, CompletionConfig(tolerated_failure_count=1)
    )  # SLF001
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


def test_concurrent_executor_nesting_type_parameter():
    """Test ConcurrentExecutor nesting_type parameter."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    # Test with NESTED (default)
    executor_nested = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.NESTED,
        operation_id_namespace=_StubNamespace(),
    )
    assert executor_nested.nesting_type is NestingType.NESTED

    # Test with FLAT
    executor_flat = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.FLAT,
        operation_id_namespace=_StubNamespace(),
    )
    assert executor_flat.nesting_type is NestingType.FLAT


def test_concurrent_executor_default_nesting_type():
    """Test ConcurrentExecutor uses NESTED as default nesting_type."""

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
        operation_id_namespace=_StubNamespace(),
    )
    assert executor.nesting_type is NestingType.NESTED


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
        operation_id_namespace=_StubNamespace(),
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
        # The pool is registered so ExecutionState.close() joins branches
        # still running after early completion at invocation end.
        execution_state.register_branch_pool.assert_called_once()


def test_concurrent_executor_create_result_with_early_exit():
    """Test ConcurrentExecutor with failed branches using public execute method."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            if executable.index == 0:
                return f"result_{executable.index}"
            msg = "Test error"
            # giving space to terminate early with
            time.sleep(0.5)
            raise ValueError(msg)

    def success_callable():
        return "test"

    def failure_callable():
        return "test2"

    executables = [Executable(0, success_callable), Executable(1, failure_callable)]
    completion_config = CompletionConfig(
        # setting min successful to None to execute all children and avoid early stopping
        min_successful=None,
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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 2
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[1].status == BatchItemStatus.FAILED
    # NEW BEHAVIOR: With empty completion config (no criteria) and failures,
    # should fail-fast and return FAILURE_TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


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
        operation_id_namespace=_StubNamespace(),
    )

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    result = executor._execute_item_in_child_context(  # noqa: SLF001
        executor_context, executables[0]
    )
    assert result == "result_0"


def test_execute_item_brand_new_branch_during_replay_starts_new():
    """A brand-new branch during a map/parallel replay must start in NEW status.

    The branch container op is resolved via child_handler, bypassing the
    parent's _replay_aware existence flip. _execute_item_in_child_context must
    replicate that flip so logs before the branch's first inner op are not
    wrongly de-duplicated.
    """

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(
            min_successful=1,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
        ),
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.NESTED,
        operation_id_namespace=_StubNamespace(),
    )

    # Parent (executor) context is replaying.
    executor_context = Mock()
    executor_context.is_replaying = lambda: True
    executor_context._create_step_id_for_logical_step = lambda *args: "branch-1"
    executor_context._parent_id = "parent"  # noqa: SLF001

    # Brand-new branch: no checkpoint for the branch op.
    not_found = CheckpointedResult.create_not_found()
    child_context = Mock()
    child_context.state.get_checkpoint_result = lambda _id: not_found
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    executor._execute_item_in_child_context(executor_context, executables[0])  # noqa: SLF001

    # The brand-new branch's child context was flipped to NEW.
    child_context._set_replay_status_new.assert_called_once()  # noqa: SLF001


def test_execute_item_replayed_branch_emits_replay_hook():
    """A branch that already has a checkpoint emits the replay hook (once)."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(
            min_successful=1,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
        ),
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.NESTED,
        operation_id_namespace=_StubNamespace(),
    )

    executor_context = Mock()
    executor_context.is_replaying = lambda: True
    executor_context._create_step_id_for_logical_step = lambda *args: "branch-1"
    executor_context._parent_id = "parent"  # noqa: SLF001

    # Replayed branch: a terminal checkpoint exists for the branch op.
    branch_op = Operation(
        operation_id="branch-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        sub_type=OperationSubType.PARALLEL_BRANCH,
    )
    existing = CheckpointedResult.create_from_operation(branch_op)
    child_context = Mock()
    child_context.state.get_checkpoint_result = lambda _id: existing
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    executor._execute_item_in_child_context(executor_context, executables[0])  # noqa: SLF001

    child_context.state.emit_operation_replay_hook.assert_called_once_with(branch_op)
    child_context._set_replay_status_new.assert_not_called()  # noqa: SLF001


def test_execute_item_virtual_branch_skips_replay_status_handling():
    """FLAT (virtual) branches don't checkpoint, so no flip or hook is attempted."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(
            min_successful=1,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
        ),
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.FLAT,
        operation_id_namespace=_StubNamespace(),
    )

    executor_context = Mock()
    executor_context.is_replaying = lambda: True
    executor_context._create_step_id_for_logical_step = lambda *args: "branch-1"
    executor_context._parent_id = "parent"  # noqa: SLF001

    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    executor._execute_item_in_child_context(executor_context, executables[0])  # noqa: SLF001

    child_context.state.emit_operation_replay_hook.assert_not_called()
    child_context._set_replay_status_new.assert_not_called()  # noqa: SLF001


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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    def mock_run_in_child_context(func, name, config):
        return func(Mock())

    result = executor.execute(execution_state, mock_run_in_child_context)
    # NEW BEHAVIOR: With tolerated_failure_count=0 and 1 failure,
    # tolerance is exceeded, so FAILURE_TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    # Should raise TimedSuspendExecution since single task suspends
    with pytest.raises(TimedSuspendExecution):
        executor.execute(execution_state, executor_context)


def test_concurrent_executor_resume_checkpoint_failure_propagates():
    """A resume-time checkpoint refresh failure propagates out of execute().

    Regression guard: the timer resubmit does a blocking checkpoint refresh.
    That refresh only raises when the checkpoint subsystem has failed, which
    is terminal. execute() must re-raise it (so the invocation fails and the
    backend retries from the last durable checkpoint) rather than leave the
    wave PENDING forever - the completion wait has no timeout, so a stranded
    PENDING branch would hang the whole map.
    """

    class TestExecutor(ConcurrentExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.calls: dict[int, int] = {}
            self.long_runner_release = threading.Event()

        def execute_item(self, child_context, executable):
            task_id = executable.index
            self.calls[task_id] = self.calls.get(task_id, 0) + 1
            if task_id == 0:
                # Long-runner keeps the map alive so task 1 resumes in-process.
                self.long_runner_release.wait(timeout=5)
                return "result_A"
            # Task 1 suspends with a past timestamp -> immediate in-process resume.
            msg = "resume-me"
            raise TimedSuspendExecution(msg, time.time() - 1)

    executables = [Executable(0, lambda: "task_A"), Executable(1, lambda: "task_B")]
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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()

    def checkpoint(*args, **kwargs):
        # The resume refresh calls create_checkpoint() with no arguments.
        # Fail that call; leave the branches' own checkpoints as no-ops.
        if not args and not kwargs:
            msg = "resume refresh failed"
            raise RuntimeError(msg)

    execution_state.create_checkpoint = Mock(side_effect=checkpoint)

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    # Must re-raise (not hang): the resume failure surfaces as the original error.
    with pytest.raises(RuntimeError, match="resume refresh failed"):
        executor.execute(execution_state, executor_context)
    executor.long_runner_release.set()


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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error is not None
    assert result.all[0].error.message == "Test error"


def test_failed_branch_error_type_matches_across_execute_and_replay():
    """A failed branch's error_type must be identical on first run and replay.

    First run builds the item error in _create_result; replay reads the branch's
    FAIL checkpoint (which records the raw escaping type). Both must surface the
    raw type ("ValueError"), not the ChildContextError wrapper class name.
    """

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            msg = "Test error"
            raise ValueError(msg)

    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=0,
        tolerated_failure_percentage=None,
    )

    def make_executor():
        return TestExecutor(
            executables=[Executable(0, lambda: "test")],
            max_concurrency=1,
            completion_config=completion_config,
            sub_type_top="TOP",
            sub_type_iteration="ITER",
            name_prefix="test_",
            serdes=None,
            operation_id_namespace=_StubNamespace(),
        )

    # First run: execute() runs the branch and builds the result live.
    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    live_error = make_executor().execute(execution_state, executor_context).all[0].error
    assert live_error is not None

    # Replay: replay() reads the child's FAIL checkpoint, which stored the raw type.
    failed_checkpoint = Mock()
    failed_checkpoint.is_succeeded.return_value = False
    failed_checkpoint.is_failed.return_value = True
    failed_checkpoint.error = ErrorObject(
        message="Test error", type="ValueError", data=None, stack_trace=None
    )
    replay_state = Mock()
    replay_state.get_checkpoint_result = Mock(return_value=failed_checkpoint)
    replay_context = Mock()
    replay_context._create_step_id_for_logical_step = lambda *args: "1"

    replay_error = make_executor().replay(replay_state, replay_context).all[0].error
    assert replay_error is not None

    # Both paths must carry the same raw error_type.
    assert replay_error.type == "ValueError"
    assert live_error.type == replay_error.type


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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    result = executor.execute(execution_state, executor_context)

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error.message == "Task failed"


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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    executor_context.create_child_context = lambda *args, **kwargs: Mock()

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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    # Should raise SuspendExecution since single task suspends
    with pytest.raises(SuspendExecution):
        executor.execute(execution_state, executor_context)


# Tests for _create_result method match statement branches
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
    """Test BatchResult from_dict infers completion reason when all items are completed."""
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

    # With no config and failures, fail-fast
    with patch(
        "aws_durable_execution_sdk_python.concurrency.models.logger"
    ) as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED
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

    def patched_child_handler(
        func,
        execution_state,
        operation_identifier,
        config: ChildConfig,
    ):
        """Patched child handler that captures operation_id -> result mapping."""
        assert config.is_virtual
        assert config.sub_type == "TEST_ITER"
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
            nesting_type=NestingType.FLAT,
            operation_id_namespace=_StubNamespace(),
        )

        # Create executor context mock
        executor_context = Mock()
        executor_context._parent_id = "parent_123"  # noqa SLF001

        def create_step_id(index):
            return f"step_{index}"

        executor_context._create_step_id_for_logical_step = create_step_id

        def create_child_context(operation_id, *, is_virtual=False):
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
        operation_id_namespace=_StubNamespace(),
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
        operation_id_namespace=_StubNamespace(),
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
    mock_executor_context._create_step_id_for_logical_step = Mock(return_value="op_1")

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
        operation_id_namespace=_StubNamespace(),
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
    mock_executor_context._create_step_id_for_logical_step = Mock(return_value="op_1")

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
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    # Should return (not hang) and batch should reflect one FAILED and one SUCCEEDED
    result = executor.execute(execution_state, executor_context)
    statuses = {item.index: item.status for item in result.all}
    assert statuses[0] == BatchItemStatus.FAILED
    assert statuses[1] == BatchItemStatus.SUCCEEDED


def test_executor_terminates_quickly_when_impossible_to_succeed():
    """Test that executor terminates when min_successful becomes impossible."""
    executed_count = {"value": 0}

    def task_func(ctx, item, idx, items):
        executed_count["value"] += 1
        if idx < 2:
            raise Exception(f"fail_{idx}")  # noqa EM102 TRY002
        time.sleep(0.05)
        return f"ok_{idx}"

    items = list(range(100))
    config = MapConfig(
        max_concurrency=10,
        completion_config=CompletionConfig(
            min_successful=99, tolerated_failure_count=1
        ),
    )

    executor = MapExecutor.from_items(
        items=items,
        func=task_func,
        config=config,
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"
    child_context = Mock()
    child_context.state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    result = executor.execute(execution_state, executor_context)

    # With tolerated_failure_count=1, executor stops when failure_count > 1 (at 2 failures)
    # Executor terminates early rather than executing all 100 tasks
    assert executed_count["value"] < 100
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED, (
        executed_count
    )
    assert sum(1 for item in result.all if item.status == BatchItemStatus.FAILED) == 2
    assert (
        sum(1 for item in result.all if item.status == BatchItemStatus.SUCCEEDED) < 98
    )


def test_executor_exits_early_with_min_successful():
    """Test that parallel exits immediately when min_successful is reached without waiting for other branches."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return executable.func()

    execution_times = []

    def fast_branch():
        execution_times.append(("fast", time.time()))
        return "fast_result"

    def slow_branch():
        execution_times.append(("slow_start", time.time()))
        time.sleep(2)  # Long sleep
        execution_times.append(("slow_end", time.time()))
        return "slow_result"

    executables = [
        Executable(0, fast_branch),
        Executable(1, slow_branch),
    ]

    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda idx: f"step_{idx}"
    executor_context._parent_id = "parent"  # noqa: SLF001

    def create_child_context(op_id, *, is_virtual=False):
        child = Mock()
        child.state = execution_state
        child.state.wrap_user_function = lambda func, *args, **kwargs: func
        return child

    executor_context.create_child_context = create_child_context

    start_time = time.time()
    result = executor.execute(execution_state, executor_context)
    elapsed_time = time.time() - start_time

    # Should complete in less than 1.5 second (not wait for 2-second sleep)
    assert elapsed_time < 1.5, f"Took {elapsed_time}s, expected < 1.5s"

    # Result should show MIN_SUCCESSFUL_REACHED
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Fast branch should succeed
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "fast_result"

    # Slow branch should be marked as STARTED (incomplete)
    assert result.all[1].status == BatchItemStatus.STARTED

    # Verify counts
    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.started_count == 1
    assert result.total_count == 2


def test_executor_returns_with_incomplete_branches():
    """Test that executor returns when min_successful is reached, leaving other branches incomplete."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return executable.func()

    operation_tracker = Mock()

    def fast_branch():
        operation_tracker.fast_executed()
        return "fast_result"

    def slow_branch():
        operation_tracker.slow_started()
        time.sleep(2)  # Long sleep
        operation_tracker.slow_completed()
        return "slow_result"

    executables = [
        Executable(0, fast_branch),
        Executable(1, slow_branch),
    ]

    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    execution_state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda idx: f"step_{idx}"
    executor_context._parent_id = "parent"  # noqa: SLF001
    executor_context.create_child_context = lambda op_id, *, is_virtual=False: Mock(
        state=execution_state
    )

    result = executor.execute(execution_state, executor_context)

    # Verify fast branch executed
    assert operation_tracker.fast_executed.call_count == 1

    # Slow branch may or may not have started (depends on thread scheduling)
    # but it definitely should not have completed
    assert operation_tracker.slow_completed.call_count == 0, (
        "Executor should return before slow branch completes"
    )

    # Result should show MIN_SUCCESSFUL_REACHED
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Verify counts - one succeeded, one incomplete
    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.started_count == 1
    assert result.total_count == 2


def test_executor_returns_before_slow_branch_completes():
    """Test that executor returns immediately when min_successful is reached, not waiting for slow branches."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return executable.func()

    slow_branch_mock = Mock()

    def fast_func():
        return "fast"

    def slow_func():
        time.sleep(3)  # Sleep
        slow_branch_mock.completed()  # Should not be called before executor returns
        return "slow"

    executables = [Executable(0, fast_func), Executable(1, slow_func)]
    completion_config = CompletionConfig(min_successful=1)

    executor = TestExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        operation_id_namespace=_StubNamespace(),
    )

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    execution_state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda idx: f"step_{idx}"
    executor_context._parent_id = "parent"  # noqa: SLF001
    executor_context.create_child_context = lambda op_id, *, is_virtual=False: Mock(
        state=execution_state
    )

    result = executor.execute(execution_state, executor_context)

    # Executor should have returned before slow branch completed
    assert not slow_branch_mock.completed.called, (
        "Executor should return before slow branch completes"
    )

    # Result should show MIN_SUCCESSFUL_REACHED
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Verify counts
    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.started_count == 1
    assert result.total_count == 2


def test_from_items_no_config_with_failures():
    """Validates: Requirements 2.4 - Fail-fast with no config."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult.from_items(items, completion_config=None)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_from_items_empty_config_with_failures():
    """Validates: Requirements 2.5 - Fail-fast with empty config."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig()  # All fields None
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_from_items_tolerance_checked_before_all_completed():
    """Validates: Requirements 2.1, 2.2 - Tolerance priority."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            2, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(tolerated_failure_count=1)
    result = BatchResult.from_items(items, completion_config=config)
    # All completed but tolerance exceeded - should return TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_from_items_all_completed_within_tolerance():
    """Validates: Requirements 1.1 - All completed."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(tolerated_failure_count=1)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


def test_from_items_min_successful_reached():
    """Validates: Requirements 1.3 - Min successful."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(2, BatchItemStatus.STARTED),
    ]
    config = CompletionConfig(min_successful=2)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED


def test_from_items_tolerance_count_exceeded():
    """Validates: Requirements 1.2 - Tolerance count."""
    items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.STARTED),
    ]
    config = CompletionConfig(tolerated_failure_count=1)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_from_items_tolerance_percentage_exceeded():
    """Validates: Requirements 1.2 - Tolerance percentage."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            2, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            3, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(tolerated_failure_percentage=50.0)
    # 3 failures out of 4 = 75% > 50%
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_from_items_tolerance_priority_over_min_successful():
    """Validates: Requirements 2.3 - Tolerance takes precedence."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            2, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            3, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(min_successful=2, tolerated_failure_count=1)
    # Min successful reached (2) but tolerance exceeded (2 > 1)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_from_items_empty_array():
    """Validates: Edge case - empty items."""
    items = []
    result = BatchResult.from_items(items, completion_config=None)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED
    assert result.total_count == 0


def test_from_items_all_succeeded():
    """Validates: All items succeeded."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok1"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="ok2"),
    ]
    result = BatchResult.from_items(items, completion_config=None)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED
    assert result.success_count == 2


# endregion Completion Reason Inference Tests

# region Virtual-context wire-format tests


def test_flat_mode_stamps_grandparent_as_inner_op_parent_id():
    """In FLAT mode, inner operations in a branch stamp the map/parallel op id as parent_id.

    This is the core FLAT-mode invariant. Inner operations must not
    stamp the branch's own operation id (that would reproduce the
    NESTED hierarchy) — they must stamp the enclosing map/parallel op
    id, so the branch is collapsed out of the observable hierarchy
    even though it still exists as a logical scope for concurrency
    and step-id prefixing.

    The test drives `_execute_item_in_child_context` with a real
    non-virtual executor context, captures the child context the
    executor builds for the branch, and asserts the branch's
    `_parent_id` equals the executor_context's own `_parent_id`.
    """

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            # Record the child context we receive so the assertions below can
            # inspect its identity fields.
            self.last_child_context = child_context
            return executable.func(child_context)

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    execution_state.wrap_user_function = lambda func, *args, **kwargs: func

    # Mock out the checkpoint so the real child_handler reports "not
    # existent" (non-existent checkpoint -> normal execution path).
    mock_checkpoint = Mock()
    mock_checkpoint.is_succeeded.return_value = False
    mock_checkpoint.is_failed.return_value = False
    mock_checkpoint.is_existent.return_value = False
    mock_checkpoint.is_replay_children.return_value = False
    execution_state.get_checkpoint_result.return_value = mock_checkpoint

    # Build a real DurableContext that represents the map/parallel op.
    map_op_id = "map-op-id"
    execution_context = ExecutionContext(
        durable_execution_arn="arn:aws:durable:us-east-1:0:execution/test"
    )
    executor_context = DurableContext(
        state=execution_state,
        execution_context=execution_context,
        parent_id=map_op_id,  # This context *is* the map/parallel op.
    )

    executables = [Executable(index=0, func=lambda ctx: "ok")]
    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        sub_type_top="MAP",
        sub_type_iteration="MAP_ITER",
        name_prefix="branch-",
        serdes=None,
        nesting_type=NestingType.FLAT,
        operation_id_namespace=_StubNamespace(),
    )

    executor._execute_item_in_child_context(executor_context, executables[0])  # noqa: SLF001

    # The branch's child context must be virtual AND propagate the
    # map/parallel op id as its _parent_id. Inner operations stamping
    # self._parent_id will therefore report to the map/parallel op.
    branch_ctx = executor.last_child_context
    assert branch_ctx.is_virtual is True
    assert branch_ctx._parent_id == map_op_id  # noqa: SLF001
    # The step-id prefix is the branch's own operation id (stable replay id).
    assert branch_ctx._step_id_prefix != map_op_id  # noqa: SLF001


def test_nested_mode_stamps_branch_op_as_inner_op_parent_id():
    """In NESTED mode, inner operations in a branch stamp the branch's own operation id as parent_id."""

    class TestExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            self.last_child_context = child_context
            return executable.func(child_context)

    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    execution_state.wrap_user_function = lambda func, *args, **kwargs: func

    mock_checkpoint = Mock()
    mock_checkpoint.is_succeeded.return_value = False
    mock_checkpoint.is_failed.return_value = False
    mock_checkpoint.is_existent.return_value = False
    mock_checkpoint.is_replay_children.return_value = False
    execution_state.get_checkpoint_result.return_value = mock_checkpoint

    map_op_id = "map-op-id"
    execution_context = ExecutionContext(
        durable_execution_arn="arn:aws:durable:us-east-1:0:execution/test"
    )
    executor_context = DurableContext(
        state=execution_state,
        execution_context=execution_context,
        parent_id=map_op_id,
    )

    executables = [Executable(index=0, func=lambda ctx: "ok")]
    executor = TestExecutor(
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        sub_type_top="MAP",
        sub_type_iteration="MAP_ITER",
        name_prefix="branch-",
        serdes=None,
        nesting_type=NestingType.NESTED,
        operation_id_namespace=_StubNamespace(),
    )

    executor._execute_item_in_child_context(executor_context, executables[0])  # noqa: SLF001

    # In NESTED mode, the branch is a regular child — its _parent_id is
    # its own operation id, not the grandparent.
    branch_ctx = executor.last_child_context
    assert branch_ctx.is_virtual is False
    assert branch_ctx._parent_id == branch_ctx._step_id_prefix  # noqa: SLF001
    assert branch_ctx._parent_id != map_op_id  # noqa: SLF001


# endregion Virtual-context wire-format tests


def test_flat_mode_produces_deterministic_step_ids_across_runs():
    """Step ids and inner parent_ids must be deterministic under FLAT mode.

    Replay depends on regenerating the same operation ids for the same
    logical inputs. This test runs the same executor twice against
    fresh executor contexts and asserts that the resulting set of
    (step_id_prefix, parent_id) pairs is identical. Any source of
    non-determinism (e.g. step prefixes that depend on thread
    completion order, or parent-id propagation that's different on
    the second run) would show up here as a mismatch and would cause
    `NonDeterministicExecutionException` at replay time in production.
    """

    class TestExecutor(ConcurrentExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.captured = []

        def execute_item(self, child_context, executable):
            self.captured.append(
                (
                    child_context._step_id_prefix,  # noqa: SLF001
                    child_context._parent_id,  # noqa: SLF001
                )
            )
            return executable.func(child_context)

    def make_run():
        execution_state = Mock()
        execution_state.create_checkpoint = Mock()

        mock_checkpoint = Mock()
        mock_checkpoint.is_succeeded.return_value = False
        mock_checkpoint.is_failed.return_value = False
        mock_checkpoint.is_existent.return_value = False
        mock_checkpoint.is_replay_children.return_value = False
        execution_state.get_checkpoint_result.return_value = mock_checkpoint

        execution_context = ExecutionContext(
            durable_execution_arn="arn:aws:durable:us-east-1:0:execution/test"
        )
        executor_context = DurableContext(
            state=execution_state,
            execution_context=execution_context,
            parent_id="map-op-id",
        )

        executables = [
            Executable(index=i, func=lambda ctx, i=i: f"r{i}") for i in range(3)
        ]
        executor = TestExecutor(
            executables=executables,
            max_concurrency=3,
            completion_config=CompletionConfig(min_successful=3),
            sub_type_top="MAP",
            sub_type_iteration="MAP_ITER",
            name_prefix="branch-",
            serdes=None,
            nesting_type=NestingType.FLAT,
            operation_id_namespace=_StubNamespace(),
        )
        executor.execute(execution_state, executor_context)
        return executor.captured

    run_a = make_run()
    run_b = make_run()

    # Ordering of captured items is non-deterministic because branches run
    # on a ThreadPoolExecutor. What matters is that the SET of (prefix,
    # parent_id) pairs is identical across runs — i.e. replay reconstructs
    # the same branch identity regardless of completion order.
    assert sorted(run_a) == sorted(run_b), (
        "FLAT-mode branch step-id prefixes and parent_ids must be identical "
        f"across runs. Run A: {run_a!r}; Run B: {run_b!r}"
    )
    # Sanity: all branches reported grandparent (map op id) as their parent.
    assert all(parent_id == "map-op-id" for _prefix, parent_id in run_a)


# region Branch state transitions


def test_branch_initial_state():
    branch = Branch(Executable(3, lambda: "test"))
    assert branch.status is BranchStatus.PENDING
    assert branch.index == 3
    assert branch.result is None
    assert branch.error is None
    assert branch.resume_at is None


def test_branch_start_clears_resume_at():
    branch = Branch(Executable(0, lambda: "test"))
    branch.suspend_until(123.0)
    assert branch.status is BranchStatus.SUSPENDED_WITH_TIMEOUT
    assert branch.resume_at == 123.0
    branch.start()
    assert branch.status is BranchStatus.RUNNING
    assert branch.resume_at is None


def test_branch_complete_stores_result():
    branch = Branch(Executable(0, lambda: "test"))
    branch.start()
    branch.complete("result")
    assert branch.status is BranchStatus.COMPLETED
    assert branch.result == "result"


def test_branch_fail_stores_error():
    branch = Branch(Executable(0, lambda: "test"))
    branch.start()
    error = ValueError("boom")
    branch.fail(error)
    assert branch.status is BranchStatus.FAILED
    assert branch.error is error


def test_branch_suspend_not_terminal():
    branch = Branch(Executable(0, lambda: "test"))
    branch.start()
    branch.suspend()
    assert branch.status is BranchStatus.SUSPENDED
    assert branch.resume_at is None


def test_branch_suspend_until_not_terminal():
    branch = Branch(Executable(0, lambda: "test"))
    branch.start()
    branch.suspend_until(456.0)
    assert branch.status is BranchStatus.SUSPENDED_WITH_TIMEOUT
    assert branch.resume_at == 456.0


def test_branch_start_rejects_invalid_source_states():
    branch = Branch(Executable(0, lambda: "test"))
    branch.start()
    branch.complete("done")
    with pytest.raises(InvalidStateError, match="Cannot start branch 0"):
        branch.start()

    suspended = Branch(Executable(1, lambda: "test"))
    suspended.start()
    suspended.suspend()
    with pytest.raises(InvalidStateError, match="Cannot start branch 1"):
        suspended.start()


def test_branch_start_allows_timed_resume():
    branch = Branch(Executable(0, lambda: "test"))
    branch.start()
    branch.suspend_until(123.0)
    branch.start()
    assert branch.status is BranchStatus.RUNNING


def test_create_result_raises_on_failed_branch_without_error():
    executables = [Executable(0, lambda: "test")]
    executor = _make_executor(executables, max_concurrency=1)
    branch = Branch(executables[0])
    branch.status = BranchStatus.FAILED
    executor.branches = [branch]
    with pytest.raises(InvalidStateError, match="unexpected state"):
        executor._create_result()  # noqa: SLF001


# endregion Branch state transitions


# region CompletionPolicy


def test_completion_policy_from_config_none():
    policy = CompletionPolicy.from_config(5, None)
    assert policy.total == 5
    assert policy.min_successful is None
    assert policy.tolerated_failure_count is None
    assert policy.tolerated_failure_percentage is None
    assert not policy.has_criteria


def test_completion_policy_from_config_copies_fields():
    config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=3,
        tolerated_failure_percentage=50.0,
    )
    policy = CompletionPolicy.from_config(10, config)
    assert policy.total == 10
    assert policy.min_successful == 2
    assert policy.tolerated_failure_count == 3
    assert policy.tolerated_failure_percentage == 50.0
    assert policy.has_criteria


def test_completion_policy_fail_fast_without_criteria():
    policy = CompletionPolicy.from_config(5, CompletionConfig())
    assert policy.should_continue(failed=0)
    assert not policy.should_continue(failed=1)
    assert policy.is_tolerance_exceeded(failed=1)


def test_completion_policy_min_successful_only_does_not_fail_fast():
    policy = CompletionPolicy.from_config(5, CompletionConfig(min_successful=2))
    assert policy.should_continue(failed=1)
    assert policy.should_continue(failed=4)


def test_completion_policy_tolerated_failure_count_boundary():
    policy = CompletionPolicy.from_config(
        10, CompletionConfig(tolerated_failure_count=2)
    )
    assert policy.should_continue(failed=2)
    assert not policy.should_continue(failed=3)


def test_completion_policy_tolerated_failure_percentage_boundary():
    policy = CompletionPolicy.from_config(
        10, CompletionConfig(tolerated_failure_percentage=30)
    )
    assert policy.should_continue(failed=3)
    assert not policy.should_continue(failed=4)


def test_completion_policy_percentage_with_zero_total():
    policy = CompletionPolicy.from_config(
        0, CompletionConfig(tolerated_failure_percentage=30)
    )
    assert not policy.is_tolerance_exceeded(failed=0)


def test_completion_policy_is_complete_all_done():
    policy = CompletionPolicy.from_config(3, CompletionConfig())
    assert not policy.is_complete(succeeded=1, failed=1)
    assert policy.is_complete(succeeded=2, failed=1)


def test_completion_policy_is_complete_min_successful():
    policy = CompletionPolicy.from_config(5, CompletionConfig(min_successful=2))
    assert not policy.is_complete(succeeded=1, failed=0)
    assert policy.is_complete(succeeded=2, failed=0)


def test_completion_policy_reason_tolerance_checked_first():
    policy = CompletionPolicy.from_config(
        2, CompletionConfig(tolerated_failure_count=0)
    )
    assert (
        policy.reason(succeeded=1, failed=1)
        is CompletionReason.FAILURE_TOLERANCE_EXCEEDED
    )


def test_completion_policy_reason_all_completed():
    policy = CompletionPolicy.from_config(
        2, CompletionConfig(tolerated_failure_count=1)
    )
    assert policy.reason(succeeded=1, failed=1) is CompletionReason.ALL_COMPLETED


def test_completion_policy_reason_min_successful():
    policy = CompletionPolicy.from_config(5, CompletionConfig(min_successful=2))
    assert (
        policy.reason(succeeded=2, failed=0) is CompletionReason.MIN_SUCCESSFUL_REACHED
    )


def test_completion_policy_reason_defaults_to_all_completed():
    policy = CompletionPolicy.from_config(5, CompletionConfig(min_successful=3))
    assert policy.reason(succeeded=1, failed=0) is CompletionReason.ALL_COMPLETED


# endregion CompletionPolicy


# region In-flight window semantics


def _make_executor_mocks():
    """Build the execution_state / executor_context mock pair used by executor tests."""
    execution_state = Mock()
    execution_state.create_checkpoint = Mock()
    execution_state.wrap_user_function = lambda func, *args, **kwargs: func
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda idx: f"step_{idx}"
    executor_context._parent_id = "parent"  # noqa: SLF001
    executor_context.create_child_context = lambda op_id, *, is_virtual=False: Mock(
        state=execution_state
    )
    return execution_state, executor_context


class _RecordingExecutor(ConcurrentExecutor):
    """Executor whose items delegate to the Executable's own callable."""

    def execute_item(self, child_context, executable):
        return executable.func()


def _make_executor(executables, max_concurrency, completion_config=None):
    return _RecordingExecutor(
        executables=executables,
        max_concurrency=max_concurrency,
        completion_config=completion_config or CompletionConfig(),
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        operation_id_namespace=_StubNamespace(),
    )


def test_max_concurrency_bounds_simultaneous_execution():
    """At most max_concurrency items execute simultaneously."""
    lock = threading.Lock()
    active = 0
    peak = 0

    def tracked():
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return "ok"

    executables = [Executable(i, tracked) for i in range(6)]
    executor = _make_executor(executables, max_concurrency=2)
    execution_state, executor_context = _make_executor_mocks()

    result = executor.execute(execution_state, executor_context)

    assert peak <= 2
    assert result.success_count == 6
    assert result.completion_reason is CompletionReason.ALL_COMPLETED


def test_suspended_branch_holds_concurrency_slot():
    """A suspended branch keeps its slot: pending items must not start (issue #279)."""
    started: list[int] = []

    def suspending(index: int):
        started.append(index)
        msg = "awaiting callback"
        raise SuspendExecution(msg)

    executables = [
        Executable(0, partial(suspending, 0)),
        Executable(1, partial(suspending, 1)),
        Executable(2, partial(suspending, 2)),
    ]
    executor = _make_executor(executables, max_concurrency=2)
    execution_state, executor_context = _make_executor_mocks()

    with pytest.raises(SuspendExecution):
        executor.execute(execution_state, executor_context)

    assert sorted(started) == [0, 1]


def test_slot_freed_on_completion_admits_next_item():
    """A terminal completion frees a slot; a suspension does not."""
    started: list[int] = []

    def completing(index: int):
        started.append(index)
        return f"result_{index}"

    def suspending(index: int):
        started.append(index)
        msg = "awaiting callback"
        raise SuspendExecution(msg)

    executables = [
        Executable(0, partial(completing, 0)),
        Executable(1, partial(suspending, 1)),
        Executable(2, partial(completing, 2)),
    ]
    executor = _make_executor(executables, max_concurrency=2)
    execution_state, executor_context = _make_executor_mocks()

    with pytest.raises(SuspendExecution):
        executor.execute(execution_state, executor_context)

    assert sorted(started) == [0, 1, 2]


def test_branches_start_in_index_order():
    started: list[int] = []

    def record(index: int):
        started.append(index)
        return index

    executables = [Executable(i, partial(record, i)) for i in range(4)]
    executor = _make_executor(executables, max_concurrency=1)
    execution_state, executor_context = _make_executor_mocks()

    result = executor.execute(execution_state, executor_context)

    assert started == [0, 1, 2, 3]
    assert result.success_count == 4


def test_timed_suspend_resumes_in_process_while_sibling_runs():
    """A due timed suspend is resumed in-process while another branch is running."""
    resumed = threading.Event()
    call_counts = {0: 0}

    def timed_then_ok():
        call_counts[0] += 1
        if call_counts[0] == 1:
            msg = "retry shortly"
            raise TimedSuspendExecution(msg, time.time() + 0.3)
        resumed.set()
        return "ok_after_resume"

    def slow_sibling():
        resumed.wait(timeout=5.0)
        return "sibling"

    executables = [Executable(0, timed_then_ok), Executable(1, slow_sibling)]
    executor = _make_executor(executables, max_concurrency=2)
    execution_state, executor_context = _make_executor_mocks()

    result = executor.execute(execution_state, executor_context)

    assert call_counts[0] == 2
    assert result.success_count == 2
    assert result.completion_reason is CompletionReason.ALL_COMPLETED
    # A resume wave refreshes state once before resubmitting.
    execution_state.create_checkpoint.assert_called()


def test_all_timed_suspended_parent_suspends_with_earliest_timestamp():
    earliest = time.time() + 100
    latest = time.time() + 200

    def suspend_at(timestamp: float):
        msg = "retry later"
        raise TimedSuspendExecution(msg, timestamp)

    executables = [
        Executable(0, partial(suspend_at, latest)),
        Executable(1, partial(suspend_at, earliest)),
    ]
    executor = _make_executor(executables, max_concurrency=2)
    execution_state, executor_context = _make_executor_mocks()

    with pytest.raises(TimedSuspendExecution) as exc_info:
        executor.execute(execution_state, executor_context)

    assert exc_info.value.scheduled_timestamp == earliest


def test_never_started_branches_omitted_from_result():
    """Branches that never started are omitted from the batch result (JS parity)."""
    release = threading.Event()

    def fast():
        return "fast"

    def slow():
        release.wait(timeout=5.0)
        return "slow"

    executables = [
        Executable(0, fast),
        Executable(1, slow),
        Executable(2, fast),
        Executable(3, fast),
    ]
    executor = _make_executor(
        executables,
        max_concurrency=2,
        completion_config=CompletionConfig(min_successful=1),
    )
    execution_state, executor_context = _make_executor_mocks()

    try:
        result = executor.execute(execution_state, executor_context)
    finally:
        release.set()

    assert result.completion_reason is CompletionReason.MIN_SUCCESSFUL_REACHED
    assert result.success_count == 1
    assert result.started_count == 1
    assert result.total_count == 2
    indexes = {item.index for item in result.all}
    assert indexes == {0, 1}


def test_tolerance_breach_stops_submission_of_pending_items():
    """Fail-fast: a failure with no completion criteria stops further submissions."""
    started: list[int] = []

    def failing(index: int):
        started.append(index)
        msg = "boom"
        raise ValueError(msg)

    executables = [Executable(i, partial(failing, i)) for i in range(4)]
    executor = _make_executor(executables, max_concurrency=1)
    execution_state, executor_context = _make_executor_mocks()

    result = executor.execute(execution_state, executor_context)

    assert started == [0]
    assert result.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED
    assert result.failure_count == 1
    assert result.total_count == 1


def test_orphaned_branch_stops_scheduling():
    """An orphaned branch means an ancestor completed: stop starting new work."""
    started: list[int] = []

    def orphaned(index: int):
        started.append(index)
        msg = "parent done"
        raise OrphanedChildException(msg, operation_id=f"step_{index}")

    executables = [Executable(i, partial(orphaned, i)) for i in range(4)]
    executor = _make_executor(executables, max_concurrency=1)
    execution_state, executor_context = _make_executor_mocks()

    result = executor.execute(execution_state, executor_context)

    assert started == [0]
    assert result.started_count == 1
    assert result.total_count == 1


def test_branch_worker_maps_outcomes_to_events():
    """_branch_worker converts each branch outcome into the matching event."""
    outcomes: dict[int, Callable] = {
        0: lambda: "done",
        1: lambda: (_ for _ in ()).throw(ValueError("boom")),
        2: lambda: (_ for _ in ()).throw(SuspendExecution("cb")),
        3: lambda: (_ for _ in ()).throw(TimedSuspendExecution("retry", 1234.5)),
        4: lambda: (_ for _ in ()).throw(
            OrphanedChildException("parent done", operation_id="step_4")
        ),
    }

    class OutcomeExecutor(ConcurrentExecutor):
        def execute_item(self, child_context, executable):
            return outcomes[executable.index]()

    executables = [Executable(i, outcomes[i]) for i in range(5)]
    execution_state, executor_context = _make_executor_mocks()
    executor = OutcomeExecutor(
        executables=executables,
        max_concurrency=5,
        completion_config=CompletionConfig(),
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        operation_id_namespace=_StubNamespace(),
    )

    events: queue.Queue = queue.Queue()
    for executable in executables:
        executor._branch_worker(executor_context, events, executable)  # noqa: SLF001

    collected = {}
    while not events.empty():
        event = events.get_nowait()
        collected[event.index] = event

    assert collected[0].kind is BranchEventKind.COMPLETED
    assert collected[0].result == "done"
    assert collected[1].kind is BranchEventKind.FAILED
    # child_handler wraps user exceptions in ChildContextError before
    # they reach the worker.
    assert isinstance(collected[1].error, ChildContextError)
    assert collected[2].kind is BranchEventKind.SUSPENDED
    assert collected[3].kind is BranchEventKind.SUSPENDED_UNTIL
    assert collected[3].resume_at == 1234.5
    assert collected[4].kind is BranchEventKind.ORPHANED


# region Completion record and replay reconstruction


def test_completion_record_from_summary_payload_started_indexes():
    payload = json.dumps(
        {
            "totalCount": 3,
            "completionReason": "MIN_SUCCESSFUL_REACHED",
            "startedIndexes": [1],
        }
    )
    record = CompletionRecord.from_summary_payload(payload)
    assert record is not None
    assert record.completion_reason is CompletionReason.MIN_SUCCESSFUL_REACHED
    assert record.started_total == 3
    assert record.started_indexes == frozenset({1})


def test_completion_record_from_summary_payload_completed_indexes():
    payload = json.dumps(
        {
            "totalCount": 4,
            "completionReason": "ALL_COMPLETED",
            "completedIndexes": [0, 2],
        }
    )
    record = CompletionRecord.from_summary_payload(payload)
    assert record is not None
    assert record.started_indexes == frozenset({1, 3})


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "",
        "not json",
        '"a json string"',
        json.dumps({"totalCount": 3, "completionReason": "MIN_SUCCESSFUL_REACHED"}),
        json.dumps({"totalCount": 3, "startedIndexes": [1]}),
        json.dumps(
            {
                "totalCount": 3,
                "completionReason": "SOME_FUTURE_REASON",
                "startedIndexes": [1],
            }
        ),
        json.dumps(
            {
                "totalCount": "3",
                "completionReason": "ALL_COMPLETED",
                "startedIndexes": [1],
            }
        ),
    ],
)
def test_completion_record_rejects_payloads_without_record(payload):
    assert CompletionRecord.from_summary_payload(payload) is None


def test_summary_index_fields_records_smaller_side():
    few_started = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="a"),
        BatchItem(1, BatchItemStatus.STARTED),
        BatchItem(2, BatchItemStatus.SUCCEEDED, result="b"),
    ]
    assert CompletionRecord.summary_index_fields(few_started) == {"startedIndexes": [1]}

    few_completed = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="a"),
        BatchItem(1, BatchItemStatus.STARTED),
        BatchItem(2, BatchItemStatus.STARTED),
    ]
    assert CompletionRecord.summary_index_fields(few_completed) == {
        "completedIndexes": [0]
    }


def _make_replay_mocks(branch_checkpoints: dict):
    """Mocks for replay: op id 'op_{index}', checkpoints per index."""
    execution_state = Mock()
    execution_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def get_checkpoint_result(operation_id: str):
        checkpoint = branch_checkpoints.get(operation_id)
        if checkpoint is not None:
            return checkpoint
        absent = Mock()
        absent.is_succeeded.return_value = False
        absent.is_failed.return_value = False
        absent.is_existent.return_value = False
        absent.is_replay_children.return_value = False
        return absent

    execution_state.get_checkpoint_result = get_checkpoint_result
    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda idx: f"op_{idx}"
    executor_context._parent_id = "parent"  # noqa: SLF001
    child_context = Mock()
    child_context.state = execution_state
    executor_context.create_child_context = Mock(return_value=child_context)
    return execution_state, executor_context


def _succeeded_checkpoint(serialized_result: str):
    checkpoint = Mock()
    checkpoint.is_succeeded.return_value = True
    checkpoint.is_failed.return_value = False
    checkpoint.is_existent.return_value = True
    checkpoint.is_replay_children.return_value = False
    checkpoint.result = serialized_result
    return checkpoint


def test_replay_round_trip_matches_live_result():
    """execute() -> summary -> replay() reconstructs the exact live result."""

    def succeed(index: int):
        return f"live_{index}"

    def suspend():
        msg = "awaiting callback"
        raise SuspendExecution(msg)

    executables = [
        Executable(0, partial(succeed, 0)),
        Executable(1, suspend),
        Executable(2, partial(succeed, 2)),
        Executable(3, partial(succeed, 3)),
    ]
    executor = _make_executor(
        executables,
        max_concurrency=2,
        completion_config=CompletionConfig(min_successful=2),
    )
    execution_state, executor_context = _make_executor_mocks()
    live: BatchResult = executor.execute(execution_state, executor_context)

    assert [(i.index, i.status) for i in live.all] == [
        (0, BatchItemStatus.SUCCEEDED),
        (1, BatchItemStatus.STARTED),
        (2, BatchItemStatus.SUCCEEDED),
    ]
    assert live.completion_reason is CompletionReason.MIN_SUCCESSFUL_REACHED

    summary: str = envelope_summary_generator("MapResult", None)(live)
    top_checkpoint = Mock()
    top_checkpoint.result = summary

    # Branch 1's checkpoint claims SUCCEEDED: the recorded STARTED must win
    # (a terminal checkpoint can land after the completion decision).
    replay_state, replay_context = _make_replay_mocks(
        {
            "op_0": _succeeded_checkpoint('"replayed_0"'),
            "op_1": _succeeded_checkpoint('"raced_1"'),
            "op_2": _succeeded_checkpoint('"replayed_2"'),
        }
    )
    replay_executor = _make_executor(
        executables,
        max_concurrency=2,
        completion_config=CompletionConfig(min_successful=2),
    )
    replayed: BatchResult = replay_executor.replay(
        replay_state, replay_context, top_checkpoint
    )

    assert [(i.index, i.status) for i in replayed.all] == [
        (i.index, i.status) for i in live.all
    ]
    assert replayed.completion_reason is live.completion_reason
    assert replayed.all[0].result == "replayed_0"
    assert replayed.all[1].result is None
    assert replayed.all[2].result == "replayed_2"


def test_replay_without_record_falls_back_to_checkpoint_derivation():
    """No record: every executable is represented, old-style."""
    executables = [Executable(i, lambda: "x") for i in range(3)]
    executor = _make_executor(executables, max_concurrency=2)
    replay_state, replay_context = _make_replay_mocks(
        {"op_0": _succeeded_checkpoint('"done_0"')}
    )

    replayed: BatchResult = executor.replay(replay_state, replay_context)

    assert [(i.index, i.status) for i in replayed.all] == [
        (0, BatchItemStatus.SUCCEEDED),
        (1, BatchItemStatus.STARTED),
        (2, BatchItemStatus.STARTED),
    ]


def test_replay_summary_without_index_keys_falls_back():
    """Summaries written before the record existed derive from checkpoints."""
    executables = [Executable(i, lambda: "x") for i in range(2)]
    executor = _make_executor(executables, max_concurrency=2)
    top_checkpoint = Mock()
    top_checkpoint.result = json.dumps(
        {
            "totalCount": 2,
            "successCount": 2,
            "failureCount": 0,
            "completionReason": "ALL_COMPLETED",
            "status": "SUCCEEDED",
            "type": "MapResult",
        }
    )
    replay_state, replay_context = _make_replay_mocks(
        {
            "op_0": _succeeded_checkpoint('"done_0"'),
            "op_1": _succeeded_checkpoint('"done_1"'),
        }
    )

    replayed: BatchResult = executor.replay(
        replay_state, replay_context, top_checkpoint
    )

    assert [(i.index, i.status) for i in replayed.all] == [
        (0, BatchItemStatus.SUCCEEDED),
        (1, BatchItemStatus.SUCCEEDED),
    ]


def test_replay_recorded_terminal_with_missing_checkpoint_is_started():
    """Nested branch recorded terminal but checkpoint absent: conservative STARTED."""
    executables = [Executable(0, lambda: "x")]
    executor = _make_executor(executables, max_concurrency=1)
    top_checkpoint = Mock()
    top_checkpoint.result = json.dumps(
        {
            "totalCount": 1,
            "completionReason": "ALL_COMPLETED",
            "startedIndexes": [],
        }
    )
    replay_state, replay_context = _make_replay_mocks({})

    replayed: BatchResult = executor.replay(
        replay_state, replay_context, top_checkpoint
    )

    assert [(i.index, i.status) for i in replayed.all] == [(0, BatchItemStatus.STARTED)]
    assert replayed.completion_reason is CompletionReason.ALL_COMPLETED


def test_replay_flat_reconstructs_terminal_statuses_by_reexecution():
    """FLAT branches have no checkpoints: re-execution discriminates outcomes."""

    def succeed():
        return "flat_ok"

    def fail():
        msg = "flat boom"
        raise ValueError(msg)

    executables = [Executable(0, succeed), Executable(1, fail)]
    executor = _RecordingExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=CompletionConfig(
            tolerated_failure_count=1,
        ),
        sub_type_top="TOP",
        sub_type_iteration="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.FLAT,
        operation_id_namespace=_StubNamespace(),
    )
    top_checkpoint = Mock()
    top_checkpoint.result = json.dumps(
        {
            "totalCount": 2,
            "completionReason": "ALL_COMPLETED",
            "startedIndexes": [],
        }
    )
    replay_state, replay_context = _make_replay_mocks({})
    replay_state.wrap_user_function = lambda func, *args, **kwargs: func

    replayed: BatchResult = executor.replay(
        replay_state, replay_context, top_checkpoint
    )

    assert [(i.index, i.status) for i in replayed.all] == [
        (0, BatchItemStatus.SUCCEEDED),
        (1, BatchItemStatus.FAILED),
    ]
    assert replayed.all[0].result == "flat_ok"
    assert replayed.all[1].error is not None
    assert replayed.all[1].error.type == "ValueError"
    assert replayed.completion_reason is CompletionReason.ALL_COMPLETED


# endregion Completion record and replay reconstruction


def test_create_result_uses_captured_completion_reason():
    """The reason captured at the completion decision wins over recount."""
    executables = [Executable(0, lambda: "x"), Executable(1, lambda: "y")]
    executor = _make_executor(executables, max_concurrency=2)
    completed = Branch(executables[0])
    completed.start()
    completed.complete("done")
    failed = Branch(executables[1])
    failed.start()
    failed.fail(ValueError("boom"))
    executor.branches = [completed, failed]

    result: BatchResult = executor._create_result(  # noqa: SLF001
        CompletionReason.MIN_SUCCESSFUL_REACHED
    )

    assert result.completion_reason is CompletionReason.MIN_SUCCESSFUL_REACHED


def test_branch_base_exception_propagates_to_caller():
    """A BaseException in a branch propagates from execute(): no hang, no conversion."""

    def exits():
        raise SystemExit(3)

    executables = [Executable(0, exits)]
    executor = _make_executor(executables, max_concurrency=1)
    execution_state, executor_context = _make_executor_mocks()

    with pytest.raises(SystemExit):
        executor.execute(execution_state, executor_context)


def test_background_thread_error_propagates_to_caller():
    """A fatal checkpoint-subsystem failure in a branch reaches the calling thread.

    BackgroundThreadError must not be recorded as an ordinary branch failure:
    tolerance logic must not absorb it and the parent must not attempt
    further checkpoints.
    """

    def checkpoint_dead():
        msg = "background checkpoint thread failed"
        raise BackgroundThreadError(msg, RuntimeError("worker died"))

    executables = [Executable(0, checkpoint_dead), Executable(1, lambda: "ok")]
    executor = _make_executor(
        executables,
        max_concurrency=2,
        completion_config=CompletionConfig(tolerated_failure_count=5),
    )
    execution_state, executor_context = _make_executor_mocks()

    with pytest.raises(BackgroundThreadError):
        executor.execute(execution_state, executor_context)


def test_unlimited_concurrency_starts_all_items():
    """max_concurrency=None keeps the previous start-everything behavior."""
    barrier = threading.Barrier(3, timeout=5.0)

    def synchronized():
        barrier.wait()
        return "ok"

    executables = [Executable(i, synchronized) for i in range(3)]
    executor = _make_executor(executables, max_concurrency=None)
    execution_state, executor_context = _make_executor_mocks()

    result = executor.execute(execution_state, executor_context)

    assert result.success_count == 3


# endregion In-flight window semantics


# region summary envelope


def _live_result_for_envelope() -> BatchResult:
    return BatchResult(
        [
            BatchItem(0, BatchItemStatus.SUCCEEDED, "r0"),
            BatchItem(1, BatchItemStatus.STARTED),
        ],
        CompletionReason.MIN_SUCCESSFUL_REACHED,
    )


def test_envelope_default_fields_and_order():
    """Without a custom generator the envelope has record + view fields, no summary."""
    payload = envelope_summary_generator("MapResult", None)(_live_result_for_envelope())
    data = json.loads(payload)
    assert data == {
        "type": "MapResult",
        "totalCount": 2,
        "completionReason": "MIN_SUCCESSFUL_REACHED",
        "startedIndexes": [1],
        "startedCount": 1,
        "successCount": 1,
        "failureCount": 0,
        "status": "SUCCEEDED",
    }
    assert list(data.keys())[:3] == ["type", "totalCount", "completionReason"]


def test_envelope_preserves_custom_summary_verbatim():
    """The customer generator output is stored verbatim under 'summary'."""
    custom = "plain text, not JSON: 100% <done>"
    payload = envelope_summary_generator("ParallelResult", lambda _r: custom)(
        _live_result_for_envelope()
    )
    data = json.loads(payload)
    assert data["summary"] == custom
    assert data["type"] == "ParallelResult"
    record = CompletionRecord.from_summary_payload(payload)
    assert record is not None
    assert record.started_total == 2
    assert record.started_indexes == frozenset({1})


def test_envelope_coerces_non_string_summary():
    """A generator returning a non-str is coerced so the payload stays a flat envelope."""
    payload = envelope_summary_generator("MapResult", lambda _r: {"not": "a str"})(
        _live_result_for_envelope()
    )
    data = json.loads(payload)
    assert isinstance(data["summary"], str)


def test_envelope_propagates_raising_generator():
    """An exception from the customer generator propagates to the caller."""

    def boom(_result):
        msg = "customer bug"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="customer bug"):
        envelope_summary_generator("MapResult", boom)(_live_result_for_envelope())


def test_envelope_never_truncates_summary():
    """The customer summary is checkpointed as provided regardless of size."""
    big = "x" * 500_000
    payload = envelope_summary_generator("MapResult", lambda _r: big)(
        _live_result_for_envelope()
    )
    data = json.loads(payload)
    assert data["summary"] == big


def test_replay_reads_record_from_envelope_with_custom_summary():
    """Replay obeys the record even when a custom summary is present."""
    executables = [Executable(0, lambda: "x"), Executable(1, lambda: "x")]
    live = _live_result_for_envelope()
    payload = envelope_summary_generator("MapResult", lambda _r: "digest")(live)
    top_checkpoint = Mock()
    top_checkpoint.result = payload

    replay_state, replay_context = _make_replay_mocks(
        {"op_0": _succeeded_checkpoint('"replayed_0"')}
    )
    replayed = _make_executor(
        executables,
        max_concurrency=2,
        completion_config=CompletionConfig(min_successful=1),
    ).replay(replay_state, replay_context, top_checkpoint)

    assert [(i.index, i.status) for i in replayed.all] == [
        (0, BatchItemStatus.SUCCEEDED),
        (1, BatchItemStatus.STARTED),
    ]
    assert replayed.completion_reason is CompletionReason.MIN_SUCCESSFUL_REACHED


def test_record_parses_pre_envelope_flat_format():
    """Payloads written before the envelope (record keys at top level) still parse."""
    flat = json.dumps(
        {
            "totalCount": 3,
            "successCount": 2,
            "failureCount": 0,
            "completionReason": "MIN_SUCCESSFUL_REACHED",
            "status": "SUCCEEDED",
            "type": "MapResult",
            "startedIndexes": [2],
        }
    )
    record = CompletionRecord.from_summary_payload(flat)
    assert record is not None
    assert record.started_total == 3
    assert record.started_indexes == frozenset({2})


@pytest.mark.parametrize(
    "payload",
    [
        '{"totalCount": 3, "completionReason": "ALL_COMPLETED", "startedIndexes": [[]]}',
        '{"totalCount": 3, "completionReason": "ALL_COMPLETED", "startedIndexes": ["1"]}',
        '{"totalCount": 3, "completionReason": "ALL_COMPLETED", "startedIndexes": [true]}',
        '{"totalCount": 3, "completionReason": "ALL_COMPLETED", "startedIndexes": [3]}',
        '{"totalCount": 3, "completionReason": "ALL_COMPLETED", "startedIndexes": [-1]}',
        '{"totalCount": 3, "completionReason": "ALL_COMPLETED", "completedIndexes": [null]}',
        '{"totalCount": true, "completionReason": "ALL_COMPLETED", "startedIndexes": []}',
        '{"totalCount": -1, "completionReason": "ALL_COMPLETED", "startedIndexes": []}',
    ],
)
def test_record_rejects_malformed_index_fields(payload):
    """Malformed or out-of-range index fields return None instead of raising."""
    assert CompletionRecord.from_summary_payload(payload) is None


# endregion summary envelope


def test_flat_replay_preserves_failed_item_error_type():
    """A failed FLAT branch carries the same error type live and on replay.

    Live construction records the raw escaping type (for example ValueError)
    from the ChildContextError wrapper. FLAT replay re-executes the virtual
    branch and must record the identical discriminator, not the wrapper
    class name.
    """

    def fails():
        msg = "boom"
        raise ValueError(msg)

    def succeed():
        return "big" * 1

    executables = [Executable(0, succeed), Executable(1, fails)]

    def make_flat_executor():
        return _RecordingExecutor(
            executables=executables,
            max_concurrency=2,
            completion_config=CompletionConfig(tolerated_failure_count=1),
            sub_type_top="TOP",
            sub_type_iteration="ITER",
            name_prefix="test_",
            serdes=None,
            nesting_type=NestingType.FLAT,
            operation_id_namespace=_StubNamespace(),
        )

    execution_state, executor_context = _make_executor_mocks()
    live: BatchResult = make_flat_executor().execute(execution_state, executor_context)
    live_failed = next(i for i in live.all if i.status is BatchItemStatus.FAILED)
    assert live_failed.error is not None

    payload = envelope_summary_generator("MapResult", None)(live)
    top_checkpoint = Mock()
    top_checkpoint.result = payload

    replay_state, replay_context = _make_replay_mocks({})
    replay_state.wrap_user_function = lambda func, *args, **kwargs: func
    replayed: BatchResult = make_flat_executor().replay(
        replay_state, replay_context, top_checkpoint
    )
    replayed_failed = next(
        i for i in replayed.all if i.status is BatchItemStatus.FAILED
    )
    assert replayed_failed.error is not None
    assert replayed_failed.error.type == live_failed.error.type
    assert replayed_failed.error.type == "ValueError"
    assert replayed_failed.error.message == live_failed.error.message


def test_all_completed_runs_every_branch_through_failures():
    """all_completed() tolerates failures: every branch runs, reason ALL_COMPLETED."""

    def fails():
        msg = "boom"
        raise ValueError(msg)

    executables = [Executable(0, fails), Executable(1, lambda: "ok")]
    executor = _make_executor(
        executables,
        max_concurrency=1,
        completion_config=CompletionConfig.all_completed(),
    )
    execution_state, executor_context = _make_executor_mocks()

    result: BatchResult = executor.execute(execution_state, executor_context)

    assert [(i.index, i.status) for i in result.all] == [
        (0, BatchItemStatus.FAILED),
        (1, BatchItemStatus.SUCCEEDED),
    ]
    assert result.completion_reason is CompletionReason.ALL_COMPLETED


def test_map_min_successful_greater_than_total_raises():
    """min_successful exceeding the total is rejected before the child context.

    The check lives on CompletionConfig and is called by context.map() /
    context.parallel() before child_handler, so it surfaces as a bare
    ValidationError with no STARTED/FAILED operation in history (matching
    wait and wait_for_condition validation, and Java's constructor throw).
    """
    with pytest.raises(ValidationError, match="min_successful cannot be greater"):
        CompletionConfig(min_successful=3)._validate_for_total(2)


def test_parallel_min_successful_greater_than_total_raises():
    """min_successful exceeding the branch count is rejected pre-context."""
    with pytest.raises(ValidationError, match="min_successful cannot be greater"):
        CompletionConfig(min_successful=2)._validate_for_total(1)


def test_validate_for_total_accepts_min_successful_equal_to_total():
    """min_successful equal to the total is valid."""
    CompletionConfig(min_successful=2)._validate_for_total(2)


def test_validate_for_total_accepts_none_min_successful():
    """A config without min_successful passes any total."""
    CompletionConfig()._validate_for_total(0)


def test_map_item_namer_empty_string_is_preserved():
    """An item_namer returning "" is honored, not replaced by the default."""
    executor: MapExecutor[str, str] = MapExecutor.from_items(
        items=["a"],
        func=lambda ctx, item, idx, items: item,
        config=MapConfig(item_namer=lambda item, index: ""),
        operation_id_namespace=OperationIdNamespace("test-prefix"),
    )
    assert executor.get_iteration_name(0) == ""


def test_parallel_branch_empty_name_is_preserved():
    """A ParallelBranch with name="" is honored, not replaced by the default."""
    executor: ParallelExecutor[int] = ParallelExecutor.from_callables(
        [ParallelBranch(func=lambda ctx: 1, name="")],
        ParallelConfig(),
        operation_id_namespace=OperationIdNamespace("test-prefix"),
    )
    assert executor.get_iteration_name(0) == ""


def test_item_namer_called_eagerly_for_unscheduled_items():
    """item_namer runs for every input at map start, including items that
    never get scheduled because of early completion."""
    named: list[int] = []

    def namer(item: int, index: int) -> str:
        named.append(index)
        return f"n-{index}"

    executor: MapExecutor[int, int] = MapExecutor.from_items(
        items=[10, 20, 30],
        func=lambda ctx, item, idx, items: item,
        config=MapConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(min_successful=1),
            item_namer=namer,
        ),
        operation_id_namespace=OperationIdNamespace("test-prefix"),
    )

    assert named == [0, 1, 2]
    assert len(executor.executables) == 3


def test_operation_id_namespace_derivation_is_stable():
    """The id scheme is pinned: prefix-step hashed with blake2b, 64 hex chars.

    Checkpointed executions depend on this derivation staying byte-identical
    across releases.
    """
    expected: str = hashlib.blake2b(b"some-operation-id-7").hexdigest()[:64]
    assert OperationIdNamespace("some-operation-id").create_id_for_step(7) == expected

    expected_no_prefix: str = hashlib.blake2b(b"7").hexdigest()[:64]
    assert OperationIdNamespace(None).create_id_for_step(7) == expected_no_prefix
