"""Tests for the parallel operation module."""

from unittest.mock import Mock, patch

import pytest

from aws_durable_functions_sdk_python.concurrency import BatchResult, Executable
from aws_durable_functions_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_functions_sdk_python.lambda_service import OperationSubType
from aws_durable_functions_sdk_python.operation.parallel import (
    ParallelExecutor,
    parallel_handler,
)
from aws_durable_functions_sdk_python.state import ExecutionState


def test_parallel_executor_init():
    """Test ParallelExecutor initialization."""
    executables = [Executable(index=0, func=lambda x: x)]
    completion_config = CompletionConfig.all_successful()

    executor = ParallelExecutor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
    )

    assert executor.executables == executables
    assert executor.max_concurrency == 2
    assert executor.completion_config == completion_config
    assert executor.sub_type_top == OperationSubType.PARALLEL
    assert executor.sub_type_iteration == OperationSubType.PARALLEL_BRANCH
    assert executor.name_prefix == "test-"


def test_parallel_executor_from_callables():
    """Test ParallelExecutor.from_callables class method."""

    def func1(ctx):
        return "result1"

    def func2(ctx):
        return "result2"

    callables = [func1, func2]
    config = ParallelConfig(max_concurrency=3)

    executor = ParallelExecutor.from_callables(callables, config)

    assert len(executor.executables) == 2
    assert executor.executables[0].index == 0
    assert executor.executables[0].func == func1
    assert executor.executables[1].index == 1
    assert executor.executables[1].func == func2
    assert executor.max_concurrency == 3
    assert executor.sub_type_top == OperationSubType.PARALLEL
    assert executor.sub_type_iteration == OperationSubType.PARALLEL_BRANCH
    assert executor.name_prefix == "parallel-branch-"


def test_parallel_executor_from_callables_default_config():
    """Test ParallelExecutor.from_callables with default config."""

    def func1(ctx):
        return "result1"

    callables = [func1]
    config = ParallelConfig()

    executor = ParallelExecutor.from_callables(callables, config)

    assert len(executor.executables) == 1
    assert executor.max_concurrency is None
    assert executor.completion_config == CompletionConfig.all_successful()


def test_parallel_executor_execute_item():
    """Test ParallelExecutor.execute_item method."""

    def test_func(ctx):
        return f"processed-{ctx}"

    executable = Executable(index=0, func=test_func)
    executor = ParallelExecutor(
        executables=[executable],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
    )

    child_context = "test-context"
    result = executor.execute_item(child_context, executable)

    assert result == "processed-test-context"


def test_parallel_executor_execute_item_with_exception():
    """Test ParallelExecutor.execute_item with callable that raises exception."""

    def failing_func(ctx):
        msg = "Test error"
        raise ValueError(msg)

    executable = Executable(index=0, func=failing_func)
    executor = ParallelExecutor(
        executables=[executable],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
    )

    child_context = "test-context"

    with pytest.raises(ValueError, match="Test error"):
        executor.execute_item(child_context, executable)


def test_parallel_handler():
    """Test parallel_handler function."""

    def func1(ctx):
        return "result1"

    def func2(ctx):
        return "result2"

    callables = [func1, func2]
    config = ParallelConfig(max_concurrency=2)
    execution_state = Mock(spec=ExecutionState)

    # Mock the run_in_child_context function
    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    # Mock the executor.execute method to return a BatchResult
    mock_batch_result = Mock(spec=BatchResult)

    with patch.object(ParallelExecutor, "execute", return_value=mock_batch_result):
        result = parallel_handler(
            callables, config, execution_state, mock_run_in_child_context
        )

        assert result == mock_batch_result


def test_parallel_handler_with_none_config():
    """Test parallel_handler function with None config."""

    def func1(ctx):
        return "result1"

    callables = [func1]
    execution_state = Mock(spec=ExecutionState)

    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    mock_batch_result = Mock(spec=BatchResult)

    with patch.object(ParallelExecutor, "execute", return_value=mock_batch_result):
        result = parallel_handler(
            callables, None, execution_state, mock_run_in_child_context
        )

        assert result == mock_batch_result


def test_parallel_handler_creates_executor_with_correct_config():
    """Test that parallel_handler creates ParallelExecutor with correct configuration."""

    def func1(ctx):
        return "result1"

    callables = [func1]
    config = ParallelConfig(max_concurrency=5)
    execution_state = Mock(spec=ExecutionState)

    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    with patch.object(ParallelExecutor, "from_callables") as mock_from_callables:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_callables.return_value = mock_executor

        result = parallel_handler(
            callables, config, execution_state, mock_run_in_child_context
        )

        mock_from_callables.assert_called_once_with(callables, config)
        mock_executor.execute.assert_called_once_with(
            execution_state, mock_run_in_child_context
        )
        assert result == mock_batch_result


def test_parallel_handler_creates_executor_with_default_config_when_none():
    """Test that parallel_handler creates ParallelExecutor with default config when None is passed."""

    def func1(ctx):
        return "result1"

    callables = [func1]
    execution_state = Mock(spec=ExecutionState)

    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    with patch.object(ParallelExecutor, "from_callables") as mock_from_callables:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_callables.return_value = mock_executor

        result = parallel_handler(
            callables, None, execution_state, mock_run_in_child_context
        )

        assert result == mock_batch_result
        # Verify that a default ParallelConfig was created
        args, kwargs = mock_from_callables.call_args
        assert args[0] == callables
        assert isinstance(args[1], ParallelConfig)
        assert args[1].max_concurrency is None
        assert args[1].completion_config == CompletionConfig.all_successful()


def test_parallel_executor_inheritance():
    """Test that ParallelExecutor properly inherits from ConcurrentExecutor."""
    from aws_durable_functions_sdk_python.concurrency import ConcurrentExecutor

    executables = [Executable(index=0, func=lambda x: x)]
    executor = ParallelExecutor(
        executables=executables,
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
    )

    assert isinstance(executor, ConcurrentExecutor)


def test_parallel_executor_from_callables_empty_list():
    """Test ParallelExecutor.from_callables with empty callables list."""
    callables = []
    config = ParallelConfig()

    executor = ParallelExecutor.from_callables(callables, config)

    assert len(executor.executables) == 0
    assert executor.max_concurrency is None


def test_parallel_executor_execute_item_return_type():
    """Test that ParallelExecutor.execute_item returns the correct type."""

    def int_func(ctx):
        return 42

    def str_func(ctx):
        return "hello"

    def dict_func(ctx):
        return {"key": "value"}

    executor = ParallelExecutor(
        executables=[],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
    )

    # Test different return types
    int_executable = Executable(index=0, func=int_func)
    str_executable = Executable(index=1, func=str_func)
    dict_executable = Executable(index=2, func=dict_func)

    assert executor.execute_item("ctx", int_executable) == 42
    assert executor.execute_item("ctx", str_executable) == "hello"
    assert executor.execute_item("ctx", dict_executable) == {"key": "value"}
