"""Tests for the parallel operation module."""

from unittest.mock import Mock, patch

import pytest

# Mock the executor.execute method to return a BatchResult
from aws_durable_execution_sdk_python.concurrency import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    ConcurrentExecutor,
    Executable,
)
from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.operation.parallel import (
    ParallelExecutor,
    parallel_handler,
)
from tests.serdes_test import CustomStrSerDes


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
        serdes=None,
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
        serdes=None,
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
        serdes=None,
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
    execution_state = Mock()

    # Mock the run_in_child_context function
    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

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
    execution_state = Mock()

    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

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
    execution_state = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    with patch.object(ParallelExecutor, "from_callables") as mock_from_callables:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_callables.return_value = mock_executor

        result = parallel_handler(callables, config, execution_state, executor_context)

        mock_from_callables.assert_called_once_with(callables, config)
        mock_executor.execute.assert_called_once_with(
            execution_state, executor_context=executor_context
        )
        assert result == mock_batch_result


def test_parallel_handler_creates_executor_with_default_config_when_none():
    """Test that parallel_handler creates ParallelExecutor with default config when None is passed."""

    def func1(ctx):
        return "result1"

    callables = [func1]
    execution_state = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    with patch.object(ParallelExecutor, "from_callables") as mock_from_callables:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_callables.return_value = mock_executor

        result = parallel_handler(callables, None, execution_state, executor_context)

        assert result == mock_batch_result
        # Verify that a default ParallelConfig was created
        args, _ = mock_from_callables.call_args
        assert args[0] == callables
        assert isinstance(args[1], ParallelConfig)
        assert args[1].max_concurrency is None
        assert args[1].completion_config == CompletionConfig.all_successful()


def test_parallel_executor_inheritance():
    """Test that ParallelExecutor properly inherits from ConcurrentExecutor."""
    executables = [Executable(index=0, func=lambda x: x)]
    executor = ParallelExecutor(
        executables=executables,
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
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
        serdes=None,
    )

    # Test different return types
    int_executable = Executable(index=0, func=int_func)
    str_executable = Executable(index=1, func=str_func)
    dict_executable = Executable(index=2, func=dict_func)

    assert executor.execute_item("ctx", int_executable) == 42
    assert executor.execute_item("ctx", str_executable) == "hello"
    assert executor.execute_item("ctx", dict_executable) == {"key": "value"}


def test_parallel_handler_with_serdes():
    """Test that parallel_handler with serdes"""

    def func1(ctx):
        return "RESULT1"

    callables = [func1]
    execution_state = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = parallel_handler(
        callables,
        ParallelConfig(serdes=CustomStrSerDes()),
        execution_state,
        executor_context,
    )

    assert result.all[0].result == "RESULT1"


def test_parallel_handler_with_summary_generator():
    """Test that parallel_handler calls executor_context methods correctly."""

    def func1(ctx):
        return "large_result" * 1000  # Create a large result

    def mock_summary_generator(result):
        return f"Summary of {len(result)} chars"

    callables = [func1]
    config = ParallelConfig(summary_generator=mock_summary_generator)
    execution_state = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(return_value="1")  # noqa SLF001
    executor_context.create_child_context = Mock(return_value=Mock())

    # Call parallel_handler
    parallel_handler(callables, config, execution_state, executor_context)

    # Verify that create_child_context was called once (N=1 job)
    assert executor_context.create_child_context.call_count == 1

    # Verify that _create_step_id_for_logical_step was called once with unique value
    assert executor_context._create_step_id_for_logical_step.call_count == 1  # noqa SLF001


def test_parallel_executor_from_callables_with_summary_generator():
    """Test ParallelExecutor.from_callables preserves summary_generator."""

    def func1(ctx):
        return "result1"

    def mock_summary_generator(result):
        return f"Summary: {result}"

    callables = [func1]
    config = ParallelConfig(summary_generator=mock_summary_generator)

    executor = ParallelExecutor.from_callables(callables, config)

    # Verify that the summary_generator is preserved in the executor
    assert executor.summary_generator is mock_summary_generator


def test_parallel_handler_default_summary_generator():
    """Test that parallel_handler calls executor_context methods correctly with default config."""

    def func1(ctx):
        return "result1"

    def func2(ctx):
        return "result2"

    callables = [func1, func2]
    execution_state = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(side_effect=["1", "2"])  # noqa SLF001
    executor_context.create_child_context = Mock(return_value=Mock())

    # Call parallel_handler with None config (should use default)
    parallel_handler(callables, None, execution_state, executor_context)

    # Verify that create_child_context was called twice (N=2 jobs)
    assert executor_context.create_child_context.call_count == 2

    # Verify that _create_step_id_for_logical_step was called twice with unique values
    assert executor_context._create_step_id_for_logical_step.call_count == 2  # noqa SLF001
    calls = executor_context._create_step_id_for_logical_step.call_args_list  # noqa SLF001
    # Verify unique values were passed
    assert calls[0] != calls[1]


def test_parallel_handler_with_explicit_none_summary_generator():
    """Test that parallel_handler calls executor_context methods correctly with explicit None summary_generator."""

    def func1(ctx):
        return "result1"

    def func2(ctx):
        return "result2"

    def func3(ctx):
        return "result3"

    callables = [func1, func2, func3]
    # Explicitly set summary_generator to None
    config = ParallelConfig(summary_generator=None)

    execution_state = Mock()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2", "3"]
    )
    executor_context.create_child_context = Mock(return_value=Mock())

    # Call parallel_handler
    parallel_handler(
        callables=callables,
        config=config,
        execution_state=execution_state,
        parallel_context=executor_context,
    )

    # Verify that create_child_context was called 3 times (N=3 jobs)
    assert executor_context.create_child_context.call_count == 3

    # Verify that _create_step_id_for_logical_step was called 3 times with unique values
    assert executor_context._create_step_id_for_logical_step.call_count == 3  # noqa SLF001
    calls = executor_context._create_step_id_for_logical_step.call_args_list  # noqa SLF001
    # Verify all calls have unique values
    call_values = [call[0][0] for call in calls]
    assert len(set(call_values)) == 3  # All unique


def test_parallel_config_with_explicit_none_summary_generator():
    """Test ParallelConfig with explicitly set None summary_generator."""
    config = ParallelConfig(summary_generator=None)

    assert config.summary_generator is None
    assert config.max_concurrency is None
    assert isinstance(config.completion_config, CompletionConfig)


def test_parallel_config_default_summary_generator_behavior():
    """Test ParallelConfig() with no summary_generator should result in empty string behavior."""
    # When creating ParallelConfig() with no summary_generator specified
    config = ParallelConfig()

    # The summary_generator should be None by default
    assert config.summary_generator is None

    # But when used in the actual child.py logic, it should result in empty string
    # This matches child.py: config.summary_generator(raw_result) if config.summary_generator else ""
    test_result = (
        config.summary_generator("test_data") if config.summary_generator else ""
    )
    assert test_result == ""  # noqa PLC1901
    assert config.serdes is None
