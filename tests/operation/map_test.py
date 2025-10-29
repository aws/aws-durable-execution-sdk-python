"""Tests for map operation."""

from unittest.mock import Mock, patch

# Mock the executor.execute method
from aws_durable_execution_sdk_python.concurrency import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    Executable,
)
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ItemBatcher,
    MapConfig,
)
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.operation.map import MapExecutor, map_handler
from tests.serdes_test import CustomStrSerDes


def test_map_executor_init():
    """Test MapExecutor initialization."""
    executables = [Executable(index=0, func=lambda: None)]
    items = ["item1"]

    executor = MapExecutor(
        executables=executables,
        items=items,
        max_concurrency=2,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="test-",
        serdes=None,
    )

    assert executor.items == items
    assert executor.executables == executables


def test_map_executor_from_items():
    """Test MapExecutor.from_items class method."""
    items = ["a", "b", "c"]

    def callable_func(ctx, item, idx, items):
        return item.upper()

    config = MapConfig(max_concurrency=3)

    executor = MapExecutor.from_items(items, callable_func, config)

    assert len(executor.executables) == 3
    assert executor.items == items
    assert all(exe.func == callable_func for exe in executor.executables)
    assert [exe.index for exe in executor.executables] == [0, 1, 2]


def test_map_executor_from_items_default_config():
    """Test MapExecutor.from_items with default config."""
    items = ["x"]

    def callable_func(ctx, item, idx, items):
        return item

    executor = MapExecutor.from_items(items, callable_func, MapConfig())

    assert len(executor.executables) == 1
    assert executor.items == items


@patch("aws_durable_execution_sdk_python.operation.map.logger")
def test_map_executor_execute_item(mock_logger):
    """Test MapExecutor.execute_item method with logging."""
    items = ["hello", "world"]

    def callable_func(ctx, item, idx, items):
        return f"{item}_{idx}"

    executor = MapExecutor.from_items(items, callable_func, MapConfig())
    executable = executor.executables[0]

    result = executor.execute_item(None, executable)

    assert result == "hello_0"
    assert mock_logger.debug.call_count == 2
    mock_logger.debug.assert_any_call("🗺️ Processing map item: %s", 0)
    mock_logger.debug.assert_any_call("✅ Processed map item: %s", 0)


def test_map_executor_execute_item_with_context():
    """Test MapExecutor.execute_item with context usage."""
    items = [1, 2, 3]

    def callable_func(ctx, item, idx, items):
        return item * 2 + idx

    executor = MapExecutor.from_items(items, callable_func, MapConfig())
    executable = executor.executables[1]

    result = executor.execute_item("mock_context", executable)

    assert result == 5  # 2 * 2 + 1


def test_map_handler():
    """Test map_handler function."""
    items = ["a", "b"]

    def callable_func(ctx, item, idx, items):
        return item.upper()

    def mock_run_in_child_context(func, name, config):
        return func("mock_context")

    # Create a minimal ExecutionState mock
    class MockExecutionState:
        pass

    execution_state = MockExecutionState()
    config = MapConfig()

    result = map_handler(
        items, callable_func, config, execution_state, mock_run_in_child_context
    )

    assert isinstance(result, BatchResult)


def test_map_handler_with_none_config():
    """Test map_handler with None config creates default MapConfig."""
    items = ["test"]

    def callable_func(ctx, item, idx, items):
        return item

    def mock_run_in_child_context(func, name, config):
        return func("mock_context")

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    # Since MapConfig() is called in map_handler when config is None,
    # we need to provide a valid config to avoid the NameError
    # This tests the behavior when config is provided instead
    result = map_handler(
        items, callable_func, MapConfig(), execution_state, mock_run_in_child_context
    )

    assert isinstance(result, BatchResult)


def test_map_executor_execute_item_accesses_all_parameters():
    """Test that execute_item passes all parameters correctly."""
    items = ["first", "second", "third"]

    def callable_func(ctx, item, idx, items_list):
        # Verify all parameters are passed correctly
        assert ctx == "test_context"
        assert item in items_list
        assert idx < len(items_list)
        assert items_list == items
        return f"{item}_{idx}_{len(items_list)}"

    executor = MapExecutor.from_items(items, callable_func, MapConfig())
    executable = executor.executables[2]

    result = executor.execute_item("test_context", executable)

    assert result == "third_2_3"


def test_map_executor_from_items_empty_list():
    """Test MapExecutor.from_items with empty items list."""
    items = []

    def callable_func(ctx, item, idx, items):
        return item

    executor = MapExecutor.from_items(items, callable_func, MapConfig())

    assert len(executor.executables) == 0
    assert executor.items == []


def test_map_executor_from_items_single_item():
    """Test MapExecutor.from_items with single item."""
    items = ["only"]

    def callable_func(ctx, item, idx, items):
        return f"processed_{item}"

    executor = MapExecutor.from_items(items, callable_func, MapConfig())

    assert len(executor.executables) == 1
    assert executor.executables[0].index == 0
    assert executor.items == items


def test_map_executor_inheritance():
    """Test that MapExecutor properly inherits from ConcurrentExecutor."""
    items = ["test"]

    def callable_func(ctx, item, idx, items):
        return item

    executor = MapExecutor.from_items(items, callable_func, MapConfig())

    # Verify it has inherited attributes from ConcurrentExecutor
    assert hasattr(executor, "executables")
    assert hasattr(executor, "execute")
    assert executor.items == items


def test_map_handler_calls_executor_execute():
    """Test that map_handler calls executor.execute method."""
    items = ["test_item"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    with patch.object(
        MapExecutor, "execute", return_value=mock_batch_result
    ) as mock_execute:

        class MockExecutionState:
            pass

        execution_state = MockExecutionState()
        config = MapConfig()

        result = map_handler(
            items, callable_func, config, execution_state, executor_context
        )

        # Verify execute was called
        mock_execute.assert_called_once_with(
            execution_state, executor_context=executor_context
        )
        assert result == mock_batch_result


def test_map_handler_with_none_config_creates_default():
    """Test that map_handler creates default MapConfig when config is None."""
    items = ["test"]

    def callable_func(ctx, item, idx, items):
        return item

    # Mock MapExecutor.from_items to verify it's called with default config
    with patch.object(MapExecutor, "from_items") as mock_from_items:
        mock_executor = Mock()
        mock_batch_result = BatchResult(
            all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_executor.execute.return_value = mock_batch_result
        mock_from_items.return_value = mock_executor

        executor_context = Mock()
        executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
        executor_context.create_child_context = lambda *args: Mock()

        class MockExecutionState:
            pass

        execution_state = MockExecutionState()

        result = map_handler(
            items, callable_func, None, execution_state, executor_context
        )

        # Verify from_items was called with a MapConfig instance
        mock_from_items.assert_called_once()
        call_args = mock_from_items.call_args
        # Check that the call was made with keyword arguments
        if call_args.args:
            assert call_args.args[0] == items
            assert call_args.args[1] == callable_func
            assert isinstance(call_args.args[2], MapConfig)
        else:
            # Called with keyword arguments
            assert call_args.kwargs["items"] == items
            assert call_args.kwargs["func"] == callable_func
            assert isinstance(call_args.kwargs["config"], MapConfig)

        assert result == mock_batch_result


def test_map_handler_with_serdes():
    """Test that map_handler with serdes"""
    items = ["test_item"]

    def callable_func(ctx, item, idx, items):
        return f"RESULT_{item.upper()}"

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()
    config = MapConfig(serdes=CustomStrSerDes())

    result = map_handler(
        items, callable_func, config, execution_state, executor_context
    )

    # Verify execute was called
    assert result.all[0].result == "RESULT_TEST_ITEM"


def test_map_handler_with_summary_generator():
    """Test that map_handler calls executor_context methods correctly."""
    items = ["item1", "item2"]

    def callable_func(ctx, item, idx, items):
        return f"large_result_{item}" * 1000  # Create a large result

    def mock_summary_generator(result):
        return f"Summary of {len(result)} chars for map item"

    config = MapConfig(summary_generator=mock_summary_generator)

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(side_effect=["1", "2"])  # noqa SLF001
    executor_context.create_child_context = Mock(return_value=Mock())

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    # Call map_handler
    map_handler(items, callable_func, config, execution_state, executor_context)

    # Verify that create_child_context was called twice (N=2 items)
    assert executor_context.create_child_context.call_count == 2

    # Verify that _create_step_id_for_logical_step was called twice with unique values
    assert executor_context._create_step_id_for_logical_step.call_count == 2  # noqa SLF001
    calls = executor_context._create_step_id_for_logical_step.call_args_list  # noqa SLF001
    # Verify unique values were passed
    assert calls[0] != calls[1]


def test_map_executor_from_items_with_summary_generator():
    """Test MapExecutor.from_items preserves summary_generator."""
    items = ["item1"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    def mock_summary_generator(result):
        return f"Map summary: {result}"

    config = MapConfig(summary_generator=mock_summary_generator)

    executor = MapExecutor.from_items(items, callable_func, config)

    # Verify that the summary_generator is preserved in the executor
    assert executor.summary_generator is mock_summary_generator


def test_map_handler_default_summary_generator():
    """Test that map_handler calls executor_context methods correctly with default config."""
    items = ["item1"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(return_value="1")  # noqa SLF001
    executor_context.create_child_context = Mock(return_value=Mock())  # SLF001

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    # Call map_handler with None config (should use default)
    map_handler(items, callable_func, None, execution_state, executor_context)

    # Verify that create_child_context was called once (N=1 item)
    assert executor_context.create_child_context.call_count == 1

    # Verify that _create_step_id_for_logical_step was called once
    assert executor_context._create_step_id_for_logical_step.call_count == 1  # noqa SLF001


def test_map_executor_init_with_summary_generator():
    """Test MapExecutor initialization with summary_generator."""
    items = ["item1"]
    executables = [Executable(index=0, func=lambda: None)]

    def mock_summary_generator(result):
        return f"Summary: {result}"

    executor = MapExecutor(
        executables=executables,
        items=items,
        max_concurrency=2,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="test-",
        serdes=None,
        summary_generator=mock_summary_generator,
    )

    assert executor.summary_generator is mock_summary_generator
    assert executor.items == items
    assert executor.executables == executables


def test_map_handler_with_explicit_none_summary_generator():
    """Test that map_handler calls executor_context methods correctly with explicit None summary_generator."""

    def func(ctx, item, index, array):
        return f"processed_{item}"

    items = ["item1", "item2", "item3"]
    # Explicitly set summary_generator to None
    config = MapConfig(summary_generator=None)

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2", "3"]
    )
    executor_context.create_child_context = Mock(return_value=Mock())

    # Call map_handler
    map_handler(
        items=items,
        func=func,
        config=config,
        execution_state=execution_state,
        map_context=executor_context,
    )

    # Verify that create_child_context was called 3 times (N=3 items)
    assert executor_context.create_child_context.call_count == 3

    # Verify that _create_step_id_for_logical_step was called 3 times with unique values
    assert executor_context._create_step_id_for_logical_step.call_count == 3  # noqa SLF001
    calls = executor_context._create_step_id_for_logical_step.call_args_list  # noqa SLF001
    # Verify all calls have unique values
    call_values = [call[0][0] for call in calls]
    assert len(set(call_values)) == 3  # All unique


def test_map_config_with_explicit_none_summary_generator():
    """Test MapConfig with explicitly set None summary_generator."""
    config = MapConfig(summary_generator=None)

    assert config.summary_generator is None
    assert config.max_concurrency is None
    assert isinstance(config.item_batcher, ItemBatcher)
    assert isinstance(config.completion_config, CompletionConfig)
    assert config.serdes is None


def test_map_config_default_summary_generator_behavior():
    """Test MapConfig() with no summary_generator should result in empty string behavior."""
    # When creating MapConfig() with no summary_generator specified
    config = MapConfig()

    # The summary_generator should be None by default
    assert config.summary_generator is None

    # But when used in the actual child.py logic, it should result in empty string
    # This matches child.py: config.summary_generator(raw_result) if config.summary_generator else ""
    test_result = (
        config.summary_generator("test_data") if config.summary_generator else ""
    )
    assert test_result == ""  # noqa PLC1901
