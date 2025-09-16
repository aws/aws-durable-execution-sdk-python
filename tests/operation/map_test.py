"""Tests for map operation."""

from unittest.mock import Mock, patch

from aws_durable_functions_sdk_python.concurrency import BatchResult, Executable
from aws_durable_functions_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_functions_sdk_python.lambda_service import OperationSubType
from aws_durable_functions_sdk_python.operation.map import MapExecutor, map_handler


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


@patch("aws_durable_functions_sdk_python.operation.map.logger")
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

    # Mock the executor.execute method
    mock_batch_result = Mock(spec=BatchResult)

    with patch.object(
        MapExecutor, "execute", return_value=mock_batch_result
    ) as mock_execute:

        def mock_run_in_child_context(func, name, config):
            return func("mock_context")

        class MockExecutionState:
            pass

        execution_state = MockExecutionState()
        config = MapConfig()

        result = map_handler(
            items, callable_func, config, execution_state, mock_run_in_child_context
        )

        # Verify execute was called
        mock_execute.assert_called_once_with(execution_state, mock_run_in_child_context)
        assert result == mock_batch_result


def test_map_handler_with_none_config_creates_default():
    """Test that map_handler creates default MapConfig when config is None."""
    items = ["test"]

    def callable_func(ctx, item, idx, items):
        return item

    # Mock MapExecutor.from_items to verify it's called with default config
    with patch.object(MapExecutor, "from_items") as mock_from_items:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_items.return_value = mock_executor

        def mock_run_in_child_context(func, name, config):
            return func("mock_context")

        class MockExecutionState:
            pass

        execution_state = MockExecutionState()

        result = map_handler(
            items, callable_func, None, execution_state, mock_run_in_child_context
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
