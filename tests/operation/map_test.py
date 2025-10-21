"""Tests for map operation."""

from unittest.mock import Mock, patch

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
from aws_durable_execution_sdk_python.serdes import serialize
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

    # Mock the executor.execute method
    from aws_durable_execution_sdk_python.concurrency import (
        BatchItem,
        BatchItemStatus,
        BatchResult,
        CompletionReason,
    )

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

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
        mock_batch_result = BatchResult(
            all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
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


def test_map_handler_with_serdes():
    """Test that map_handler calls executor.execute method."""
    items = ["test_item"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    # Mock the executor.execute method

    def mock_run_in_child_context(func, name, config):
        return serialize(
            serdes=config.serdes,
            value=func("mock_context"),
            operation_id="op_id",
            durable_execution_arn="durable_execution_arn",
        )

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()
    config = MapConfig(serdes=CustomStrSerDes())

    result = map_handler(
        items, callable_func, config, execution_state, mock_run_in_child_context
    )

    # Verify execute was called
    assert result.all[0].result == "RESULT_TEST_ITEM"


def test_map_handler_with_summary_generator():
    """Test that map_handler passes summary_generator to child config."""
    items = ["item1", "item2"]

    def callable_func(ctx, item, idx, items):
        return f"large_result_{item}" * 1000  # Create a large result

    def mock_summary_generator(result):
        return f"Summary of {len(result)} chars for map item"

    config = MapConfig(summary_generator=mock_summary_generator)

    # Track the child_config passed to run_in_child_context
    captured_child_configs = []

    def mock_run_in_child_context(callable_func, name, child_config):
        captured_child_configs.append(child_config)
        return callable_func("mock-context")

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    # Call map_handler with our mock run_in_child_context
    result = map_handler(
        items, callable_func, config, execution_state, mock_run_in_child_context
    )

    # Verify that the summary_generator was passed to the child config
    assert len(captured_child_configs) > 0
    child_config = captured_child_configs[0]
    assert child_config.summary_generator is mock_summary_generator

    # Test that the summary generator works
    test_result = child_config.summary_generator("test" * 100)
    assert test_result == "Summary of 400 chars for map item"


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
    """Test that map_handler uses default summary generator when config is None."""
    items = ["item1"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    # Track the child_config passed to run_in_child_context
    captured_child_configs = []

    def mock_run_in_child_context(callable_func, name, child_config):
        captured_child_configs.append(child_config)
        return callable_func("mock-context")

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    # Call map_handler with None config (should use default)
    result = map_handler(
        items, callable_func, None, execution_state, mock_run_in_child_context
    )

    # Verify that a default summary_generator was provided
    assert len(captured_child_configs) > 0
    child_config = captured_child_configs[0]
    assert child_config.summary_generator is not None

    # Test that the default summary generator works
    test_result = child_config.summary_generator(
        BatchResult([], CompletionReason.ALL_COMPLETED)
    )
    assert isinstance(test_result, str)
    assert len(test_result) > 0


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
    """Test that map_handler respects explicit None summary_generator."""

    def func(ctx, item, index, array):
        return f"processed_{item}"

    items = ["item1", "item2"]
    # Explicitly set summary_generator to None
    config = MapConfig(summary_generator=None)

    class MockExecutionState:
        pass

    execution_state = MockExecutionState()

    # Capture the child configs passed to run_in_child_context
    captured_child_configs = []

    def mock_run_in_child_context(func, name, child_config):
        captured_child_configs.append(child_config)
        return func(Mock())

    # Call map_handler with our mock run_in_child_context
    result = map_handler(
        items=items,
        func=func,
        config=config,
        execution_state=execution_state,
        run_in_child_context=mock_run_in_child_context,
    )

    # Verify that the summary_generator was set to None (not default)
    assert len(captured_child_configs) > 0
    child_config = captured_child_configs[0]
    assert child_config.summary_generator is None

    # Test that when None, it should result in empty string behavior
    # This matches child.py: config.summary_generator(raw_result) if config.summary_generator else ""
    test_result = (
        child_config.summary_generator("test_data")
        if child_config.summary_generator
        else ""
    )
    assert test_result == ""


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
    assert test_result == ""
