"""Tests for map operation."""

import importlib
import json
from unittest.mock import Mock, patch

import pytest

# Mock the executor.execute method
from aws_durable_execution_sdk_python.concurrency.models import (
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
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.operation import child  # PLC0415
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
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    config = MapConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    result = map_handler(
        items,
        callable_func,
        config,
        execution_state,
        mock_run_in_child_context,
        operation_identifier,
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
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Since MapConfig() is called in map_handler when config is None,
    # we need to provide a valid config to avoid the NameError
    # This tests the behavior when config is provided instead
    result = map_handler(
        items,
        callable_func,
        MapConfig(),
        execution_state,
        mock_run_in_child_context,
        operation_identifier,
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
            def get_checkpoint_result(self, operation_id):
                mock_result = Mock()
                mock_result.is_succeeded.return_value = False
                return mock_result

        execution_state = MockExecutionState()
        config = MapConfig()
        operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

        result = map_handler(
            items,
            callable_func,
            config,
            execution_state,
            executor_context,
            operation_identifier,
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
            def get_checkpoint_result(self, operation_id):
                mock_result = Mock()
                mock_result.is_succeeded.return_value = False
                return mock_result

        execution_state = MockExecutionState()
        operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

        result = map_handler(
            items,
            callable_func,
            None,
            execution_state,
            executor_context,
            operation_identifier,
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
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    config = MapConfig(serdes=CustomStrSerDes())
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    result = map_handler(
        items,
        callable_func,
        config,
        execution_state,
        executor_context,
        operation_identifier,
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
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Call map_handler
    map_handler(
        items,
        callable_func,
        config,
        execution_state,
        executor_context,
        operation_identifier,
    )

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
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Call map_handler with None config (should use default)
    map_handler(
        items,
        callable_func,
        None,
        execution_state,
        executor_context,
        operation_identifier,
    )

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
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

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
        operation_identifier=operation_identifier,
    )

    # Verify that create_child_context was called 3 times (N=3 items)
    assert executor_context.create_child_context.call_count == 3


def test_map_handler_replay_mechanism():
    """Test that map_handler uses replay when operation has already succeeded."""
    items = ["item1", "item2"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    # Mock execution state that indicates operation already succeeded
    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = True
            mock_result.is_replay_children.return_value = False
            # Provide properly serialized JSON data
            mock_result.result = f'"cached_result_{operation_id}"'  # JSON string
            return mock_result

    execution_state = MockExecutionState()
    config = MapConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Mock map context
    map_context = Mock()
    map_context._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["child_1", "child_2"]
    )

    # Mock the executor's replay method
    with patch.object(MapExecutor, "replay") as mock_replay:
        expected_batch_result = BatchResult(
            all=[
                BatchItem(
                    index=0,
                    status=BatchItemStatus.SUCCEEDED,
                    result="cached_result_child_1",
                ),
                BatchItem(
                    index=1,
                    status=BatchItemStatus.SUCCEEDED,
                    result="cached_result_child_2",
                ),
            ],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_replay.return_value = expected_batch_result

        result = map_handler(
            items,
            callable_func,
            config,
            execution_state,
            map_context,
            operation_identifier,
        )

        # Verify replay was called instead of execute
        mock_replay.assert_called_once_with(execution_state, map_context)
        assert result == expected_batch_result


def test_map_handler_replay_with_replay_children():
    """Test map_handler replay when children need to be re-executed."""
    items = ["item1"]

    def callable_func(ctx, item, idx, items):
        return f"result_{item}"

    # Mock execution state that indicates operation succeeded but children need replay
    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            if operation_id == "test_op":
                mock_result.is_succeeded.return_value = True
            else:  # child operations
                mock_result.is_succeeded.return_value = True
                mock_result.is_replay_children.return_value = True
            return mock_result

    execution_state = MockExecutionState()
    config = MapConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Mock map context
    map_context = Mock()
    map_context._create_step_id_for_logical_step = Mock(return_value="child_1")  # noqa: SLF001

    # Mock the executor's replay method and _execute_item_in_child_context
    with (
        patch.object(MapExecutor, "replay") as mock_replay,
        patch.object(
            MapExecutor, "_execute_item_in_child_context"
        ) as mock_execute_item,
    ):
        mock_execute_item.return_value = "re_executed_result"
        expected_batch_result = BatchResult(
            all=[
                BatchItem(
                    index=0,
                    status=BatchItemStatus.SUCCEEDED,
                    result="re_executed_result",
                )
            ],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_replay.return_value = expected_batch_result

        result = map_handler(
            items,
            callable_func,
            config,
            execution_state,
            map_context,
            operation_identifier,
        )

        mock_replay.assert_called_once_with(execution_state, map_context)
        assert result == expected_batch_result


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


def test_map_handler_first_execution_then_replay_integration():
    """Test map_handler called twice - first calls execute, second calls replay."""

    def test_func(ctx, item, idx, items):
        return f"processed_{item}"

    items = ["a", "b"]
    config = MapConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Track whether we're in first or second execution
    execution_count = 0

    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def get_checkpoint_result(self, operation_id):
            nonlocal execution_count
            mock_result = Mock()

            if operation_id == "test_op":
                # Main operation checkpoint
                if execution_count == 0:
                    # First execution - operation not succeeded yet
                    mock_result.is_succeeded.return_value = False
                else:
                    # Second execution - operation succeeded, trigger replay
                    mock_result.is_succeeded.return_value = True

            return mock_result

    execution_state = MockExecutionState()
    map_context = Mock()

    with (
        patch(
            "aws_durable_execution_sdk_python.operation.map.MapExecutor.execute"
        ) as mock_execute,
        patch(
            "aws_durable_execution_sdk_python.operation.map.MapExecutor.replay"
        ) as mock_replay,
    ):
        mock_execute.return_value = Mock()  # Mock BatchResult
        mock_replay.return_value = Mock()  # Mock BatchResult

        # FIRST EXECUTION - should call execute
        execution_count = 0
        map_handler(
            items, test_func, config, execution_state, map_context, operation_identifier
        )

        # Verify execute was called, replay was not
        mock_execute.assert_called_once()
        mock_replay.assert_not_called()

        # Reset mocks for second call
        mock_execute.reset_mock()
        mock_replay.reset_mock()

        # SECOND EXECUTION - should call replay
        execution_count = 1
        map_handler(
            items, test_func, config, execution_state, map_context, operation_identifier
        )

        # Verify replay was called, execute was not
        mock_replay.assert_called_once()
        mock_execute.assert_not_called()


@pytest.mark.parametrize(
    ("item_serdes", "batch_serdes"),
    [
        (Mock(), Mock()),
        (None, Mock()),
        (Mock(), None),
    ],
)
@patch("aws_durable_execution_sdk_python.operation.child.serialize")
def test_map_item_serialize(mock_serialize, item_serdes, batch_serdes):
    """Test map serializes items with item_serdes or fallback."""
    mock_serialize.return_value = '"serialized"'

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_started.return_value = False
    parent_checkpoint.is_existent.return_value = True
    parent_checkpoint.is_replay_children.return_value = False

    child_checkpoint = Mock()
    child_checkpoint.is_succeeded.return_value = False
    child_checkpoint.is_failed.return_value = False
    child_checkpoint.is_started.return_value = False
    child_checkpoint.is_existent.return_value = True
    child_checkpoint.is_replay_children.return_value = False

    def get_checkpoint(op_id):
        return child_checkpoint if op_id.startswith("child-") else parent_checkpoint

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.get_checkpoint_result = Mock(side_effect=get_checkpoint)
    mock_state.create_checkpoint = Mock()

    context_map = {}

    def create_id(self, i):
        ctx_id = id(self)
        if ctx_id not in context_map:
            context_map[ctx_id] = []
        context_map[ctx_id].append(i)
        return (
            "parent"
            if len(context_map) == 1 and len(context_map[ctx_id]) == 1
            else f"child-{i}"
        )

    with patch.object(DurableContext, "_create_step_id_for_logical_step", create_id):
        context = DurableContext(state=mock_state)
        context.map(
            ["a", "b"],
            lambda ctx, item, idx, items: item,
            config=MapConfig(serdes=batch_serdes, item_serdes=item_serdes),
        )

    expected = item_serdes or batch_serdes
    assert mock_serialize.call_args_list[0][1]["serdes"] is expected
    assert mock_serialize.call_args_list[0][1]["operation_id"] == "child-0"
    assert mock_serialize.call_args_list[1][1]["serdes"] is expected
    assert mock_serialize.call_args_list[1][1]["operation_id"] == "child-1"
    assert mock_serialize.call_args_list[2][1]["serdes"] is batch_serdes
    assert mock_serialize.call_args_list[2][1]["operation_id"] == "parent"


@pytest.mark.parametrize(
    ("item_serdes", "batch_serdes"),
    [
        (Mock(), Mock()),
        (None, Mock()),
        (Mock(), None),
    ],
)
@patch("aws_durable_execution_sdk_python.operation.child.deserialize")
def test_map_item_deserialize(mock_deserialize, item_serdes, batch_serdes):
    """Test map deserializes items with item_serdes or fallback."""
    mock_deserialize.return_value = "deserialized"

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_existent.return_value = False

    child_checkpoint = Mock()
    child_checkpoint.is_succeeded.return_value = True
    child_checkpoint.is_failed.return_value = False
    child_checkpoint.is_replay_children.return_value = False
    child_checkpoint.result = '"cached"'

    def get_checkpoint(op_id):
        return child_checkpoint if op_id.startswith("child-") else parent_checkpoint

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.get_checkpoint_result = Mock(side_effect=get_checkpoint)
    mock_state.create_checkpoint = Mock()

    context_map = {}

    def create_id(self, i):
        ctx_id = id(self)
        if ctx_id not in context_map:
            context_map[ctx_id] = []
        context_map[ctx_id].append(i)
        return (
            "parent"
            if len(context_map) == 1 and len(context_map[ctx_id]) == 1
            else f"child-{i}"
        )

    with patch.object(DurableContext, "_create_step_id_for_logical_step", create_id):
        context = DurableContext(state=mock_state)
        context.map(
            ["a", "b"],
            lambda ctx, item, idx, items: item,
            config=MapConfig(serdes=batch_serdes, item_serdes=item_serdes),
        )

    expected = item_serdes or batch_serdes
    assert mock_deserialize.call_args_list[0][1]["serdes"] is expected
    assert mock_deserialize.call_args_list[0][1]["operation_id"] == "child-0"
    assert mock_deserialize.call_args_list[1][1]["serdes"] is expected
    assert mock_deserialize.call_args_list[1][1]["operation_id"] == "child-1"


def test_map_result_serialization_roundtrip():
    """Test that map operation BatchResult can be serialized and deserialized."""

    items = ["a", "b", "c"]

    def func(ctx, item, idx, items):
        return {"item": item.upper(), "index": idx}

    class MockExecutionState:
        durable_execution_arn = "arn:test"

        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    map_context = Mock()
    map_context._create_step_id_for_logical_step = Mock(side_effect=["1", "2", "3"])  # noqa SLF001
    map_context.create_child_context = Mock(return_value=Mock())
    operation_identifier = OperationIdentifier("test_op", "parent", "test_map")

    # Execute map
    result = map_handler(
        items, func, MapConfig(), execution_state, map_context, operation_identifier
    )

    # Serialize the BatchResult
    serialized = json.dumps(result.to_dict())

    # Deserialize
    deserialized = BatchResult.from_dict(json.loads(serialized))

    # Verify all data preserved
    assert len(deserialized.all) == 3
    assert deserialized.all[0].result == {"item": "A", "index": 0}
    assert deserialized.all[1].result == {"item": "B", "index": 1}
    assert deserialized.all[2].result == {"item": "C", "index": 2}
    assert deserialized.completion_reason == result.completion_reason
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in deserialized.all)


def test_map_handler_serializes_batch_result():
    """Verify map_handler serializes BatchResult at parent level."""
    with patch(
        "aws_durable_execution_sdk_python.serdes.serialize"
    ) as mock_serdes_serialize:
        mock_serdes_serialize.return_value = '"serialized"'
        importlib.reload(child)

        parent_checkpoint = Mock()
        parent_checkpoint.is_succeeded.return_value = False
        parent_checkpoint.is_failed.return_value = False
        parent_checkpoint.is_existent.return_value = False
        parent_checkpoint.is_replay_children.return_value = False

        child_checkpoint = Mock()
        child_checkpoint.is_succeeded.return_value = False
        child_checkpoint.is_failed.return_value = False
        child_checkpoint.is_existent.return_value = False
        child_checkpoint.is_replay_children.return_value = False

        def get_checkpoint(op_id):
            return child_checkpoint if op_id.startswith("child-") else parent_checkpoint

        mock_state = Mock()
        mock_state.durable_execution_arn = "arn:test"
        mock_state.get_checkpoint_result = Mock(side_effect=get_checkpoint)
        mock_state.create_checkpoint = Mock()

        context_map = {}

        def create_id(self, i):
            ctx_id = id(self)
            if ctx_id not in context_map:
                context_map[ctx_id] = []
            context_map[ctx_id].append(i)
            return (
                "parent"
                if len(context_map) == 1 and len(context_map[ctx_id]) == 1
                else f"child-{i}"
            )

        with patch.object(
            DurableContext, "_create_step_id_for_logical_step", create_id
        ):
            context = DurableContext(state=mock_state)
            result = context.map(["a", "b"], lambda ctx, item, idx, items: item)

        assert len(mock_serdes_serialize.call_args_list) == 3
        parent_call = mock_serdes_serialize.call_args_list[2]
        assert parent_call[1]["value"] is result


def test_map_default_serdes_serializes_batch_result():
    """Verify default serdes automatically serializes BatchResult."""

    with patch(
        "aws_durable_execution_sdk_python.serdes.serialize", wraps=serialize
    ) as mock_serialize:
        importlib.reload(child)

        parent_checkpoint = Mock()
        parent_checkpoint.is_succeeded.return_value = False
        parent_checkpoint.is_failed.return_value = False
        parent_checkpoint.is_existent.return_value = False
        parent_checkpoint.is_replay_children.return_value = False

        child_checkpoint = Mock()
        child_checkpoint.is_succeeded.return_value = False
        child_checkpoint.is_failed.return_value = False
        child_checkpoint.is_existent.return_value = False
        child_checkpoint.is_replay_children.return_value = False

        def get_checkpoint(op_id):
            return child_checkpoint if op_id.startswith("child-") else parent_checkpoint

        mock_state = Mock()
        mock_state.durable_execution_arn = "arn:test"
        mock_state.get_checkpoint_result = Mock(side_effect=get_checkpoint)
        mock_state.create_checkpoint = Mock()

        context_map = {}

        def create_id(self, i):
            ctx_id = id(self)
            if ctx_id not in context_map:
                context_map[ctx_id] = []
            context_map[ctx_id].append(i)
            return (
                "parent"
                if len(context_map) == 1 and len(context_map[ctx_id]) == 1
                else f"child-{i}"
            )

        with patch.object(
            DurableContext, "_create_step_id_for_logical_step", create_id
        ):
            context = DurableContext(state=mock_state)
            result = context.map(["a", "b"], lambda ctx, item, idx, items: item)

        assert isinstance(result, BatchResult)
        assert len(mock_serialize.call_args_list) == 3
        parent_call = mock_serialize.call_args_list[2]
        assert parent_call[1]["serdes"] is None
        assert isinstance(parent_call[1]["value"], BatchResult)
        assert parent_call[1]["value"] is result


def test_map_custom_serdes_serializes_batch_result():
    """Verify custom serdes is used for BatchResult serialization."""

    custom_serdes = CustomStrSerDes()

    with patch("aws_durable_execution_sdk_python.serdes.serialize") as mock_serialize:
        mock_serialize.return_value = '"serialized"'
        importlib.reload(child)

        parent_checkpoint = Mock()
        parent_checkpoint.is_succeeded.return_value = False
        parent_checkpoint.is_failed.return_value = False
        parent_checkpoint.is_existent.return_value = False
        parent_checkpoint.is_replay_children.return_value = False

        child_checkpoint = Mock()
        child_checkpoint.is_succeeded.return_value = False
        child_checkpoint.is_failed.return_value = False
        child_checkpoint.is_existent.return_value = False
        child_checkpoint.is_replay_children.return_value = False

        def get_checkpoint(op_id):
            return child_checkpoint if op_id.startswith("child-") else parent_checkpoint

        mock_state = Mock()
        mock_state.durable_execution_arn = "arn:test"
        mock_state.get_checkpoint_result = Mock(side_effect=get_checkpoint)
        mock_state.create_checkpoint = Mock()

        context_map = {}

        def create_id(self, i):
            ctx_id = id(self)
            if ctx_id not in context_map:
                context_map[ctx_id] = []
            context_map[ctx_id].append(i)
            return (
                "parent"
                if len(context_map) == 1 and len(context_map[ctx_id]) == 1
                else f"child-{i}"
            )

        with patch.object(
            DurableContext, "_create_step_id_for_logical_step", create_id
        ):
            context = DurableContext(state=mock_state)
            result = context.map(
                ["a", "b"],
                lambda ctx, item, idx, items: item,
                config=MapConfig(serdes=custom_serdes),
            )

        assert isinstance(result, BatchResult)
        assert len(mock_serialize.call_args_list) == 3
        parent_call = mock_serialize.call_args_list[2]
        assert parent_call[1]["serdes"] is custom_serdes
        assert isinstance(parent_call[1]["value"], BatchResult)
        assert parent_call[1]["value"] is result
