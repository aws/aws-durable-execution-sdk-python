"""Tests for the parallel operation module."""

import json
from unittest.mock import Mock, patch

import pytest

# Mock the executor.execute method to return a BatchResult
from aws_durable_execution_sdk_python.concurrency import (
    ConcurrentExecutor,
    Executable,
)
from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.operation.parallel import (
    ParallelExecutor,
    parallel_handler,
)
from aws_durable_execution_sdk_python.types import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
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

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    # Mock the run_in_child_context function
    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    with patch.object(ParallelExecutor, "execute", return_value=mock_batch_result):
        result = parallel_handler(
            callables,
            config,
            execution_state,
            mock_run_in_child_context,
            operation_identifier,
        )

        assert result == mock_batch_result


def test_parallel_handler_with_none_config():
    """Test parallel_handler function with None config."""

    def func1(ctx):
        return "result1"

    callables = [func1]

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    def mock_run_in_child_context(callable_func, name, child_config):
        return callable_func("mock-context")

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    with patch.object(ParallelExecutor, "execute", return_value=mock_batch_result):
        result = parallel_handler(
            callables,
            None,
            execution_state,
            mock_run_in_child_context,
            operation_identifier,
        )

        assert result == mock_batch_result


def test_parallel_handler_creates_executor_with_correct_config():
    """Test that parallel_handler creates ParallelExecutor with correct configuration."""

    def func1(ctx):
        return "result1"

    callables = [func1]
    config = ParallelConfig(max_concurrency=5)

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    with patch.object(ParallelExecutor, "from_callables") as mock_from_callables:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_callables.return_value = mock_executor

        result = parallel_handler(
            callables, config, execution_state, executor_context, operation_identifier
        )

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

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    with patch.object(ParallelExecutor, "from_callables") as mock_from_callables:
        mock_executor = Mock()
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor.execute.return_value = mock_batch_result
        mock_from_callables.return_value = mock_executor

        result = parallel_handler(
            callables, None, execution_state, executor_context, operation_identifier
        )

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

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = lambda *args: "1"  # noqa SLF001
    executor_context.create_child_context = lambda *args: Mock()

    result = parallel_handler(
        callables,
        ParallelConfig(serdes=CustomStrSerDes()),
        execution_state,
        executor_context,
        operation_identifier,
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

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(return_value="1")  # noqa SLF001
    executor_context.create_child_context = Mock(return_value=Mock())

    # Call parallel_handler
    parallel_handler(
        callables, config, execution_state, executor_context, operation_identifier
    )

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

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    executor_context = Mock()
    executor_context._create_step_id_for_logical_step = Mock(side_effect=["1", "2"])  # noqa SLF001
    executor_context.create_child_context = Mock(return_value=Mock())

    # Call parallel_handler with None config (should use default)
    parallel_handler(
        callables, None, execution_state, executor_context, operation_identifier
    )

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

    class MockExecutionState:
        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

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
        operation_identifier=operation_identifier,
    )

    # Verify that create_child_context was called 3 times (N=3 jobs)
    assert executor_context.create_child_context.call_count == 3


def test_parallel_handler_replay_mechanism():
    """Test that parallel_handler uses replay when operation has already succeeded."""

    def func1(ctx):
        return "result1"

    def func2(ctx):
        return "result2"

    callables = [func1, func2]

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
    config = ParallelConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    # Mock parallel context
    parallel_context = Mock()
    parallel_context._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["child_1", "child_2"]
    )

    # Mock the executor's replay method
    with patch.object(ParallelExecutor, "replay") as mock_replay:
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

        result = parallel_handler(
            callables, config, execution_state, parallel_context, operation_identifier
        )

        # Verify replay was called instead of execute
        mock_replay.assert_called_once_with(execution_state, parallel_context)
        assert result == expected_batch_result


def test_parallel_handler_replay_with_replay_children():
    """Test parallel_handler replay when children need to be re-executed."""

    def func1(ctx):
        return "result1"

    callables = [func1]

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
    config = ParallelConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    # Mock parallel context
    parallel_context = Mock()
    parallel_context._create_step_id_for_logical_step = Mock(return_value="child_1")  # noqa: SLF001

    # Mock the executor's replay method and _execute_item_in_child_context
    with (
        patch.object(ParallelExecutor, "replay") as mock_replay,
        patch.object(
            ParallelExecutor, "_execute_item_in_child_context"
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

        result = parallel_handler(
            callables, config, execution_state, parallel_context, operation_identifier
        )

        mock_replay.assert_called_once_with(execution_state, parallel_context)
        assert result == expected_batch_result


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


def test_parallel_handler_first_execution_then_replay():
    """Test parallel_handler called twice - first calls execute, second calls replay."""

    def task1(ctx):
        return "result1"

    def task2(ctx):
        return "result2"

    callables = [task1, task2]
    config = ParallelConfig()
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

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
    parallel_context = Mock()

    with (
        patch(
            "aws_durable_execution_sdk_python.operation.parallel.ParallelExecutor.execute"
        ) as mock_execute,
        patch(
            "aws_durable_execution_sdk_python.operation.parallel.ParallelExecutor.replay"
        ) as mock_replay,
    ):
        mock_execute.return_value = Mock()  # Mock BatchResult
        mock_replay.return_value = Mock()  # Mock BatchResult

        # FIRST EXECUTION - should call execute
        execution_count = 0
        parallel_handler(
            callables, config, execution_state, parallel_context, operation_identifier
        )

        # Verify execute was called, replay was not
        mock_execute.assert_called_once()
        mock_replay.assert_not_called()

        # Reset mocks for second call
        mock_execute.reset_mock()
        mock_replay.reset_mock()

        # SECOND EXECUTION - should call replay
        execution_count = 1
        parallel_handler(
            callables, config, execution_state, parallel_context, operation_identifier
        )

        # Verify replay was called, execute was not
        mock_replay.assert_called_once()
        mock_execute.assert_not_called()


def test_parallel_result_serialization_roundtrip():
    """Test that parallel operation BatchResult can be serialized and deserialized."""

    def func1(ctx):
        return [1, 2, 3]

    def func2(ctx):
        return {"status": "complete", "count": 42}

    def func3(ctx):
        return "simple string"

    callables = [func1, func2, func3]

    class MockExecutionState:
        durable_execution_arn = "arn:test"

        def get_checkpoint_result(self, operation_id):
            mock_result = Mock()
            mock_result.is_succeeded.return_value = False
            return mock_result

    execution_state = MockExecutionState()
    parallel_context = Mock()
    parallel_context._create_step_id_for_logical_step = Mock(  # noqa SLF001
        side_effect=["1", "2", "3"]
    )
    parallel_context.create_child_context = Mock(return_value=Mock())
    operation_identifier = OperationIdentifier("test_op", "parent", "test_parallel")

    # Execute parallel
    result = parallel_handler(
        callables,
        ParallelConfig(),
        execution_state,
        parallel_context,
        operation_identifier,
    )

    # Serialize the BatchResult
    serialized = json.dumps(result.to_dict())

    # Deserialize
    deserialized = BatchResult.from_dict(json.loads(serialized))

    # Verify all data preserved
    assert len(deserialized.all) == 3
    assert deserialized.all[0].result == [1, 2, 3]
    assert deserialized.all[1].result == {"status": "complete", "count": 42}
    assert deserialized.all[2].result == "simple string"
    assert deserialized.completion_reason == result.completion_reason
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in deserialized.all)


@pytest.mark.parametrize(
    ("item_serdes", "batch_serdes"),
    [
        (Mock(), Mock()),
        (None, Mock()),
        (Mock(), None),
    ],
)
@patch("aws_durable_execution_sdk_python.operation.child.serialize")
def test_parallel_item_serialize(mock_serialize, item_serdes, batch_serdes):
    """Test parallel serializes branches with item_serdes or fallback."""
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
        context.parallel(
            [lambda ctx: "a", lambda ctx: "b"],
            config=ParallelConfig(serdes=batch_serdes, item_serdes=item_serdes),
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
def test_parallel_item_deserialize(mock_deserialize, item_serdes, batch_serdes):
    """Test parallel deserializes branches with item_serdes or fallback."""
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
        context.parallel(
            [lambda ctx: "a", lambda ctx: "b"],
            config=ParallelConfig(serdes=batch_serdes, item_serdes=item_serdes),
        )

    expected = item_serdes or batch_serdes
    assert mock_deserialize.call_args_list[0][1]["serdes"] is expected
    assert mock_deserialize.call_args_list[0][1]["operation_id"] == "child-0"
    assert mock_deserialize.call_args_list[1][1]["serdes"] is expected
    assert mock_deserialize.call_args_list[1][1]["operation_id"] == "child-1"
