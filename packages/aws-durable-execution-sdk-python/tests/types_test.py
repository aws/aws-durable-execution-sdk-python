"""Tests for the types module."""

from unittest.mock import Mock

from aws_durable_execution_sdk_python.config import (
    BatchedInput,
    CallbackConfig,
    ChildConfig,
    MapConfig,
    ParallelConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.types import Callback, DurableContext


def test_callback_protocol():
    """Test Callback protocol implementation."""
    # Create a mock that implements the Callback protocol
    mock_callback = Mock(spec=Callback)
    mock_callback.callback_id = "test-callback-123"
    mock_callback.result.return_value = "test_result"

    # Test protocol methods
    assert mock_callback.callback_id == "test-callback-123"
    result = mock_callback.result()
    assert result == "test_result"


def test_durable_context_protocol():
    """Test DurableContext protocol implementation."""
    # Create a mock that implements the DurableContext protocol
    mock_context = Mock(spec=DurableContext)

    # Test step method
    def test_callable():
        return "step_result"

    mock_context.step.return_value = "step_result"
    result = mock_context.step(test_callable, name="test_step", config=StepConfig())
    assert result == "step_result"
    mock_context.step.assert_called_once_with(
        test_callable, name="test_step", config=StepConfig()
    )

    # Test run_in_child_context method
    def child_callable(ctx):
        return "child_result"

    mock_context.run_in_child_context.return_value = "child_result"
    result = mock_context.run_in_child_context(
        child_callable, name="test_child", config=ChildConfig()
    )
    assert result == "child_result"
    mock_context.run_in_child_context.assert_called_once_with(
        child_callable, name="test_child", config=ChildConfig()
    )

    # Test map method
    def map_function(ctx, item, index, items):
        return f"mapped_{item}"

    inputs = ["a", "b", "c"]
    mock_context.map.return_value = ["mapped_a", "mapped_b", "mapped_c"]
    result = mock_context.map(inputs, map_function, name="test_map", config=MapConfig())
    assert result == ["mapped_a", "mapped_b", "mapped_c"]
    mock_context.map.assert_called_once_with(
        inputs, map_function, name="test_map", config=MapConfig()
    )

    # Test parallel method
    def callable1():
        return "result1"

    def callable2():
        return "result2"

    callables = [callable1, callable2]
    mock_context.parallel.return_value = ["result1", "result2"]
    result = mock_context.parallel(
        callables, name="test_parallel", config=ParallelConfig()
    )
    assert result == ["result1", "result2"]
    mock_context.parallel.assert_called_once_with(
        callables, name="test_parallel", config=ParallelConfig()
    )

    # Test wait method
    mock_context.wait(10, name="test_wait")
    mock_context.wait.assert_called_once_with(10, name="test_wait")

    # Test create_callback method
    mock_callback = Mock(spec=Callback)
    mock_context.create_callback.return_value = mock_callback
    result = mock_context.create_callback(name="test_callback", config=CallbackConfig())
    assert result == mock_callback
    mock_context.create_callback.assert_called_once_with(
        name="test_callback", config=CallbackConfig()
    )


def test_callback_protocol_with_none_values():
    """Test Callback protocol with None values."""
    mock_callback = Mock(spec=Callback)
    mock_callback.callback_id = "test-callback-456"
    mock_callback.result.return_value = None

    # Test with None result
    result = mock_callback.result()
    assert result is None


def test_durable_context_protocol_with_none_values():
    """Test DurableContext protocol with None values."""
    mock_context = Mock(spec=DurableContext)

    def test_callable():
        return "result"

    # Test methods with None names and configs
    mock_context.step.return_value = "result"
    mock_context.step(test_callable, name=None, config=None)
    mock_context.step.assert_called_once_with(test_callable, name=None, config=None)

    mock_context.run_in_child_context.return_value = "child_result"
    mock_context.run_in_child_context(test_callable, name=None, config=None)
    mock_context.run_in_child_context.assert_called_once_with(
        test_callable, name=None, config=None
    )

    mock_context.map.return_value = []
    mock_context.map([], test_callable, name=None, config=None)
    mock_context.map.assert_called_once_with([], test_callable, name=None, config=None)

    mock_context.parallel.return_value = []
    mock_context.parallel([], name=None, config=None)
    mock_context.parallel.assert_called_once_with([], name=None, config=None)

    mock_context.wait(5, name=None)
    mock_context.wait.assert_called_once_with(5, name=None)

    mock_callback = Mock(spec=Callback)
    mock_context.create_callback.return_value = mock_callback
    mock_context.create_callback(name=None, config=None)
    mock_context.create_callback.assert_called_once_with(name=None, config=None)


def test_map_with_batched_input():
    """Test map method with BatchedInput type."""
    mock_context = Mock(spec=DurableContext)

    def map_function(ctx, item, index, items):
        # item can be U or BatchedInput[Any, U]
        if isinstance(item, BatchedInput):
            return f"batched_{len(item.items)}"
        return f"single_{item}"

    # Test with regular inputs
    inputs = ["x", "y"]
    mock_context.map.return_value = ["single_x", "single_y"]
    result = mock_context.map(inputs, map_function)
    assert result == ["single_x", "single_y"]

    # Test with BatchedInput (correct constructor)
    batched_input = BatchedInput(batch_input="batch_data", items=["a", "b", "c"])
    inputs_with_batch = [batched_input]
    mock_context.map.return_value = ["batched_3"]
    result = mock_context.map(inputs_with_batch, map_function)
    assert result == ["batched_3"]


def test_protocol_abstract_methods():
    """Test that protocol methods are abstract and contain ellipsis."""
    # Test that the protocols have the expected abstract methods
    assert hasattr(Callback, "result")

    assert hasattr(DurableContext, "step")
    assert hasattr(DurableContext, "run_in_child_context")
    assert hasattr(DurableContext, "map")
    assert hasattr(DurableContext, "parallel")
    assert hasattr(DurableContext, "wait")
    assert hasattr(DurableContext, "create_callback")


def test_concrete_callback_implementation():
    """Test a concrete implementation of Callback protocol."""

    class ConcreteCallback:
        def __init__(self, callback_id: str):
            self.callback_id = callback_id
            self._result = None

        def result(self):
            return self._result

    # Test the concrete implementation
    callback = ConcreteCallback("test-123")
    assert callback.callback_id == "test-123"
    assert callback.result() is None
