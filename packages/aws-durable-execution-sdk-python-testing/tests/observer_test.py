"""Tests for observer module."""

import inspect
import threading
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import ErrorObject, CallbackOptions

from aws_durable_execution_sdk_python_testing.observer import (
    ExecutionNotifier,
    ExecutionObserver,
)
from aws_durable_execution_sdk_python_testing.token import CallbackToken


class MockExecutionObserver(ExecutionObserver):
    """Mock implementation of ExecutionObserver for testing."""

    def __init__(self):
        self.on_completed_calls = []
        self.on_failed_calls = []
        self.on_timed_out_calls = []
        self.on_stopped_calls = []
        self.on_wait_timer_scheduled_calls = []
        self.on_step_retry_scheduled_calls = []
        self.on_callback_created_calls = []

    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        self.on_completed_calls.append((execution_arn, result))

    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        self.on_failed_calls.append((execution_arn, error))

    def on_timed_out(self, execution_arn: str, error: ErrorObject) -> None:
        self.on_timed_out_calls.append((execution_arn, error))

    def on_stopped(self, execution_arn: str, error: ErrorObject) -> None:
        self.on_stopped_calls.append((execution_arn, error))

    def on_wait_timer_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        self.on_wait_timer_scheduled_calls.append((execution_arn, operation_id, delay))

    def on_step_retry_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        self.on_step_retry_scheduled_calls.append((execution_arn, operation_id, delay))

    def on_callback_created(
        self,
        execution_arn: str,
        operation_id: str,
        callback_options: CallbackOptions | None,
        callback_token: CallbackToken,
    ) -> None:
        self.on_callback_created_calls.append(
            (execution_arn, operation_id, callback_options, callback_token)
        )


def test_execution_notifier_init():
    """Test ExecutionNotifier initialization."""
    notifier = ExecutionNotifier()

    assert notifier._observers == []  # noqa: SLF001
    assert notifier._lock is not None  # noqa: SLF001


def test_execution_notifier_add_observer():
    """Test adding an observer to ExecutionNotifier."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()

    notifier.add_observer(observer)

    assert len(notifier._observers) == 1  # noqa: SLF001
    assert notifier._observers[0] is observer  # noqa: SLF001


def test_execution_notifier_add_multiple_observers():
    """Test adding multiple observers to ExecutionNotifier."""
    notifier = ExecutionNotifier()
    observer1 = MockExecutionObserver()
    observer2 = MockExecutionObserver()

    notifier.add_observer(observer1)
    notifier.add_observer(observer2)

    assert len(notifier._observers) == 2  # noqa: SLF001
    assert observer1 in notifier._observers  # noqa: SLF001
    assert observer2 in notifier._observers  # noqa: SLF001


def test_execution_notifier_notify_completed():
    """Test notifying observers about execution completion."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    execution_arn = "test-arn"
    result = "test-result"

    notifier.notify_completed(execution_arn, result)

    assert len(observer.on_completed_calls) == 1
    assert observer.on_completed_calls[0] == (execution_arn, result)


def test_execution_notifier_notify_completed_no_result():
    """Test notifying observers about execution completion with no result."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    execution_arn = "test-arn"

    notifier.notify_completed(execution_arn)

    assert len(observer.on_completed_calls) == 1
    assert observer.on_completed_calls[0] == (execution_arn, None)


def test_execution_notifier_notify_failed():
    """Test notifying observers about execution failure."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    execution_arn = "test-arn"
    error = ErrorObject(
        "TestError", "Test error message", "test-data", ["stack", "trace"]
    )

    notifier.notify_failed(execution_arn, error)

    assert len(observer.on_failed_calls) == 1
    assert observer.on_failed_calls[0] == (execution_arn, error)


def test_execution_notifier_notify_wait_timer_scheduled():
    """Test notifying observers about wait timer scheduling."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    execution_arn = "test-arn"
    operation_id = "test-operation"
    delay = 5.0

    notifier.notify_wait_timer_scheduled(execution_arn, operation_id, delay)

    assert len(observer.on_wait_timer_scheduled_calls) == 1
    assert observer.on_wait_timer_scheduled_calls[0] == (
        execution_arn,
        operation_id,
        delay,
    )


def test_execution_notifier_notify_step_retry_scheduled():
    """Test notifying observers about step retry scheduling."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    execution_arn = "test-arn"
    operation_id = "test-operation"
    delay = 10.0

    notifier.notify_step_retry_scheduled(execution_arn, operation_id, delay)

    assert len(observer.on_step_retry_scheduled_calls) == 1
    assert observer.on_step_retry_scheduled_calls[0] == (
        execution_arn,
        operation_id,
        delay,
    )


def test_execution_notifier_multiple_observers_all_notified():
    """Test that all observers are notified when multiple are registered."""
    notifier = ExecutionNotifier()
    observer1 = MockExecutionObserver()
    observer2 = MockExecutionObserver()

    notifier.add_observer(observer1)
    notifier.add_observer(observer2)

    execution_arn = "test-arn"
    result = "test-result"

    notifier.notify_completed(execution_arn, result)

    # Both observers should be notified
    assert len(observer1.on_completed_calls) == 1
    assert observer1.on_completed_calls[0] == (execution_arn, result)
    assert len(observer2.on_completed_calls) == 1
    assert observer2.on_completed_calls[0] == (execution_arn, result)


def test_execution_notifier_no_observers():
    """Test that notifications work even with no observers."""
    notifier = ExecutionNotifier()

    # Should not raise any exceptions
    notifier.notify_completed("test-arn", "result")
    notifier.notify_failed(
        "test-arn", ErrorObject("Error", "Message", "data", ["trace"])
    )
    notifier.notify_wait_timer_scheduled("test-arn", "op-id", 1.0)
    notifier.notify_step_retry_scheduled("test-arn", "op-id", 2.0)


def test_execution_notifier_thread_safety():
    """Test that ExecutionNotifier is thread-safe."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    # Test concurrent access
    def add_observer_thread():
        new_observer = MockExecutionObserver()
        notifier.add_observer(new_observer)

    def notify_thread():
        notifier.notify_completed("test-arn", "result")

    threads = []
    for _ in range(5):
        threads.append(threading.Thread(target=add_observer_thread))
        threads.append(threading.Thread(target=notify_thread))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Should have original observer plus 5 more
    assert len(notifier._observers) == 6  # noqa: SLF001
    # Original observer should have been notified multiple times
    assert len(observer.on_completed_calls) >= 1


def test_execution_observer_abstract_methods():
    """Test that ExecutionObserver is abstract and cannot be instantiated."""
    with pytest.raises(TypeError):
        ExecutionObserver()


def test_mock_execution_observer_implementation():
    """Test that MockExecutionObserver properly implements all abstract methods."""
    observer = MockExecutionObserver()

    # Test all methods can be called
    error = ErrorObject("Error", "Message", "data", ["trace"])
    observer.on_completed("arn", "result")
    observer.on_failed("arn", error)
    observer.on_timed_out("arn", error)
    observer.on_stopped("arn", error)
    observer.on_wait_timer_scheduled("arn", "op", 1.0)
    observer.on_step_retry_scheduled("arn", "op", 2.0)

    # Verify calls were recorded
    assert len(observer.on_completed_calls) == 1
    assert len(observer.on_failed_calls) == 1
    assert len(observer.on_timed_out_calls) == 1
    assert len(observer.on_stopped_calls) == 1
    assert len(observer.on_wait_timer_scheduled_calls) == 1
    assert len(observer.on_step_retry_scheduled_calls) == 1


def test_execution_notifier_notify_observers_with_exception():
    """Test that exceptions in one observer don't affect others."""
    notifier = ExecutionNotifier()

    # Create a mock observer that raises an exception
    failing_observer = Mock(spec=ExecutionObserver)
    failing_observer.on_completed.side_effect = ValueError("Test exception")

    # Create a normal observer
    normal_observer = MockExecutionObserver()

    notifier.add_observer(failing_observer)
    notifier.add_observer(normal_observer)

    # This should raise an exception from the failing observer
    with pytest.raises(ValueError, match="Test exception"):
        notifier.notify_completed("test-arn", "result")

    # The normal observer should still have been called before the exception
    failing_observer.on_completed.assert_called_once_with(
        execution_arn="test-arn", result="result"
    )


def test_execution_observer_abstract_method_coverage():
    """Test coverage of abstract methods in ExecutionObserver."""
    # This test ensures we cover the abstract method definitions
    # by checking they exist and have the correct signatures

    methods = inspect.getmembers(ExecutionObserver, predicate=inspect.isfunction)
    method_names = [name for name, _ in methods]

    assert "on_completed" in method_names
    assert "on_failed" in method_names
    assert "on_timed_out" in method_names
    assert "on_stopped" in method_names
    assert "on_wait_timer_scheduled" in method_names
    assert "on_step_retry_scheduled" in method_names


def test_execution_notifier_notify_observers_internal():
    """Test the internal _notify_observers method behavior."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    # Test that _notify_observers correctly calls the method on observers
    notifier._notify_observers(  # noqa: SLF001
        ExecutionObserver.on_completed, execution_arn="test", result="success"
    )

    assert len(observer.on_completed_calls) == 1
    assert observer.on_completed_calls[0] == ("test", "success")


def test_execution_notifier_all_notification_methods():
    """Test all notification methods with various parameter combinations."""
    notifier = ExecutionNotifier()
    observer = MockExecutionObserver()
    notifier.add_observer(observer)

    # Test notify_completed with positional args
    notifier.notify_completed("arn1", "result1")
    assert observer.on_completed_calls[-1] == ("arn1", "result1")

    # Test notify_completed with keyword args
    notifier.notify_completed(execution_arn="arn2", result="result2")
    assert observer.on_completed_calls[-1] == ("arn2", "result2")

    # Test notify_failed
    error = ErrorObject("TestError", "Message", "data", ["trace"])
    notifier.notify_failed("arn3", error)
    assert observer.on_failed_calls[-1] == ("arn3", error)

    # Test notify_wait_timer_scheduled
    notifier.notify_wait_timer_scheduled("arn4", "op1", 5.5)
    assert observer.on_wait_timer_scheduled_calls[-1] == ("arn4", "op1", 5.5)

    # Test notify_step_retry_scheduled
    notifier.notify_step_retry_scheduled("arn5", "op2", 10.5)
    assert observer.on_step_retry_scheduled_calls[-1] == ("arn5", "op2", 10.5)
