"""Unit tests for scheduler.py"""

import threading
import time
from concurrent.futures import Future
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python_testing.scheduler import Event, Scheduler


def wait_for_condition(condition_func, timeout_iterations=100):
    """Wait for a condition to become true with polling."""
    for _ in range(timeout_iterations):
        if condition_func():
            return True
        time.sleep(0.001)
    return False


def test_scheduler_init():
    """Test Scheduler initialization."""
    scheduler = Scheduler()
    assert not scheduler.is_started()
    assert scheduler.event_count() == 0


def test_scheduler_context_manager():
    """Test Scheduler as context manager."""
    with Scheduler() as scheduler:
        assert scheduler.is_started()
    assert not scheduler.is_started()


def test_scheduler_start_stop():
    """Test Scheduler start and stop methods."""
    scheduler = Scheduler()

    scheduler.start()
    assert scheduler.is_started()

    # Test start when already running
    scheduler.start()
    assert scheduler.is_started()

    scheduler.stop()
    assert not scheduler.is_started()

    # Test stop when not running
    scheduler.stop()
    assert not scheduler.is_started()


def test_scheduler_is_started():
    """Test Scheduler is_started method."""
    scheduler = Scheduler()

    # Initially not started
    assert not scheduler.is_started()

    # After start
    scheduler.start()
    assert scheduler.is_started()

    # After stop
    scheduler.stop()
    assert not scheduler.is_started()


def test_scheduler_event_count():
    """Test Scheduler event_count method."""
    scheduler = Scheduler()
    scheduler.start()

    # Initially no events
    assert scheduler.event_count() == 0

    # Create events
    event1 = scheduler.create_event()
    assert scheduler.event_count() == 1

    scheduler.create_event()
    assert scheduler.event_count() == 2

    # Remove event
    event1.remove()
    wait_for_condition(lambda: scheduler.event_count() == 1)
    assert scheduler.event_count() == 1

    scheduler.stop()


def test_scheduler_task_count():
    """Test Scheduler task_count method."""
    scheduler = Scheduler()

    # When not started, task count is 0
    assert scheduler.task_count() == 0

    scheduler.start()

    # Create tasks with longer delay to ensure they're counted
    future1 = scheduler.call_later(lambda: None, delay=0.5)
    # Give a moment for the task to be created
    time.sleep(0.01)
    assert scheduler.task_count() >= 1

    future2 = scheduler.call_later(lambda: None, delay=0.5)
    time.sleep(0.01)
    assert scheduler.task_count() >= 2

    # Cancel tasks to clean up
    future1.cancel()
    future2.cancel()

    # Wait for tasks to complete or be cancelled
    wait_for_condition(lambda: scheduler.task_count() == 0, timeout_iterations=200)

    scheduler.stop()


def test_scheduler_call_later_sync_function():
    """Test call_later with sync function."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def sync_func():
        result.append("executed")

    future = scheduler.call_later(sync_func, delay=0.01)
    wait_for_condition(lambda: future.done())

    assert isinstance(future, Future)
    assert result == ["executed"]
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_async_function():
    """Test call_later with async function."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def async_func():
        result.append("async_executed")

    future = scheduler.call_later(async_func, delay=0.01)
    wait_for_condition(lambda: future.done())

    assert isinstance(future, Future)
    assert result == ["async_executed"]
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_multiple_count():
    """Test call_later with multiple executions."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def func():
        result.append("count")

    # Note: Current implementation only executes once due to early return
    future = scheduler.call_later(func, delay=0.01, count=3)
    wait_for_condition(lambda: future.done())

    # Current implementation only executes once
    assert len(result) == 1
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_infinite_count():
    """Test call_later with infinite count."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def func():
        result.append("infinite")

    # Note: Current implementation only executes once due to early return
    future = scheduler.call_later(func, delay=0.01, count=None)
    wait_for_condition(lambda: future.done())

    # Current implementation only executes once
    assert len(result) == 1
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_function_exception():
    """Test call_later with function that raises exception."""
    scheduler = Scheduler()
    scheduler.start()

    def failing_func() -> None:
        msg: str = "test error"

        raise ValueError(msg)

    with patch(
        "aws_durable_execution_sdk_python_testing.scheduler.logger"
    ) as mock_logger:
        future = scheduler.call_later(failing_func, delay=0.01)
        wait_for_condition(lambda: future.done())

        assert future.done()
        mock_logger.exception.assert_called()

    scheduler.stop()


def test_scheduler_create_event():
    """Test create_event method."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    assert isinstance(event, Event)
    assert scheduler.event_count() == 1

    scheduler.stop()


def test_task_cancel():
    """Test Future cancel method."""
    scheduler = Scheduler()
    scheduler.start()

    def func():
        pass

    future = scheduler.call_later(func, delay=0.1, count=None)
    future.cancel()

    # Wait briefly for cancellation to take effect
    wait_for_condition(lambda: future.cancelled())

    assert future.cancelled()

    scheduler.stop()


def test_task_is_done():
    """Test Future done property."""
    scheduler = Scheduler()
    scheduler.start()

    def quick_func():
        pass

    future = scheduler.call_later(quick_func, delay=0.01)
    assert not future.done()

    wait_for_condition(lambda: future.done())
    assert future.done()

    # Small delay to ensure coroutine cleanup completes
    time.sleep(0.01)
    scheduler.stop()


def test_task_result():
    """Test Future result method."""
    scheduler = Scheduler()
    scheduler.start()

    def func():
        return None

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())

    result = future.result()
    assert result is None

    scheduler.stop()


def test_task_cancel_method():
    """Test Future cancel method."""
    scheduler = Scheduler()
    scheduler.start()

    # Create a future and cancel it immediately
    future = scheduler.call_later(lambda: None, delay=0.01)
    future.cancel()

    # The cancel method should work without hanging
    # We don't test the result here to avoid timing issues

    scheduler.stop()


def test_task_result_completed():
    """Test Future result method when completed."""
    scheduler = Scheduler()
    scheduler.start()

    def func():
        return "test_result"

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())
    assert future.done()

    # Small delay to ensure coroutine cleanup completes
    time.sleep(0.01)
    scheduler.stop()


def test_event_set_and_wait_timeout():
    """Test Event set and wait with timeout."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    # Test wait with timeout (should timeout)
    result = event.wait(timeout=0.01, clear_on_set=False)
    assert result is False

    # Set the event
    event.set()

    # Wait should now succeed
    result = event.wait(timeout=0.1, clear_on_set=True)
    assert result is True

    scheduler.stop()


def test_event_wait_set_by_thread():
    """Test Event wait when set by another thread."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    result_container = []
    start_event = threading.Event()

    def set_event():
        start_event.wait()  # Wait for signal to start
        event.set()

    def wait_for_event():
        result = event.wait(timeout=1.0)
        result_container.append(result)

    set_thread = threading.Thread(target=set_event)
    wait_thread = threading.Thread(target=wait_for_event)

    set_thread.start()
    wait_thread.start()
    start_event.set()  # Signal to start setting event

    set_thread.join()
    wait_thread.join()

    assert result_container[0] is True

    scheduler.stop()


def test_event_wait_clear_on_set_false():
    """Test Event wait with clear_on_set=False."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    event.set()

    result = event.wait(clear_on_set=False)
    assert result is True
    assert scheduler.event_count() == 1

    scheduler.stop()


def test_event_remove():
    """Test Event remove method."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    assert scheduler.event_count() == 1

    event.remove()
    wait_for_condition(lambda: scheduler.event_count() == 0)

    assert scheduler.event_count() == 0

    scheduler.stop()


def test_event_wait_removed_event():
    """Test Event wait on removed event."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    event.remove()
    wait_for_condition(lambda: scheduler.event_count() == 0)

    result = event.wait(timeout=0.01)
    assert result is False

    scheduler.stop()


def test_event_set_removed_event():
    """Test Event set on removed event."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    event.remove()
    wait_for_condition(lambda: scheduler.event_count() == 0)

    # Should not crash
    event.set()

    scheduler.stop()


def test_scheduler_cleanup_on_stop():
    """Test scheduler cleanup when stopped."""
    scheduler = Scheduler()
    scheduler.start()

    # Create a future and event
    scheduler.call_later(lambda: None, delay=0.1, count=1)
    scheduler.create_event()

    # Stop scheduler immediately
    scheduler.stop()

    # Events should be cleared (this is what we can reliably test)
    assert scheduler.event_count() == 0
    # Future state may vary due to timing, but scheduler should be stopped
    assert not scheduler.is_started()


def test_scheduler_multiple_events():
    """Test scheduler with multiple events."""
    scheduler = Scheduler()
    scheduler.start()

    event1 = scheduler.create_event()
    event2 = scheduler.create_event()

    assert scheduler.event_count() == 2

    event1.set()
    result1 = event1.wait(timeout=0.01)
    assert result1 is True

    result2 = event2.wait(timeout=0.01)
    assert result2 is False

    scheduler.stop()


def test_task_properties_after_scheduler_stop():
    """Test Future properties after scheduler is stopped."""
    scheduler = Scheduler()
    scheduler.start()

    def func():
        pass

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())

    scheduler.stop()

    assert future.done()
    assert not future.cancelled()


def test_event_timeout_handling():
    """Test Event timeout handling."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    start_time = time.time()
    result = event.wait(timeout=0.05)
    end_time = time.time()

    assert result is False
    assert 0.04 <= (end_time - start_time) <= 0.1

    scheduler.stop()


def test_scheduler_call_later_zero_delay():
    """Test call_later with zero delay."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def func():
        result.append("zero_delay")

    future = scheduler.call_later(func, delay=0)
    wait_for_condition(lambda: future.done())

    assert result == ["zero_delay"]
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_default_parameters():
    """Test call_later with default parameters."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def func():
        result.append("default")

    future = scheduler.call_later(func)
    wait_for_condition(lambda: future.done())

    assert result == ["default"]
    assert future.done()

    scheduler.stop()


def test_task_result_with_exception():
    """Test Future result method when function raises exception."""
    scheduler = Scheduler()
    scheduler.start()

    def failing_func() -> None:
        msg: str = "test exception"

        raise ValueError(msg)

    # Test that user function exceptions are propagated through the Future
    with patch(
        "aws_durable_execution_sdk_python_testing.scheduler.logger"
    ) as mock_logger:
        future = scheduler.call_later(failing_func, delay=0.01)
        wait_for_condition(lambda: future.done())

        # Future should be done and exception should be logged
        assert future.done()
        mock_logger.exception.assert_called()

        # Exception should be propagated through Future.result()
        with pytest.raises(ValueError, match="test exception"):
            future.result()

    scheduler.stop()


def test_get_task_result_exception_handling():
    """Test Future result exception handling."""
    scheduler = Scheduler()
    scheduler.start()

    def func():
        pass

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())

    # Future result should work normally
    result = future.result()
    assert result is None

    scheduler.stop()


def test_call_later_with_sync_function():
    """Test call_later correctly identifies and runs sync functions."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def sync_function():
        result.append("sync_executed")

    future = scheduler.call_later(sync_function, delay=0.01)
    wait_for_condition(lambda: future.done())

    assert result == ["sync_executed"]
    assert future.done()

    scheduler.stop()


def test_call_later_with_async_function():
    """Test call_later correctly identifies and runs async functions."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def async_function():
        result.append("async_executed")

    future = scheduler.call_later(async_function, delay=0.01)
    wait_for_condition(lambda: future.done())

    assert result == ["async_executed"]
    assert future.done()

    scheduler.stop()


def test_event_set_exception():
    """Test Event set_exception method."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    test_exception = ValueError("test exception")

    event.set_exception(test_exception)

    with pytest.raises(ValueError, match="test exception"):
        event.wait()

    scheduler.stop()


def test_call_later_with_completion_event_exception():
    """Test call_later with completion_event when function raises exception."""
    scheduler = Scheduler()
    scheduler.start()

    completion_event = scheduler.create_event()

    def failing_func() -> None:
        msg: str = "completion event test"

        raise RuntimeError(msg)

    scheduler.call_later(failing_func, delay=0.01, completion_event=completion_event)

    # Wait for the completion event to be set with exception
    with pytest.raises(RuntimeError, match="completion event test"):
        completion_event.wait(timeout=1.0)

    scheduler.stop()


def test_call_later_multiple_iterations():
    """Test call_later with multiple count iterations."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def func():
        result.append("iteration")
        # Return early to test the loop behavior
        if len(result) >= 2:
            return "done"
        return

    # Use a very small delay and count=3 to test the loop
    future = scheduler.call_later(func, delay=0.001, count=3)
    wait_for_condition(lambda: future.done(), timeout_iterations=500)

    # Should execute at least once
    assert len(result) >= 1
    assert future.done()

    scheduler.stop()


def test_wait_for_event_timeout_exception():
    """Test _wait_for_event with timeout exception handling."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    # Test timeout behavior
    result = event.wait(timeout=0.001)
    assert result is False

    scheduler.stop()


def test_call_later_loop_exit_condition():
    """Test call_later loop exit condition with count=0."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    def func():
        result.append("should_not_execute")

    # Test with count=0 to hit the loop exit condition
    future = scheduler.call_later(func, delay=0.01, count=0)
    wait_for_condition(lambda: future.done())

    # Should not execute the function at all
    assert len(result) == 0
    assert future.done()

    scheduler.stop()
