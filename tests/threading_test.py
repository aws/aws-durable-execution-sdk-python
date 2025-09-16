"""Tests for threading module."""

import threading
import time

import pytest

from aws_durable_functions_sdk_python.exceptions import OrderedLockError
from aws_durable_functions_sdk_python.threading import OrderedCounter, OrderedLock


# region OrderedLock
def test_ordered_lock_init():
    """Test OrderedLock initialization."""
    lock = OrderedLock()
    assert len(lock._waiters) == 0  # noqa: SLF001
    assert not lock.is_broken()
    assert lock._exception is None  # noqa: SLF001


def test_ordered_lock_acquire_release():
    """Test basic acquire and release functionality."""
    lock = OrderedLock()

    # First acquire should succeed immediately
    result = lock.acquire()
    assert result is True
    assert len(lock._waiters) == 1  # noqa: SLF001

    # Release should work
    lock.release()
    assert len(lock._waiters) == 0  # noqa: SLF001


def test_ordered_lock_context_manager():
    """Test OrderedLock as context manager."""
    lock = OrderedLock()

    with lock as acquired_lock:
        assert acquired_lock is lock
        assert len(lock._waiters) == 1  # noqa: SLF001

    assert len(lock._waiters) == 0  # noqa: SLF001


def test_ordered_lock_context_manager_with_exception():
    """Test OrderedLock context manager when exception occurs."""
    lock = OrderedLock()
    test_exception = ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        with lock:
            raise test_exception

    assert lock.is_broken()
    assert lock._exception is test_exception  # noqa: SLF001


def test_ordered_lock_acquire_broken_after_wait():
    """Test acquire fails when lock becomes broken after waiting."""
    lock = OrderedLock()
    test_exception = RuntimeError("test error")

    exception_container = []

    def first_thread():
        """Thread that will acquire and raise exception to break the lock."""
        try:
            with lock:
                time.sleep(0.1)  # Hold lock briefly
                raise test_exception
        except RuntimeError:
            pass  # Expected to raise

    def second_thread():
        """Thread that will wait and should get OrderedLockError."""
        try:
            lock.acquire()
        except OrderedLockError as e:
            exception_container.append(e)

    # Start first thread to acquire lock
    thread1 = threading.Thread(target=first_thread)
    thread1.start()

    # Give first thread time to acquire
    time.sleep(0.05)

    # Start second thread that will wait
    thread2 = threading.Thread(target=second_thread)
    thread2.start()

    # Wait for both threads
    thread1.join()
    thread2.join()

    # Second thread should have received OrderedLockError
    assert len(exception_container) == 1
    assert isinstance(exception_container[0], OrderedLockError)
    assert exception_container[0].source_exception is test_exception


def test_ordered_lock_ordering():
    """Test that locks are acquired in order."""
    lock = OrderedLock()
    results = []

    def worker(worker_id):
        lock.acquire()
        results.append(worker_id)
        time.sleep(0.1)  # Hold lock briefly
        lock.release()

    # Start multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
        time.sleep(0.01)  # Small delay to ensure order

    for thread in threads:
        thread.join()

    assert results == [0, 1, 2, 3, 4]


def test_ordered_lock_reset_success():
    """Test successful reset when no waiters."""
    lock = OrderedLock()
    test_exception = ValueError("test error")

    # Break the lock naturally using context manager
    with pytest.raises(ValueError, match="test error"):
        with lock:
            raise test_exception

    # Reset should succeed when no waiters
    lock.reset()

    # After reset, should be able to acquire again
    assert lock.acquire() is True
    lock.release()


def test_ordered_lock_reset_with_waiters():
    """Test reset fails when there are waiters."""
    lock = OrderedLock()
    reset_exception = None

    def waiting_thread():
        """Thread that will wait for the lock."""
        lock.acquire()
        lock.release()

    def reset_thread():
        """Thread that will try to reset while there are waiters."""
        nonlocal reset_exception
        try:
            time.sleep(0.1)  # Give waiting thread time to start waiting
            lock.reset()
        except OrderedLockError as e:
            reset_exception = e

    # First acquire the lock
    lock.acquire()

    # Start waiting thread
    waiter = threading.Thread(target=waiting_thread)
    waiter.start()

    # Give waiting thread time to start waiting
    time.sleep(0.05)

    # Start reset thread
    resetter = threading.Thread(target=reset_thread)
    resetter.start()

    # Wait for reset attempt
    resetter.join()

    # Release the lock to let waiter finish
    lock.release()
    waiter.join()

    # Reset should have failed
    assert reset_exception is not None
    assert isinstance(reset_exception, OrderedLockError)
    assert "Cannot reset lock because there are callers waiting" in str(reset_exception)
    assert reset_exception.source_exception is None


def test_ordered_lock_release_with_waiters():
    """Test release notifies next waiter after proper acquire."""
    lock = OrderedLock()

    # Properly acquire first
    lock.acquire()

    # Manually add another waiter to test release logic
    event2 = threading.Event()
    lock._waiters.append(event2)  # noqa: SLF001

    # Release should remove first waiter and set second
    lock.release()

    assert len(lock._waiters) == 1  # noqa: SLF001
    assert event2.is_set()


def test_ordered_lock_release_when_broken():
    """Test release doesn't notify next waiter when broken."""
    lock = OrderedLock()

    # Properly acquire first
    lock.acquire()

    # Add another waiter and break the lock
    event2 = threading.Event()
    lock._waiters.append(event2)  # noqa: SLF001
    lock._is_broken = True  # noqa: SLF001

    # Release should remove first waiter but not notify second
    lock.release()

    assert len(lock._waiters) == 1  # noqa: SLF001
    assert not event2.is_set()


def test_ordered_lock_exception_propagation() -> None:
    """Test exception propagation to waiting threads."""
    lock = OrderedLock()
    results: list[str] = []
    exceptions: list[tuple[int, Exception]] = []

    def worker(worker_id: int) -> None:
        try:
            with lock:
                results.append(f"acquired_{worker_id}")
                if worker_id == 0:
                    msg = "first worker error"
                    raise ValueError(msg)
                time.sleep(0.1)
        except (OrderedLockError, ValueError) as e:
            exceptions.append((worker_id, e))

    # Start multiple threads
    threads: list[threading.Thread] = []
    for i in range(3):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
        time.sleep(0.01)

    for thread in threads:
        thread.join()

    # First worker should have acquired and raised exception
    assert "acquired_0" in results

    # All workers should have exceptions
    assert len(exceptions) == 3

    # First exception should be the original ValueError
    first_exception = next(e for i, e in exceptions if i == 0)
    assert isinstance(first_exception, ValueError)

    # Other exceptions should be OrderedLockError
    other_exceptions = [e for i, e in exceptions if i != 0]
    for exc in other_exceptions:
        assert isinstance(exc, OrderedLockError)


def test_ordered_lock_multiple_acquire_release_cycles():
    """Test multiple acquire/release cycles work correctly."""
    lock = OrderedLock()

    for _ in range(5):
        assert lock.acquire() is True
        assert len(lock._waiters) == 1  # noqa: SLF001
        lock.release()
        assert len(lock._waiters) == 0  # noqa: SLF001


def test_ordered_lock_context_manager_normal_exit():
    """Test context manager with normal exit (no exception)."""
    lock = OrderedLock()

    with lock:
        assert len(lock._waiters) == 1  # noqa: SLF001
        assert not lock.is_broken()

    assert len(lock._waiters) == 0  # noqa: SLF001
    assert not lock.is_broken()


def test_ordered_lock_release_without_acquire():
    """Test release without acquire throws exception."""
    lock = OrderedLock()

    # Release without acquire should throw exception
    with pytest.raises(OrderedLockError):
        lock.release()


def test_ordered_lock_release_empty_queue_after_acquire():
    """Test release after manually clearing queue throws exception."""
    lock = OrderedLock()

    # Acquire properly first
    lock.acquire()

    # Manually clear the queue to simulate edge case
    lock._waiters.clear()  # noqa: SLF001

    # Release on empty queue should throw exception
    with pytest.raises(OrderedLockError):
        lock.release()


# endregion OrderedLock


# region OrderedCounter tests
def test_ordered_counter_init():
    """Test OrderedCounter initialization."""
    counter = OrderedCounter()
    assert counter.get_current() == 0


def test_ordered_counter_increment():
    """Test basic increment functionality."""
    counter = OrderedCounter()

    assert counter.increment() == 1
    assert counter.get_current() == 1

    assert counter.increment() == 2
    assert counter.get_current() == 2


def test_ordered_counter_decrement():
    """Test basic decrement functionality."""
    counter = OrderedCounter()

    counter.increment()
    counter.increment()
    assert counter.get_current() == 2

    assert counter.decrement() == 1
    assert counter.get_current() == 1

    assert counter.decrement() == 0
    assert counter.get_current() == 0


def test_ordered_counter_decrement_negative():
    """Test decrement can go negative."""
    counter = OrderedCounter()

    result = counter.decrement()
    assert result == -1
    assert counter.get_current() == -1


def test_ordered_counter_mixed_operations():
    """Test mixed increment and decrement operations."""
    counter = OrderedCounter()

    assert counter.increment() == 1
    assert counter.increment() == 2
    assert counter.decrement() == 1
    assert counter.increment() == 2
    assert counter.decrement() == 1
    assert counter.decrement() == 0
    assert counter.get_current() == 0


def test_ordered_counter_concurrent_increments():
    """Test concurrent increments maintain uniqueness and sequential values."""
    counter = OrderedCounter()
    results = []
    barrier = threading.Barrier(5)

    def worker(worker_id):
        barrier.wait()  # All threads start at the same time
        result = counter.increment()
        results.append((worker_id, result))

    # Start multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Each thread should get a unique counter value
    counter_values = [result[1] for result in results]
    assert sorted(counter_values) == [1, 2, 3, 4, 5]
    assert counter.get_current() == 5


def test_ordered_counter_concurrent_decrements():
    """Test concurrent decrements maintain uniqueness and sequential values."""
    counter = OrderedCounter()
    # Start with counter at 10
    for _ in range(10):
        counter.increment()

    results = []
    barrier = threading.Barrier(5)

    def worker(worker_id):
        barrier.wait()  # All threads start at the same time
        result = counter.decrement()
        results.append((worker_id, result))

    # Start multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Each thread should get a unique counter value
    counter_values = [result[1] for result in results]
    assert len(set(counter_values)) == 5  # All unique
    assert sorted(counter_values, reverse=True) == [9, 8, 7, 6, 5]
    assert counter.get_current() == 5


def test_ordered_counter_concurrent_mixed_operations():
    """Test concurrent mixed increment and decrement operations."""
    counter = OrderedCounter()
    results = []
    barrier = threading.Barrier(6)

    def increment_worker(worker_id):
        barrier.wait()  # increase contention deliberately for test - all increments start at same time
        result = counter.increment()
        results.append((f"inc_{worker_id}", result))

    def decrement_worker(worker_id):
        barrier.wait()  # All threads start at the same time
        result = counter.decrement()
        results.append((f"dec_{worker_id}", result))

    # Start mixed threads
    threads = []
    for i in range(3):
        inc_thread = threading.Thread(target=increment_worker, args=(i,))
        dec_thread = threading.Thread(target=decrement_worker, args=(i,))
        threads.extend([inc_thread, dec_thread])
        inc_thread.start()
        dec_thread.start()

    for thread in threads:
        thread.join()

    # Should have 6 operations total
    assert len(results) == 6

    # Final counter should be 0 (3 increments - 3 decrements)
    assert counter.get_current() == 0

    # All operations should complete (no race conditions)
    operation_results = [result[1] for result in results]
    assert len(operation_results) == 6


def test_ordered_counter_get_current_concurrent():
    """Test get_current works correctly during concurrent operations."""
    counter = OrderedCounter()
    get_current_results = []
    increment_results = []

    def get_current_worker():
        time.sleep(0.05)  # Let some increments happen first
        result = counter.get_current()
        get_current_results.append(result)

    def increment_worker():
        result = counter.increment()
        increment_results.append(result)
        time.sleep(0.01)

    # Start get_current thread
    get_thread = threading.Thread(target=get_current_worker)
    get_thread.start()

    # Start increment threads
    inc_threads = []
    for _ in range(3):
        thread = threading.Thread(target=increment_worker)
        inc_threads.append(thread)
        thread.start()

    # Wait for all threads
    get_thread.join()
    for thread in inc_threads:
        thread.join()

    # get_current should return a valid intermediate state
    assert len(get_current_results) == 1
    current_value = get_current_results[0]
    assert 0 <= current_value <= 3
    assert counter.get_current() == 3


def test_ordered_counter_ordering_guarantee() -> None:
    """Test that operations are processed in order even under contention."""
    counter = OrderedCounter()
    operation_order: list[tuple[int, str, int]] = []

    def worker(worker_id: int, operation: str) -> None:
        result = counter.increment() if operation == "+" else counter.decrement()

        operation_order.append((worker_id, operation, result))

    threads: list[threading.Thread] = []
    expected_operations: list[tuple[int, str]] = []

    for i in range(5):
        # Alternate between increment and decrement
        op = "+" if i % 2 == 0 else "-"
        expected_operations.append((i, op))
        thread = threading.Thread(target=worker, args=(i, op))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Operations should be processed in order
    assert len(operation_order) == 5

    # Check that we got the expected sequence of operations and results
    expected_results = [1, 0, 1, 0, 1]  # +1, -1, +1, -1, +1
    for i, (worker_id, op, result) in enumerate(operation_order):
        expected_worker_id, expected_op = expected_operations[i]
        assert worker_id == expected_worker_id
        assert op == expected_op
        assert result == expected_results[i]


def test_ordered_counter_exception_handling() -> None:
    """Test counter behavior when underlying lock encounters exceptions."""
    counter = OrderedCounter()
    results = []
    exceptions: list[tuple[int, Exception]] = []

    def worker_with_exception(worker_id: int) -> None:
        try:
            # deliberately messing with internal state here to make test work, thus noqa
            with counter._lock:  # noqa: SLF001
                counter._counter += 1  # noqa: SLF001
                result = counter._counter  # noqa: SLF001
                results.append((worker_id, result))
                if worker_id == 0:
                    msg = "test exception"
                    raise ValueError(msg)
        except (OrderedLockError, ValueError) as e:
            exceptions.append((worker_id, e))

    # Start multiple threads
    threads = []
    for i in range(3):
        thread = threading.Thread(target=worker_with_exception, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # First worker should have succeeded before exception
    assert len(results) >= 1
    assert results[0] == (0, 1)

    # All workers should have exceptions due to broken lock
    assert len(exceptions) == 3

    # After exception, OrderedLock is in fatal state - counter operations should fail
    with pytest.raises(OrderedLockError):
        counter.increment()

    with pytest.raises(OrderedLockError):
        counter.decrement()

    with pytest.raises(OrderedLockError):
        counter.get_current()


# endregion OrderedCounter tests
