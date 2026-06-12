"""Concurrent access tests for execution stores."""

import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.sqlite import SQLiteExecutionStore


def test_concurrent_save_load():
    """Test concurrent save and load operations."""
    store = InMemoryExecutionStore()
    results = []
    results_lock = threading.Lock()

    def save_execution(i: int):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        store.save(execution)
        with results_lock:
            results.append(f"saved-{i}")

    def load_execution(i: int):
        try:
            execution = store.load(f"arn-{i}")
            with results_lock:
                results.append(f"loaded-{execution.start_input.execution_name}")
        except KeyError:
            with results_lock:
                results.append(f"not-found-{i}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit save operations first
        futures = [executor.submit(save_execution, i) for i in range(5)]
        # Wait for saves to complete
        for future in as_completed(futures):
            future.result()

        # Then submit load operations
        futures = []
        for i in range(5):
            futures.append(executor.submit(load_execution, i))
        # Wait for loads to complete
        for future in as_completed(futures):
            future.result()

    assert len(results) == 10


def test_concurrent_update_list():
    """Test concurrent update and list operations."""
    store = InMemoryExecutionStore()
    results = []
    results_lock = threading.Lock()

    # Pre-populate store
    for i in range(3):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        store.save(execution)

    def update_execution(i: int):
        execution = store.load(f"arn-{i}")
        execution.is_complete = True
        store.update(execution)
        with results_lock:
            results.append(f"updated-{i}")

    def list_executions():
        executions = store.list_all()
        with results_lock:
            results.append(f"listed-{len(executions)}")

    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit update operations
        futures = [executor.submit(update_execution, i) for i in range(3)]
        # Submit list operations
        futures.extend([executor.submit(list_executions) for _ in range(3)])

        # Wait for all operations to complete
        for future in as_completed(futures):
            future.result()

    assert len(results) == 6
    final_list = store.list_all()
    assert len(final_list) == 3


@pytest.fixture
def temp_storage_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = Path(temp_file.name)
    yield temp_path
    if temp_path.exists():
        temp_path.unlink()


def test_concurrent_filesystem_save_load(temp_storage_dir):
    """Test concurrent save and load operations with filesystem store."""
    store = FileSystemExecutionStore.create(temp_storage_dir)
    results = []
    results_lock = threading.Lock()

    def save_execution(i: int):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        execution.start()
        store.save(execution)
        with results_lock:
            results.append(f"saved-{i}")

    def load_execution(i: int):
        try:
            execution = store.load(f"arn-{i}")
            with results_lock:
                results.append(f"loaded-{execution.start_input.execution_name}")
        except KeyError:
            with results_lock:
                results.append(f"not-found-{i}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit save operations first
        futures = [executor.submit(save_execution, i) for i in range(4)]
        for future in as_completed(futures):
            future.result()

        # Then submit load operations
        futures = [executor.submit(load_execution, i) for i in range(4)]
        for future in as_completed(futures):
            future.result()

    assert len(results) == 8


def test_concurrent_sqlite_save_load(temp_db_path):
    """Test concurrent save and load operations with SQLite store."""
    store = SQLiteExecutionStore.create_and_initialize(temp_db_path)
    results = []
    results_lock = threading.Lock()

    def save_execution(i: int):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        execution.start()
        store.save(execution)
        with results_lock:
            results.append(f"saved-{i}")

    def load_execution(i: int):
        try:
            execution = store.load(f"arn-{i}")
            with results_lock:
                results.append(f"loaded-{execution.start_input.execution_name}")
        except KeyError:
            with results_lock:
                results.append(f"not-found-{i}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit save operations first
        futures = [executor.submit(save_execution, i) for i in range(4)]
        for future in as_completed(futures):
            future.result()

        # Then submit load operations
        futures = [executor.submit(load_execution, i) for i in range(4)]
        for future in as_completed(futures):
            future.result()

    assert len(results) == 8


def test_concurrent_query_operations():
    """Test concurrent query operations on memory store."""
    store = InMemoryExecutionStore()
    results = []
    results_lock = threading.Lock()

    # Pre-populate store with test data
    for i in range(10):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name=f"function-{i % 3}",  # 3 different functions
            function_qualifier="$LATEST",
            execution_name=f"exec-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
        )
        execution = Execution.new(input_data)
        execution.start()
        # Complete some executions
        if i % 4 == 0:
            execution.complete_success("success")
        store.save(execution)

    def query_store(query_type: str):
        if query_type == "function":
            executions, next_marker = store.query(function_name="function-1")
        elif query_type == "status":
            executions, next_marker = store.query(status_filter="SUCCEEDED")
        elif query_type == "pagination":
            executions, next_marker = store.query(limit=3, offset=2)
        else:
            executions, next_marker = store.query()

        with results_lock:
            results.append(f"{query_type}-{len(executions)}")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(query_store, "function"),
            executor.submit(query_store, "status"),
            executor.submit(query_store, "pagination"),
            executor.submit(query_store, "all"),
        ]
        for future in as_completed(futures):
            future.result()

    assert len(results) == 4
