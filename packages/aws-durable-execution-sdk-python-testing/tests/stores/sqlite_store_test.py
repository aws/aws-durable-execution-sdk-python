"""Tests for SQLiteExecutionStore."""

import tempfile
import time
from datetime import datetime, UTC
from pathlib import Path

import pytest

from aws_durable_execution_sdk_python_testing.exceptions import (
    ResourceNotFoundException,
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import (
    ExecutionStatus,
    Execution,
)
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.sqlite import SQLiteExecutionStore


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = Path(temp_file.name)
    yield temp_path
    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def store(temp_db_path):
    """Create a SQLiteExecutionStore with temporary database."""
    return SQLiteExecutionStore.create_and_initialize(temp_db_path)


@pytest.fixture
def sample_execution():
    """Create a sample execution for testing."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    return Execution.new(input_data)


def test_sqlite_execution_store_save_and_load(store, sample_execution):
    """Test saving and loading an execution."""
    sample_execution.start()
    store.save(sample_execution)
    loaded_execution = store.load(sample_execution.durable_execution_arn)

    assert (
        loaded_execution.durable_execution_arn == sample_execution.durable_execution_arn
    )
    assert (
        loaded_execution.start_input.function_name
        == sample_execution.start_input.function_name
    )
    assert (
        loaded_execution.start_input.execution_name
        == sample_execution.start_input.execution_name
    )
    assert loaded_execution.token_sequence == sample_execution.token_sequence
    assert loaded_execution.is_complete == sample_execution.is_complete


def test_sqlite_execution_store_load_nonexistent(store):
    """Test loading a nonexistent execution raises KeyError."""
    with pytest.raises(
        ResourceNotFoundException, match="Execution nonexistent-arn not found"
    ):
        store.load("nonexistent-arn")


def test_sqlite_execution_store_update(store, sample_execution):
    """Test updating an execution."""
    sample_execution.start()
    store.save(sample_execution)

    sample_execution.is_complete = True
    sample_execution.close_status = ExecutionStatus.SUCCEEDED
    for _ in range(5):
        sample_execution.get_new_checkpoint_token()
    store.update(sample_execution)

    loaded_execution = store.load(sample_execution.durable_execution_arn)
    assert loaded_execution.is_complete is True
    assert loaded_execution.token_sequence == 5


def test_sqlite_execution_store_update_overwrites(store):
    """Test that update overwrites existing execution."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution1 = Execution.new(input_data)
    execution1.start()
    execution2 = Execution.new(input_data)
    execution2.start()
    execution2.durable_execution_arn = execution1.durable_execution_arn
    for _ in range(10):
        execution2.get_new_checkpoint_token()

    store.save(execution1)
    store.update(execution2)

    loaded_execution = store.load(execution1.durable_execution_arn)
    assert loaded_execution.token_sequence == 10


def test_sqlite_execution_store_multiple_executions(store):
    """Test storing multiple executions."""
    input_data1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-1",
        function_qualifier="$LATEST",
        execution_name="test-execution-1",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id-1",
    )
    input_data2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-2",
        function_qualifier="$LATEST",
        execution_name="test-execution-2",
        execution_timeout_seconds=600,
        execution_retention_period_days=14,
        invocation_id="test-invocation-id-2",
    )

    execution1 = Execution.new(input_data1)
    execution1.start()
    execution2 = Execution.new(input_data2)
    execution2.start()

    store.save(execution1)
    store.save(execution2)

    loaded_execution1 = store.load(execution1.durable_execution_arn)
    loaded_execution2 = store.load(execution2.durable_execution_arn)

    assert loaded_execution1.durable_execution_arn == execution1.durable_execution_arn
    assert loaded_execution2.durable_execution_arn == execution2.durable_execution_arn
    assert loaded_execution1.start_input.function_name == "test-function-1"
    assert loaded_execution2.start_input.function_name == "test-function-2"


def test_sqlite_execution_store_list_all_empty(store):
    """Test list_all method with empty store."""
    result = store.list_all()
    assert result == []


def test_sqlite_execution_store_list_all_with_executions(store):
    """Test list_all method with multiple executions."""
    # Create test executions
    executions = []
    for i in range(3):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name=f"test-function-{i}",
            function_qualifier="$LATEST",
            execution_name=f"test-execution-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"test-invocation-id-{i}",
        )
        execution = Execution.new(input_data)
        execution.start()
        executions.append(execution)
        store.save(execution)

    # Test list_all
    result = store.list_all()

    assert len(result) == 3
    arns = {execution.durable_execution_arn for execution in result}
    for execution in executions:
        assert execution.durable_execution_arn in arns


def test_sqlite_execution_store_query_empty(store):
    """Test query method with empty store."""
    executions, next_marker = store.query()

    assert executions == []
    assert next_marker is None


def test_sqlite_execution_store_query_by_function_name(store):
    """Test query filtering by function name."""
    # Create executions with different function names
    input1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="function-a",
        function_qualifier="$LATEST",
        execution_name="exec-1",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-1",
    )
    input2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="function-b",
        function_qualifier="$LATEST",
        execution_name="exec-2",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-2",
    )

    exec1 = Execution.new(input1)
    exec1.start()
    exec2 = Execution.new(input2)
    exec2.start()
    store.save(exec1)
    store.save(exec2)

    # Query for function-a only
    executions, next_marker = store.query(function_name="function-a")

    assert len(executions) == 1
    assert executions[0].durable_execution_arn == exec1.durable_execution_arn
    assert next_marker is None


def test_sqlite_execution_store_query_by_execution_name(store):
    """Test query filtering by execution name."""
    input1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="exec-alpha",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-1",
    )
    input2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="exec-beta",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-2",
    )

    exec1 = Execution.new(input1)
    exec1.start()
    exec2 = Execution.new(input2)
    exec2.start()
    store.save(exec1)
    store.save(exec2)

    executions, next_marker = store.query(execution_name="exec-beta")

    assert len(executions) == 1
    assert executions[0].durable_execution_arn == exec2.durable_execution_arn


def test_sqlite_execution_store_query_by_status(store):
    """Test query filtering by status."""
    # Create running execution
    input1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="running-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-1",
    )
    exec1 = Execution.new(input1)
    exec1.start()

    # Create completed execution
    input2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="completed-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-2",
    )
    exec2 = Execution.new(input2)
    exec2.start()
    exec2.complete_success("success result")

    store.save(exec1)
    store.save(exec2)

    # Query for running executions
    executions, next_marker = store.query(status_filter="RUNNING")

    assert len(executions) == 1
    assert executions[0].durable_execution_arn == exec1.durable_execution_arn

    # Query for succeeded executions
    executions, next_marker = store.query(status_filter="SUCCEEDED")

    assert len(executions) == 1
    assert executions[0].durable_execution_arn == exec2.durable_execution_arn


def test_sqlite_execution_store_query_pagination(store):
    """Test query pagination."""
    # Create multiple executions
    executions = []
    for i in range(5):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"exec-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"invocation-{i}",
        )
        exec_obj = Execution.new(input_data)
        exec_obj.start()
        executions.append(exec_obj)
        store.save(exec_obj)

    # Test first page
    executions, next_marker = store.query(limit=2, offset=0)

    assert len(executions) == 2
    assert next_marker is not None

    # Test second page
    executions, next_marker = store.query(limit=2, offset=2)

    assert len(executions) == 2
    assert next_marker is not None

    # Test last page
    executions, next_marker = store.query(limit=2, offset=4)

    assert len(executions) == 1
    assert next_marker is None


def test_sqlite_execution_store_query_sorting(store):
    """Test query sorting by timestamp."""
    # Create executions - they will be sorted by creation order
    executions = []
    for i in range(3):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"exec-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"invocation-{i}",
        )
        exec_obj = Execution.new(input_data)
        exec_obj.start()
        executions.append(exec_obj)
        store.save(exec_obj)

    # Test ascending order (default)
    executions, next_marker = store.query(reverse_order=False)

    assert len(executions) == 3

    # Test descending order
    executions, next_marker = store.query(reverse_order=True)

    assert len(executions) == 3


def test_sqlite_execution_store_query_combined_filters(store):
    """Test query with multiple filters combined."""
    # Create various executions
    inputs = [
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function-a",
            function_qualifier="$LATEST",
            execution_name="target-exec",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-1",
        ),
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function-b",
            function_qualifier="$LATEST",
            execution_name="target-exec",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-2",
        ),
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function-a",
            function_qualifier="$LATEST",
            execution_name="other-exec",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-3",
        ),
    ]

    executions = []
    for input_data in inputs:
        exec_obj = Execution.new(input_data)
        exec_obj.start()
        executions.append(exec_obj)
        store.save(exec_obj)

    # Query with both function_name and execution_name filters
    filtered_executions, next_marker = store.query(
        function_name="function-a", execution_name="target-exec"
    )

    assert len(filtered_executions) == 1
    assert (
        filtered_executions[0].durable_execution_arn
        == executions[0].durable_execution_arn
    )


def test_sqlite_execution_store_database_initialization(temp_db_path):
    """Test that database is properly initialized with schema."""
    store = SQLiteExecutionStore.create_and_initialize(temp_db_path)

    # Verify database file exists
    assert temp_db_path.exists()

    # Verify we can perform basic operations
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()

    store.save(execution)
    loaded = store.load(execution.durable_execution_arn)
    assert loaded.durable_execution_arn == execution.durable_execution_arn


def test_sqlite_execution_store_custom_db_path():
    """Test creating store with custom database path."""
    with tempfile.TemporaryDirectory() as temp_dir:
        custom_path = Path(temp_dir) / "custom" / "executions.db"
        store = SQLiteExecutionStore.create_and_initialize(custom_path)

        # Directory should be created
        assert custom_path.parent.exists()
        assert custom_path.exists()

        # Verify functionality
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name="test-execution",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="test-invocation-id",
        )
        execution = Execution.new(input_data)
        execution.start()

        store.save(execution)
        loaded = store.load(execution.durable_execution_arn)
        assert loaded.durable_execution_arn == execution.durable_execution_arn


def test_sqlite_execution_store_failed_execution_status(store):
    """Test that failed executions are properly stored and queried."""
    from aws_durable_execution_sdk_python.lambda_service import ErrorObject

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="failed-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()

    # Complete with failure
    error = ErrorObject(
        type="TestError", message="Test failure", data=None, stack_trace=None
    )
    execution.complete_fail(error)

    store.save(execution)

    # Query for failed executions
    executions, next_marker = store.query(status_filter="FAILED")

    assert len(executions) == 1
    assert executions[0].durable_execution_arn == execution.durable_execution_arn
    assert executions[0].is_complete is True


def test_sqlite_execution_store_error_handling(temp_db_path):
    """Test error handling for database operations."""
    store = SQLiteExecutionStore.create_and_initialize(temp_db_path)

    # Test with corrupted database by removing the file after creation
    temp_db_path.unlink()

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()

    # Should raise RuntimeError for database operations
    with pytest.raises(RuntimeError, match="Failed to save execution"):
        store.save(execution)


def test_sqlite_execution_store_invalid_execution_data(store):
    """Test handling of invalid execution data."""
    # Create execution and start it
    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name="test-execution",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="test-invocation-id",
        )
    )
    execution.start()

    # Corrupt the execution object to trigger serialization error
    execution.start_input = None

    with pytest.raises(ValueError, match="Invalid execution data"):
        store.save(execution)


def test_sqlite_execution_store_sql_injection_protection(store):
    """Test SQL injection protection in query parameters."""
    # Create test execution
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()
    store.save(execution)

    # Try SQL injection attempts - should be safely parameterized
    malicious_inputs = [
        "'; DROP TABLE executions; --",
        "test' OR '1'='1",
        "test'; DELETE FROM executions; --",
        "test' UNION SELECT * FROM executions --",
    ]

    for malicious_input in malicious_inputs:
        # These should return empty results, not cause SQL errors
        executions, _ = store.query(function_name=malicious_input)
        assert executions == []

        executions, _ = store.query(execution_name=malicious_input)
        assert executions == []

        executions, _ = store.query(status_filter=malicious_input)
        assert executions == []


def test_sqlite_execution_store_time_filtering(store):
    """Test time-based filtering with edge cases."""

    # Create executions at different times
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )

    execution1 = Execution.new(input_data)
    execution1.start()
    store.save(execution1)

    # Small delay to ensure different timestamps
    time.sleep(0.01)

    execution2 = Execution.new(input_data)
    execution2.start()
    store.save(execution2)

    # Get timestamps as ISO strings
    start_time_iso = (
        execution1.get_operation_execution_started().start_timestamp.isoformat()
    )
    mid_time = (
        execution1.get_operation_execution_started().start_timestamp.timestamp() + 0.005
    )
    mid_time_iso = datetime.fromtimestamp(mid_time, tz=UTC).isoformat()
    end_time_iso = datetime.fromtimestamp(
        execution2.get_operation_execution_started().start_timestamp.timestamp() + 1,
        tz=UTC,
    ).isoformat()

    # Test started_after filter
    executions, _ = store.query(started_after=mid_time_iso)
    assert len(executions) == 1

    # Test started_before filter
    executions, _ = store.query(started_before=mid_time_iso)
    assert len(executions) == 1

    # Test both filters
    executions, _ = store.query(
        started_after=start_time_iso, started_before=end_time_iso
    )
    assert len(executions) == 2


def test_sqlite_execution_store_corrupted_data_handling(store, temp_db_path):
    """Test handling of corrupted JSON data in database."""
    import sqlite3

    # Insert corrupted JSON data directly
    with sqlite3.connect(temp_db_path) as conn:
        conn.execute(
            """
            INSERT INTO executions
            (durable_execution_arn, function_name, execution_name, status, start_timestamp, end_timestamp, data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "corrupted-arn",
                "test-function",
                "test-execution",
                "RUNNING",
                1234567890.0,
                None,
                "invalid json data {{{",
            ),
        )

    # Loading corrupted data should raise ValueError
    with pytest.raises(ValueError, match="Corrupted execution data"):
        store.load("corrupted-arn")

    # Query should skip corrupted records and continue
    executions, _ = store.query()
    # Should not include the corrupted record
    assert all(exec.durable_execution_arn != "corrupted-arn" for exec in executions)


def test_sqlite_execution_store_get_execution_metadata(store):
    """Test get_execution_metadata method."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()
    store.save(execution)

    # Test existing execution
    metadata = store.get_execution_metadata(execution.durable_execution_arn)
    assert metadata is not None
    assert metadata["durable_execution_arn"] == execution.durable_execution_arn
    assert metadata["function_name"] == "test-function"
    assert metadata["execution_name"] == "test-execution"
    assert metadata["status"] == "RUNNING"
    assert metadata["start_timestamp"] is not None

    # Test nonexistent execution
    metadata = store.get_execution_metadata("nonexistent-arn")
    assert metadata is None


def test_sqlite_execution_store_database_init_error():
    """Test database initialization error handling."""
    # Try to create database in non-existent directory without permission
    invalid_path = Path("/invalid/path/that/does/not/exist/test.db")

    with pytest.raises(RuntimeError, match="Failed to initialize database"):
        store = SQLiteExecutionStore(invalid_path)
        store._init_db()


def test_sqlite_execution_store_query_invalid_parameters(store):
    """Test query with invalid parameters."""
    # Test with invalid time parameters
    with pytest.raises(
        InvalidParameterValueException, match="Invalid query parameters"
    ):
        store.query(started_after="invalid_timestamp")

    with pytest.raises(
        InvalidParameterValueException, match="Invalid query parameters"
    ):
        store.query(started_before="not_a_number")


def test_sqlite_execution_store_query_no_limit_no_offset(store):
    """Test query without limit and offset parameters."""
    # Create test execution
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()
    store.save(execution)

    # Query without limit should use different code path
    executions, next_marker = store.query()
    assert len(executions) == 1
    assert next_marker is None


def test_sqlite_execution_store_query_with_end_timestamp(store):
    """Test execution with end timestamp."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()
    execution.complete_success("result")  # This should set end_timestamp
    store.save(execution)

    loaded = store.load(execution.durable_execution_arn)
    assert loaded.is_complete is True


def test_sqlite_execution_store_metadata_error_handling(temp_db_path):
    """Test metadata retrieval error handling."""
    store = SQLiteExecutionStore.create_and_initialize(temp_db_path)

    # Remove database file to trigger error
    temp_db_path.unlink()

    with pytest.raises(RuntimeError, match="Failed to get metadata"):
        store.get_execution_metadata("test-arn")


def test_sqlite_execution_store_load_error_handling(temp_db_path):
    """Test load error handling."""
    store = SQLiteExecutionStore.create_and_initialize(temp_db_path)

    # Remove database file to trigger error
    temp_db_path.unlink()

    with pytest.raises(RuntimeError, match="Failed to load execution"):
        store.load("test-arn")


def test_sqlite_execution_store_query_with_corrupted_data_warning(
    store, temp_db_path, capsys
):
    """Test that corrupted data in query results prints warning and continues."""
    import sqlite3

    # Create a valid execution first
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.start()
    store.save(execution)

    # Insert corrupted JSON data directly
    with sqlite3.connect(temp_db_path) as conn:
        conn.execute(
            """
            INSERT INTO executions
            (durable_execution_arn, function_name, execution_name, status, start_timestamp, end_timestamp, data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "corrupted-arn-2",
                "test-function",
                "test-execution",
                "RUNNING",
                1234567890.0,
                None,
                "invalid json data {{{",
            ),
        )

    # Query should skip corrupted records and print warning
    executions, _ = store.query()

    # Should get the valid execution, skip the corrupted one
    assert len(executions) == 1
    assert executions[0].durable_execution_arn == execution.durable_execution_arn

    # Check that warning was printed
    captured = capsys.readouterr()
    assert "Warning: Skipping corrupted execution corrupted-arn-2" in captured.out
