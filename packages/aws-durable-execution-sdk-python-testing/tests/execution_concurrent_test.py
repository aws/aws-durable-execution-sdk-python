"""Concurrent access tests for Execution class."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


def test_concurrent_token_generation():
    """Test concurrent checkpoint token generation."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
        input='{"test": "data"}',
    )
    execution = Execution.new(input_data)
    tokens = []
    tokens_lock = threading.Lock()

    def generate_token():
        token = execution.get_new_checkpoint_token()
        with tokens_lock:
            tokens.append(token)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(generate_token) for _ in range(20)]

        for future in as_completed(futures):
            future.result()

    # All tokens should be unique and sequential
    assert len(tokens) == 20
    assert len(set(tokens)) == 20  # All unique
    assert execution.token_sequence == 20


def test_concurrent_operations_modification():
    """Test concurrent operations list modifications."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
        input='{"test": "data"}',
    )
    execution = Execution.new(input_data)
    results = []
    results_lock = threading.Lock()

    def start_execution():
        execution.start()
        with results_lock:
            results.append("started")

    def get_operations():
        ops = execution.get_navigable_operations()
        with results_lock:
            results.append(f"ops-{len(ops)}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        # One start operation
        futures.append(executor.submit(start_execution))
        # Multiple read operations
        futures.extend([executor.submit(get_operations) for _ in range(4)])

        for future in as_completed(futures):
            future.result()

    assert len(results) == 5
    assert "started" in results
    # Should have at least one operation after start
    final_ops = execution.get_navigable_operations()
    assert len(final_ops) >= 1
