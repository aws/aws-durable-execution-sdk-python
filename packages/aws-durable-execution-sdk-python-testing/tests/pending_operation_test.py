# """Test for pending operation handling in get_execution_history."""
#
# from datetime import UTC, datetime
# from unittest.mock import Mock
#
# from aws_durable_execution_sdk_python.lambda_service import (
#     OperationStatus,
#     OperationType,
# )
#
# from aws_durable_execution_sdk_python_testing.executor import Executor
# from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
#
#
# def test_get_execution_history_with_pending_chained_invoke():
#     """Test get_execution_history handles pending CHAINED_INVOKE operations correctly."""
#     # Create mocks
#     mock_store = Mock()
#     mock_scheduler = Mock()
#     mock_invoker = Mock()
#     mock_checkpoint_processor = Mock()
#
#     executor = Executor(mock_store, mock_scheduler, mock_invoker, mock_checkpoint_processor)
#
#     # Create mock execution
#     mock_execution = Mock()
#     mock_execution.durable_execution_arn = "test-arn"
#     mock_execution.start_input = StartDurableExecutionInput(
#         account_id="123",
#         function_name="test",
#         function_qualifier="$LATEST",
#         execution_name="test",
#         execution_timeout_seconds=300,
#         execution_retention_period_days=7,
#     )
#     mock_execution.result = None
#     mock_execution.updates = []
#
#     # Create a pending CHAINED_INVOKE operation with start_timestamp
#     pending_op = Mock()
#     pending_op.operation_id = "invoke-1"
#     pending_op.operation_type = OperationType.CHAINED_INVOKE
#     pending_op.status = OperationStatus.PENDING
#     pending_op.start_timestamp = datetime.now(UTC)
#     pending_op.end_timestamp = None
#
#     # Create a non-CHAINED_INVOKE pending operation (should be skipped)
#     pending_step = Mock()
#     pending_step.operation_id = "step-1"
#     pending_step.operation_type = OperationType.STEP
#     pending_step.status = OperationStatus.PENDING
#     pending_step.start_timestamp = datetime.now(UTC)
#     pending_step.end_timestamp = None
#
#     # Create a CHAINED_INVOKE pending operation without start_timestamp (should be skipped)
#     pending_invoke_no_timestamp = Mock()
#     pending_invoke_no_timestamp.operation_id = "invoke-2"
#     pending_invoke_no_timestamp.operation_type = OperationType.CHAINED_INVOKE
#     pending_invoke_no_timestamp.status = OperationStatus.PENDING
#     pending_invoke_no_timestamp.start_timestamp = None
#     pending_invoke_no_timestamp.end_timestamp = None
#
#     mock_execution.operations = [pending_op, pending_step, pending_invoke_no_timestamp]
#     mock_store.load.return_value = mock_execution
#
#     # Call get_execution_history
#     result = executor.get_execution_history("test-arn", include_execution_data=True)
#
#     # Should have 2 events: 1 pending event + 1 started event for the valid pending CHAINED_INVOKE
#     assert len(result.events) == 2
#
#     # First event should be the pending event
#     assert result.events[0].event_type == "ChainedInvokeStarted"
#     assert result.events[0].operation_id == "invoke-1"
#     assert result.events[0].chained_invoke_pending_details is not None
#
#     # Second event should be the started event
#     assert result.events[1].event_type == "ChainedInvokeStarted"
#     assert result.events[1].operation_id == "invoke-1"
#     assert result.events[1].chained_invoke_started_details is not None
#
#
# def test_get_execution_history_skips_invalid_pending_operations():
#     """Test that invalid pending operations are skipped."""
#     # Create mocks
#     mock_store = Mock()
#     mock_scheduler = Mock()
#     mock_invoker = Mock()
#     mock_checkpoint_processor = Mock()
#
#     executor = Executor(mock_store, mock_scheduler, mock_invoker, mock_checkpoint_processor)
#
#     # Create mock execution
#     mock_execution = Mock()
#     mock_execution.durable_execution_arn = "test-arn"
#     mock_execution.start_input = StartDurableExecutionInput(
#         account_id="123",
#         function_name="test",
#         function_qualifier="$LATEST",
#         execution_name="test",
#         execution_timeout_seconds=300,
#         execution_retention_period_days=7,
#     )
#     mock_execution.result = None
#     mock_execution.updates = []
#
#     # Create operations that should be skipped
#     # 1. Non-CHAINED_INVOKE pending operation
#     pending_step = Mock()
#     pending_step.operation_id = "step-1"
#     pending_step.operation_type = OperationType.STEP
#     pending_step.status = OperationStatus.PENDING
#     pending_step.start_timestamp = datetime.now(UTC)
#
#     # 2. CHAINED_INVOKE pending operation without start_timestamp
#     pending_invoke_no_timestamp = Mock()
#     pending_invoke_no_timestamp.operation_id = "invoke-1"
#     pending_invoke_no_timestamp.operation_type = OperationType.CHAINED_INVOKE
#     pending_invoke_no_timestamp.status = OperationStatus.PENDING
#     pending_invoke_no_timestamp.start_timestamp = None
#
#     mock_execution.operations = [pending_step, pending_invoke_no_timestamp]
#     mock_store.load.return_value = mock_execution
#
#     # Call get_execution_history
#     result = executor.get_execution_history("test-arn")
#
#     # Should have no events since all pending operations are invalid
#     assert len(result.events) == 0
