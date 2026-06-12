"""Unit tests for runner module."""

import datetime
import json
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ChainedInvokeDetails,
    ContextDetails,
    ExecutionDetails,
    OperationStatus,
    OperationType,
    StepDetails,
    WaitDetails,
)
from aws_durable_execution_sdk_python.lambda_service import Operation as SvcOperation

from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    GetDurableExecutionHistoryResponse,
)
from aws_durable_execution_sdk_python_testing.runner import (
    OPERATION_FACTORIES,
    CallbackOperation,
    ContextOperation,
    DurableChildContextTestRunner,
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
    ExecutionOperation,
    InvokeOperation,
    Operation,
    StepOperation,
    WaitOperation,
    create_operation,
)


def test_operation_creation():
    """Test basic Operation creation."""
    op = Operation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent-id",
        name="test-name",
        sub_type="test-subtype",
        start_timestamp=datetime.datetime.now(tz=datetime.UTC),
        end_timestamp=datetime.datetime.now(tz=datetime.UTC),
    )

    assert op.operation_id == "test-id"
    assert op.operation_type is OperationType.STEP
    assert op.status is OperationStatus.SUCCEEDED
    assert op.parent_id == "parent-id"
    assert op.name == "test-name"
    assert op.sub_type == "test-subtype"


def test_execution_operation_from_svc_operation():
    """Test ExecutionOperation creation from service operation."""
    execution_details = ExecutionDetails(input_payload="test-input")
    svc_op = SvcOperation(
        operation_id="exec-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=execution_details,
    )

    exec_op = ExecutionOperation.from_svc_operation(svc_op)

    assert exec_op.operation_id == "exec-id"
    assert exec_op.operation_type is OperationType.EXECUTION
    assert exec_op.input_payload == "test-input"


def test_execution_operation_wrong_type():
    """Test ExecutionOperation raises error for wrong operation type."""
    svc_op = SvcOperation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Expected EXECUTION operation, got OperationType.STEP",
    ):
        ExecutionOperation.from_svc_operation(svc_op)


def test_context_operation_from_svc_operation():
    """Test ContextOperation creation from service operation."""
    context_details = ContextDetails(result=json.dumps("test-result"), error=None)
    svc_op = SvcOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=context_details,
    )

    ctx_op = ContextOperation.from_svc_operation(svc_op)

    assert ctx_op.operation_id == "ctx-id"
    assert ctx_op.operation_type is OperationType.CONTEXT
    assert ctx_op.result == json.dumps("test-result")
    assert ctx_op.child_operations == []


def test_context_operation_with_children():
    """Test ContextOperation with child operations."""
    parent_op = SvcOperation(
        operation_id="parent-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(result=json.dumps("parent-result")),
    )

    child_op = SvcOperation(
        operation_id="child-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent-id",
        name="child-step",
        step_details=StepDetails(result=json.dumps("child-result")),
    )

    all_ops = [parent_op, child_op]
    ctx_op = ContextOperation.from_svc_operation(parent_op, all_ops)

    assert len(ctx_op.child_operations) == 1
    assert ctx_op.child_operations[0].name == "child-step"


def test_context_operation_get_operation_by_name():
    """Test ContextOperation get_operation_by_name method."""
    child_op = Operation(
        operation_id="child-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-child",
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[child_op],
    )

    found_op = ctx_op.get_operation_by_name("test-child")
    assert found_op == child_op


def test_context_operation_get_operation_by_name_not_found():
    """Test ContextOperation get_operation_by_name raises error when not found."""
    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[],
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Child Operation with name 'missing' not found"
    ):
        ctx_op.get_operation_by_name("missing")


def test_context_operation_get_step():
    """Test ContextOperation get_step method."""
    step_op = StepOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-step",
        child_operations=[],
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[step_op],
    )

    found_step = ctx_op.get_step("test-step")
    assert isinstance(found_step, StepOperation)
    assert found_step.name == "test-step"


def test_context_operation_get_wait():
    """Test ContextOperation get_wait method."""
    wait_op = WaitOperation(
        operation_id="wait-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
        name="test-wait",
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[wait_op],
    )

    found_wait = ctx_op.get_wait("test-wait")
    assert isinstance(found_wait, WaitOperation)
    assert found_wait.name == "test-wait"


def test_context_operation_get_context():
    """Test ContextOperation get_context method."""
    nested_ctx_op = ContextOperation(
        operation_id="nested-ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        name="nested-context",
        child_operations=[],
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[nested_ctx_op],
    )

    found_ctx = ctx_op.get_context("nested-context")
    assert isinstance(found_ctx, ContextOperation)
    assert found_ctx.name == "nested-context"


def test_context_operation_get_callback():
    """Test ContextOperation get_callback method."""
    callback_op = CallbackOperation(
        operation_id="callback-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        name="test-callback",
        child_operations=[],
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[callback_op],
    )

    found_callback = ctx_op.get_callback("test-callback")
    assert isinstance(found_callback, CallbackOperation)
    assert found_callback.name == "test-callback"


def test_context_operation_get_invoke():
    """Test ContextOperation get_invoke method."""
    invoke_op = InvokeOperation(
        operation_id="invoke-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        name="test-invoke",
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[invoke_op],
    )

    found_invoke = ctx_op.get_invoke("test-invoke")
    assert isinstance(found_invoke, InvokeOperation)
    assert found_invoke.name == "test-invoke"


def test_context_operation_get_execution():
    """Test ContextOperation get_execution method."""
    exec_op = ExecutionOperation(
        operation_id="exec-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        name="test-execution",
    )

    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        child_operations=[exec_op],
    )

    found_exec = ctx_op.get_execution("test-execution")
    assert isinstance(found_exec, ExecutionOperation)
    assert found_exec.name == "test-execution"


def test_step_operation_from_svc_operation():
    """Test StepOperation creation from service operation."""
    step_details = StepDetails(attempt=2, result=json.dumps("step-result"), error=None)
    svc_op = SvcOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=step_details,
    )

    step_op = StepOperation.from_svc_operation(svc_op)

    assert step_op.operation_id == "step-id"
    assert step_op.operation_type is OperationType.STEP
    assert step_op.attempt == 2
    assert step_op.result == json.dumps("step-result")


def test_step_operation_wrong_type():
    """Test StepOperation raises error for wrong operation type."""
    svc_op = SvcOperation(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Expected STEP operation, got OperationType.CONTEXT",
    ):
        StepOperation.from_svc_operation(svc_op)


def test_wait_operation_from_svc_operation():
    """Test WaitOperation creation from service operation."""
    scheduled_time = datetime.datetime.now(tz=datetime.UTC)
    wait_details = WaitDetails(scheduled_end_timestamp=scheduled_time)
    svc_op = SvcOperation(
        operation_id="wait-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
        wait_details=wait_details,
    )

    wait_op = WaitOperation.from_svc_operation(svc_op)

    assert wait_op.operation_id == "wait-id"
    assert wait_op.operation_type is OperationType.WAIT
    assert wait_op.scheduled_end_timestamp == scheduled_time


def test_wait_operation_wrong_type():
    """Test WaitOperation raises error for wrong operation type."""
    svc_op = SvcOperation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Expected WAIT operation, got OperationType.STEP",
    ):
        WaitOperation.from_svc_operation(svc_op)


def test_callback_operation_from_svc_operation():
    """Test CallbackOperation creation from service operation."""
    callback_details = CallbackDetails(
        callback_id="cb-123", result=json.dumps("callback-result")
    )
    svc_op = SvcOperation(
        operation_id="callback-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )

    callback_op = CallbackOperation.from_svc_operation(svc_op)

    assert callback_op.operation_id == "callback-id"
    assert callback_op.operation_type is OperationType.CALLBACK
    assert callback_op.callback_id == "cb-123"
    assert callback_op.result == json.dumps("callback-result")


def test_callback_operation_wrong_type():
    """Test CallbackOperation raises error for wrong operation type."""
    svc_op = SvcOperation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Expected CALLBACK operation, got OperationType.STEP",
    ):
        CallbackOperation.from_svc_operation(svc_op)


def test_invoke_operation_from_svc_operation():
    """Test InvokeOperation creation from service operation."""
    invoke_details = ChainedInvokeDetails(
        result=json.dumps("invoke-result"),
    )
    svc_op = SvcOperation(
        operation_id="invoke-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=invoke_details,
    )

    invoke_op = InvokeOperation.from_svc_operation(svc_op)

    assert invoke_op.operation_id == "invoke-id"
    assert invoke_op.operation_type is OperationType.CHAINED_INVOKE
    assert invoke_op.result == json.dumps("invoke-result")


def test_invoke_operation_wrong_type():
    """Test InvokeOperation raises error for wrong operation type."""
    svc_op = SvcOperation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Expected INVOKE operation, got OperationType.STEP",
    ):
        InvokeOperation.from_svc_operation(svc_op)


def test_operation_factories_mapping():
    """Test OPERATION_FACTORIES contains all expected mappings."""
    expected_types = {
        OperationType.EXECUTION: ExecutionOperation,
        OperationType.CONTEXT: ContextOperation,
        OperationType.STEP: StepOperation,
        OperationType.WAIT: WaitOperation,
        OperationType.CHAINED_INVOKE: InvokeOperation,
        OperationType.CALLBACK: CallbackOperation,
    }

    assert expected_types == OPERATION_FACTORIES


def test_create_operation_step():
    """Test create_operation function with STEP operation."""
    svc_op = SvcOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps("test-result")),
    )

    operation = create_operation(svc_op)

    assert isinstance(operation, StepOperation)
    assert operation.operation_id == "step-id"


def test_create_operation_unknown_type():
    """Test create_operation raises error for unknown operation type."""
    # Create a mock operation with an invalid type
    svc_op = Mock()
    svc_op.operation_type = "UNKNOWN_TYPE"

    with pytest.raises(
        DurableFunctionsTestError, match="Unknown operation type: UNKNOWN_TYPE"
    ):
        create_operation(svc_op)


def test_durable_function_test_result_create():
    """Test DurableFunctionTestResult.create method."""
    # Create mock execution with operations
    execution = Mock(spec=Execution)

    # Create mock operations - one EXECUTION (should be filtered) and one STEP
    exec_op = Mock()
    exec_op.operation_type = OperationType.EXECUTION
    exec_op.parent_id = None

    step_op = Mock()
    step_op.operation_type = OperationType.STEP
    step_op.parent_id = None
    step_op.operation_id = "step-id"
    step_op.status = OperationStatus.SUCCEEDED
    step_op.name = "test-step"
    step_op.step_details = StepDetails(result=json.dumps("step-result"))

    execution.operations = [exec_op, step_op]

    # Mock execution result
    execution.result = Mock()
    execution.result.status = InvocationStatus.SUCCEEDED
    execution.result.result = json.dumps("test-result")
    execution.result.error = None

    result = DurableFunctionTestResult.create(execution)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps("test-result")
    assert result.error is None
    assert len(result.operations) == 1  # EXECUTION operation filtered out


def test_durable_function_test_result_get_operation_by_name():
    """Test DurableFunctionTestResult get_operation_by_name method."""
    step_op = StepOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-step",
        child_operations=[],
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[step_op],
    )

    found_op = result.get_operation_by_name("test-step")
    assert found_op == step_op


def test_durable_function_test_result_get_operation_by_name_not_found():
    """Test DurableFunctionTestResult get_operation_by_name raises error when not found."""
    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[],
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Operation with name 'missing' not found"
    ):
        result.get_operation_by_name("missing")


def test_durable_function_test_result_get_step():
    """Test DurableFunctionTestResult get_step method."""
    step_op = StepOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        name="test-step",
        child_operations=[],
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[step_op],
    )

    found_step = result.get_step("test-step")
    assert isinstance(found_step, StepOperation)
    assert found_step.name == "test-step"


def test_durable_function_test_result_get_wait():
    """Test DurableFunctionTestResult get_wait method."""
    wait_op = WaitOperation(
        operation_id="wait-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
        name="test-wait",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[wait_op],
    )

    found_wait = result.get_wait("test-wait")
    assert isinstance(found_wait, WaitOperation)
    assert found_wait.name == "test-wait"


def test_durable_function_test_result_get_context():
    """Test DurableFunctionTestResult get_context method."""
    ctx_op = ContextOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        name="test-context",
        child_operations=[],
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[ctx_op],
    )

    found_ctx = result.get_context("test-context")
    assert isinstance(found_ctx, ContextOperation)
    assert found_ctx.name == "test-context"


def test_durable_function_test_result_get_callback():
    """Test DurableFunctionTestResult get_callback method."""
    callback_op = CallbackOperation(
        operation_id="callback-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        name="test-callback",
        child_operations=[],
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[callback_op],
    )

    found_callback = result.get_callback("test-callback")
    assert isinstance(found_callback, CallbackOperation)
    assert found_callback.name == "test-callback"


def test_durable_function_test_result_get_invoke():
    """Test DurableFunctionTestResult get_invoke method."""
    invoke_op = InvokeOperation(
        operation_id="invoke-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        name="test-invoke",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[invoke_op],
    )

    found_invoke = result.get_invoke("test-invoke")
    assert isinstance(found_invoke, InvokeOperation)
    assert found_invoke.name == "test-invoke"


def test_durable_function_test_result_get_execution():
    """Test DurableFunctionTestResult get_execution method."""
    exec_op = ExecutionOperation(
        operation_id="exec-id",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        name="test-execution",
    )

    result = DurableFunctionTestResult(
        status=InvocationStatus.SUCCEEDED,
        operations=[exec_op],
    )

    found_exec = result.get_execution("test-execution")
    assert isinstance(found_exec, ExecutionOperation)
    assert found_exec.name == "test-execution"


@patch("aws_durable_execution_sdk_python_testing.runner.Scheduler")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore")
@patch("aws_durable_execution_sdk_python_testing.runner.CheckpointProcessor")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryServiceClient")
@patch("aws_durable_execution_sdk_python_testing.runner.InProcessInvoker")
@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
def test_durable_function_test_runner_init(
    mock_executor, mock_invoker, mock_client, mock_processor, mock_store, mock_scheduler
):
    """Test DurableFunctionTestRunner initialization."""
    handler = Mock()

    DurableFunctionTestRunner(handler)

    # Verify all components are initialized
    mock_scheduler.assert_called_once()
    mock_scheduler.return_value.start.assert_called_once()
    mock_store.assert_called_once()
    mock_processor.assert_called_once()
    mock_client.assert_called_once()
    mock_invoker.assert_called_once_with(handler, mock_client.return_value)
    mock_executor.assert_called_once()

    # Verify observer pattern setup
    mock_processor.return_value.add_execution_observer.assert_called_once_with(
        mock_executor.return_value
    )


def test_durable_function_test_runner_context_manager():
    """Test DurableFunctionTestRunner context manager."""
    handler = Mock()

    with patch.object(DurableFunctionTestRunner, "__init__", return_value=None):
        with patch.object(DurableFunctionTestRunner, "close") as mock_close:
            runner = DurableFunctionTestRunner(handler)

            with runner:
                pass

            mock_close.assert_called_once()


@patch("aws_durable_execution_sdk_python_testing.runner.Scheduler")
def test_durable_function_test_runner_close(mock_scheduler):
    """Test DurableFunctionTestRunner close method."""
    handler = Mock()

    # Let the constructor run normally with mocked dependencies
    mock_scheduler_instance = Mock()
    mock_scheduler.return_value = mock_scheduler_instance

    runner = DurableFunctionTestRunner(handler)
    runner.close()

    # Verify scheduler.stop() was called
    mock_scheduler_instance.stop.assert_called_once()


@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore")
def test_durable_function_test_runner_run(mock_store_class, mock_executor_class):
    """Test DurableFunctionTestRunner run method."""
    handler = Mock()

    # Mock the class instances
    mock_executor = Mock()
    mock_store = Mock()
    mock_executor_class.return_value = mock_executor
    mock_store_class.return_value = mock_store

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete.return_value = True

    # Mock execution for result creation
    mock_execution = Mock(spec=Execution)
    mock_execution.operations = []
    mock_execution.result = Mock()
    mock_execution.result.status = InvocationStatus.SUCCEEDED
    mock_execution.result.result = json.dumps("test-result")
    mock_execution.result.error = None
    mock_store.load.return_value = mock_execution

    runner = DurableFunctionTestRunner(handler)
    result = runner.run("test-input")

    # Verify start_execution was called with correct input
    mock_executor.start_execution.assert_called_once()
    start_input = mock_executor.start_execution.call_args[0][0]
    assert isinstance(start_input, StartDurableExecutionInput)
    assert start_input.input == "test-input"
    assert start_input.function_name == "test-function"
    assert start_input.execution_name == "execution-name"
    assert start_input.account_id == "123456789012"

    # Verify wait_until_complete was called
    mock_executor.wait_until_complete.assert_called_once_with("test-arn", 900)

    # Verify store.load was called
    mock_store.load.assert_called_once_with("test-arn")

    # Verify result
    assert isinstance(result, DurableFunctionTestResult)
    assert result.status is InvocationStatus.SUCCEEDED


@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore")
def test_durable_function_test_runner_run_with_custom_params(
    mock_store_class, mock_executor_class
):
    """Test DurableFunctionTestRunner run method with custom parameters."""
    handler = Mock()

    # Mock the class instances
    mock_executor = Mock()
    mock_store = Mock()
    mock_executor_class.return_value = mock_executor
    mock_store_class.return_value = mock_store

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete.return_value = True

    # Mock execution for result creation
    mock_execution = Mock(spec=Execution)
    mock_execution.operations = []
    mock_execution.result = Mock()
    mock_execution.result.status = InvocationStatus.SUCCEEDED
    mock_execution.result.result = json.dumps("test-result")
    mock_execution.result.error = None
    mock_store.load.return_value = mock_execution

    runner = DurableFunctionTestRunner(handler)
    result = runner.run(
        input="custom-input",
        timeout=1800,
        function_name="custom-function",
        execution_name="custom-execution",
        account_id="987654321098",
    )

    # Verify start_execution was called with custom parameters
    start_input = mock_executor.start_execution.call_args[0][0]
    assert start_input.input == "custom-input"
    assert start_input.function_name == "custom-function"
    assert start_input.execution_name == "custom-execution"
    assert start_input.account_id == "987654321098"
    assert start_input.execution_timeout_seconds == 1800

    # Verify wait_until_complete was called with custom timeout
    mock_executor.wait_until_complete.assert_called_once_with("test-arn", 1800)

    assert result.status is InvocationStatus.SUCCEEDED


@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
def test_durable_function_test_runner_run_timeout(mock_executor_class):
    """Test DurableFunctionTestRunner run method with timeout."""
    handler = Mock()

    # Mock the class instance
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    # Mock execution output
    output = StartDurableExecutionOutput(execution_arn="test-arn")
    mock_executor.start_execution.return_value = output
    mock_executor.wait_until_complete.return_value = False  # Timeout

    runner = DurableFunctionTestRunner(handler)

    with pytest.raises(TimeoutError, match="Execution did not complete within timeout"):
        runner.run("test-input")


def test_context_operation_wrong_type():
    """Test ContextOperation raises error for wrong operation type."""
    svc_op = SvcOperation(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Expected CONTEXT operation, got OperationType.STEP",
    ):
        ContextOperation.from_svc_operation(svc_op)


def test_context_operation_with_child_operations_none():
    """Test ContextOperation with None child operations."""
    svc_op = SvcOperation(
        operation_id="ctx-id",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(result=json.dumps("test-result")),
    )

    ctx_op = ContextOperation.from_svc_operation(svc_op, None)

    assert ctx_op.child_operations == []


def test_callback_operation_with_child_operations_none():
    """Test CallbackOperation with None child operations."""
    svc_op = SvcOperation(
        operation_id="callback-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(callback_id="cb-123"),
    )

    callback_op = CallbackOperation.from_svc_operation(svc_op, None)

    assert callback_op.child_operations == []


def test_step_operation_with_child_operations_none():
    """Test StepOperation with None child operations."""
    svc_op = SvcOperation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps("step-result")),
    )

    step_op = StepOperation.from_svc_operation(svc_op, None)

    assert step_op.child_operations == []


def test_durable_function_test_result_create_with_parent_operations():
    """Test DurableFunctionTestResult.create with operations that have parent_id."""
    execution = Mock(spec=Execution)

    # Create operation with parent_id (should be filtered out)
    child_op = Mock()
    child_op.operation_type = OperationType.STEP
    child_op.parent_id = "parent-id"

    # Create operation without parent_id (should be included)
    root_op = Mock()
    root_op.operation_type = OperationType.STEP
    root_op.parent_id = None
    root_op.operation_id = "root-id"
    root_op.status = OperationStatus.SUCCEEDED
    root_op.name = "root-step"
    root_op.step_details = StepDetails(result=json.dumps("root-result"))

    execution.operations = [child_op, root_op]
    execution.result = Mock()
    execution.result.status = InvocationStatus.SUCCEEDED
    execution.result.result = json.dumps("test-result")
    execution.result.error = None

    result = DurableFunctionTestResult.create(execution)

    assert len(result.operations) == 1  # Only root operation included


@patch("aws_durable_execution_sdk_python_testing.runner.Scheduler")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore")
@patch("aws_durable_execution_sdk_python_testing.runner.CheckpointProcessor")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryServiceClient")
@patch("aws_durable_execution_sdk_python_testing.runner.InProcessInvoker")
@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
@patch("aws_durable_execution_sdk_python_testing.runner.durable_execution")
def test_durable_context_test_runner_init(
    mock_durable_execution_handler,
    mock_executor,
    mock_invoker,
    mock_client,
    mock_processor,
    mock_store,
    mock_scheduler,
):
    """Test DurableContextTestRunner initialization."""
    handler = Mock()
    decorated_handler = Mock()
    mock_durable_execution_handler.return_value = decorated_handler

    DurableChildContextTestRunner(handler)  # type: ignore

    # Verify all components are initialized
    mock_scheduler.assert_called_once()
    mock_scheduler.return_value.start.assert_called_once()
    mock_store.assert_called_once()
    mock_processor.assert_called_once()
    mock_client.assert_called_once()
    mock_invoker.assert_called_once_with(decorated_handler, mock_client.return_value)
    mock_executor.assert_called_once()

    # Verify observer pattern setup
    mock_processor.return_value.add_execution_observer.assert_called_once_with(
        mock_executor.return_value
    )

    # Verify durable_execution was called (with internal lambda function)
    mock_durable_execution_handler.assert_called_once()

    # Verify the lambda function calls our handler
    durable_execution_func = mock_durable_execution_handler.call_args.args[0]
    assert callable(durable_execution_func)

    # verify handler is called when durable function is invoked
    durable_execution_func(Mock(), Mock())
    handler.assert_called_once()


@patch("aws_durable_execution_sdk_python_testing.runner.Scheduler")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore")
@patch("aws_durable_execution_sdk_python_testing.runner.CheckpointProcessor")
@patch("aws_durable_execution_sdk_python_testing.runner.InMemoryServiceClient")
@patch("aws_durable_execution_sdk_python_testing.runner.InProcessInvoker")
@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
@patch("aws_durable_execution_sdk_python_testing.runner.durable_execution")
def test_durable_child_context_test_runner_init_with_args(
    mock_durable_execution_handler,
    mock_executor,
    mock_invoker,
    mock_client,
    mock_processor,
    mock_store,
    mock_scheduler,
):
    """Test DurableChildContextTestRunner initialization with additional args."""
    handler = Mock()
    decorated_handler = Mock()
    mock_durable_execution_handler.return_value = decorated_handler

    str_input = "a random string input"
    num_input = 10
    DurableChildContextTestRunner(handler, str_input, num=num_input)  # type: ignore

    # Verify all components are initialized
    mock_scheduler.assert_called_once()
    mock_scheduler.return_value.start.assert_called_once()
    mock_store.assert_called_once()
    mock_processor.assert_called_once()
    mock_client.assert_called_once()
    mock_invoker.assert_called_once_with(decorated_handler, mock_client.return_value)
    mock_executor.assert_called_once()

    # Verify observer pattern setup
    mock_processor.return_value.add_execution_observer.assert_called_once_with(
        mock_executor.return_value
    )

    # Verify durable_execution was called (with internal lambda function)
    mock_durable_execution_handler.assert_called_once()
    # Verify the lambda function calls our handler
    durable_execution_func = mock_durable_execution_handler.call_args.args[0]
    assert callable(durable_execution_func)

    # verify that handler is called with expected args when durable function is invoked
    durable_execution_func(Mock(), Mock())
    handler.assert_called_once_with(str_input, num=num_input)


# Tests for DurableFunctionCloudTestRunner and from_execution_history


def test_durable_function_test_result_from_execution_history():
    """Test DurableFunctionTestResult.from_execution_history factory method."""
    import datetime

    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        EventResult,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
        StepSucceededDetails,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        end_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        result="test-result",
        error=None,
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="ExecutionStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
                ),
                operation_id="exec-1",
            ),
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 10, tzinfo=datetime.UTC
                ),
                operation_id="step-1",
                name="test-step",
            ),
            Event(
                event_type="StepSucceeded",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 20, tzinfo=datetime.UTC
                ),
                operation_id="step-1",
                step_succeeded_details=StepSucceededDetails(
                    result=EventResult(payload="step-result", truncated=False)
                ),
            ),
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "test-result"
    assert result.error is None
    assert len(result.operations) == 1
    assert result.operations[0].name == "test-step"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_init(mock_boto3):
    """Test DurableFunctionCloudTestRunner initialization."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function",
        region="us-west-2",
        poll_interval=0.5,
    )

    assert runner.function_name == "test-function"
    assert runner.region == "us-west-2"
    assert runner.poll_interval == 0.5
    mock_boto3.client.assert_called_once()


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with successful execution."""
    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": Mock(read=lambda: b'{"result": "success"}'),
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
    }

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "SUCCEEDED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Result": "test-result",
    }

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "ExecutionStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "exec-1",
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    result = runner.run(input="test-input", timeout=10)

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "test-result"
    mock_client.invoke.assert_called_once()


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_invoke_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with invoke failure."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_client.invoke.side_effect = Exception("Invoke failed")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to invoke Lambda function"
    ):
        runner.run(input="test-input")


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
@patch("aws_durable_execution_sdk_python_testing.runner.time")
def test_cloud_runner_wait_for_completion_timeout(mock_time, mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with timeout."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_time.time.side_effect = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "RUNNING",
        "StartTimestamp": "2023-01-01T00:00:00Z",
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Execution did not complete within"):
        runner._wait_for_completion("test-arn", timeout=2)


def test_durable_function_test_result_from_execution_history_with_exception():
    """Test from_execution_history handles events_to_operations exception."""
    import datetime

    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
                ),
                operation_id=None,
            )
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.SUCCEEDED
    assert len(result.operations) == 0


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_completion_failed_status(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with FAILED status."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "FAILED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Error": {"ErrorMessage": "execution failed"},
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "FAILED"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_bad_status_code(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with bad HTTP status code."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 500,
        "Payload": Mock(read=lambda: b"Internal Server Error"),
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Lambda invocation failed with status 500"
    ):
        runner.run(input="test-input")


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_function_error(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with function error."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": Mock(read=lambda: b'{"errorMessage": "Function failed"}'),
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
    }

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "FAILED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Error": {"ErrorMessage": "execution failed"},
    }

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "ExecutionStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "exec-1",
            }
        ]
    }
    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner.run(input="test-input")
    assert result.status is InvocationStatus.FAILED


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_missing_execution_arn(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run with missing execution ARN."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": Mock(read=lambda: b'{"result": "success"}'),
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="No DurableExecutionArn in response"
    ):
        runner.run(input="test-input")


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_completion_get_execution_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with API failure."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_client.get_durable_execution.side_effect = Exception("API error")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to get execution status"
    ):
        runner._wait_for_completion("test-arn", timeout=10)


def test_durable_function_test_result_from_execution_history_filters_execution_type():
    """Test from_execution_history filters out EXECUTION type operations."""
    import datetime

    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="ExecutionStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
                ),
                operation_id="exec-1",
            ),
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert len(result.operations) == 0


def test_durable_function_test_result_from_execution_history_unknown_status():
    """Test from_execution_history with unknown status defaults to FAILED."""
    import datetime

    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.model import (
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="UNKNOWN_STATUS",
        start_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
    )

    history_response = GetDurableExecutionHistoryResponse(events=[])

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.FAILED


def test_durable_function_test_result_from_execution_history_with_parent_operations():
    """Test from_execution_history filters operations with parent_id."""
    import datetime

    from aws_durable_execution_sdk_python.execution import InvocationStatus

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="SUCCEEDED",
        start_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
    )

    history_response = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
                ),
                operation_id="step-1",
                name="parent-step",
            ),
            Event(
                event_type="StepStarted",
                event_timestamp=datetime.datetime(
                    2023, 1, 1, 0, 0, 10, tzinfo=datetime.UTC
                ),
                operation_id="step-2",
                name="child-step",
                parent_id="step-1",
            ),
        ]
    )

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert len(result.operations) == 1
    assert result.operations[0].name == "parent-step"


def test_durable_function_test_result_from_execution_history_failed():
    """Test from_execution_history with failed execution."""
    import datetime

    from aws_durable_execution_sdk_python.execution import InvocationStatus
    from aws_durable_execution_sdk_python.lambda_service import ErrorObject

    from aws_durable_execution_sdk_python_testing.model import (
        GetDurableExecutionHistoryResponse,
        GetDurableExecutionResponse,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestResult,
    )

    execution_response = GetDurableExecutionResponse(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test",
        status="FAILED",
        start_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        end_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        error=ErrorObject(
            message="execution failed", type=None, data=None, stack_trace=None
        ),
    )

    history_response = GetDurableExecutionHistoryResponse(events=[])

    result = DurableFunctionTestResult.from_execution_history(
        execution_response, history_response
    )

    assert result.status == InvocationStatus.FAILED
    assert result.error.message == "execution failed"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_completion_timed_out_status(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with TIMED_OUT status."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "TIMED_OUT",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "TIMED_OUT"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_completion_aborted_status(mock_boto3):
    """Test DurableFunctionCloudTestRunner._wait_for_completion with ABORTED status."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution.return_value = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        "Status": "ABORTED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "EndTimestamp": "2023-01-01T00:01:00Z",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    result = runner._wait_for_completion("test-arn", timeout=10)

    assert result.status == "ABORTED"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_async_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run_async with successful invocation."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 202,
        "Payload": Mock(read=lambda: b'{"result": "success"}'),
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1",
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    execution_arn = runner.run_async(input="test-input")

    assert (
        execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:test:execution:exec-1"
    )
    mock_client.invoke.assert_called_once_with(
        FunctionName="test-function",
        InvocationType="Event",
        Payload='"test-input"',
    )


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_async_with_400(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run_async with successful invocation."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.invoke.return_value = {
        "StatusCode": 400,
        "Payload": Mock(read=lambda: b'{"result": "failed"}'),
    }

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Lambda invocation failed with status 400"
    ):
        runner.run_async(input="test-input")


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_run_async_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner.run_async with invocation failure."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_client.invoke.side_effect = Exception("Async invoke failed")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to invoke Lambda function"
    ):
        runner.run_async(input="test-input")


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_send_callback_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.send_callback_success."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner.send_callback_success("callback-123")

    mock_client.send_durable_execution_callback_success.assert_called_once_with(
        CallbackId="callback-123", Result=None
    )


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_send_callback_failure(mock_boto3):
    """Test DurableFunctionCloudTestRunner.send_callback_failure."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner.send_callback_failure("callback-123")

    mock_client.send_durable_execution_callback_failure.assert_called_once_with(
        CallbackId="callback-123", Error=None
    )


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_send_callback_heartbeat(mock_boto3):
    """Test DurableFunctionCloudTestRunner.send_callback_heartbeat."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner.send_callback_heartbeat("callback-123")

    mock_client.send_durable_execution_callback_heartbeat.assert_called_once_with(
        CallbackId="callback-123"
    )


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_send_callback_error(mock_boto3):
    """Test DurableFunctionCloudTestRunner callback methods with API errors."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_client.send_durable_execution_callback_success.side_effect = Exception(
        "API error"
    )

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to send callback success"
    ):
        runner.send_callback_success("callback-123")


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_success(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback success."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = runner.wait_for_callback("test-arn", name="test-callback", timeout=10)

    assert callback_id == "callback-123"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_none(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback none."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Callback did not available within"):
        runner.wait_for_callback("test-arn", name="test-callback1", timeout=2)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_success_without_name(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback success."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            }
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = runner.wait_for_callback("test-arn")

    assert callback_id == "callback-123"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_all_done_without_name(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback all_done_without_name."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            },
            {
                "EventType": "CallbackSucceeded",
                "EventTimestamp": "2023-01-01T00:05:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    with pytest.raises(TimeoutError, match="Callback did not available within"):
        runner.wait_for_callback("test-arn", timeout=2)


@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
def test_local_runner_wait_for_callback_all_done_without_name(mock_executor_class):
    """Test DurableFunctionCloudTestRunner.wait_for_callback all_done_without_name."""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.return_value = (
        GetDurableExecutionHistoryResponse.from_dict(
            {
                "Events": [
                    {
                        "EventType": "CallbackStarted",
                        "EventTimestamp": "2023-01-01T00:00:00Z",
                        "Id": "callback-event-1",
                        "Name": "test-callback",
                        "CallbackStartedDetails": {"CallbackId": "callback-123"},
                    },
                    {
                        "EventType": "CallbackSucceeded",
                        "EventTimestamp": "2023-01-01T00:05:00Z",
                        "Id": "callback-event-1",
                        "Name": "test-callback",
                    },
                ]
            }
        )
    )

    runner = DurableFunctionTestRunner(handler)
    with pytest.raises(TimeoutError, match="Callback did not available within"):
        runner.wait_for_callback("test-arn", timeout=2)


@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
def test_local_runner_wait_for_callback_with_exception(mock_executor_class):
    """Test DurableFunctionCloudTestRunner.wait_for_callback with exception"""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.side_effect = Exception("error")

    runner = DurableFunctionTestRunner(handler)
    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        runner.wait_for_callback("test-arn", timeout=10)


@patch("aws_durable_execution_sdk_python_testing.runner.Executor")
def test_local_runner_wait_for_callback_with_resource_not_found_exception(
    mock_executor_class,
):
    """Test DurableFunctionCloudTestRunner.wait_for_callback with resource_not_found exception"""
    handler = Mock()
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor
    mock_executor.get_execution_history.side_effect = ResourceNotFoundException("error")

    runner = DurableFunctionTestRunner(handler)
    with pytest.raises(TimeoutError, match="Callback did not available within"):
        runner.wait_for_callback("test-arn", timeout=2)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
@patch("aws_durable_execution_sdk_python_testing.runner.time")
def test_cloud_runner_wait_for_callback_timeout(mock_time, mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback timeout."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client
    mock_time.time.side_effect = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

    mock_client.get_durable_execution_history.return_value = {"Events": []}

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(TimeoutError, match="Callback did not available within"):
        runner.wait_for_callback("test-arn", timeout=2)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_already_completed(mock_boto3):
    """Test DurableFunctionCloudTestRunner.wait_for_callback already completed."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "EventTimestamp": "2023-01-01T00:00:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
                "CallbackStartedDetails": {"CallbackId": "callback-123"},
            },
            {
                "EventType": "CallbackSucceeded",
                "EventTimestamp": "2023-01-01T00:05:00Z",
                "Id": "callback-event-1",
                "Name": "test-callback",
            },
        ]
    }

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Callback test-callback has already completed"
    ):
        runner.wait_for_callback("test-arn", "test-callback", timeout=2)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_client_error_retryable(mock_boto3):
    """Test wait_for_callback with retryable ClientError."""
    from botocore.exceptions import ClientError  # type: ignore
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    # First call raises ResourceNotFoundException, second succeeds
    mock_client.get_durable_execution_history.side_effect = [
        ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="GetDurableExecutionHistory",
        ),
        {
            "Events": [
                {
                    "EventType": "CallbackStarted",
                    "EventTimestamp": "2023-01-01T00:00:00Z",
                    "Id": "callback-event-1",
                    "Name": "test-callback",
                    "CallbackStartedDetails": {"CallbackId": "callback-123"},
                }
            ]
        },
    ]

    runner = DurableFunctionCloudTestRunner(
        function_name="test-function", poll_interval=0.01
    )
    callback_id = runner.wait_for_callback("test-arn", name="test-callback", timeout=10)

    assert callback_id == "callback-123"


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_client_error_non_retryable(
    mock_boto3,
):
    """Test wait_for_callback with non-retryable ClientError."""
    from botocore.exceptions import ClientError
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDeniedException"}},
        operation_name="GetDurableExecutionHistory",
    )

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        runner.wait_for_callback("test-arn", timeout=10)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_callback_generic_exception(mock_boto3):
    """Test wait_for_callback with generic Exception."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    mock_client.get_durable_execution_history.side_effect = Exception("Network error")

    runner = DurableFunctionCloudTestRunner(function_name="test-function")

    with pytest.raises(
        DurableFunctionsTestError, match="Failed to fetch execution history"
    ):
        runner.wait_for_callback("test-arn", timeout=10)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_result_fetch_history_exception(mock_boto3):
    """Test wait_for_result with exception in _fetch_execution_history."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    # Mock successful _wait_for_completion
    mock_execution_response = Mock()
    mock_execution_response.status = "SUCCEEDED"

    # Mock _fetch_execution_history to raise exception
    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner._wait_for_completion = Mock(return_value=mock_execution_response)
    runner._fetch_execution_history = Mock(
        side_effect=Exception("History fetch failed")
    )

    with pytest.raises(
        DurableFunctionsTestError,
        match="Failed to fetch execution history: History fetch failed",
    ):
        runner.wait_for_result("test-arn", timeout=60)


@patch("aws_durable_execution_sdk_python_testing.runner.boto3")
def test_cloud_runner_wait_for_result_success(mock_boto3):
    """Test wait_for_result successful execution."""
    from aws_durable_execution_sdk_python.execution import InvocationStatus
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionCloudTestRunner,
    )

    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    # Mock successful responses
    mock_execution_response = Mock()
    mock_execution_response.status = "SUCCEEDED"
    mock_history_response = Mock()
    mock_history_response.events = []

    runner = DurableFunctionCloudTestRunner(function_name="test-function")
    runner._wait_for_completion = Mock(return_value=mock_execution_response)
    runner._fetch_execution_history = Mock(return_value=mock_history_response)

    # Mock the from_execution_history method
    with patch(
        "aws_durable_execution_sdk_python_testing.runner.DurableFunctionTestResult.from_execution_history"
    ) as mock_from_history:
        mock_result = Mock()
        mock_result.status = InvocationStatus.SUCCEEDED
        mock_from_history.return_value = mock_result

        result = runner.wait_for_result("test-arn", timeout=60)

        assert result.status == InvocationStatus.SUCCEEDED
        mock_from_history.assert_called_once_with(
            mock_execution_response, mock_history_response
        )
