"""Unit tests for invoke operation validator."""

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.invoke import (
    ChainedInvokeOperationValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput

PARENT_ACCOUNT = "123456789012"


def _execution(region: str | None = "us-west-2") -> Execution:
    execution = Execution.new(
        StartDurableExecutionInput(
            account_id=PARENT_ACCOUNT,
            function_name="parent",
            function_qualifier="$LATEST",
            execution_name="run",
            execution_timeout_seconds=60,
            execution_retention_period_days=1,
        )
    )
    execution.region = region
    return execution


def _start_update(
    operation_id: str = "test-id",
    function_name: str = "child-function",
    tenant_id: str | None = None,
) -> OperationUpdate:
    return OperationUpdate(
        operation_id=operation_id,
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        chained_invoke_options=ChainedInvokeOptions(
            function_name=function_name, tenant_id=tenant_id
        ),
    )


def test_validate_start_action_with_no_current_state():
    """Test START action with no current state."""
    ChainedInvokeOperationValidator.validate(None, _start_update(), _execution())


@pytest.mark.parametrize(
    "function_name",
    [
        "child",
        "child:prod",
        f"{PARENT_ACCOUNT}:function:child",
        f"arn:aws:lambda:us-west-2:{PARENT_ACCOUNT}:function:child",
        f"arn:aws:lambda:us-west-2:{PARENT_ACCOUNT}:function:child:$LATEST",
        "namespace.child",
        "child:$LATEST.PUBLISHED",
        "x" * 256,
    ],
)
def test_validate_accepts_every_lambda_function_name_form(function_name: str):
    ChainedInvokeOperationValidator.validate(
        None, _start_update(function_name=function_name), _execution()
    )


@pytest.mark.parametrize(
    "function_name",
    [
        "",
        "has space",
        "arn:aws:lambda:us-west-2:function:child",
        "arn:aws:lambda:function:child",
        "x" * 257,
        "child\n",
    ],
)
def test_validate_rejects_malformed_target(function_name: str):
    with pytest.raises(InvalidParameterValueException) as exc_info:
        ChainedInvokeOperationValidator.validate(
            None, _start_update(function_name=function_name), _execution()
        )
    assert str(exc_info.value) == f"Invalid function ARN '{function_name}'"


def test_validate_rejects_target_in_another_account():
    target = "arn:aws:lambda:us-west-2:999999999999:function:child"
    with pytest.raises(InvalidParameterValueException) as exc_info:
        ChainedInvokeOperationValidator.validate(
            None, _start_update(function_name=target), _execution()
        )
    assert (
        str(exc_info.value)
        == "Cannot start a CHAINED_INVOKE on a function in another account."
    )


def test_validate_rejects_target_in_another_region():
    target = f"arn:aws:lambda:eu-west-1:{PARENT_ACCOUNT}:function:child"
    with pytest.raises(InvalidParameterValueException) as exc_info:
        ChainedInvokeOperationValidator.validate(
            None, _start_update(function_name=target), _execution()
        )
    assert (
        str(exc_info.value)
        == "Cannot start a CHAINED_INVOKE on a function in another region."
    )


@pytest.mark.parametrize("tenant_id", ["tenant-a", "a.b_c:d/e=f+g-h@i j", "x" * 256])
def test_validate_accepts_valid_tenant_ids(tenant_id: str):
    ChainedInvokeOperationValidator.validate(
        None, _start_update(tenant_id=tenant_id), _execution()
    )


@pytest.mark.parametrize(
    "tenant_id", ["", "x" * 257, "bad#tenant", "tab\there", "tenant\n"]
)
def test_validate_rejects_invalid_tenant_ids(tenant_id: str):
    with pytest.raises(InvalidParameterValueException) as exc_info:
        ChainedInvokeOperationValidator.validate(
            None, _start_update(tenant_id=tenant_id), _execution()
        )
    assert str(exc_info.value) == (
        "TenantId must be 1 to 256 characters matching [a-zA-Z0-9._:/=+-@ ]."
    )


def test_validate_skips_region_check_when_execution_has_no_region():
    target = f"arn:aws:lambda:eu-west-1:{PARENT_ACCOUNT}:function:child"
    ChainedInvokeOperationValidator.validate(
        None, _start_update(function_name=target), _execution(region=None)
    )


def test_validate_start_action_with_existing_state():
    """Test START action with existing state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot start a CHAINED_INVOKE operation that already exists",
    ):
        ChainedInvokeOperationValidator.validate(
            current_state, _start_update(), _execution()
        )


def test_validate_start_action_without_options():
    """Test START action without ChainedInvokeOptions raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Update for CHAINED_INVOKE operation requires ChainedInvokeOptions",
    ):
        ChainedInvokeOperationValidator.validate(None, update, _execution())


@pytest.mark.parametrize(
    "action",
    [
        OperationAction.CANCEL,
        OperationAction.SUCCEED,
        OperationAction.FAIL,
        OperationAction.RETRY,
    ],
)
def test_validate_non_start_action_rejected(action: OperationAction):
    """Test every non-START action raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=action,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for CHAINED_INVOKE operation",
    ):
        ChainedInvokeOperationValidator.validate(current_state, update, _execution())


@pytest.mark.parametrize("function_name", [None, 42, ["child"]])
def test_validate_rejects_a_non_string_function_name(function_name):
    """The API model requires FunctionName as a string. Another type is a
    400 validation error, not a 500 from a TypeError."""
    with pytest.raises(InvalidParameterValueException) as exc_info:
        ChainedInvokeOperationValidator.validate(
            None, _start_update(function_name=function_name), _execution()
        )
    assert str(exc_info.value) == f"Invalid function ARN '{function_name}'"


@pytest.mark.parametrize("tenant_id", [7, 1.5, ["t"]])
def test_validate_rejects_a_non_string_tenant_id(tenant_id):
    with pytest.raises(InvalidParameterValueException) as exc_info:
        ChainedInvokeOperationValidator.validate(
            None, _start_update(tenant_id=tenant_id), _execution()
        )
    assert str(exc_info.value) == (
        "TenantId must be 1 to 256 characters matching [a-zA-Z0-9._:/=+-@ ]."
    )
