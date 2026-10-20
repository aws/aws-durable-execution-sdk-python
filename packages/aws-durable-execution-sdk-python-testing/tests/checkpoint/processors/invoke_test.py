"""Tests for the CHAINED_INVOKE operation processor."""

from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    ErrorObject,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
    ChainedInvokeStarted,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.invoke import (
    ChainedInvokeProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier

NOW = datetime(2024, 1, 1, tzinfo=UTC)


def _start_update(
    options: ChainedInvokeOptions | None,
    payload: str | None = '{"n": 1}',
) -> OperationUpdate:
    return OperationUpdate(
        operation_id="invoke-1",
        operation_type=OperationType.CHAINED_INVOKE,
        sub_type=OperationSubType.CHAINED_INVOKE,
        action=OperationAction.START,
        name="double",
        payload=payload,
        chained_invoke_options=options,
    )


def test_start_records_started_operation_and_started_effect():
    notifier = ExecutionNotifier()
    update = _start_update(
        ChainedInvokeOptions(function_name="child", tenant_id="tenant-a")
    )

    operation = ChainedInvokeProcessor().process(
        update, None, notifier, "arn-parent", NOW
    )

    # STARTED at checkpoint, as the service's checkpoint response carries
    # a resolvable target; no other status is visible before completion.
    assert operation.status == OperationStatus.STARTED
    assert operation.operation_type == OperationType.CHAINED_INVOKE
    assert operation.start_timestamp == NOW
    assert operation.end_timestamp is None
    assert operation.chained_invoke_details is not None
    assert operation.chained_invoke_details.result is None
    assert operation.chained_invoke_details.error is None

    assert notifier.effects == [
        ChainedInvokeStarted(
            execution_arn="arn-parent",
            operation_id="invoke-1",
            function_name="child",
            tenant_id="tenant-a",
            payload='{"n": 1}',
        )
    ]


def test_start_with_a_passing_preflight_dispatches():
    seen: list[str] = []

    def preflight(function_name: str) -> ErrorObject | None:
        seen.append(function_name)
        return None

    notifier = ExecutionNotifier()
    operation = ChainedInvokeProcessor(preflight).process(
        _start_update(ChainedInvokeOptions(function_name="child:prod", tenant_id=None)),
        None,
        notifier,
        "arn-parent",
        NOW,
    )

    assert seen == ["child:prod"]  # the identifier as the handler wrote it
    assert operation.status == OperationStatus.STARTED
    assert len(notifier.effects) == 1


def test_start_with_a_failing_preflight_fails_in_the_checkpoint_response():
    """The service returns a target it cannot resolve as FAILED in the
    checkpoint response, so the handler raises without suspending. The
    processor records the failure at once and dispatches nothing."""
    error = ErrorObject(
        message="Function not found: child.",
        type="ResourceNotFoundException",
        data=None,
        stack_trace=None,
    )
    notifier = ExecutionNotifier()

    operation = ChainedInvokeProcessor(lambda _name: error).process(
        _start_update(ChainedInvokeOptions(function_name="child", tenant_id=None)),
        None,
        notifier,
        "arn-parent",
        NOW,
    )

    assert operation.status == OperationStatus.FAILED
    assert operation.start_timestamp == NOW
    assert operation.end_timestamp == NOW
    assert operation.chained_invoke_details is not None
    assert operation.chained_invoke_details.error == error
    assert operation.chained_invoke_details.result is None
    assert notifier.effects == []


def test_start_without_options_is_rejected():
    with pytest.raises(InvalidParameterValueException) as excinfo:
        ChainedInvokeProcessor().process(
            _start_update(options=None), None, ExecutionNotifier(), "arn", NOW
        )
    assert "requires ChainedInvokeOptions" in str(excinfo.value)


@pytest.mark.parametrize(
    "action",
    [OperationAction.CANCEL, OperationAction.SUCCEED, OperationAction.FAIL],
)
def test_non_start_action_is_rejected(action: OperationAction):
    update = OperationUpdate(
        operation_id="invoke-1",
        operation_type=OperationType.CHAINED_INVOKE,
        action=action,
        chained_invoke_options=ChainedInvokeOptions(function_name="child"),
    )
    with pytest.raises(InvalidParameterValueException) as excinfo:
        ChainedInvokeProcessor().process(update, None, ExecutionNotifier(), "arn", NOW)
    assert str(excinfo.value) == "Invalid action for CHAINED_INVOKE operation."


def test_a_failed_preflight_stores_the_update_without_its_input():
    """The service keeps no input for a target it cannot resolve. The
    stored update keeps the function name and drops the payload, so
    history shows the name and the error and the input weighs nothing."""
    error = ErrorObject.from_message("Function not found: child.")
    processor = ChainedInvokeProcessor(lambda _name: error)
    update = _start_update(ChainedInvokeOptions(function_name="child", tenant_id=None))
    failed = processor.process(update, None, ExecutionNotifier(), "arn-parent", NOW)

    stored = processor.stored_update(update, None, failed)

    assert stored.payload is None
    assert stored.chained_invoke_options.function_name == "child"


def test_a_started_invoke_stores_the_update_as_sent():
    processor = ChainedInvokeProcessor(lambda _name: None)
    update = _start_update(ChainedInvokeOptions(function_name="child", tenant_id=None))
    started = processor.process(update, None, ExecutionNotifier(), "arn-parent", NOW)

    assert processor.stored_update(update, None, started) is update
