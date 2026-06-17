"""Unit tests for CheckpointRequestDispatcher.

Covers ``apply_updates(execution, updates, client_token, touch)``: it
mutates ``execution.operations`` in place, records per-op size in
``execution.operation_size_bytes``, calls ``touch`` once per accepted
update, and returns the lifecycle effects raised by the updates.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.effects import Completed
from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (
    CheckpointRequestDispatcher,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


class _MockProcessor(OperationProcessor):
    """Hand-rolled processor stub. Records calls, returns a configured
    Operation (or None) for each invocation."""

    def __init__(self, return_value=None):
        self.return_value = return_value
        self.calls: list[tuple] = []

    def process(self, update, current_op, notifier, execution_arn):
        self.calls.append((update, current_op, notifier, execution_arn))
        return self.return_value


def _make_execution(operations: list | None = None) -> Execution:
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    return Execution("test-arn", start_input, operations or [])


def test_dispatcher_init_uses_default_processors():
    dispatcher = CheckpointRequestDispatcher()

    assert OperationType.STEP in dispatcher.processors
    assert OperationType.WAIT in dispatcher.processors
    assert OperationType.CONTEXT in dispatcher.processors
    assert OperationType.CALLBACK in dispatcher.processors
    assert OperationType.EXECUTION in dispatcher.processors


def test_dispatcher_init_accepts_custom_processors():
    custom = {OperationType.STEP: _MockProcessor()}
    dispatcher = CheckpointRequestDispatcher(processors=custom)

    assert dispatcher.processors is custom


def test_apply_updates_with_empty_list_is_a_noop():
    dispatcher = CheckpointRequestDispatcher()
    execution = _make_execution()
    touched: list[str] = []

    dispatcher.apply_updates(
        execution=execution,
        updates=[],
        client_token=None,
        touch=touched.append,
    )

    assert execution.operations == []
    assert execution.operation_size_bytes == {}
    assert touched == []


def test_apply_updates_unknown_type_raises():
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: _MockProcessor()},
    )
    execution = _make_execution()
    update = OperationUpdate(
        operation_id="some-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Checkpoint for OperationType.WAIT is not implemented yet.",
    ):
        dispatcher.apply_updates(
            execution=execution,
            updates=[update],
            client_token=None,
            touch=lambda _: None,
        )


def test_apply_updates_skips_ops_when_processor_returns_none():
    mock_processor = _MockProcessor(return_value=None)
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: mock_processor},
    )
    execution = _make_execution()
    update = OperationUpdate(
        operation_id="skipped",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    touched: list[str] = []

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=touched.append,
    )

    assert execution.operations == []
    assert touched == []
    # Update is still recorded for audit purposes.
    assert execution.updates == [update]


def test_apply_updates_appends_new_operation_and_touches():
    new_op = Mock()
    new_op.operation_id = "new-op"
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: _MockProcessor(return_value=new_op)},
    )
    execution = _make_execution()
    touched: list[str] = []

    update = OperationUpdate(
        operation_id="new-op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=touched.append,
    )

    assert execution.operations == [new_op]
    assert touched == ["new-op"]
    assert "new-op" in execution.operation_size_bytes


def test_apply_updates_replaces_existing_operation_in_place():
    existing = Mock()
    existing.operation_id = "target"
    replaced = Mock()
    replaced.operation_id = "target"

    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: _MockProcessor(return_value=replaced)},
    )
    execution = _make_execution([existing])

    update = OperationUpdate(
        operation_id="target",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=lambda _: None,
    )

    assert execution.operations == [replaced]


def test_apply_updates_preserves_order_across_multiple_updates():
    op1 = Mock(operation_id="op1")
    op1.operation_id = "op1"
    op2 = Mock()
    op2.operation_id = "op2"
    op3 = Mock()
    op3.operation_id = "op3"

    updated_op2 = Mock()
    updated_op2.operation_id = "op2"
    new_op4 = Mock()
    new_op4.operation_id = "op4"

    processor = _MockProcessor()
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: processor},
    )
    execution = _make_execution([op1, op2, op3])
    touched: list[str] = []

    # First: update op2 in the middle.
    processor.return_value = updated_op2
    dispatcher.apply_updates(
        execution=execution,
        updates=[
            OperationUpdate(
                operation_id="op2",
                operation_type=OperationType.STEP,
                action=OperationAction.SUCCEED,
            ),
        ],
        client_token=None,
        touch=touched.append,
    )
    assert execution.operations == [op1, updated_op2, op3]

    # Second: append op4 at the end.
    processor.return_value = new_op4
    dispatcher.apply_updates(
        execution=execution,
        updates=[
            OperationUpdate(
                operation_id="op4",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            ),
        ],
        client_token=None,
        touch=touched.append,
    )
    assert execution.operations == [op1, updated_op2, op3, new_op4]
    assert touched == ["op2", "op4"]


def test_apply_updates_dispatches_by_operation_type():
    step_op = Mock()
    step_op.operation_id = "step-id"
    wait_op = Mock()
    wait_op.operation_id = "wait-id"

    step_processor = _MockProcessor(return_value=step_op)
    wait_processor = _MockProcessor(return_value=wait_op)

    dispatcher = CheckpointRequestDispatcher(
        processors={
            OperationType.STEP: step_processor,
            OperationType.WAIT: wait_processor,
        },
    )
    execution = _make_execution()

    dispatcher.apply_updates(
        execution=execution,
        updates=[
            OperationUpdate(
                operation_id="step-id",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            ),
            OperationUpdate(
                operation_id="wait-id",
                operation_type=OperationType.WAIT,
                action=OperationAction.START,
            ),
        ],
        client_token=None,
        touch=lambda _: None,
    )

    assert execution.operations == [step_op, wait_op]
    assert len(step_processor.calls) == 1
    assert len(wait_processor.calls) == 1


def test_apply_updates_forwards_arn_notifier_and_current_op_to_processor():
    existing = Mock()
    existing.operation_id = "id"
    processor = _MockProcessor(return_value=existing)
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: processor},
    )
    execution = _make_execution([existing])

    update = OperationUpdate(
        operation_id="id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=lambda _: None,
    )

    forwarded_update, forwarded_current_op, forwarded_notifier, forwarded_arn = (
        processor.calls[0]
    )
    assert forwarded_update == update
    assert forwarded_current_op == existing
    assert isinstance(forwarded_notifier, ExecutionNotifier)
    assert forwarded_arn == execution.durable_execution_arn


def test_apply_updates_returns_completion_effect():
    """An EXECUTION SUCCEED update surfaces a Completed effect for the
    caller to apply after the write."""
    dispatcher = CheckpointRequestDispatcher()
    execution = _make_execution()

    update = OperationUpdate(
        operation_id="exec-op",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="final-result",
    )

    effects = dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=lambda _: None,
    )

    assert effects == [
        Completed(execution_arn=execution.durable_execution_arn, result="final-result")
    ]


def test_apply_updates_returns_no_effects_for_plain_step():
    """A STEP START produces an operation but no lifecycle effect."""
    new_op = Mock()
    new_op.operation_id = "step-op"
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: _MockProcessor(return_value=new_op)},
    )
    execution = _make_execution()

    effects = dispatcher.apply_updates(
        execution=execution,
        updates=[
            OperationUpdate(
                operation_id="step-op",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            )
        ],
        client_token=None,
        touch=lambda _: None,
    )

    assert effects == []


def test_apply_updates_records_payload_size_for_paging():
    """The sidecar dict feeds OperationPaginatorState._size_for."""
    new_op = Mock()
    new_op.operation_id = "with-payload"
    processor = _MockProcessor(return_value=new_op)
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: processor},
    )
    execution = _make_execution()

    update = OperationUpdate(
        operation_id="with-payload",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="hello-world-payload",
    )

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=lambda _: None,
    )

    assert execution.operation_size_bytes["with-payload"] >= len(b"hello-world-payload")


def test_apply_updates_records_size_for_error_payload():
    """Covers the error-branch of _estimate_payload_size. When the
    update carries an error (not a payload), the size estimate should
    include the JSON-serialised error dict."""
    new_op = Mock()
    new_op.operation_id = "with-error"
    processor = _MockProcessor(return_value=new_op)
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: processor},
    )
    execution = _make_execution()

    update = OperationUpdate(
        operation_id="with-error",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        error=ErrorObject.from_message("something broke"),
    )

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=lambda _: None,
    )

    # Some non-zero size was recorded (exact value depends on
    # error.to_dict() shape; just assert > 0).
    assert execution.operation_size_bytes["with-error"] > 0


def test_apply_updates_records_size_for_bytes_payload():
    """Covers the bytes-payload branch of _byte_length."""
    new_op = Mock()
    new_op.operation_id = "with-bytes"
    processor = _MockProcessor(return_value=new_op)
    dispatcher = CheckpointRequestDispatcher(
        processors={OperationType.STEP: processor},
    )
    execution = _make_execution()

    update = OperationUpdate(
        operation_id="with-bytes",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload=b"binary-bytes",
    )

    dispatcher.apply_updates(
        execution=execution,
        updates=[update],
        client_token=None,
        touch=lambda _: None,
    )

    # bytes payload length == 12.
    assert execution.operation_size_bytes["with-bytes"] == 12
