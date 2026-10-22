"""Invocation acknowledgments preserve newer checkpoint and completion state."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python_testing.execution import (
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.invoker import InvokeResponse
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.token import CallbackToken


@pytest.mark.parametrize("checkpoint_during_invocation", [False, True])
def test_input_ack_preserves_newer_completion_and_checkpoint_watermark(
    checkpoint_during_invocation,
):
    store = InMemoryExecutionStore()
    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=30,
            execution_retention_period_days=1,
            invocation_id="invocation",
        )
    )
    execution.start()
    arn = execution.durable_execution_arn
    tokens = {}
    for name in ("a", "b", "c"):
        tokens[name] = CallbackToken(execution_arn=arn, operation_id=name).to_str()
        execution.operations.append(
            Operation(
                operation_id=name,
                operation_type=OperationType.CALLBACK,
                status=OperationStatus.STARTED,
                callback_details=CallbackDetails(callback_id=tokens[name]),
            )
        )
        execution.touch_operation(name)
    OperationPaginatorState.pin(execution).advance_handler_seen(execution.seq_counter)
    execution.complete_callback_success(tokens["a"], b'"A"')
    input_sequence = execution.seq_counter
    if checkpoint_during_invocation:
        execution.complete_callback_success(tokens["c"], b'"C"')
        OperationPaginatorState.pin(execution).advance_handler_seen(
            execution.seq_counter
        )
    already_seen = execution.handler_seen_seq
    execution.complete_callback_success(tokens["b"], b'"B"')
    execution.needs_reinvoke = True
    store.save(execution)
    executor = Executor(store, Mock(), Mock(), Mock())
    try:
        with patch.object(executor, "_invoke_execution") as reinvoke:
            executor._finish_invocation(  # noqa: SLF001
                arn,
                InvokeResponse(
                    invocation_output=DurableExecutionInvocationOutput(
                        status=InvocationStatus.PENDING
                    ),
                    request_id="request",
                ),
                datetime.now(UTC),
                datetime.now(UTC),
                input_sequence,
            )
        saved = store.load(arn)
        assert saved.handler_seen_seq == max(already_seen, input_sequence)
        assert saved.updated_operation_ids == ["b"]
        assert [
            op.operation_id
            for op in OperationPaginatorState.pin(saved).unseen_operations()
        ] == ["b"]
        reinvoke.assert_called_once_with(arn)
    finally:
        executor.shutdown()
        executor._registry.shutdown()  # noqa: SLF001
