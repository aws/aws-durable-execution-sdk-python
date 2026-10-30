"""Only successful state delivery acknowledges pending operation versions."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import (
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.executor import Executor, InvocationState
from aws_durable_execution_sdk_python_testing.invoker import (
    InProcessInvoker,
    InvokeResponse,
)
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


@pytest.fixture(params=["memory", "filesystem"])
def stored_execution(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[tuple[Executor, ExecutionStore, str]]:
    store: ExecutionStore = (
        FileSystemExecutionStore.create(tmp_path / "store")
        if request.param == "filesystem"
        else InMemoryExecutionStore()
    )
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
    for name in ("delivered", "later", "checkpoint"):
        execution.operations.append(
            Operation(
                operation_id=name,
                operation_type=OperationType.WAIT,
                status=OperationStatus.STARTED,
            )
        )
        execution.touch_operation(name)
    execution.handler_seen_seq = execution.seq_counter
    execution.complete_wait("delivered")
    store.save(execution)
    executor = Executor(store, Mock(), InProcessInvoker(Mock(), Mock()), Mock())
    try:
        yield executor, store, execution.durable_execution_arn
    finally:
        executor.shutdown()
        executor._registry.shutdown()  # noqa: SLF001


def test_building_input_does_not_acknowledge_delivery(
    stored_execution: tuple[Executor, ExecutionStore, str],
) -> None:
    executor, store, arn = stored_execution
    before = store.load(arn).handler_seen_seq
    claim = executor._begin_invocation(arn)  # noqa: SLF001
    assert claim is not None
    _, invocation_input, sequence = claim
    assert invocation_input.updated_operation_ids == ["delivered"]
    saved = store.load(arn)
    assert saved.updated_operation_ids == ["delivered"]
    assert saved.handler_seen_seq == before < sequence


@pytest.mark.parametrize("status", list(InvocationStatus))
def test_only_valid_response_acknowledges_input(
    stored_execution: tuple[Executor, ExecutionStore, str],
    status: InvocationStatus,
) -> None:
    executor, store, arn = stored_execution
    execution = store.load(arn)
    sequence = execution.seq_counter
    before = execution.handler_seen_seq
    executor._finish_invocation(  # noqa: SLF001
        arn,
        InvokeResponse(
            invocation_output=DurableExecutionInvocationOutput(status=status),
            request_id="request",
        ),
        datetime.now(UTC),
        datetime.now(UTC),
        sequence,
    )
    saved = store.load(arn)
    valid = status is not InvocationStatus.RETRY
    assert saved.handler_seen_seq == (sequence if valid else before)
    assert saved.updated_operation_ids == ([] if valid else ["delivered"])


@pytest.mark.parametrize("checkpoint_during_invocation", [False, True])
def test_input_ack_preserves_newer_completion_and_checkpoint_watermark(
    stored_execution: tuple[Executor, ExecutionStore, str],
    checkpoint_during_invocation: bool,
) -> None:
    executor, store, arn = stored_execution
    execution = store.load(arn)
    input_sequence = execution.seq_counter
    if checkpoint_during_invocation:
        execution.complete_wait("checkpoint")
        OperationPaginatorState.pin(execution).advance_handler_seen(
            execution.seq_counter
        )
    already_seen = execution.handler_seen_seq
    execution.complete_wait("later")
    execution.needs_reinvoke = True
    store.save(execution)
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
    assert saved.updated_operation_ids == ["later"]
    assert [
        op.operation_id for op in OperationPaginatorState.pin(saved).unseen_operations()
    ] == ["later"]
    reinvoke.assert_called_once_with(arn)


@pytest.mark.parametrize("seen,ack", [(0, 0), (5, 5), (5, 3)])
def test_acknowledgment_prunes_only_covered_versions_in_legacy_state(
    stored_execution: tuple[Executor, ExecutionStore, str], seen: int, ack: int
) -> None:
    _, store, arn = stored_execution
    snapshot = store.load(arn).to_json_dict()
    for field in (
        "SeqCounter",
        "HandlerSeenSeq",
        "OperationLastTouchedSeq",
        "OperationSizeBytes",
    ):
        snapshot.pop(field)
    snapshot["UpdatedOperationIds"] = ["legacy", "covered", "newer"]
    restored = Execution.from_json_dict(snapshot)
    restored.handler_seen_seq = seen
    restored.operation_last_touched_seq.update(covered=ack, newer=ack + 1)
    store.save(restored)
    execution = store.load(arn)
    paginator = OperationPaginatorState.pin(execution)
    paginator.advance_handler_seen(ack)
    paginator.advance_handler_seen(ack)
    assert execution.updated_operation_ids == ["newer"]
    assert execution.handler_seen_seq == seen
    assert execution.seq_counter == 0


def test_older_acknowledgment_preserves_newer_version_of_same_operation(
    stored_execution: tuple[Executor, ExecutionStore, str],
) -> None:
    _, store, arn = stored_execution
    execution = store.load(arn)
    sequence = execution.seq_counter
    # A new version of an already-pending ID must survive the old snapshot's ack.
    execution.touch_operation("delivered")
    OperationPaginatorState.pin(execution).advance_handler_seen(sequence)
    assert execution.updated_operation_ids == ["delivered"]
    OperationPaginatorState.pin(execution).advance_handler_seen(execution.seq_counter)
    assert execution.updated_operation_ids == []


@pytest.mark.parametrize("rejection", ["token", "stale", "operation"])
def test_rejected_checkpoint_retains_undelivered_update(
    stored_execution: tuple[Executor, ExecutionStore, str], rejection: str
) -> None:
    executor, store, arn = stored_execution
    execution = store.load(arn)
    token = execution.get_new_checkpoint_token()
    if rejection == "stale":
        execution.advance_token_sequence()
    store.save(execution)
    before = (
        execution.token_sequence,
        execution.handler_seen_seq,
        execution.seq_counter,
    )
    executor._set_invocation_gate(arn, InvocationState.INVOKING)  # noqa: SLF001
    with pytest.raises(InvalidParameterValueException):
        executor.checkpoint_execution(
            execution_arn=arn,
            checkpoint_token="invalid" if rejection == "token" else token,
            updates=(
                [
                    OperationUpdate(
                        operation_id="delivered",
                        operation_type=OperationType.WAIT,
                        action=OperationAction.FAIL,
                        error=ErrorObject.from_message("invalid wait failure"),
                    )
                ]
                if rejection == "operation"
                else []
            ),
        )
    saved = store.load(arn)
    assert saved.updated_operation_ids == ["delivered"]
    assert (saved.token_sequence, saved.handler_seen_seq, saved.seq_counter) == before
