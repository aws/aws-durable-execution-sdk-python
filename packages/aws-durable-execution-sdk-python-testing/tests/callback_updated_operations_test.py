"""Callback terminal transitions retain first-delivery update metadata."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.exceptions import IllegalStateException
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.invoker import (
    InProcessInvoker,
    LambdaInvoker,
)
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


Outcome = Literal["success", "failure", "timeout"]
NOW = datetime(2026, 10, 27, tzinfo=UTC)
ERROR = ErrorObject(
    "callback failure", "CallbackRejected", "details", ["original frame"]
)


def _complete(execution: Execution, outcome: Outcome, callback_id: str) -> Operation:
    if outcome == "success":
        return execution.complete_callback_success(callback_id, b'"result"', now=NOW)
    if outcome == "failure":
        return execution.complete_callback_failure(callback_id, ERROR, now=NOW)
    return execution.complete_callback_timeout(callback_id, ERROR, now=NOW)


@pytest.mark.parametrize("outcome", ["success", "failure", "timeout"])
@pytest.mark.parametrize("filesystem", [False, True])
@pytest.mark.parametrize("already_pending", [False, True])
def test_callback_completion_records_only_successful_state_changes(
    outcome: Outcome, filesystem: bool, already_pending: bool, tmp_path: Path
) -> None:
    target = Operation(
        operation_id="callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="callback-id"),
    )
    other = Operation(
        operation_id="other",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(callback_id="other-id", result='"other"'),
    )
    execution = Execution(
        "test-arn",
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name="test-execution",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-id",
        ),
        [target, other],
    )
    execution.updated_operation_ids = (
        ["other", "callback"] if already_pending else ["other"]
    )
    before = execution.to_json_dict()
    with pytest.raises(IllegalStateException):
        _complete(execution, outcome, "missing")
    assert execution.to_json_dict() == before

    completed = _complete(execution, outcome, "callback-id")
    assert execution.updated_operation_ids == ["other", "callback"]
    assert execution.seq_counter == 1
    assert execution.operation_last_touched_seq == {"callback": 1}
    assert execution.token_sequence == 0
    assert (
        completed.status.value
        == {"success": "SUCCEEDED", "failure": "FAILED", "timeout": "TIMED_OUT"}[
            outcome
        ]
    )
    assert completed.end_timestamp == NOW
    assert completed.callback_details is not None
    assert completed.callback_details.result == (
        '"result"' if outcome == "success" else None
    )
    assert completed.callback_details.error == (None if outcome == "success" else ERROR)
    assert execution.operations[1] == other

    store: ExecutionStore = (
        FileSystemExecutionStore.create(tmp_path)
        if filesystem
        else InMemoryExecutionStore()
    )
    store.save(execution)
    restored = store.load(execution.durable_execution_arn)
    assert restored.updated_operation_ids == ["other", "callback"]
    assert restored.operations == execution.operations
    assert restored.seq_counter == 1
    assert restored.operation_last_touched_seq == {"callback": 1}
    for invoker in [InProcessInvoker(Mock(), Mock()), LambdaInvoker(Mock())]:
        invocation_input = invoker.create_invocation_input(restored)
        assert invocation_input.updated_operation_ids == ["other", "callback"]
        assert invocation_input.initial_execution_state.operations == [completed, other]

    # Rejecting a second completion must not resurrect an already-delivered ID.
    restored.mark_state_delivered()
    before = restored.to_json_dict()
    with pytest.raises(IllegalStateException, match="not in STARTED state"):
        _complete(restored, outcome, "callback-id")
    assert restored.to_json_dict() == before
    assert restored.updated_operation_ids == []
