"""Real callback failure keeps runtime diagnostics and history consistent."""

from datetime import datetime
from pathlib import Path
import time
from typing import Any

import pytest

from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallbackError,
    CallbackExternalError,
)
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationStatus,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import (
    GetDurableExecutionHistoryResponse,
    events_to_operations,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


@pytest.mark.parametrize("filesystem", [False, True])
@pytest.mark.parametrize("error_kind", ["omitted", "empty", "rich"])
def test_live_callback_failure_preserves_error_through_persistence(
    filesystem: bool,
    error_kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store: ExecutionStore = (
        FileSystemExecutionStore.create(tmp_path / "store")
        if filesystem
        else InMemoryExecutionStore()
    )
    supplied_error = (
        ErrorObject(
            message="rejected",
            type="RejectedError",
            data='{"reason":"test"}',
            stack_trace=["original frame"],
        )
        if error_kind == "rich"
        else ErrorObject.from_dict({})
    )
    expected_error = supplied_error
    completed_operations: list[Operation] = []
    original_complete = Execution.complete_callback_failure

    def capture_live_completion(
        execution: Execution,
        callback_id: str,
        error: ErrorObject,
        now: datetime | None = None,
    ) -> Operation:
        # Capture the real API transition before the executor persists it.
        # The same error object must survive both memory and filesystem storage.
        operation = original_complete(execution, callback_id, error, now=now)
        completed_operations.append(operation)
        return operation

    monkeypatch.setattr(Execution, "complete_callback_failure", capture_live_completion)
    step_calls: list[str] = []
    caught_errors: list[CallbackError] = []

    @durable_step
    def saved_step(_context: Any) -> str:
        step_calls.append("saved")
        return "saved"

    def durable_handler(_event: Any, context: DurableContext) -> str:
        saved = context.step(saved_step(), name="saved-step")
        callback = context.create_callback(name="failed-callback")
        try:
            callback.result()
        except CallbackError as error:
            caught_errors.append(error)
            return saved
        raise AssertionError("Callback failure was not raised")

    handler = durable_execution(durable_handler)
    with DurableFunctionTestRunner(
        handler=handler,
        store=store,
        skip_time=False,
        execution_timeout=15,
        poll_interval=0.01,
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = runner.wait_for_callback(arn, name="failed-callback", timeout=5)
        deadline = time.monotonic() + 5
        while not store.load(arn).invocation_completions:
            assert time.monotonic() < deadline, "Initial invocation did not suspend"
            time.sleep(0.01)
        if error_kind == "omitted":
            runner.send_callback_failure(callback_id)
        else:
            runner.send_callback_failure(callback_id, supplied_error)
        result = runner.wait_for_result(arn, timeout=10)
        detailed = runner.get_execution_history(arn, include_execution_data=True)
        metadata_only = runner.get_execution_history(arn, include_execution_data=False)
        saved_execution = store.load(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == '"saved"'
    assert step_calls == ["saved"]
    assert len(saved_execution.invocation_completions) == 2
    assert len(caught_errors) == 1
    caught = caught_errors[0]
    assert isinstance(caught, CallbackExternalError)
    assert caught.message == (expected_error.message or "Callback failed")
    assert caught.data == expected_error.data
    assert caught.stack_trace == expected_error.stack_trace

    assert len(completed_operations) == 1
    live = completed_operations[0]
    persisted = next(
        op for op in saved_execution.operations if op.operation_id == live.operation_id
    )
    reloaded = Execution.from_json_dict(saved_execution.to_json_dict())
    roundtripped = next(
        op for op in reloaded.operations if op.operation_id == live.operation_id
    )
    for operation in (live, persisted, roundtripped):
        assert operation.status is OperationStatus.FAILED
        assert operation.callback_details is not None
        assert operation.callback_details.error == expected_error

    for history, include_data in ((detailed, True), (metadata_only, False)):
        wire = history.to_dict()
        failed = next(
            event for event in wire["Events"] if event["EventType"] == "CallbackFailed"
        )
        expected: dict[str, Any] = {"Truncated": not include_data}
        if include_data:
            expected["Payload"] = expected_error.to_dict()
        assert failed["CallbackFailedDetails"]["Error"] == expected
        decoded = GetDurableExecutionHistoryResponse.from_dict(wire)
        decoded_failed = next(
            event for event in decoded.events if event.event_type == "CallbackFailed"
        )
        assert decoded_failed.callback_failed_details is not None
        assert decoded_failed.callback_failed_details.error is not None
        assert decoded_failed.callback_failed_details.error.to_dict() == expected
        for snapshot in (history, decoded):
            reconstructed = next(
                op
                for op in events_to_operations(snapshot.events)
                if op.operation_id == live.operation_id
            )
            assert reconstructed.status is OperationStatus.FAILED
            assert reconstructed.callback_details is not None
            assert reconstructed.callback_details.error == (
                expected_error if include_data else None
            )
        assert history.to_dict() == wire

    assert live.callback_details is not None
    assert live.callback_details.error == expected_error
