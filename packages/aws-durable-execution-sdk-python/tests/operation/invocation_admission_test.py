"""Saved replay is allowed without reopening completed contexts to new work."""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import deadline as deadline_module
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.deadline import InvocationDeadline
from aws_durable_execution_sdk_python.exceptions import (
    InvocationDeadlineExceeded,
    OrphanedChildException,
    StepError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ContextDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StepDetails,
)
from aws_durable_execution_sdk_python.operation.step import StepOperationExecutor
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.state import ExecutionState


def complete_context(state: ExecutionState, identifier: OperationIdentifier) -> None:
    state.create_checkpoint(
        OperationUpdate.create_context_succeed(
            identifier, "null", OperationSubType.RUN_IN_CHILD_CONTEXT
        ),
        is_sync=False,
    )


@pytest.mark.parametrize("fail", [False, True])
def test_completed_parent_allows_saved_step_result_without_user_work(
    fail: bool,
) -> None:
    body = Mock()
    client = Mock()
    identifier = OperationIdentifier(
        "saved",
        OperationSubType.STEP,
        parent_id="parent",
        operation_type=OperationType.STEP,
    )
    operation = Operation(
        operation_id="saved",
        parent_id="parent",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        status=OperationStatus.FAILED if fail else OperationStatus.SUCCEEDED,
        step_details=StepDetails(
            result=None if fail else '"saved"',
            error=ErrorObject.from_exception(ValueError("saved failure"))
            if fail
            else None,
        ),
    )
    state = ExecutionState(
        "execution", "token", {"saved": operation}, client, PluginExecutor([])
    )
    complete_context(
        state,
        OperationIdentifier(
            "parent",
            OperationSubType.RUN_IN_CHILD_CONTEXT,
            operation_type=OperationType.CONTEXT,
        ),
    )
    executor = StepOperationExecutor(body, StepConfig(), state, identifier, Mock())
    if fail:
        with pytest.raises(StepError, match="saved failure"):
            executor.process()
    else:
        assert executor.process() == "saved"
    body.assert_not_called()
    client.checkpoint.assert_not_called()


@pytest.mark.parametrize("fail", [False, True])
def test_virtual_reconstruction_scope_never_admits_new_work_or_leaks(
    fail: bool,
) -> None:
    replay_id = OperationIdentifier(
        "replay",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        operation_type=OperationType.CONTEXT,
    )
    virtual_id = OperationIdentifier(
        "virtual",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        parent_id="replay",
        operation_type=OperationType.CONTEXT,
    )
    operation = Operation(
        operation_id="replay",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(replay_children=True),
    )
    state = ExecutionState(
        "execution", "token", {"replay": operation}, Mock(), PluginExecutor([])
    )
    complete_context(state, replay_id)
    new_body = Mock()
    virtual_body = Mock(return_value="reconstructed")
    virtual = state.wrap_user_function(virtual_body, virtual_id, is_virtual=True)

    def reconstruct() -> str:
        assert virtual() == "reconstructed"
        identifier = OperationIdentifier(
            "new",
            OperationSubType.STEP,
            parent_id="replay",
            operation_type=OperationType.STEP,
        )
        with pytest.raises(OrphanedChildException):
            StepOperationExecutor(
                new_body, StepConfig(), state, identifier, Mock()
            ).process()
        if fail:
            raise ValueError("reconstruction failed")
        return "saved"

    replay = state.wrap_user_function(reconstruct, replay_id, is_replay_children=True)
    if fail:
        with pytest.raises(ValueError, match="reconstruction failed"):
            replay()
    else:
        assert replay() == "saved"
    with pytest.raises(OrphanedChildException):
        virtual()
    virtual_body.assert_called_once()
    new_body.assert_not_called()


def test_unsent_terminal_checkpoint_expires_without_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    monkeypatch.setattr(
        deadline_module, "time", SimpleNamespace(monotonic=lambda: now[0])
    )
    client = Mock()
    state = ExecutionState(
        "execution",
        "token",
        {},
        client,
        PluginExecutor([]),
        invocation_deadline=InvocationDeadline(1.0),
    )
    with ThreadPoolExecutor(max_workers=1) as callers:
        future = callers.submit(
            state.create_checkpoint_sync,
            OperationUpdate.create_execution_succeed('"saved"'),
        )
        queued = state._checkpoint_queue.get(timeout=5)  # noqa: SLF001
        state._checkpoint_queue.task_done()  # noqa: SLF001
        now[0] = 2.0
        state._checkpoint_queue.put(queued)  # noqa: SLF001
        state.checkpoint_batches_forever()
        with pytest.raises(InvocationDeadlineExceeded):
            future.result(timeout=5)
    client.checkpoint.assert_not_called()
