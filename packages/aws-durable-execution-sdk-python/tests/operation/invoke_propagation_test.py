"""A propagation hook belongs to a new invoke START, never to a replay read."""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.exceptions import (
    CheckpointError,
    InvokeError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.operation.invoke import InvokeOperationExecutor
from aws_durable_execution_sdk_python.plugin import PropagationMetadata
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState


IDENTIFIER = OperationIdentifier(
    "invoke", OperationSubType.CHAINED_INVOKE, "parent", "named-invoke"
)


def _executor(state: Mock) -> InvokeOperationExecutor:
    return InvokeOperationExecutor(
        "child:live",
        {"payload": [1, 2]},
        state,
        IDENTIFIER,
        InvokeConfig(tenant_id="tenant-a"),
    )


def test_new_start_collects_before_checkpoint_and_preserves_owned_fields() -> None:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "execution"
    state.get_checkpoint_result.return_value = CheckpointedResult.create_not_found()
    events: list[str] = []

    def collect(*args):
        events.append("collect")
        assert args == (IDENTIFIER, "child:live")
        return PropagationMetadata("opaque-header")

    state.provide_propagation_metadata.side_effect = collect
    state.create_checkpoint.side_effect = lambda **_kwargs: events.append("checkpoint")
    assert not _executor(state).check_result_status().is_ready_to_execute
    assert events == ["collect", "checkpoint"]
    update = state.create_checkpoint.call_args.kwargs["operation_update"]
    assert state.create_checkpoint.call_args.kwargs["is_sync"] is True
    assert update.operation_id == "invoke" and update.parent_id == "parent"
    assert update.name == "named-invoke"
    assert update.to_dict()["Payload"] == '{"payload": [1, 2]}'
    assert update.to_dict()["ChainedInvokeOptions"] == {
        "FunctionName": "child:live",
        "TenantId": "tenant-a",
        "XAmznTraceId": "opaque-header",
    }


@pytest.mark.parametrize(
    "status",
    [
        OperationStatus.STARTED,
        OperationStatus.PENDING,
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
    ],
)
def test_checkpointed_start_or_terminal_replay_never_collects(
    status: OperationStatus,
) -> None:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "execution"
    state.get_checkpoint_result.return_value = CheckpointedResult.create_from_operation(
        Operation(
            operation_id="invoke",
            operation_type=OperationType.CHAINED_INVOKE,
            sub_type=OperationSubType.CHAINED_INVOKE,
            parent_id="parent",
            name="named-invoke",
            status=status,
            chained_invoke_details=ChainedInvokeDetails(
                result='"saved"',
                error=ErrorObject(
                    message="saved failure",
                    type="SavedError",
                    data=None,
                    stack_trace=None,
                ),
            ),
        )
    )
    if status is OperationStatus.SUCCEEDED:
        assert _executor(state).process() == "saved"
    elif status in (OperationStatus.STARTED, OperationStatus.PENDING):
        with pytest.raises(SuspendExecution):
            _executor(state).process()
    else:
        with pytest.raises(InvokeError, match="saved failure"):
            _executor(state).process()
    state.provide_propagation_metadata.assert_not_called()
    state.create_checkpoint.assert_not_called()


def test_uncommitted_start_can_recollect_after_checkpoint_failure() -> None:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "execution"
    state.get_checkpoint_result.return_value = CheckpointedResult.create_not_found()
    state.provide_propagation_metadata.side_effect = [
        PropagationMetadata("first"),
        PropagationMetadata("retry"),
    ]
    state.create_checkpoint.side_effect = CheckpointError("not committed")
    for _ in range(2):
        with pytest.raises(CheckpointError, match="not committed"):
            _executor(state).check_result_status()
    updates = [
        call.kwargs["operation_update"]
        for call in state.create_checkpoint.call_args_list
    ]
    assert [update.chained_invoke_options.x_amzn_trace_id for update in updates] == [
        "first",
        "retry",
    ]
    assert all(update.operation_id == "invoke" for update in updates)


@pytest.mark.parametrize(
    "failure", [KeyboardInterrupt, SystemExit, GeneratorExit, asyncio.CancelledError]
)
def test_fatal_hook_failure_prevents_start_checkpoint(
    failure: type[BaseException],
) -> None:
    from aws_durable_execution_sdk_python.plugin import (
        DurableInstrumentationPlugin,
        PluginExecutor,
        PropagationInput,
    )

    class FatalPlugin(DurableInstrumentationPlugin):
        def provide_propagation_metadata(
            self, info: PropagationInput
        ) -> PropagationMetadata:
            raise failure("control flow")

    service = Mock()
    state = ExecutionState(
        "execution", "token", {}, service, PluginExecutor([FatalPlugin()])
    )
    executor: InvokeOperationExecutor[str] = InvokeOperationExecutor(
        "child:live", {}, state, IDENTIFIER, InvokeConfig()
    )
    with pytest.raises(failure, match="control flow"):
        executor.check_result_status()
    service.checkpoint.assert_not_called()
