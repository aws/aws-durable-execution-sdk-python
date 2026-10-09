"""Public invoke/checkpoint/replay behavior with a recording service boundary.

These tests exercise the SDK lifecycle, not generated wire support. The latter
is checked independently against the installed client in invoke_wire_propagation_test.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.exceptions import CheckpointError
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    PropagationInput,
    PropagationMetadata,
)


ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:parent:1/durable-execution/test/id"
)
START = datetime(2026, 10, 5, tzinfo=UTC)


def lambda_context() -> Mock:
    context = Mock()
    context.aws_request_id = "request"
    context.client_context = None
    context.identity = None
    context._epoch_deadline_time_in_ms = 0
    context.invoked_function_arn = (
        "arn:aws:lambda:us-west-2:123456789012:function:parent:1"
    )
    context.tenant_id = "runtime-tenant"
    return context


def event(operations: list[Operation] | None = None) -> dict[str, Any]:
    return {
        "DurableExecutionArn": ARN,
        "CheckpointToken": "checkpoint-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "id",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "StartTimestamp": int(START.timestamp() * 1000),
                    "ExecutionDetails": {"InputPayload": "{}"},
                },
                *(operation.to_json_dict() for operation in operations or []),
            ],
            "NextMarker": "",
        },
    }


class RecordingService:
    def __init__(self) -> None:
        self.updates: list[OperationUpdate] = []
        self.operations: list[Operation] = []
        self.fail = False

    def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        assert durable_execution_arn == ARN
        assert checkpoint_token
        self.updates.extend(updates)
        if self.fail:
            raise CheckpointError("uncommitted START")
        for update in updates:
            self.operations.append(
                Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    parent_id=update.parent_id,
                    name=update.name,
                    sub_type=update.sub_type,
                    status=OperationStatus.STARTED,
                    start_timestamp=START,
                )
            )
        return CheckpointOutput(
            "next-token",
            CheckpointUpdatedExecutionState(operations=self.operations.copy()),
        )


class RecordingPlugin(DurableInstrumentationPlugin):
    def __init__(self, mode: str = "value") -> None:
        self.calls: list[PropagationInput] = []
        self.mode = mode

    def provide_propagation_metadata(
        self, info: PropagationInput
    ) -> PropagationMetadata | None:
        self.calls.append(info)
        if self.mode == "error":
            raise ValueError("instrumentation failed")
        if self.mode == "absent":
            return None
        if self.mode == "blank":
            return PropagationMetadata(" \t\n")
        return PropagationMetadata(f"header-{len(self.calls)}")


def body(_event: Any, context: DurableContext) -> Any:
    return context.invoke(
        "child:live",
        {"payload": [1, 2]},
        name="call-child",
        config=InvokeConfig(tenant_id="explicit-tenant"),
    )


@pytest.mark.parametrize(
    "terminal",
    [
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
    ],
)
def test_public_invoke_pending_and_terminal_replay_do_not_recollect(
    monkeypatch: pytest.MonkeyPatch, terminal: OperationStatus
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    plugin = RecordingPlugin()
    service = RecordingService()
    handler = durable_execution(body, plugins=[plugin])
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
        return_value=service,
    ):
        first = handler(event(), lambda_context())
        assert first["Status"] == "PENDING"
        assert len(plugin.calls) == 1 and len(service.updates) == 1
        update = service.updates[0]
        assert plugin.calls == [
            PropagationInput(ARN, update.operation_id, "child:live", None)
        ]
        assert update.name == "call-child"
        assert update.payload is not None
        assert json.loads(update.payload) == {"payload": [1, 2]}
        assert update.to_dict()["ChainedInvokeOptions"] == {
            "FunctionName": "child:live",
            "TenantId": "explicit-tenant",
            "XAmznTraceId": "header-1",
        }
        pending = handler(event(service.operations), lambda_context())
        assert pending["Status"] == "PENDING"
        saved = replace(
            service.operations[0],
            status=terminal,
            end_timestamp=START,
            chained_invoke_details=ChainedInvokeDetails(
                result='"saved child result"'
                if terminal is OperationStatus.SUCCEEDED
                else None,
                error=None
                if terminal is OperationStatus.SUCCEEDED
                else ErrorObject("saved child error", "ChildError", None, None),
            ),
        )
        result = handler(event([saved]), lambda_context())
    assert len(plugin.calls) == 1 and len(service.updates) == 1
    if terminal is OperationStatus.SUCCEEDED:
        assert result == {"Status": "SUCCEEDED", "Result": '"saved child result"'}
    else:
        assert result["Status"] == "FAILED"
        assert result["Error"]["ErrorMessage"] == "saved child error"
        assert result["Error"]["ErrorType"].endswith("InvokeError")


@pytest.mark.parametrize(
    "mode", ["no-plugins", "absent-hook", "absent", "blank", "error"]
)
def test_public_invoke_without_a_contribution_keeps_legacy_request(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    plugin = RecordingPlugin(mode)
    plugins: list[DurableInstrumentationPlugin] = (
        []
        if mode == "no-plugins"
        else [DurableInstrumentationPlugin()]
        if mode == "absent-hook"
        else [plugin]
    )
    service = RecordingService()
    handler = durable_execution(body, plugins=plugins)
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
        return_value=service,
    ):
        assert handler(event(), lambda_context())["Status"] == "PENDING"
    assert len(service.updates) == 1
    assert service.updates[0].to_dict()["ChainedInvokeOptions"] == {
        "FunctionName": "child:live",
        "TenantId": "explicit-tenant",
    }


def test_public_invoke_recollects_after_failed_uncommitted_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    plugin = RecordingPlugin()
    service = RecordingService()
    handler = durable_execution(body, plugins=[plugin])
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
        return_value=service,
    ):
        service.fail = True
        with pytest.raises(CheckpointError, match="uncommitted START"):
            handler(event(), lambda_context())
        service.fail = False
        assert handler(event(), lambda_context())["Status"] == "PENDING"
        assert (
            handler(event(service.operations), lambda_context())["Status"] == "PENDING"
        )
    assert len(plugin.calls) == 2
    assert plugin.calls[0] == plugin.calls[1]
    headers: list[str | None] = []
    for update in service.updates:
        assert update.chained_invoke_options is not None
        headers.append(update.chained_invoke_options.x_amzn_trace_id)
    assert headers == ["header-1", "header-2"]
    assert service.updates[0].operation_id == service.updates[1].operation_id


def test_blank_plugin_does_not_mask_later_header_on_real_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    blank, healthy = RecordingPlugin("blank"), RecordingPlugin()
    service = RecordingService()
    handler = durable_execution(body, plugins=[blank, healthy])
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
        return_value=service,
    ):
        assert handler(event(), lambda_context())["Status"] == "PENDING"
    assert blank.calls == healthy.calls
    assert (
        service.updates[0].to_dict()["ChainedInvokeOptions"]["XAmznTraceId"]
        == "header-1"
    )
