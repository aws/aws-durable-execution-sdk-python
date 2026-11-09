"""Regression tests for exposing a terminal execution outcome to plugins.

Issue #882: when a durable execution is stopped or times out while a step is
still running, the surviving invocation's next checkpoint is rejected and the
invocation-end hook reports only a RETRY disposition. No further invocation
follows, so plugins never learn that the execution ended. These tests pin the
shared lifecycle contract: the end hook must carry the authoritative execution
status and end time when the execution has terminated, and preserve uncertainty
otherwise. Only symbols that exist before the fix are imported, and the new
field is read via ``getattr(..., None)``, so on the unfixed code the tests fail
as assertions rather than at import time.
"""

import datetime
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import InvocationError
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInputWithClient,
    InitialExecutionState,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ExecutionDetails,
    LambdaClient,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from tests.test_helpers import plugin_factory


class _EndRecordingPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self.end_infos: list[Any] = []

    def on_invocation_end(self, info) -> None:
        self.end_infos.append(info)


def _invocation_input(service_client: Any) -> DurableExecutionInvocationInputWithClient:
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    return DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=InitialExecutionState(
            operations=[operation], next_marker=""
        ),
        service_client=service_client,
    )


def _lambda_context() -> Mock:
    ctx = Mock()
    ctx.aws_request_id = "test-request"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    ctx.invoked_function_arn = None
    ctx.tenant_id = None
    return ctx


def _run_retry(client: Any) -> _EndRecordingPlugin:
    plugin = _EndRecordingPlugin()

    @durable_execution(plugins=[plugin_factory(plugin)])
    def handler(event: Any, context: DurableContext) -> dict:
        raise InvocationError("checkpoint rejected")

    with pytest.raises(InvocationError):
        handler(_invocation_input(client), _lambda_context())
    return plugin


def test_stopped_execution_surfaces_terminal_status_to_plugin():
    """A RETRY invocation of a STOPPED execution exposes STOPPED and end time."""
    end_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    client = Mock()
    client.get_execution.return_value = Mock(status="STOPPED", end_timestamp=end_time)

    info = _run_retry(client).end_infos[0]

    assert info.status.value == "RETRY"
    status = getattr(info, "execution_status", None)
    assert status is not None
    assert status.value == "STOPPED"
    assert getattr(info, "execution_end_time", None) == end_time


def test_timed_out_execution_surfaces_terminal_status_to_plugin():
    """A RETRY invocation of a TIMED_OUT execution exposes TIMED_OUT."""
    client = Mock()
    client.get_execution.return_value = Mock(status="TIMED_OUT")

    status = getattr(_run_retry(client).end_infos[0], "execution_status", None)
    assert status is not None
    assert status.value == "TIMED_OUT"


def test_running_execution_preserves_retry_uncertainty():
    """A still-running execution leaves the terminal outcome unknown."""
    client = Mock()
    client.get_execution.return_value = Mock(status="RUNNING")

    info = _run_retry(client).end_infos[0]
    assert getattr(info, "execution_status", None) is None
    assert getattr(info, "execution_end_time", None) is None


def test_lookup_failure_preserves_uncertainty_without_blocking():
    """A failing status lookup leaves the outcome unknown and does not raise."""
    client = Mock()
    client.get_execution.side_effect = RuntimeError("service unavailable")

    info = _run_retry(client).end_infos[0]
    assert getattr(info, "execution_status", None) is None
    assert getattr(info, "execution_end_time", None) is None


def test_lambda_client_get_execution_parses_status_and_end_time():
    """LambdaClient.get_execution maps the service model to ExecutionInfo."""
    end_time = datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.UTC)
    client = Mock()
    client.get_durable_execution.return_value = {
        "Status": "STOPPED",
        "EndTimestamp": end_time,
    }

    result = LambdaClient(client).get_execution("arn123")

    client.get_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123"
    )
    assert result.status.value == "STOPPED"
    assert result.end_timestamp == end_time
