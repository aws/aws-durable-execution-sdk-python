"""A terminal plugin cannot change the failed checkpoint replay raises."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    DurableOperationError,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
)
from aws_durable_execution_sdk_python.types import StepContext
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from tests.e2e.checkpoint_response_int_test import (
    create_mock_checkpoint_with_operations,
)
from tests.test_helpers import operation_id_sequence


def test_terminal_error_mutation_preserves_failure_on_handler_replay() -> None:
    step_id = next(operation_id_sequence())
    terminal_errors: list[list[str]] = []
    invocation_errors: list[list[str]] = []
    executed: list[str] = []

    class MutatingPlugin(DurableInstrumentationPlugin):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.operation_id == step_id:
                assert info.error is not None
                assert info.error.stack_trace is not None
                terminal_errors.append(list(info.error.stack_trace))
                info.error.stack_trace.clear()

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            error = info.operations[step_id].error
            assert error is not None
            assert error.stack_trace is not None
            invocation_errors.append(list(error.stack_trace))

    class Factory:
        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            return MutatingPlugin()

    def charge(_context: StepContext) -> str:
        executed.append("charged")
        return "unexpected execution"

    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(charge, name="charge")

    wrapped = durable_execution(handler, plugins=[Factory()])
    stored_error = {
        "ErrorType": "ValueError",
        "ErrorMessage": "original failure",
        "StackTrace": ["original frame"],
    }
    event: dict[str, Any] = {
        "DurableExecutionArn": "test-arn/execution-1",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                },
                {
                    "Id": step_id,
                    "Type": "STEP",
                    "Status": "FAILED",
                    "SubType": "Step",
                    "Name": "charge",
                    "StepDetails": {"Error": stored_error},
                },
            ],
            "NextMarker": "",
        },
        "UpdatedOperationIds": [step_id],
        "LocalRunner": True,
    }
    lambda_context = Mock()
    lambda_context.aws_request_id = "request-1"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    lambda_context.invoked_function_arn = "test-arn"
    lambda_context.tenant_id = None

    with patch("aws_durable_execution_sdk_python.execution.LambdaClient") as client:
        checkpoint, _calls = create_mock_checkpoint_with_operations()
        client.initialize_client.return_value.checkpoint = checkpoint
        first = wrapped(event, lambda_context)
        event["UpdatedOperationIds"] = []
        lambda_context.aws_request_id = "request-2"
        replay = wrapped(event, lambda_context)

    for result in (first, replay):
        assert result["Status"] == "FAILED"
        assert result["Error"]["ErrorType"] == (
            "aws_durable_execution_sdk_python.exceptions.StepError"
        )
        assert result["Error"]["ErrorMessage"] == "original failure"
        assert result["Error"]["StackTrace"] == ["original frame"]
    assert terminal_errors == [["original frame"]]
    assert invocation_errors == [["original frame"], ["original frame"]]
    assert stored_error["StackTrace"] == ["original frame"]
    assert executed == []


def test_virtual_child_terminal_snapshot_preserves_raised_error() -> None:
    original = DurableOperationError(
        message="original failure", data="details", stack_trace=["original frame"]
    )
    terminal_infos: list[OperationEndInfo] = []
    caught: list[ChildContextError] = []

    class MutatingPlugin(DurableInstrumentationPlugin):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.name == "virtual-child":
                assert info.error is not None
                assert info.error.stack_trace is not None
                terminal_infos.append(info)
                info.error.stack_trace.clear()

    class Factory:
        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            return MutatingPlugin()

    def fail(_context: DurableContext) -> None:
        raise original

    def handler(_event: Any, context: DurableContext) -> str:
        try:
            context.run_in_child_context(
                fail, name="virtual-child", config=ChildConfig(is_virtual=True)
            )
        except ChildContextError as error:
            caught.append(error)
            return "caught"
        raise AssertionError("Expected the virtual child's original failure")

    with DurableFunctionTestRunner(
        handler=durable_execution(handler, plugins=[Factory()])
    ) as runner:
        result = runner.run(input="{}", execution_timeout=15)

    assert result.status.value == "SUCCEEDED"
    assert result.result == '"caught"'
    (terminal_info,) = terminal_infos
    assert terminal_info.start_time is None
    assert terminal_info.error is not None
    assert terminal_info.error.stack_trace == []
    (raised,) = caught
    assert raised.message == "original failure"
    assert raised.error_type == original.error_type
    assert raised.data == "details"
    assert raised.stack_trace == ["original frame"]
    assert original.stack_trace == ["original frame"]
