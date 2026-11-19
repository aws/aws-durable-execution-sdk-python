# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for agentcore_session and the validation of its config."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    InvocationError,
    NonDeterministicExecutionError,
    RetryableSerDesError,
    ValidationError,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import SerDes
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.compute.agentcore import (
    AgentcoreSessionConfig,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore.session import (
    _SessionHandle,
    _validate_session_config,
    agentcore_session,
)


WAIT = 30
ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/reviewer-abc123"

Body = Callable[[_SessionHandle, DurableContext], Any]


def session_config(client: Any, **fields: Any) -> AgentcoreSessionConfig:
    values: dict[str, Any] = {
        "agent_runtime_arn": ARN,
        "timeout": Duration.from_minutes(30),
        "client": client,
        "retry_strategy": lambda error, attempt: RetryDecision.no_retry(),
        **fields,
    }
    return AgentcoreSessionConfig(**values)


def session_handler(config: AgentcoreSessionConfig, body: Body) -> Callable[..., Any]:
    """A durable handler that runs ``body`` in a session named ``review``,
    and returns the message of an error that leaves it."""

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            return {
                "result": agentcore_session(
                    context, "review", config=config, handler=body
                )
            }
        except ChildContextError as error:
            return {"error": "ChildContextError", "message": error.message}

    return handler


def run(handler: Callable[..., Any]) -> DurableFunctionTestResult:
    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        return runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()


def outcome(result: DurableFunctionTestResult) -> Any:
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return json.loads(result.result) if result.result is not None else None


def operation_names(result: DurableFunctionTestResult) -> list[str | None]:
    return [child.name for child in result.get_context("review").child_operations]


def test_returns_the_handlers_value_and_stops_the_session(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()

    result = run(
        session_handler(
            session_config(client),
            lambda session, ctx: {"session": session.runtime_session_id},
        )
    )

    [stop] = client.requests("stop_runtime_session")
    session_id = stop["runtimeSessionId"]
    assert len(session_id) >= 33
    assert stop == {"agentRuntimeArn": ARN, "runtimeSessionId": session_id}
    assert outcome(result) == {"result": {"session": session_id}}
    assert operation_names(result) == ["review.session", "review.stop"]
    session = result.get_context("review").get_step("review.session")
    assert session.result is not None
    assert json.loads(session.result) == session_id


def test_stops_the_session_after_a_handler_error(fake_agentcore_client: Any) -> None:
    client = fake_agentcore_client()

    def body(session: _SessionHandle, ctx: DurableContext) -> Any:
        msg = "no diff"
        raise LookupError(msg)

    result = run(session_handler(session_config(client), body))

    assert outcome(result) == {"error": "ChildContextError", "message": "no diff"}
    assert len(client.requests("stop_runtime_session")) == 1


def test_returns_the_handlers_value_when_the_stop_fails(
    fake_agentcore_client: Any,
) -> None:
    denied: Any = {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}
    client = fake_agentcore_client(stops=[ClientError(denied, "StopRuntimeSession")])

    result = run(session_handler(session_config(client), lambda session, ctx: 7))

    assert outcome(result) == {"result": 7}
    assert len(client.requests("stop_runtime_session")) == 1


def test_retries_a_throttled_stop_with_the_default_strategy(
    fake_agentcore_client: Any,
) -> None:
    throttled: Any = {"Error": {"Code": "ThrottlingException", "Message": "slow"}}
    client = fake_agentcore_client(stops=[ClientError(throttled, "StopRuntimeSession")])

    result = run(
        session_handler(
            session_config(client, retry_strategy=None), lambda session, ctx: 7
        )
    )

    assert outcome(result) == {"result": 7}
    assert len(client.requests("stop_runtime_session")) == 2


def test_does_not_retry_a_denied_stop_with_the_default_strategy(
    fake_agentcore_client: Any,
) -> None:
    denied: Any = {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}
    client = fake_agentcore_client(
        stops=[ClientError(denied, "StopRuntimeSession")] * 5
    )

    result = run(
        session_handler(
            session_config(client, retry_strategy=None), lambda session, ctx: 7
        )
    )

    assert outcome(result) == {"result": 7}
    assert len(client.requests("stop_runtime_session")) == 1


def test_stops_the_session_only_after_a_wait_in_the_handler_ends(
    fake_agentcore_client: Any,
) -> None:
    # SuspendExecution ends the invocation during the wait. A stop for it
    # would end the session that the replay still uses.
    client = fake_agentcore_client()

    def body(session: _SessionHandle, ctx: DurableContext) -> Any:
        return ctx.create_callback(name="approval").result()

    runner = DurableFunctionTestRunner(
        handler=session_handler(session_config(client), body), poll_interval=0.05
    )
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        approval = runner.wait_for_callback(arn, name="approval", timeout=WAIT)
        assert client.requests("stop_runtime_session") == []
        runner.send_callback_success(approval, b"approved")
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert outcome(result) == {"result": "approved"}
    assert len(client.requests("stop_runtime_session")) == 1
    assert operation_names(result) == ["review.session", "approval", "review.stop"]


def test_does_not_stop_the_session_for_an_execution_error(
    fake_agentcore_client: Any,
) -> None:
    # The core SDK records no outcome for an ExecutionError. A stop step
    # recorded for it would change that history.
    client = fake_agentcore_client()

    def body(session: _SessionHandle, ctx: DurableContext) -> Any:
        msg = "history does not match"
        raise NonDeterministicExecutionError(msg)

    result = run(session_handler(session_config(client), body))

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "history does not match" in (result.error.message or "")
    assert client.requests("stop_runtime_session") == []


class FlakySerDes(SerDes[Any]):
    """A serdes whose store fails once, for a reason that a retry can fix."""

    def __init__(self) -> None:
        self.failed = False

    def serialize(self, value: Any, serdes_context: Any) -> str:
        if not self.failed:
            self.failed = True
            msg = "store throttled"
            raise RetryableSerDesError(msg)
        return json.dumps(value)

    def deserialize(self, data: str, serdes_context: Any) -> Any:
        return json.loads(data)


def test_keeps_the_session_for_a_retryable_invocation_error(
    fake_agentcore_client: Any,
) -> None:
    # Lambda runs the invocation again, and the handler replays in the same
    # session. A stop step recorded for the error would take the position
    # of the handler's next operation.
    order: list[str] = []
    client = fake_agentcore_client(stops=[lambda request: order.append("stop")])
    serdes = FlakySerDes()

    def fetch(_: object) -> int:
        order.append("fetch")
        return 1

    def body(session: _SessionHandle, ctx: DurableContext) -> Any:
        return ctx.step(fetch, name="fetch", config=StepConfig(serdes=serdes))

    result = run(session_handler(session_config(client), body))

    assert outcome(result) == {"result": 1}
    assert serdes.failed
    # The replay runs the fetch again. Only then does the session stop.
    assert order == ["fetch", "fetch", "stop"]
    assert operation_names(result) == ["review.session", "fetch", "review.stop"]


def test_stops_the_session_for_an_invocation_error_that_is_not_retryable(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()

    class Fatal(InvocationError):
        def is_retryable(self) -> bool:
            return False

    def body(session: _SessionHandle, ctx: DurableContext) -> Any:
        msg = "checkpoint rejected"
        raise Fatal(msg)

    result = run(session_handler(session_config(client), body))

    assert outcome(result) == {
        "error": "ChildContextError",
        "message": "checkpoint rejected",
    }
    assert len(client.requests("stop_runtime_session")) == 1


def test_validates_the_config_before_any_durable_operation(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()
    calls: list[str] = []

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore_session(
                context,
                "review",
                config=session_config(client, timeout=Duration.from_seconds(0)),
                handler=lambda session, ctx: calls.append("handler"),
            )
        except ValidationError as error:
            return str(error)
        return "no error"

    result = run(handler)

    assert outcome(result) == 'AgentCore "review": timeout must be at least 1 second'
    assert result.operations == []
    assert calls == []
    assert client.calls == []


def test_accepts_a_valid_config() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_minutes(30)
    )

    assert _validate_session_config("review", config) == 1800


def test_config_has_no_default_runtime_arn() -> None:
    fields: dict[str, Any] = {"timeout": Duration.from_minutes(30)}

    with pytest.raises(TypeError, match="agent_runtime_arn"):
        AgentcoreSessionConfig(**fields)


def test_rejects_an_empty_name() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_minutes(30)
    )

    with pytest.raises(
        ValidationError, match="an AgentCore operation requires a non-empty name"
    ):
        _validate_session_config("", config)


def test_rejects_a_non_string_name() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_minutes(30)
    )

    with pytest.raises(
        ValidationError, match="an AgentCore operation requires a non-empty name"
    ):
        _validate_session_config(1, config)  # type: ignore[arg-type]


def test_rejects_a_timeout_under_1_second() -> None:
    config = AgentcoreSessionConfig(
        agent_runtime_arn=ARN, timeout=Duration.from_seconds(0)
    )

    with pytest.raises(
        ValidationError, match='AgentCore "review": timeout must be at least 1 second'
    ):
        _validate_session_config("review", config)
