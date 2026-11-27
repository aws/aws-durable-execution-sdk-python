# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for agentcore_session and the validation of its config."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest
from aws_durable_execution_sdk_python.config import (
    Duration,
    StepConfig,
    StepSemantics,
)
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
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationAction,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import SerDes
from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extensions.compute.agentcore import (
    MAX_JOB_INPUT_BYTES,
    AgentcoreError,
    AgentcoreInvokeOptions,
    AgentcoreSession,
    AgentcoreSessionConfig,
    agentcore_session,
)
from aws_durable_execution_sdk_python_extensions.compute.agentcore.operation import (
    _validate_name,
)


WAIT = 30
ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/reviewer-abc123"

Body = Callable[[AgentcoreSession, DurableContext], Any]


def session_config(client: Any, **fields: Any) -> AgentcoreSessionConfig:
    values: dict[str, Any] = {
        "agent_runtime_arn": ARN,
        "client": client,
        "retry_strategy": lambda error, attempt: RetryDecision.no_retry(),
        **fields,
    }
    return AgentcoreSessionConfig(**values)


def session_handler(
    config: AgentcoreSessionConfig,
    body: Body,
    default_step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE,
) -> Callable[..., Any]:
    """A durable handler that runs ``body`` in a session named ``review``,
    and returns the message of an error that leaves it."""

    @durable_execution(default_step_semantics=default_step_semantics)
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

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
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

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
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

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
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

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
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

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        msg = "checkpoint rejected"
        raise Fatal(msg)

    result = run(session_handler(session_config(client), body))

    assert outcome(result) == {
        "error": "ChildContextError",
        "message": "checkpoint rejected",
    }
    assert len(client.requests("stop_runtime_session")) == 1


def test_validates_the_name_before_any_durable_operation(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()
    calls: list[str] = []

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore_session(
                context,
                "",
                config=session_config(client),
                handler=lambda session, ctx: calls.append("handler"),
            )
        except ValidationError as error:
            return str(error)
        return "no error"

    result = run(handler)

    assert outcome(result) == "an AgentCore operation requires a non-empty name"
    assert result.operations == []
    assert calls == []
    assert client.calls == []


def test_accepts_a_valid_name() -> None:
    _validate_name("review")


def test_config_has_no_default_runtime_arn() -> None:
    with pytest.raises(TypeError, match="agent_runtime_arn"):
        AgentcoreSessionConfig()  # type: ignore[call-arg]


def test_config_has_no_timeout() -> None:
    fields: dict[str, Any] = {"timeout": Duration.from_minutes(30)}

    with pytest.raises(TypeError, match="timeout"):
        AgentcoreSessionConfig(agent_runtime_arn=ARN, **fields)


def test_rejects_a_non_string_name() -> None:
    with pytest.raises(
        ValidationError, match="an AgentCore operation requires a non-empty name"
    ):
        _validate_name(1)  # type: ignore[arg-type]


OPTIONS = AgentcoreInvokeOptions(timeout=Duration.from_minutes(5))
DENIED: Any = {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}


def run_jobs(
    handler: Callable[..., Any], complete: dict[str, bytes | ErrorObject]
) -> DurableFunctionTestResult:
    """Run ``handler``, and complete each named callback in order."""
    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        for name, answer in complete.items():
            callback_id = runner.wait_for_callback(arn, name=name, timeout=WAIT)
            if isinstance(answer, ErrorObject):
                runner.send_callback_failure(callback_id, answer)
            else:
                runner.send_callback_success(callback_id, answer)
        return runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()


def payload(request: dict[str, Any]) -> Any:
    return json.loads(request["payload"])


FAILED = ErrorObject(
    message="3 tests failed", type="RuntimeError", data=None, stack_trace=None
)


def test_two_jobs_use_the_same_session_and_their_own_callbacks(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        build = session.invoke(ctx, "build", job_input={"step": 1}, options=OPTIONS)
        return session.invoke(ctx, "test", job_input=build, options=OPTIONS)

    result = run_jobs(
        session_handler(session_config(client), body),
        {"build.callback": b'{"built": true}', "test.callback": b'"passed"'},
    )

    assert outcome(result) == {"result": "passed"}
    build, test = client.requests("invoke_agent_runtime")
    [stop] = client.requests("stop_runtime_session")
    assert build["runtimeSessionId"] == stop["runtimeSessionId"]
    assert test["runtimeSessionId"] == stop["runtimeSessionId"]
    assert payload(build)["input"] == {"step": 1}
    assert payload(test)["input"] == {"built": True}
    assert payload(build)["callbackId"] != payload(test)["callbackId"]
    assert operation_names(result) == ["review.session", "build", "test", "review.stop"]
    job = result.get_context("review").get_context("build")
    assert [child.name for child in job.child_operations] == [
        "build.callback",
        "build.request",
    ]


def test_labels_every_operation_with_its_subtype(fake_agentcore_client: Any) -> None:
    client = fake_agentcore_client()

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        session.invoke(ctx, "build", job_input={}, options=OPTIONS)
        return session.invoke(ctx, "test", job_input={}, options=OPTIONS)

    runner = DurableFunctionTestRunner(
        handler=session_handler(session_config(client), body), poll_interval=0.05
    )
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        for name in ("build.callback", "test.callback"):
            callback_id = runner.wait_for_callback(arn, name=name, timeout=WAIT)
            runner.send_callback_success(callback_id, b'"done"')
        result = runner.wait_for_result(arn, timeout=WAIT)
        history = runner.get_execution_history(arn)
    finally:
        runner.close()

    assert outcome(result) == {"result": "done"}
    expected = {
        "review": "AgentcoreSession",
        "review.session": "AgentcoreSessionId",
        "build": "AgentcoreSessionJob",
        "build.callback": "AgentcoreCallback",
        "build.request": "AgentcoreRequest",
        "test": "AgentcoreSessionJob",
        "test.callback": "AgentcoreCallback",
        "test.request": "AgentcoreRequest",
        "review.stop": "AgentcoreStop",
    }
    context = result.get_context("review")
    recorded_tree: dict[str | None, str | None] = {"review": context.sub_type}
    for child in context.child_operations:
        recorded_tree[child.name] = child.sub_type
        if child.name in ("build", "test"):
            job = context.get_context(child.name)
            for step in job.child_operations:
                recorded_tree[step.name] = step.sub_type
    assert recorded_tree == expected
    # The execution history carries the subtype on every event of each
    # operation.
    recorded: dict[str, set[str | None]] = {}
    for event in history.events:
        if event.name in expected:
            recorded.setdefault(event.name, set()).add(event.sub_type)
    assert recorded == {name: {sub_type} for name, sub_type in expected.items()}


@pytest.mark.parametrize(
    ("fields", "invokes", "complete", "expected"),
    [
        ({}, [], {"build.callback": FAILED}, "AgentcoreJobFailedError"),
        (
            {"timeout": Duration.from_seconds(2)},
            [],
            {},
            "AgentcoreTimeoutError",
        ),
        ({}, [ClientError(DENIED, "InvokeAgentRuntime")], {}, "AgentcoreDeliveryError"),
    ],
)
def test_a_job_error_reaches_the_handler_with_its_agentcore_class(
    fake_agentcore_client: Any,
    fields: dict[str, Any],
    invokes: list[Any],
    complete: dict[str, Any],
    expected: str,
) -> None:
    client = fake_agentcore_client(invokes=invokes)
    options = AgentcoreInvokeOptions(**{"timeout": OPTIONS.timeout, **fields})

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        try:
            return session.invoke(ctx, "build", job_input={}, options=options)
        except AgentcoreError as error:
            return type(error).__name__

    result = run_jobs(session_handler(session_config(client), body), complete)

    assert outcome(result) == {"result": expected}


def test_a_job_error_that_leaves_the_handler_keeps_its_class(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()
    config = session_config(client)

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        return session.invoke(ctx, "build", job_input={}, options=OPTIONS)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore_session(context, "review", config=config, handler=body)
        except AgentcoreError as error:
            return {"error": type(error).__name__, "message": error.message}
        return None

    result = run_jobs(handler, {"build.callback": FAILED})

    assert outcome(result) == {
        "error": "AgentcoreJobFailedError",
        "message": 'AgentCore "build": the job failed (RuntimeError): 3 tests failed',
    }
    assert len(client.requests("stop_runtime_session")) == 1


def test_a_failed_job_does_not_stop_the_session(fake_agentcore_client: Any) -> None:
    client = fake_agentcore_client(invokes=[ClientError(DENIED, "InvokeAgentRuntime")])

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        try:
            session.invoke(ctx, "build", job_input={}, options=OPTIONS)
        except AgentcoreError:
            pass
        return session.invoke(ctx, "retry", job_input={}, options=OPTIONS)

    result = run_jobs(
        session_handler(session_config(client), body), {"retry.callback": b"1"}
    )

    assert outcome(result) == {"result": 1}
    assert [call for call, _ in client.calls] == [
        "invoke_agent_runtime",
        "invoke_agent_runtime",
        "stop_runtime_session",
    ]


def test_checks_the_job_input_size_before_any_job_operation(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        job_input = "x" * MAX_JOB_INPUT_BYTES
        try:
            session.invoke(ctx, "build", job_input=job_input, options=OPTIONS)
        except ValueError as error:
            return str(error)
        return None

    result = run_jobs(session_handler(session_config(client), body), {})

    assert "the job input is" in outcome(result)["result"]
    assert operation_names(result) == ["review.session", "review.stop"]
    assert client.requests("invoke_agent_runtime") == []


def test_rejects_the_outer_context_before_the_job_starts(
    fake_agentcore_client: Any,
) -> None:
    # A job created in the outer context lands outside the session's child
    # context. The core SDK does not refuse it, and the next replay fails
    # with NonDeterministicExecutionError after the job has run.
    client = fake_agentcore_client()
    config = session_config(client)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
            try:
                session.invoke(context, "build", job_input={}, options=OPTIONS)
            except TypeError as error:
                return str(error)
            return None

        return agentcore_session(context, "review", config=config, handler=body)

    result = run_jobs(handler, {})

    assert outcome(result) == (
        'AgentCore session "review": invoke received the context that '
        "agentcore_session was called with. Pass the context that the handler "
        "receives, or a context nested in it."
    )
    assert client.requests("invoke_agent_runtime") == []
    assert operation_names(result) == ["review.session", "review.stop"]


def test_a_job_uses_the_session_config_and_the_job_options(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client(invokes=[ClientError(DENIED, "InvokeAgentRuntime")])
    config = session_config(
        client,
        qualifier="DEFAULT",
        # The default strategy does not retry a denied request. So a retry
        # shows that the job uses the session's strategy.
        retry_strategy=lambda error, attempt: (
            RetryDecision.retry(Duration.from_seconds(1))
            if attempt < 2
            else RetryDecision.no_retry()
        ),
    )
    options = AgentcoreInvokeOptions(
        timeout=OPTIONS.timeout, heartbeat_timeout=Duration.from_minutes(1)
    )

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        return session.invoke(ctx, "build", job_input={}, options=options)

    result = run_jobs(session_handler(config, body), {"build.callback": b"1"})

    assert outcome(result) == {"result": 1}
    denied, accepted = client.requests("invoke_agent_runtime")
    assert denied["qualifier"] == "DEFAULT"
    assert accepted["qualifier"] == "DEFAULT"
    assert payload(accepted)["heartbeatTimeoutSeconds"] == 60


class ReplayedSession:
    """A session with two jobs and an approval wait between them.

    Each wait ends an invocation, and the next invocation replays the
    handler. So the handler runs at least once per wait. ``session_ids``
    holds the session ID that each run of the handler sees.
    ``at_approval`` holds the calls that the client had received while the
    execution waited for the approval.
    """

    def __init__(self, client: Any) -> None:
        self.client = client
        self.session_ids: list[str] = []
        self.at_approval: list[str] = []

    def body(self, session: AgentcoreSession, ctx: DurableContext) -> Any:
        self.session_ids.append(session.runtime_session_id)
        build = session.invoke(ctx, "build", job_input={"step": 1}, options=OPTIONS)
        approval = ctx.create_callback(name="approval").result()
        return session.invoke(
            ctx,
            "test",
            job_input={"build": build, "approval": approval},
            options=OPTIONS,
        )

    def run(self) -> DurableFunctionTestResult:
        runner = DurableFunctionTestRunner(
            handler=session_handler(session_config(self.client), self.body),
            poll_interval=0.05,
        )
        try:
            arn = runner.run_async(input="{}", execution_timeout=WAIT)
            build = runner.wait_for_callback(arn, name="build.callback", timeout=WAIT)
            runner.send_callback_success(build, b'{"built": true}')
            approval = runner.wait_for_callback(arn, name="approval", timeout=WAIT)
            self.at_approval = [call for call, _ in self.client.calls]
            runner.send_callback_success(approval, b"yes")
            test = runner.wait_for_callback(arn, name="test.callback", timeout=WAIT)
            runner.send_callback_success(test, b'"passed"')
            return runner.wait_for_result(arn, timeout=WAIT)
        finally:
            runner.close()


def test_replay_keeps_the_same_session_id(fake_agentcore_client: Any) -> None:
    client = fake_agentcore_client()
    session = ReplayedSession(client)

    result = session.run()

    assert outcome(result) == {"result": "passed"}
    # The first run, and one replay after each of the three waits.
    assert len(session.session_ids) >= 4
    [stop] = client.requests("stop_runtime_session")
    build, test = client.requests("invoke_agent_runtime")
    assert set(session.session_ids) == {stop["runtimeSessionId"]}
    assert build["runtimeSessionId"] == stop["runtimeSessionId"]
    assert test["runtimeSessionId"] == stop["runtimeSessionId"]


def test_replay_sends_no_second_request_for_a_completed_job(
    fake_agentcore_client: Any,
) -> None:
    client = fake_agentcore_client()
    session = ReplayedSession(client)

    result = session.run()

    assert outcome(result) == {"result": "passed"}
    assert session.at_approval == ["invoke_agent_runtime"]
    build, test = client.requests("invoke_agent_runtime")
    assert payload(build)["input"] == {"step": 1}
    assert payload(test)["input"] == {"build": {"built": True}, "approval": "yes"}


def test_a_wait_between_jobs_records_no_stop(fake_agentcore_client: Any) -> None:
    client = fake_agentcore_client()
    session = ReplayedSession(client)

    result = session.run()

    assert outcome(result) == {"result": "passed"}
    assert "stop_runtime_session" not in session.at_approval
    assert [call for call, _ in client.calls] == [
        "invoke_agent_runtime",
        "invoke_agent_runtime",
        "stop_runtime_session",
    ]
    assert operation_names(result) == [
        "review.session",
        "build",
        "approval",
        "test",
        "review.stop",
    ]


def test_replay_reads_job_results_and_errors_from_checkpoints(
    fake_agentcore_client: Any,
) -> None:
    # Each job's callback wait ends an invocation. So the later runs of the
    # handler read the outcome of each earlier job from its checkpoint.
    client = fake_agentcore_client()
    config = session_config(client)
    seen: list[tuple[str, Any]] = []

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        try:
            session.invoke(ctx, "build", job_input={}, options=OPTIONS)
        except AgentcoreError as error:
            seen.append(("build", type(error).__name__))
        seen.append(
            ("lint", session.invoke(ctx, "lint", job_input={}, options=OPTIONS))
        )
        return session.invoke(ctx, "test", job_input={}, options=OPTIONS)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore_session(context, "review", config=config, handler=body)
        except AgentcoreError as error:
            return {"error": type(error).__name__, "message": error.message}
        return None

    result = run_jobs(
        handler,
        {
            "build.callback": FAILED,
            "lint.callback": b'{"ok": true}',
            "test.callback": FAILED,
        },
    )

    assert outcome(result) == {
        "error": "AgentcoreJobFailedError",
        "message": 'AgentCore "test": the job failed (RuntimeError): 3 tests failed',
    }
    builds = [value for job, value in seen if job == "build"]
    lints = [value for job, value in seen if job == "lint"]
    # The build error is read in the run after the build, and in the
    # replays after the lint and the test.
    assert len(builds) >= 3
    assert set(builds) == {"AgentcoreJobFailedError"}
    assert len(lints) >= 2
    assert all(lint == {"ok": True} for lint in lints)
    # The failed test job still stops the session, once.
    assert len(client.requests("invoke_agent_runtime")) == 3
    assert len(client.requests("stop_runtime_session")) == 1
    assert operation_names(result) == [
        "review.session",
        "build",
        "lint",
        "test",
        "review.stop",
    ]


def test_session_id_step_waits_for_its_save(fake_agentcore_client: Any) -> None:
    """The session ID becomes the runtime session of later steps. So its step
    waits for its SUCCEED checkpoint, even when default_step_semantics is
    AT_LEAST_ONCE_ASYNC."""
    client = fake_agentcore_client()
    original_checkpoint = InMemoryServiceClient.checkpoint
    events: list[str] = []

    def checkpoint(
        self: InMemoryServiceClient,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> Any:
        saves_id = any(
            update.name == "review.session" and update.action is OperationAction.SUCCEED
            for update in updates
        )
        if saves_id:
            # A step that does not wait for this save lets the handler run now.
            time.sleep(0.5)
        output = original_checkpoint(
            self, durable_execution_arn, checkpoint_token, updates, client_token
        )
        if saves_id:
            events.append("session ID saved")
        return output

    def body(session: AgentcoreSession, ctx: DurableContext) -> Any:
        events.append("handler started")
        return session.invoke(ctx, "build", job_input=1, options=OPTIONS)

    with patch.object(InMemoryServiceClient, "checkpoint", checkpoint):
        result = run_jobs(
            session_handler(
                session_config(client),
                body,
                default_step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC,
            ),
            {"build.callback": b'"built"'},
        )

    assert outcome(result) == {"result": "built"}
    # The service answered the save before the handler first started. The
    # handler starts again in the replay after the callback.
    assert events[:2] == ["session ID saved", "handler started"]
    session = result.get_context("review").get_step("review.session")
    assert session.result is not None
    [request] = client.requests("invoke_agent_runtime")
    assert request["runtimeSessionId"] == json.loads(session.result)
