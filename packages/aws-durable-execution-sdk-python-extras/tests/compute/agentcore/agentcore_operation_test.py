# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the agentcore operation, run on the local test runner."""

from __future__ import annotations

import io
import json
import threading
import time
from collections.abc import Callable, Iterator
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import Callback, DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    InvocationError,
    NonDeterministicExecutionError,
    ValidationError,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

from aws_durable_execution_sdk_python_extras.compute.agentcore import (
    MAX_JOB_INPUT_BYTES,
    AgentcoreConfig,
    AgentcoreDeliveryError,
    AgentcoreError,
    AgentcoreNotAcceptedError,
    ComputeError,
    agentcore,
    default_agentcore_retry_strategy,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore import operation


WAIT = 30
ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/reviewer-abc123"


def agentcore_error(code: str, status: int = 400) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, "InvokeAgentRuntime")


def config_with(client: Any, **fields: Any) -> AgentcoreConfig:
    values: dict[str, Any] = {
        "agent_runtime_arn": ARN,
        "timeout": Duration.from_minutes(5),
        "client": client,
        # One quick retry keeps the retry tests fast.
        "retry_strategy": lambda error, attempt: (
            RetryDecision.retry(Duration.from_seconds(1))
            if attempt < 2
            and getattr(error, "response", {}).get("Error", {}).get("Code")
            == "ThrottlingException"
            else RetryDecision.no_retry()
        ),
        **fields,
    }
    return AgentcoreConfig(**values)


def outcome(result: DurableFunctionTestResult) -> Any:
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return json.loads(result.result) if result.result is not None else None


Complete = Callable[[DurableFunctionTestRunner, str], None]


@pytest.fixture
def run() -> Iterator[Callable[..., Any]]:
    """Run a handler on the local test runner, and close the runner after."""
    runners: list[DurableFunctionTestRunner] = []

    def _run(
        handler: Callable[..., Any], event: Any, complete: Complete | None = None
    ) -> DurableFunctionTestResult:
        runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
        runners.append(runner)
        arn = runner.run_async(input=json.dumps(event), execution_timeout=WAIT)
        if complete is not None:
            callback_id = runner.wait_for_callback(
                arn, name="review.callback", timeout=WAIT
            )
            complete(runner, callback_id)
        return runner.wait_for_result(arn, timeout=WAIT)

    yield _run
    for runner in runners:
        runner.close()


def succeed_with(value: Any) -> Complete:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_success(callback_id, json.dumps(value).encode())

    return complete


def fail_with(error_type: str | None, message: str) -> Complete:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_failure(
            callback_id,
            ErrorObject(message=message, type=error_type, data=None, stack_trace=None),
        )

    return complete


def catching(config: AgentcoreConfig) -> Callable[..., Any]:
    """A handler that returns the class and message of an AgentCore error."""

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            return {
                "result": agentcore(context, "review", job_input=event, config=config)
            }
        except AgentcoreError as error:
            return {"error": type(error).__name__, "message": error.message}

    return handler


def test_delivers_the_job_returns_the_result_and_stops_the_session(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()
    config = config_with(client, heartbeat_timeout=Duration.from_seconds(60))

    result = run(catching(config), {"pr": 7}, succeed_with({"approved": True}))

    assert outcome(result) == {"result": {"approved": True}}
    [invoke] = client.requests("invoke_agent_runtime")
    session_id = invoke["runtimeSessionId"]
    assert len(session_id) >= 33
    payload = json.loads(invoke.pop("payload"))
    assert invoke == {
        "agentRuntimeArn": ARN,
        "runtimeSessionId": session_id,
        "contentType": "application/json",
        "accept": "application/json",
    }
    assert payload == {
        "version": 1,
        "region": "us-west-2",
        "callbackId": payload["callbackId"],
        "heartbeatTimeoutSeconds": 60,
        "input": {"pr": 7},
    }
    assert client.requests("stop_runtime_session") == [
        {"agentRuntimeArn": ARN, "runtimeSessionId": session_id}
    ]
    assert [stream.closed for stream in client.streams] == [True]


def test_records_the_operations_in_one_child_context(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()

    result = run(catching(config_with(client)), {}, succeed_with(1))

    context = result.get_context("review")
    assert [child.name for child in context.child_operations] == [
        "review.session",
        "review.callback",
        "review.request",
        "review.stop",
    ]
    session = context.get_step("review.session")
    assert session.result is not None
    [invoke] = client.requests("invoke_agent_runtime")
    assert json.loads(session.result) == invoke["runtimeSessionId"]


def test_passes_the_qualifier_to_both_calls(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()

    run(catching(config_with(client, qualifier="DEFAULT")), {}, succeed_with(1))

    [invoke] = client.requests("invoke_agent_runtime")
    [stop] = client.requests("stop_runtime_session")
    assert invoke["qualifier"] == "DEFAULT"
    assert stop["qualifier"] == "DEFAULT"


def test_replay_and_a_retried_request_reuse_the_session_id(
    run: Any, fake_agentcore_client: Any
) -> None:
    # The callback wait ends the first invocation. The stop step runs in a
    # replay, so its session ID comes from the session step's checkpoint.
    client = fake_agentcore_client(
        invokes=[agentcore_error("ThrottlingException", 429)]
    )

    result = run(catching(config_with(client)), {}, succeed_with("ok"))

    assert outcome(result) == {"result": "ok"}
    invokes = client.requests("invoke_agent_runtime")
    [stop] = client.requests("stop_runtime_session")
    assert len(invokes) == 2
    assert {invoke["runtimeSessionId"] for invoke in invokes} == {
        stop["runtimeSessionId"]
    }
    assert invokes[0]["payload"] == invokes[1]["payload"]


def test_fails_with_the_job_failed_error_and_still_stops(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()

    result = run(
        catching(config_with(client)), {}, fail_with("RuntimeError", "3 tests failed")
    )

    assert outcome(result) == {
        "error": "AgentcoreJobFailedError",
        "message": 'AgentCore "review": the job failed (RuntimeError): 3 tests failed',
    }
    assert len(client.requests("stop_runtime_session")) == 1


def test_fails_with_the_job_failed_error_for_a_result_that_is_not_json(
    run: Any, fake_agentcore_client: Any
) -> None:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_success(callback_id, b"not json")

    result = run(catching(config_with(fake_agentcore_client())), {}, complete)

    assert outcome(result)["error"] == "AgentcoreJobFailedError"


def test_fails_with_a_timeout_error_when_no_result_arrives(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()
    config = config_with(client, timeout=Duration.from_seconds(2))

    result = run(catching(config), {})

    error = outcome(result)
    assert error["error"] == "AgentcoreTimeoutError"
    assert error["message"].startswith('AgentCore "review": the job timed out: ')
    assert len(client.requests("stop_runtime_session")) == 1


def test_fails_with_a_delivery_error_and_stops_when_the_request_fails(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client(invokes=[agentcore_error("AccessDeniedException")])

    result = run(catching(config_with(client)), {})

    error = outcome(result)
    assert error["error"] == "AgentcoreDeliveryError"
    assert error["message"].startswith(
        'AgentCore "review": the job could not be delivered: '
    )
    assert "AccessDeniedException" in error["message"]
    assert len(client.requests("invoke_agent_runtime")) == 1
    assert len(client.requests("stop_runtime_session")) == 1


@pytest.mark.parametrize("answer", [b'{"accepted":false}', b"[]", b"not json", b""])
def test_fails_with_a_delivery_error_when_the_agent_does_not_accept(
    run: Any, fake_agentcore_client: Any, answer: bytes
) -> None:
    client = fake_agentcore_client(invokes=[answer])

    result = run(catching(config_with(client)), {})

    error = outcome(result)
    assert error["error"] == "AgentcoreDeliveryError"
    assert 'did not answer {"accepted": true}' in error["message"]
    # The default strategy does not retry it, and neither does the test's.
    assert len(client.requests("invoke_agent_runtime")) == 1
    assert len(client.requests("stop_runtime_session")) == 1


def test_returns_the_result_when_stop_fails(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client(stops=[agentcore_error("AccessDeniedException")])

    result = run(catching(config_with(client)), {}, succeed_with("done"))

    assert outcome(result) == {"result": "done"}
    stop = result.get_context("review").get_step("review.stop")
    assert stop.error is not None


def test_keeps_the_job_error_when_stop_fails(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client(stops=[agentcore_error("AccessDeniedException")])

    result = run(catching(config_with(client)), {}, fail_with("ValueError", "bad"))

    assert outcome(result)["error"] == "AgentcoreJobFailedError"


def test_treats_resource_not_found_from_stop_as_stopped(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client(
        stops=[agentcore_error("ResourceNotFoundException", 404)]
    )

    result = run(catching(config_with(client)), {}, succeed_with("done"))

    assert outcome(result) == {"result": "done"}
    assert result.get_context("review").get_step("review.stop").error is None


def test_raises_the_same_class_after_the_handler_stops_catching(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client(invokes=[agentcore_error("ValidationException")])
    config = config_with(client)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore(context, "review", job_input=event, config=config)
        except ComputeError as error:
            return type(error).__name__
        return None

    assert outcome(run(handler, {})) == "AgentcoreDeliveryError"


@pytest.mark.parametrize(
    ("name", "fields", "match"),
    [
        ("", {}, "non-empty name"),
        ("review", {"timeout": Duration.from_seconds(0)}, "timeout must be at least"),
        (
            "review",
            {"heartbeat_timeout": Duration.from_seconds(0)},
            "heartbeat_timeout must be at least",
        ),
    ],
)
def test_rejects_a_config_that_is_not_valid_before_any_operation(
    fake_agentcore_client: Any, name: str, fields: dict[str, Any], match: str
) -> None:
    client = fake_agentcore_client()
    context: Any = None

    with pytest.raises(ValidationError, match=match):
        agentcore(context, name, job_input={}, config=config_with(client, **fields))
    assert client.calls == []


def test_rejects_a_job_input_over_the_size_limit_before_the_request(
    run: Any, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()
    config = config_with(client)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore(
                context,
                "review",
                job_input="x" * (MAX_JOB_INPUT_BYTES - 1),
                config=config,
            )
        except ValueError as error:
            return str(error)
        return None

    message = outcome(run(handler, {}))
    assert "the job input is" in message
    assert client.calls == []


def test_rejects_a_job_input_that_is_not_json(
    run: Any, fake_agentcore_client: Any
) -> None:
    config = config_with(fake_agentcore_client())

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore(context, "review", job_input={1, 2}, config=config)
        except TypeError as error:
            return str(error)
        return None

    assert "JSON-serializable" in outcome(run(handler, {}))


def test_requests_match_the_bedrock_agentcore_api_model(run: Any) -> None:
    # A real boto3 client with a Stubber validates each request against the
    # botocore model, and the answer has a real streaming body.
    client = boto3.client(
        "bedrock-agentcore",
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(client)
    answer = b'{"accepted": true}'
    stubber.add_response(
        "invoke_agent_runtime",
        {
            "response": StreamingBody(io.BytesIO(answer), len(answer)),
            "contentType": "application/json",
            "statusCode": 200,
        },
        {
            "agentRuntimeArn": ARN,
            "runtimeSessionId": ANY,
            "qualifier": "DEFAULT",
            "contentType": "application/json",
            "accept": "application/json",
            "payload": ANY,
        },
    )
    stubber.add_response(
        "stop_runtime_session",
        {"statusCode": 200},
        {"agentRuntimeArn": ARN, "runtimeSessionId": ANY, "qualifier": "DEFAULT"},
    )
    with stubber:
        result = run(
            catching(config_with(client, qualifier="DEFAULT")),
            {},
            succeed_with("ok"),
        )
        stubber.assert_no_pending_responses()

    assert outcome(result) == {"result": "ok"}


def test_uses_the_default_client_of_the_runtime_region(
    run: Any, monkeypatch: pytest.MonkeyPatch, fake_agentcore_client: Any
) -> None:
    client = fake_agentcore_client()
    regions: list[str] = []

    class Clients:
        def get(self, region: str) -> Any:
            regions.append(region)
            return client

    monkeypatch.setattr(operation, "_DEFAULT_CLIENTS", Clients())
    arn = ARN.replace("us-west-2", "eu-west-1")

    result = run(
        catching(config_with(None, agent_runtime_arn=arn)), {}, succeed_with(1)
    )

    assert outcome(result) == {"result": 1}
    assert set(regions) == {"eu-west-1"}
    # The callback calls go to the durable function's Region.
    [invoke] = client.requests("invoke_agent_runtime")
    assert json.loads(invoke["payload"])["region"] == "us-west-2"


def test_default_clients_creates_one_client_per_region() -> None:
    clients = operation._DefaultClients()  # noqa: SLF001

    first = clients.get("us-west-2")
    assert clients.get("us-west-2") is first
    assert first.meta.region_name == "us-west-2"
    assert clients.get("eu-west-1") is not first


@pytest.mark.parametrize(
    ("error", "retried"),
    [
        (agentcore_error("ThrottlingException", 429), True),
        (agentcore_error("RetryableConflictException", 409), True),
        (agentcore_error("InternalServerException", 500), True),
        (agentcore_error("SomethingElse", 503), True),
        (agentcore_error("ValidationException", 400), False),
        (agentcore_error("AccessDeniedException", 403), False),
        (agentcore_error("RuntimeClientError", 424), False),
        (TimeoutError(), True),
        (AgentcoreNotAcceptedError("no"), False),
        (AgentcoreDeliveryError("no"), False),
    ],
)
def test_default_retry_strategy(error: Exception, retried: bool) -> None:
    assert default_agentcore_retry_strategy(error, 1).should_retry is retried
    assert not default_agentcore_retry_strategy(error, 5).should_retry


@pytest.mark.parametrize("error_type", [None, "Error"])
def test_leaves_out_an_error_type_that_names_nothing(
    run: Any, fake_agentcore_client: Any, error_type: str | None
) -> None:
    result = run(
        catching(config_with(fake_agentcore_client())),
        {},
        fail_with(error_type, "broken"),
    )

    assert outcome(result)["message"] == 'AgentCore "review": the job failed: broken'


def test_leaves_out_the_error_type_when_the_checkpoint_shape_changed() -> None:
    # The checkpoint state is internal to the SDK, and can change in a
    # release.
    callback: Any = SimpleNamespace(state=object(), operation_id="op")

    assert operation._reported_error_type(callback) is None  # noqa: SLF001


def test_rejects_a_call_without_a_region(
    run: Any, fake_agentcore_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The local runner's execution ARN is not an ARN. So without AWS_REGION
    # the operation has no Region for the job document.
    monkeypatch.delenv("AWS_REGION")
    client = fake_agentcore_client()
    config = config_with(client)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            agentcore(context, "review", job_input={}, config=config)
        except ValidationError as error:
            return str(error)
        return None

    assert "cannot determine the AWS Region" in outcome(run(handler, {}))
    assert client.calls == []


def test_default_clients_uses_the_default_region_for_an_arn_without_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    clients = operation._DefaultClients()  # noqa: SLF001

    assert clients.get("").meta.region_name == "ap-south-1"


@pytest.mark.parametrize("retryable", [True, False])
def test_passes_an_invocation_error_of_the_wait_through(
    fake_agentcore_client: Any, monkeypatch: pytest.MonkeyPatch, retryable: bool
) -> None:
    # An SDK invocation error is not a job outcome. A retryable one makes
    # Lambda run the invocation again, and the replay waits for the same
    # session. So only an error that is not retryable stops it.
    class Interrupted(InvocationError):
        def is_retryable(self) -> bool:
            return retryable

    original = Callback.result
    raised = threading.Event()

    def result_once(self: Any) -> Any:
        if not raised.is_set():
            raised.set()
            msg = "interrupted"
            raise Interrupted(msg)
        return original(self)

    monkeypatch.setattr(Callback, "result", result_once)
    order: list[str] = []
    client = fake_agentcore_client(stops=[lambda request: order.append("stop")])
    runner = DurableFunctionTestRunner(
        handler=catching(config_with(client)), poll_interval=0.05
    )
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        if retryable:
            callback_id = runner.wait_for_callback(
                arn, name="review.callback", timeout=WAIT
            )
            # A stop for the error would run at once after the raise, in the
            # same thread. So 0.5 seconds after it, the order shows whether
            # one ran.
            assert raised.wait(WAIT)
            time.sleep(0.5)
            order.append("complete")
            runner.send_callback_success(callback_id, b'"done"')
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert outcome(result) == (
        {"result": "done"}
        if retryable
        else {"error": "AgentcoreError", "message": 'AgentCore "review": interrupted'}
    )
    assert order == (["complete", "stop"] if retryable else ["stop"])


def test_passes_an_execution_error_of_the_wait_through(
    fake_agentcore_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The core SDK records no outcome for an ExecutionError, and the
    # execution fails without a retry. A stop step recorded for it would
    # change that history.
    def result_fails(self: Any) -> Any:
        msg = "history does not match"
        raise NonDeterministicExecutionError(msg)

    monkeypatch.setattr(Callback, "result", result_fails)
    client = fake_agentcore_client()
    runner = DurableFunctionTestRunner(
        handler=catching(config_with(client)), poll_interval=0.05
    )
    try:
        arn = runner.run_async(input="{}", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "history does not match" in (result.error.message or "")
    assert client.requests("stop_runtime_session") == []
