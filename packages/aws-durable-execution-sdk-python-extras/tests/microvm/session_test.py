# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the microvm_session operation, run on the local test runner."""

from __future__ import annotations

import datetime
import hashlib
import json
from collections.abc import Callable, Iterator
from typing import Any

import boto3
import pytest
from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import Callback, DurableContext
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
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import SerDes
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from botocore.stub import ANY, Stubber

from aws_durable_execution_sdk_python_extras.microvm import (
    DEFAULT_MICROVM_JOB_PATH,
    MicrovmError,
    MicrovmInvokeOptions,
    MicrovmJobFailedError,
    MicrovmNotRunningError,
    MicrovmSession,
    MicrovmSessionConfig,
    microvm_session,
    operation,
)


WAIT = 30
INGRESS = (
    "arn:aws:lambda:us-west-2:aws:network-connector:aws-network-connector:ALL_INGRESS"
)

Complete = Callable[[DurableFunctionTestRunner, str], None]


def session_config(client: Any, **fields: Any) -> MicrovmSessionConfig:
    values: dict[str, Any] = {
        "image_identifier": "arn:aws:lambda:us-west-2:123456789012:microvm-image:img",
        "execution_role_arn": "arn:aws:iam::123456789012:role/microvm",
        "timeout": Duration.from_hours(1),
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
    return MicrovmSessionConfig(**values)


def job(path: str | None = None, **fields: Any) -> MicrovmInvokeOptions:
    return MicrovmInvokeOptions(path=path, timeout=Duration.from_minutes(5), **fields)


def succeed_with(value: Any) -> Complete:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_success(callback_id, json.dumps(value).encode())

    return complete


def fail_with(error_type: str, message: str) -> Complete:
    def complete(runner: DurableFunctionTestRunner, callback_id: str) -> None:
        runner.send_callback_failure(
            callback_id,
            ErrorObject(message=message, type=error_type, data=None, stack_trace=None),
        )

    return complete


def outcome(result: DurableFunctionTestResult) -> Any:
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return json.loads(result.result) if result.result is not None else None


@pytest.fixture
def run() -> Iterator[Callable[..., DurableFunctionTestResult]]:
    """Run a handler on the local test runner, and complete the named callbacks
    in order."""
    runners: list[DurableFunctionTestRunner] = []

    def _run(
        handler: Callable[..., Any],
        event: Any = None,
        completions: list[tuple[str, Complete]] | None = None,
    ) -> DurableFunctionTestResult:
        runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
        runners.append(runner)
        arn = runner.run_async(input=json.dumps(event), execution_timeout=WAIT)
        for name, complete in completions or []:
            callback_id = runner.wait_for_callback(
                arn, name=f"{name}.callback", timeout=WAIT
            )
            complete(runner, callback_id)
        return runner.wait_for_result(arn, timeout=WAIT)

    yield _run
    for runner in runners:
        runner.close()


def session_handler(
    config: MicrovmSessionConfig,
    body: Callable[[MicrovmSession, DurableContext, Any], Any],
) -> Callable[..., Any]:
    """A durable handler that runs ``body`` in a session named ``pipeline``,
    and returns the class and message of an error that leaves it."""

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            return {
                "result": microvm_session(
                    context,
                    "pipeline",
                    config=config,
                    handler=lambda vm, ctx: body(vm, ctx, event),
                )
            }
        except MicrovmError as error:
            return {"error": type(error).__name__, "message": error.message}
        except ChildContextError as error:
            return {"error": "ChildContextError", "message": error.message}

    return handler


def two_jobs(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
    build = vm.invoke(ctx, "build", job_input=event, options=job("/build"))
    test = vm.invoke(ctx, "test", job_input=build, options=job())
    return {"build": build, "test": test, "microvm": vm.microvm_id}


def test_sends_two_jobs_to_one_microvm_and_returns_the_handlers_value(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint()
    client = fake_client(endpoint=endpoint.url)
    config = session_config(client)

    result = run(
        session_handler(config, two_jobs),
        {"repo": "org/app"},
        [("build", succeed_with({"artifact": "a.zip"})), ("test", succeed_with("ok"))],
    )

    assert outcome(result) == {
        "result": {"build": {"artifact": "a.zip"}, "test": "ok", "microvm": "mvm-1"}
    }
    session = result.get_context("pipeline")
    assert [operation.name for operation in session.child_operations] == [
        "pipeline.session",
        "pipeline.launch",
        "build",
        "test",
        "pipeline.terminate",
    ]
    assert [
        operation.name for operation in session.get_context("build").child_operations
    ] == [
        "build.callback",
        "build.request",
    ]
    session_id = json.loads(session.get_step("pipeline.session").result or "null")
    [launch] = client.requests("run_microvm")
    assert launch["clientToken"] == hashlib.sha256(session_id.encode()).hexdigest()
    assert launch["ingressNetworkConnectors"] == [INGRESS]
    assert launch["maximumDurationInSeconds"] == 3600 + 300
    assert json.loads(launch["runHookPayload"]) == {"version": 1, "region": "us-west-2"}
    assert "idlePolicy" not in launch
    # The job without a path goes to the worker's handler.
    assert [request["path"] for request in endpoint.requests] == [
        "/build",
        DEFAULT_MICROVM_JOB_PATH,
    ]
    first, second = (request["body"] for request in endpoint.requests)
    assert first["microvmId"] == "mvm-1"
    assert first["input"] == {"repo": "org/app"}
    assert second["input"] == {"artifact": "a.zip"}
    assert first["callbackId"] != second["callbackId"]
    # Without suspending, the session checks no state.
    assert client.requests("get_microvm") == []
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_allows_durable_waits_between_jobs(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        first = vm.invoke(ctx, "build", job_input=1, options=job())
        ctx.wait(Duration.from_seconds(1), name="cool-down")
        return [first, vm.invoke(ctx, "test", job_input=2, options=job())]

    result = run(
        session_handler(session_config(client), body),
        None,
        [("build", succeed_with(10)), ("test", succeed_with(20))],
    )

    assert outcome(result) == {"result": [10, 20]}
    assert len(client.requests("run_microvm")) == 1
    assert len(client.requests("terminate_microvm")) == 1


def test_runs_jobs_from_map_items_with_each_items_context(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint()
    client = fake_client(endpoint=endpoint.url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return ctx.map(
            [1, 2, 3],
            lambda item_context, item, index, items: vm.invoke(
                item_context, f"shard-{item}", job_input=item, options=job()
            ),
            name="shards",
        ).get_results()

    result = run(
        session_handler(session_config(client), body),
        None,
        [(f"shard-{item}", succeed_with(item * 10)) for item in [1, 2, 3]],
    )

    assert outcome(result) == {"result": [10, 20, 30]}
    assert len(client.requests("run_microvm")) == 1
    assert sorted(request["body"]["input"] for request in endpoint.requests) == [
        1,
        2,
        3,
    ]


def test_lets_the_handler_catch_a_failed_job_and_continue(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        try:
            vm.invoke(ctx, "build", job_input=1, options=job())
        except MicrovmJobFailedError as error:
            caught = error.message
        return [caught, vm.invoke(ctx, "retry", job_input=2, options=job())]

    result = run(
        session_handler(session_config(client), body),
        None,
        [
            ("build", fail_with("BuildError", "compile failed")),
            ("retry", succeed_with("ok")),
        ],
    )

    assert outcome(result) == {
        "result": ['MicroVM "build": the job failed (BuildError): compile failed', "ok"]
    }
    assert len(client.requests("terminate_microvm")) == 1


def test_fails_with_an_uncaught_job_error_and_still_terminates(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    result = run(
        session_handler(session_config(client), two_jobs),
        None,
        [("build", fail_with("BuildError", "compile failed"))],
    )

    # The caller gets the MicroVM class, not ChildContextError.
    assert outcome(result) == {
        "error": "MicrovmJobFailedError",
        "message": 'MicroVM "build": the job failed (BuildError): compile failed',
    }
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_fails_with_the_handlers_own_error_and_still_terminates(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        msg = "no artifact"
        raise LookupError(msg)

    result = run(session_handler(session_config(client), body))

    # Caller code keeps the SDK's error.
    assert outcome(result) == {"error": "ChildContextError", "message": "no artifact"}
    assert len(client.requests("terminate_microvm")) == 1


@pytest.mark.parametrize(
    ("call", "error", "message"),
    [
        (
            lambda vm, ctx: vm.invoke("build", "build", job_input=1, options=job()),  # type: ignore[arg-type]
            "TypeError",
            "invoke requires the durable context",
        ),
        (
            lambda vm, ctx: vm.invoke(ctx, "", job_input=1, options=job()),
            "ValidationError",
            "non-empty job name",
        ),
        (
            lambda vm, ctx: vm.invoke(ctx, "build", job_input=1, options=job("job")),
            "ValidationError",
            "RFC 3986 path",
        ),
        (
            lambda vm, ctx: vm.invoke(
                ctx,
                "build",
                job_input=1,
                options=MicrovmInvokeOptions(timeout=Duration.from_hours(9)),
            ),
            "ValidationError",
            "timeout must be between 1 second",
        ),
        (
            lambda vm, ctx: vm.invoke(
                ctx, "build", job_input=1, options=job(heartbeat_timeout=Duration())
            ),
            "ValidationError",
            "heartbeat_timeout must be at least 1 second",
        ),
        (
            lambda vm, ctx: vm.invoke(ctx, "build", job_input={1, 2}, options=job()),
            "TypeError",
            "must be JSON-serializable",
        ),
    ],
)
def test_rejects_an_invalid_job_inside_the_handler_where_it_can_be_caught(
    run: Any,
    fake_client: Any,
    fake_endpoint: Any,
    call: Callable[[MicrovmSession, DurableContext], Any],
    error: str,
    message: str,
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        try:
            call(vm, ctx)
        except (TypeError, ValidationError) as raised:
            return [type(raised).__name__, str(raised)]
        return None

    result = run(session_handler(session_config(client), body))

    [name, text] = outcome(result)["result"]
    assert name == error
    assert message in text
    # No job operation started.
    session = result.get_context("pipeline")
    assert [operation.name for operation in session.child_operations] == [
        "pipeline.session",
        "pipeline.launch",
        "pipeline.terminate",
    ]


def test_retries_a_throttled_launch_with_the_same_client_token(
    run: Any, fake_client: Any, fake_endpoint: Any, make_error: Any
) -> None:
    client = fake_client(
        launches=[make_error("ThrottlingException", 429)],
        endpoint=fake_endpoint().url,
    )

    result = run(
        session_handler(session_config(client), lambda vm, ctx, event: vm.microvm_id)
    )

    assert outcome(result) == {"result": "mvm-1"}
    first, second = client.requests("run_microvm")
    assert first["clientToken"] == second["clientToken"]


def test_fails_with_the_launch_error_and_skips_terminate(
    run: Any, fake_client: Any, make_error: Any
) -> None:
    client = fake_client(launches=[make_error("ValidationException")])

    result = run(session_handler(session_config(client), lambda vm, ctx, event: None))

    error = outcome(result)
    assert error["error"] == "MicrovmLaunchError"
    assert error["message"].startswith('MicroVM "pipeline": the launch failed:')
    assert client.requests("terminate_microvm") == []


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"image_identifier": ""}, "image_identifier is required"),
        ({"timeout": Duration.from_hours(9)}, "timeout must be between 1 second"),
        (
            {"auto_suspend_idle_time": Duration.from_seconds(30)},
            "auto_suspend_idle_time is set, but auto_suspend_on_idle is not True",
        ),
        (
            {
                "auto_suspend_on_idle": True,
                "auto_suspend_idle_time": Duration.from_seconds(9),
            },
            "auto_suspend_idle_time must be at least 10 seconds",
        ),
        (
            {
                "auto_suspend_on_idle": True,
                "timeout": Duration.from_minutes(1),
                "auto_suspend_idle_time": Duration.from_minutes(2),
            },
            "auto_suspend_idle_time must not be longer than timeout",
        ),
    ],
)
def test_rejects_an_invalid_config_before_any_durable_operation(
    run: Any, fake_client: Any, fields: dict[str, Any], message: str
) -> None:
    client = fake_client()

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            microvm_session(
                context,
                "pipeline",
                config=session_config(client, **fields),
                handler=lambda vm, ctx: None,
            )
        except ValidationError as error:
            return str(error)
        return None

    result = run(handler)

    assert message in outcome(result)
    assert result.get_all_operations() == []
    assert client.calls == []


def test_rejects_a_handler_that_is_not_callable(run: Any, fake_client: Any) -> None:
    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            microvm_session(
                context,
                "pipeline",
                config=session_config(fake_client()),
                handler="build",  # type: ignore[arg-type]
            )
        except TypeError as error:
            return str(error)
        return None

    assert (
        outcome(run(handler)) == 'MicroVM session "pipeline": handler must be callable'
    )


def test_passes_an_idle_policy_through_and_resumes_before_each_job(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(
        endpoint=fake_endpoint().url, states=["RUNNING", "SUSPENDED", "RUNNING"]
    )
    policy = {
        "maxIdleDurationSeconds": 300,
        "suspendedDurationSeconds": 3600,
        "autoResumeEnabled": True,
    }

    result = run(
        session_handler(session_config(client, idle_policy=policy), two_jobs),
        None,
        [("build", succeed_with(1)), ("test", succeed_with(2))],
    )

    assert outcome(result)["result"]["test"] == 2
    [launch] = client.requests("run_microvm")
    assert launch["idlePolicy"] == policy
    # An idle policy does not ask the worker to suspend.
    assert "autoSuspendIdleSeconds" not in json.loads(launch["runHookPayload"])
    assert len(client.requests("get_microvm")) == 3
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_asks_the_worker_to_suspend_after_60_seconds_by_default(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), two_jobs),
        None,
        [("build", succeed_with(1)), ("test", succeed_with(2))],
    )

    assert outcome(result)["result"]["test"] == 2
    [launch] = client.requests("run_microvm")
    assert json.loads(launch["runHookPayload"]) == {
        "version": 1,
        "region": "us-west-2",
        "autoSuspendIdleSeconds": 60,
    }
    assert len(client.requests("get_microvm")) == 2


@pytest.mark.parametrize(
    ("fields", "idle_seconds"),
    [
        ({"auto_suspend_idle_time": Duration.from_seconds(90)}, 90),
        ({"timeout": Duration.from_seconds(30)}, 30),
        ({"timeout": Duration.from_seconds(9)}, None),
    ],
)
def test_sends_the_idle_time_to_the_worker(
    run: Any,
    fake_client: Any,
    fake_endpoint: Any,
    fields: dict[str, Any],
    idle_seconds: int | None,
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)
    config = session_config(client, auto_suspend_on_idle=True, **fields)

    result = run(session_handler(config, lambda vm, ctx, event: None))

    assert outcome(result) == {"result": None}
    payload = json.loads(client.requests("run_microvm")[0]["runHookPayload"])
    assert payload.get("autoSuspendIdleSeconds") == idle_seconds


def test_resumes_a_microvm_that_the_worker_suspended_and_uses_its_endpoint(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    launched_at = fake_endpoint()
    resumed_at = fake_endpoint()
    client = fake_client(
        endpoint=launched_at.url, states=["SUSPENDED", "SUSPENDED", "RUNNING"]
    )

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        # The resumed MicroVM reports a new endpoint.
        client.endpoint = resumed_at.url
        return vm.invoke(ctx, "build", job_input=1, options=job())

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body),
        None,
        [("build", succeed_with("done"))],
    )

    assert outcome(result) == {"result": "done"}
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
    assert launched_at.requests == []
    assert len(resumed_at.requests) == 1


def test_checks_the_state_again_after_a_gateway_error(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    # The MicroVM suspends itself between the state check and the request.
    # The endpoint answers 502, and the worker answers 503 while it suspends.
    endpoint = fake_endpoint([502, 503])
    client = fake_client(
        endpoint=endpoint.url, states=["RUNNING", "SUSPENDED", "RUNNING", "RUNNING"]
    )

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return vm.invoke(ctx, "build", job_input=1, options=job())

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body),
        None,
        [("build", succeed_with("done"))],
    )

    assert outcome(result) == {"result": "done"}
    assert len(endpoint.requests) == 3
    assert len(client.requests("get_microvm")) == 4
    assert len(client.requests("resume_microvm")) == 1


def test_does_not_check_the_state_again_after_a_rejection(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint([429])
    client = fake_client(endpoint=endpoint.url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return vm.invoke(ctx, "build", job_input=1, options=job())

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body),
        None,
        [("build", succeed_with("done"))],
    )

    assert outcome(result) == {"result": "done"}
    assert len(endpoint.requests) == 2
    assert len(client.requests("get_microvm")) == 1


def test_fails_the_job_at_once_when_the_microvm_no_longer_exists(
    run: Any, fake_client: Any, fake_endpoint: Any, make_error: Any
) -> None:
    endpoint = fake_endpoint()
    client = fake_client(
        endpoint=endpoint.url,
        states=[make_error("ResourceNotFoundException", 404)],
    )

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return vm.invoke(ctx, "build", job_input=1, options=job())

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body)
    )

    error = outcome(result)
    assert error["error"] == "MicrovmNotRunningError"
    assert error["message"].startswith(
        'MicroVM "build": the job could not be delivered: MicroVM mvm-1 no longer exists.'
    )
    # The state error is not retried, and no request is sent.
    assert len(client.requests("get_microvm")) == 1
    assert endpoint.requests == []
    assert len(client.requests("terminate_microvm")) == 1


def test_lets_the_handler_catch_the_not_running_error_on_replay_too(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url, states=["TERMINATED"])

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        try:
            vm.invoke(ctx, "build", job_input=1, options=job())
        except MicrovmNotRunningError as error:
            caught = type(error).__name__
        # The wait ends the invocation. The next invocation replays the
        # failed job, and the handler catches the same class.
        ctx.wait(Duration.from_seconds(1), name="pause")
        return caught

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body)
    )

    assert outcome(result) == {"result": "MicrovmNotRunningError"}
    assert len(client.requests("get_microvm")) == 1


def test_terminates_only_after_the_handler_ends(
    fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)
    handler = session_handler(session_config(client), two_jobs)
    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    try:
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        build = runner.wait_for_callback(arn, name="build.callback", timeout=WAIT)
        # The invocation ended while the handler waited for the first job.
        assert client.requests("terminate_microvm") == []
        runner.send_callback_success(build, b"1")
        test = runner.wait_for_callback(arn, name="test.callback", timeout=WAIT)
        assert client.requests("terminate_microvm") == []
        runner.send_callback_success(test, b"2")
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert outcome(result)["result"]["test"] == 2
    assert len(client.requests("terminate_microvm")) == 1


def test_session_requests_match_the_lambda_microvms_api_model(
    run: Any, fake_endpoint: Any
) -> None:
    """botocore validates RunMicrovm with an idle policy, GetMicrovm, and
    ResumeMicrovm against the service model."""

    endpoint = fake_endpoint()
    client = boto3.session.Session().client("lambda-microvms", region_name="us-west-2")
    policy = {
        "maxIdleDurationSeconds": 300,
        "suspendedDurationSeconds": 3600,
        "autoResumeEnabled": True,
    }
    config = session_config(client, idle_policy=policy, auto_suspend_on_idle=True)
    started = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    microvm = {
        "microvmId": "mvm-1",
        "endpoint": endpoint.url,
        "imageArn": config.image_identifier,
        "imageVersion": "1",
        "maximumDurationInSeconds": 3900,
        "startedAt": started,
    }
    with Stubber(client) as stub:
        stub.add_response(
            "run_microvm",
            {**microvm, "state": "PENDING"},
            {
                "imageIdentifier": config.image_identifier,
                "executionRoleArn": config.execution_role_arn,
                "clientToken": ANY,
                "ingressNetworkConnectors": [INGRESS],
                "egressNetworkConnectors": ANY,
                "maximumDurationInSeconds": 3900,
                "runHookPayload": ANY,
                "idlePolicy": policy,
            },
        )
        stub.add_response(
            "get_microvm",
            {**microvm, "state": "SUSPENDED"},
            {"microvmIdentifier": "mvm-1"},
        )
        stub.add_response("resume_microvm", {}, {"microvmIdentifier": "mvm-1"})
        stub.add_response(
            "get_microvm",
            {**microvm, "state": "RUNNING"},
            {"microvmIdentifier": "mvm-1"},
        )
        stub.add_response(
            "create_microvm_auth_token",
            {"authToken": {"x-amz-microvm-token": "t"}},
            {
                "microvmIdentifier": "mvm-1",
                "expirationInMinutes": 5,
                "allowedPorts": [{"port": 8080}],
            },
        )
        stub.add_response("terminate_microvm", {}, {"microvmIdentifier": "mvm-1"})

        def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
            return vm.invoke(ctx, "build", job_input=1, options=job())

        result = run(session_handler(config, body), None, [("build", succeed_with(3))])

        stub.assert_no_pending_responses()
    client.close()
    assert outcome(result) == {"result": 3}


def test_terminates_and_fails_for_a_launch_checkpoint_of_another_delivery(
    run: Any, fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A launch checkpoint from another release, for example after a rollback.
    original = operation._MicrovmScope._launch_call  # noqa: SLF001

    def launch_for_another(self: Any, *args: Any) -> Any:
        launched = original(self, *args)
        return operation.LaunchResult(
            microvm_id=launched.microvm_id, delivery="smoke-signal"
        )

    monkeypatch.setattr(operation._MicrovmScope, "_launch_call", launch_for_another)  # noqa: SLF001
    client = fake_client()

    result = run(session_handler(session_config(client), two_jobs))

    assert outcome(result) == {
        "error": "MicrovmDeliveryError",
        "message": 'MicroVM "pipeline": the launch prepared the delivery '
        '"smoke-signal", which this release does not support',
    }
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


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


def test_keeps_the_microvm_when_the_handler_raises_a_retryable_invocation_error(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    # A retryable InvocationError makes Lambda run the invocation again. A
    # terminate step recorded for it would take the position of the next
    # job, and the replay would fail with NonDeterministicExecutionError.
    client = fake_client(endpoint=fake_endpoint().url)
    serdes = FlakySerDes()

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        fetched = ctx.step(
            lambda _: {"n": 1}, name="fetch", config=StepConfig(serdes=serdes)
        )
        return vm.invoke(ctx, "build", job_input=fetched, options=job())

    result = run(
        session_handler(session_config(client), body),
        None,
        [("build", succeed_with("built"))],
    )

    assert outcome(result) == {"result": "built"}
    assert serdes.failed
    assert [name for name, _ in client.calls] == [
        "run_microvm",
        "create_microvm_auth_token",
        "terminate_microvm",
    ]


def test_passes_a_retryable_invocation_error_of_a_job_wait_through(
    run: Any,
    fake_client: Any,
    fake_endpoint: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The SDK error is not a job outcome. So the job does not fail with it,
    # and the next invocation waits for the same job.
    original = Callback.result
    raised: list[bool] = []

    def result_once(self: Any) -> Any:
        if not raised:
            raised.append(True)
            msg = "store throttled"
            raise RetryableSerDesError(msg)
        return original(self)

    monkeypatch.setattr(Callback, "result", result_once)
    endpoint = fake_endpoint()
    client = fake_client(endpoint=endpoint.url)

    result = run(
        session_handler(
            session_config(client),
            lambda vm, ctx, event: vm.invoke(ctx, "build", job_input=1, options=job()),
        ),
        None,
        [("build", succeed_with("built"))],
    )

    assert outcome(result) == {"result": "built"}
    assert raised == [True]
    assert len(endpoint.requests) == 1
    assert len(client.requests("terminate_microvm")) == 1


def test_terminates_for_an_invocation_error_that_is_not_retryable(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)

    class Fatal(InvocationError):
        def is_retryable(self) -> bool:
            return False

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        msg = "checkpoint rejected"
        raise Fatal(msg)

    result = run(session_handler(session_config(client), body))

    # The SDK records the error as the session's failure.
    assert outcome(result) == {
        "error": "ChildContextError",
        "message": "checkpoint rejected",
    }
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_runs_jobs_in_parallel_branches(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    endpoint = fake_endpoint()
    client = fake_client(endpoint=endpoint.url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return ctx.parallel(
            [
                lambda branch: vm.invoke(branch, "lint", job_input="a", options=job()),
                lambda branch: vm.invoke(branch, "unit", job_input="b", options=job()),
            ],
            name="checks",
        ).get_results()

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body),
        None,
        [("lint", succeed_with("A")), ("unit", succeed_with("B"))],
    )

    assert outcome(result) == {"result": ["A", "B"]}
    assert len(client.requests("run_microvm")) == 1
    assert sorted(request["body"]["input"] for request in endpoint.requests) == [
        "a",
        "b",
    ]
    assert len(client.requests("get_microvm")) == 2
    assert len(client.requests("terminate_microvm")) == 1


def test_resumes_once_when_two_items_find_the_microvm_suspended(
    run: Any, fake_client: Any, fake_endpoint: Any, make_error: Any
) -> None:
    # Both items find the MicroVM SUSPENDED and call ResumeMicrovm. The
    # second call finds the resume in progress.
    endpoint = fake_endpoint()
    client = fake_client(
        endpoint=endpoint.url,
        states=["SUSPENDED"] * 4,
        resumes=[None, make_error("ConflictException", 409)],
    )

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return ctx.map(
            ["a", "b"],
            lambda item_context, item, index, items: vm.invoke(
                item_context, f"shard-{item}", job_input=item, options=job()
            ),
            name="shards",
        ).get_results()

    result = run(
        session_handler(session_config(client, auto_suspend_on_idle=True), body),
        None,
        [("shard-a", succeed_with("A")), ("shard-b", succeed_with("B"))],
    )

    assert outcome(result) == {"result": ["A", "B"]}
    assert len(client.requests("resume_microvm")) == 2
    assert len(endpoint.requests) == 2


def test_rejects_the_outer_context_before_the_job_starts(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    # A job created in the outer context lands outside the session's child
    # context. The core SDK does not refuse it, and the next replay fails
    # with NonDeterministicExecutionError after the job has run.
    endpoint = fake_endpoint()
    client = fake_client(endpoint=endpoint.url)
    config = session_config(client)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        def body(vm: MicrovmSession, ctx: DurableContext) -> Any:
            try:
                vm.invoke(context, "build", job_input=1, options=job())
            except TypeError as error:
                return str(error)
            return None

        return microvm_session(context, "pipeline", config=config, handler=body)

    result = run(handler)

    assert outcome(result) == (
        'MicroVM session "pipeline": invoke received the context that '
        "microvm_session was called with. Pass the context that the handler "
        "receives, or a context nested in it."
    )
    assert endpoint.requests == []
    session = result.get_context("pipeline")
    assert [operation.name for operation in session.child_operations] == [
        "pipeline.session",
        "pipeline.launch",
        "pipeline.terminate",
    ]


def test_wraps_a_job_error_that_leaves_a_child_context_of_the_handler(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    # Only an error that the handler raises itself keeps its MicroVM class.
    # The SDK records one error type per child context. So an error from the
    # handler's own child context reaches the caller as ChildContextError,
    # with the MicroVM error's message. Inside the handler, the
    # ChildContextError's __cause__ is still the MicroVM class.
    client = fake_client(endpoint=fake_endpoint().url)
    config = session_config(client)
    inside: list[str] = []

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        def body(vm: MicrovmSession, ctx: DurableContext) -> Any:
            try:
                return ctx.run_in_child_context(
                    lambda stage: vm.invoke(stage, "build", job_input=1, options=job()),
                    name="stage",
                )
            except ChildContextError as error:
                inside.append(type(error.__cause__).__name__)
                raise

        try:
            microvm_session(context, "pipeline", config=config, handler=body)
        except ChildContextError as error:
            return [type(error.__cause__).__name__, error.message]
        return None

    result = run(handler, None, [("build", fail_with("BuildError", "failed"))])

    assert outcome(result) == [
        "ChildContextError",
        'MicroVM "build": the job failed (BuildError): failed',
    ]
    assert "MicrovmJobFailedError" in inside
    assert len(client.requests("terminate_microvm")) == 1


def test_records_the_handlers_non_determinism_and_does_not_terminate(
    fake_client: Any, fake_endpoint: Any
) -> None:
    client = fake_client(endpoint=fake_endpoint().url)
    names = ["build"]

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        first = vm.invoke(ctx, names[0], job_input=1, options=job())
        return [first, vm.invoke(ctx, "test", job_input=2, options=job())]

    runner = DurableFunctionTestRunner(
        handler=session_handler(session_config(client), body), poll_interval=0.05
    )
    try:
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        build = runner.wait_for_callback(arn, name="build.callback", timeout=WAIT)
        runner.send_callback_success(build, b"1")
        test = runner.wait_for_callback(arn, name="test.callback", timeout=WAIT)
        # A deploy renames the first job while the execution waits.
        names[0] = "compile"
        runner.send_callback_success(test, b"2")
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "current='compile'" in (result.error.message or "")
    assert client.requests("terminate_microvm") == []


def test_wraps_an_sdk_error_that_leaves_the_handler(
    run: Any, fake_client: Any, fake_endpoint: Any
) -> None:
    # The JavaScript operation passes an SDK error to the caller with its own
    # class. This port follows run_in_child_context instead.
    client = fake_client(endpoint=fake_endpoint().url)
    config = session_config(client)

    def fail(_: Any) -> Any:
        msg = "no artifact"
        raise LookupError(msg)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        def body(vm: MicrovmSession, ctx: DurableContext) -> Any:
            return ctx.step(
                fail,
                name="fetch",
                config=StepConfig(retry_strategy=lambda e, a: RetryDecision.no_retry()),
            )

        try:
            microvm_session(context, "pipeline", config=config, handler=body)
        except ChildContextError as error:
            return [type(error.__cause__).__name__, error.message]
        return None

    result = run(handler)

    assert outcome(result) == ["StepError", "no artifact"]
    assert len(client.requests("terminate_microvm")) == 1


def test_passes_an_execution_error_of_a_job_wait_through(
    fake_client: Any, fake_endpoint: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The core SDK records no outcome for an ExecutionError. So the job does
    # not fail with it, and the session does not terminate for it.
    def result_fails(self: Any) -> Any:
        msg = "history does not match"
        raise NonDeterministicExecutionError(msg)

    monkeypatch.setattr(Callback, "result", result_fails)
    client = fake_client(endpoint=fake_endpoint().url)

    def body(vm: MicrovmSession, ctx: DurableContext, event: Any) -> Any:
        return vm.invoke(ctx, "build", job_input=1, options=job())

    runner = DurableFunctionTestRunner(
        handler=session_handler(session_config(client), body), poll_interval=0.05
    )
    try:
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "history does not match" in (result.error.message or "")
    assert client.requests("terminate_microvm") == []
