# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the request IDs of failed AWS calls in MicroVM errors, run on
the local test runner."""

from __future__ import annotations

import datetime
import functools
import gc
import logging
import re
import threading
import weakref
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import boto3
import botocore.session
import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.retries import (
    RetryDecision,
    RetryStrategyConfig,
    create_retry_strategy,
)
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from botocore.credentials import CredentialProvider, RefreshableCredentials
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmConfig,
    MicrovmDeliveryError,
    MicrovmError,
    MicrovmInvokeOptions,
    MicrovmLaunchError,
    MicrovmNotRunningError,
    MicrovmRequestConfig,
    MicrovmSessionConfig,
    aws_errors,
    microvm,
    microvm_session,
)
from aws_durable_execution_sdk_python_extras.microvm import (
    operation as operation_module,
)


WAIT = 30
REMOVED = (
    "MicroVM mvm-1 no longer exists. A MicroVM lives at most its `timeout` plus "
    "5 minutes, and never longer than 8 hours."
)
DELIVERY = 'MicroVM "job": the job could not be delivered: '
LAUNCH_FAILED = 'MicroVM "job": the launch failed: '
STATE_ERROR = (
    "aws_durable_execution_sdk_python_extras.microvm.lifecycle.MicrovmStateError"
)
STS_DENIED = (
    "An error occurred (AccessDenied) when calling the AssumeRole operation: "
    "not authorized to assume role"
)


@functools.cache
def _exceptions() -> Any:
    """The modeled error classes of the lambda-microvms client."""
    return boto3.client("lambda-microvms", region_name="us-west-2").exceptions


def service_error(
    code: str,
    message: str,
    operation: str,
    request_id: str | None = "req-1",
    status: int = 400,
) -> ClientError:
    """An error of the class that botocore raises for ``code``, with the
    ``response`` of a real answer."""
    metadata: dict[str, Any] = {"HTTPStatusCode": status}
    if request_id is not None:
        metadata["RequestId"] = request_id
    response: Any = {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": metadata,
    }
    error: ClientError = _exceptions().from_code(code)(response, operation)
    return error


def metadata(request_id: str | None) -> dict[str, Any]:
    """The ``ResponseMetadata`` of an output."""
    if request_id is None:
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}
    return {"ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": request_id}}


Answer = BaseException | Callable[[], Any] | dict[str, Any]


class ScriptedClient:
    """Answers each Lambda MicroVMs call from a list, and counts the calls.

    Each answer is an exception to raise, a function to call, or the output
    to return as it is. After a list ends, each call returns a success.
    """

    def __init__(self, endpoint: str, **answers: list[Answer]) -> None:
        self.endpoint = endpoint
        self.calls: Counter[str] = Counter()
        self._answers = {name: list(values) for name, values in answers.items()}
        self._lock = threading.Lock()

    def _answer(self, name: str, success: dict[str, Any]) -> Any:
        with self._lock:
            self.calls[name] += 1
            answers = self._answers.get(name, [])
            answer = answers.pop(0) if answers else success
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            return answer()
        return answer

    def run_microvm(self, **request: Any) -> Any:
        microvm = {"microvmId": "mvm-1", "state": "PENDING", "endpoint": self.endpoint}
        return self._answer("run_microvm", microvm)

    def get_microvm(self, **request: Any) -> Any:
        microvm = {"microvmId": "mvm-1", "state": "RUNNING", "endpoint": self.endpoint}
        return self._answer("get_microvm", microvm)

    def resume_microvm(self, **request: Any) -> Any:
        return self._answer("resume_microvm", {})

    def create_microvm_auth_token(self, **request: Any) -> Any:
        return self._answer("create_microvm_auth_token", {"authToken": {"x-t": "t"}})

    def terminate_microvm(self, **request: Any) -> Any:
        return self._answer("terminate_microvm", {})


def base_fields(client: Any, strategy: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "image_identifier": "arn:aws:lambda:us-west-2:123456789012:microvm-image:img",
        "execution_role_arn": "arn:aws:iam::123456789012:role/microvm",
        "client": client,
    }
    if strategy is not None:
        fields["retry_strategy"] = strategy
    return fields


def catching(
    kind: str, client: Any, caught: list[MicrovmError], strategy: Any = None
) -> Callable[..., Any]:
    """A durable handler that runs one failing job and catches its error.

    ``kind`` is the operation:

    - ``microvm``: one job over HTTP, because the config sets a path.
    - ``session``: a session with the default config. It makes no
      ``GetMicrovm`` call.
    - ``checking session``: a session with ``auto_suspend_on_idle``. It calls
      ``GetMicrovm`` before each job.

    After the error, a durable wait ends the invocation. The next invocation
    replays the failed operation from its checkpoint and catches the error
    again. So ``caught`` holds the error of the first run and of the replay.
    """
    fields = base_fields(client, strategy)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            if kind == "microvm":
                microvm(
                    context,
                    "job",
                    job_input=1,
                    config=MicrovmConfig(
                        **fields,
                        timeout=Duration.from_minutes(10),
                        request=MicrovmRequestConfig(path="/job"),
                    ),
                )
            else:
                microvm_session(
                    context,
                    "pipeline",
                    config=MicrovmSessionConfig(
                        **fields,
                        timeout=Duration.from_hours(1),
                        auto_suspend_on_idle=kind == "checking session",
                    ),
                    handler=lambda vm, ctx: vm.invoke(
                        ctx,
                        "job",
                        job_input=1,
                        options=MicrovmInvokeOptions(
                            path="/job", timeout=Duration.from_minutes(10)
                        ),
                    ),
                )
        except MicrovmError as error:
            caught.append(error)
        else:
            return "unexpected"
        context.wait(Duration.from_seconds(1), name="replay")
        return "caught"

    return handler


@pytest.fixture
def run() -> Iterator[Callable[..., DurableFunctionTestResult]]:
    """Run a handler on the local test runner, and complete the named
    callbacks with ``"ok"``."""
    runners: list[DurableFunctionTestRunner] = []

    def _run(
        handler: Callable[..., Any], callbacks: tuple[str, ...] = ()
    ) -> DurableFunctionTestResult:
        runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
        runners.append(runner)
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        for name in callbacks:
            callback_id = runner.wait_for_callback(arn, name=name, timeout=WAIT)
            runner.send_callback_success(callback_id, b'"ok"')
        return runner.wait_for_result(arn, timeout=WAIT)

    yield _run
    for runner in runners:
        runner.close()


def job_context(result: DurableFunctionTestResult, kind: str) -> Any:
    """The child context of the job named ``job``."""
    if kind == "microvm":
        return result.get_context("job")
    return result.get_context("pipeline").get_context("job")


def assert_caught(
    result: DurableFunctionTestResult,
    caught: list[MicrovmError],
    error_class: type[MicrovmError],
    message: str,
) -> None:
    """The handler caught ``error_class`` with ``message`` on the first run
    and on replay."""
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.result == '"caught"'
    assert len(caught) >= 2
    for error in caught:
        assert type(error) is error_class
        assert error.message == message


@dataclass(frozen=True)
class Scenario:
    kind: str
    answers: Callable[[str | None], dict[str, list[Answer]]]
    """The answers of the client, for a request ID or ``None``."""
    error_class: type[MicrovmError]
    prefix: str
    step: str
    step_error_type: str
    step_message: tuple[str, str]
    """The step message without and with the request ID ``req-1``."""
    call: str
    calls: int
    statuses: tuple[int, ...] = ()


def _denied(operation: str) -> tuple[str, str]:
    text = (
        f"An error occurred (AccessDeniedException) when calling the {operation} "
        "operation: denied"
    )
    return text, f"{text}. {operation} request ID: req-1."


SCENARIOS: dict[str, Scenario] = {
    "CreateMicrovmAuthToken reports the MicroVM removed, for the first token "
    "of a session job": Scenario(
        kind="session",
        answers=lambda rid: {
            "create_microvm_auth_token": [
                service_error(
                    "ResourceNotFoundException", "gone", "CreateMicrovmAuthToken", rid
                )
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(REMOVED, f"{REMOVED} CreateMicrovmAuthToken request ID: req-1."),
        call="create_microvm_auth_token",
        calls=1,
    ),
    "CreateMicrovmAuthToken reports the MicroVM removed, for the new token "
    "after a 401": Scenario(
        kind="session",
        answers=lambda rid: {
            "create_microvm_auth_token": [
                {"authToken": {"x-t": "t"}},
                service_error(
                    "ResourceNotFoundException", "gone", "CreateMicrovmAuthToken", rid
                ),
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(REMOVED, f"{REMOVED} CreateMicrovmAuthToken request ID: req-1."),
        call="create_microvm_auth_token",
        calls=2,
        statuses=(401,),
    ),
    "CreateMicrovmAuthToken reports the MicroVM removed, in a session that "
    "checks the state": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "create_microvm_auth_token": [
                service_error(
                    "ResourceNotFoundException", "gone", "CreateMicrovmAuthToken", rid
                )
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(REMOVED, f"{REMOVED} CreateMicrovmAuthToken request ID: req-1."),
        call="create_microvm_auth_token",
        calls=1,
    ),
    "CreateMicrovmAuthToken reports the MicroVM removed, for a microvm job": Scenario(
        kind="microvm",
        answers=lambda rid: {
            "create_microvm_auth_token": [
                service_error(
                    "ResourceNotFoundException", "gone", "CreateMicrovmAuthToken", rid
                )
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(REMOVED, f"{REMOVED} CreateMicrovmAuthToken request ID: req-1."),
        call="create_microvm_auth_token",
        calls=1,
    ),
    "GetMicrovm reports the MicroVM removed": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "get_microvm": [
                service_error(
                    "ResourceNotFoundException", "gone", "GetMicrovm", rid, 404
                )
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(REMOVED, f"{REMOVED} GetMicrovm request ID: req-1."),
        call="get_microvm",
        calls=1,
    ),
    "ResumeMicrovm reports the MicroVM removed": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "get_microvm": [
                {"microvmId": "mvm-1", "state": "SUSPENDED", **metadata("get-1")}
            ],
            "resume_microvm": [
                service_error(
                    "ResourceNotFoundException", "gone", "ResumeMicrovm", rid, 404
                )
            ],
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(REMOVED, f"{REMOVED} ResumeMicrovm request ID: req-1."),
        call="resume_microvm",
        calls=1,
    ),
    # The request ID of an STS error belongs to STS. So the message names no
    # RunMicrovm request ID, with or without a request ID on the error.
    "the credential provider's STS call fails inside RunMicrovm": Scenario(
        kind="microvm",
        answers=lambda rid: {
            "run_microvm": [
                ClientError(
                    {
                        "Error": {
                            "Code": "AccessDenied",
                            "Message": "not authorized to assume role",
                        },
                        **metadata(rid),  # type: ignore[typeddict-item]
                    },
                    "AssumeRole",
                )
            ]
        },
        error_class=MicrovmLaunchError,
        prefix=LAUNCH_FAILED,
        step="job.launch",
        step_error_type="botocore.exceptions.ClientError",
        step_message=(STS_DENIED, STS_DENIED),
        call="run_microvm",
        calls=1,
    ),
    "GetMicrovm reports a terminated MicroVM": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "get_microvm": [
                {
                    "microvmId": "mvm-1",
                    "state": "TERMINATED",
                    "stateReason": "MaximumDuration",
                    **metadata(rid),
                }
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(
            "MicroVM mvm-1 is TERMINATED: MaximumDuration.",
            "MicroVM mvm-1 is TERMINATED: MaximumDuration. GetMicrovm request ID: "
            "req-1.",
        ),
        call="get_microvm",
        calls=1,
    ),
    "GetMicrovm reports a terminating MicroVM without a reason": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "get_microvm": [
                {"microvmId": "mvm-1", "state": "TERMINATING", **metadata(rid)}
            ]
        },
        error_class=MicrovmNotRunningError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type=STATE_ERROR,
        step_message=(
            "MicroVM mvm-1 is TERMINATING.",
            "MicroVM mvm-1 is TERMINATING. GetMicrovm request ID: req-1.",
        ),
        call="get_microvm",
        calls=1,
    ),
    "RunMicrovm rejects the launch": Scenario(
        kind="microvm",
        answers=lambda rid: {
            "run_microvm": [
                service_error("ValidationException", "bad image", "RunMicrovm", rid)
            ]
        },
        error_class=MicrovmLaunchError,
        prefix=LAUNCH_FAILED,
        step="job.launch",
        step_error_type="botocore.errorfactory.ValidationException",
        step_message=(
            "An error occurred (ValidationException) when calling the RunMicrovm "
            "operation: bad image",
            "An error occurred (ValidationException) when calling the RunMicrovm "
            "operation: bad image. RunMicrovm request ID: req-1.",
        ),
        call="run_microvm",
        calls=1,
    ),
    "RunMicrovm returns no microvmId": Scenario(
        kind="microvm",
        answers=lambda rid: {"run_microvm": [{"state": "PENDING", **metadata(rid)}]},
        error_class=MicrovmLaunchError,
        prefix=LAUNCH_FAILED,
        step="job.launch",
        step_error_type="RuntimeError",
        step_message=(
            "RunMicrovm returned no microvmId",
            "RunMicrovm returned no microvmId. RunMicrovm request ID: req-1.",
        ),
        call="run_microvm",
        calls=1,
    ),
    "RunMicrovm returns no endpoint for HTTP delivery": Scenario(
        kind="microvm",
        answers=lambda rid: {
            "run_microvm": [{"microvmId": "mvm-1", "state": "PENDING", **metadata(rid)}]
        },
        error_class=MicrovmLaunchError,
        prefix=LAUNCH_FAILED,
        step="job.launch",
        step_error_type="RuntimeError",
        step_message=(
            "RunMicrovm returned no endpoint",
            "RunMicrovm returned no endpoint. RunMicrovm request ID: req-1.",
        ),
        call="run_microvm",
        calls=1,
    ),
    "GetMicrovm denies access": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "get_microvm": [
                service_error("AccessDeniedException", "denied", "GetMicrovm", rid, 403)
            ]
        },
        error_class=MicrovmDeliveryError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type="botocore.errorfactory.AccessDeniedException",
        step_message=_denied("GetMicrovm"),
        call="get_microvm",
        calls=1,
    ),
    "ResumeMicrovm denies access": Scenario(
        kind="checking session",
        answers=lambda rid: {
            "get_microvm": [{"microvmId": "mvm-1", "state": "SUSPENDED"}],
            "resume_microvm": [
                service_error(
                    "AccessDeniedException", "denied", "ResumeMicrovm", rid, 403
                )
            ],
        },
        error_class=MicrovmDeliveryError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type="botocore.errorfactory.AccessDeniedException",
        step_message=_denied("ResumeMicrovm"),
        call="resume_microvm",
        calls=1,
    ),
    "CreateMicrovmAuthToken denies access": Scenario(
        kind="microvm",
        answers=lambda rid: {
            "create_microvm_auth_token": [
                service_error(
                    "AccessDeniedException",
                    "denied",
                    "CreateMicrovmAuthToken",
                    rid,
                    403,
                )
            ]
        },
        error_class=MicrovmDeliveryError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type="botocore.errorfactory.AccessDeniedException",
        step_message=_denied("CreateMicrovmAuthToken"),
        call="create_microvm_auth_token",
        calls=1,
    ),
    "CreateMicrovmAuthToken returns no authToken": Scenario(
        kind="session",
        answers=lambda rid: {"create_microvm_auth_token": [metadata(rid)]},
        error_class=MicrovmDeliveryError,
        prefix=DELIVERY,
        step="job.request",
        step_error_type="RuntimeError",
        step_message=(
            "CreateMicrovmAuthToken returned no authToken",
            "CreateMicrovmAuthToken returned no authToken. CreateMicrovmAuthToken "
            "request ID: req-1.",
        ),
        call="create_microvm_auth_token",
        calls=1,
    ),
}


@pytest.mark.parametrize("label", list(SCENARIOS))
@pytest.mark.parametrize(
    ("request_id", "with_id"), [("req-1", 1), (None, 0)], ids=["with-id", "without-id"]
)
def test_shows_the_step_message_on_the_first_run_and_on_replay_without_a_retry(
    run: Any, fake_endpoint: Any, label: str, request_id: str | None, with_id: int
) -> None:
    scenario = SCENARIOS[label]
    endpoint = fake_endpoint(list(scenario.statuses))
    client = ScriptedClient(endpoint.url, **scenario.answers(request_id))
    caught: list[MicrovmError] = []

    result = run(catching(scenario.kind, client, caught))

    step_message = scenario.step_message[with_id]
    expected = f"{scenario.prefix}{step_message}"
    assert_caught(result, caught, scenario.error_class, expected)
    # The execution history keeps the same message.
    context = job_context(result, scenario.kind)
    step_error = context.get_step(scenario.step).error
    assert step_error is not None
    assert step_error.type == scenario.step_error_type
    assert step_error.message == step_message
    assert context.error is not None
    assert context.error.message == expected
    # The default retry strategy fails the step at once.
    assert client.calls[scenario.call] == scenario.calls


def test_names_the_request_id_that_botocore_parses_from_a_real_answer(
    run: Any, fake_endpoint: Any
) -> None:
    """A real lambda-microvms client, with botocore's Stubber, raises the
    modeled error with the request ID in ``ResponseMetadata``."""
    endpoint = fake_endpoint()
    client = boto3.session.Session().client("lambda-microvms", region_name="us-west-2")
    started = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    caught: list[MicrovmError] = []
    with Stubber(client) as stub:
        stub.add_response(
            "run_microvm",
            {
                "microvmId": "mvm-1",
                "state": "PENDING",
                "endpoint": endpoint.url,
                "imageArn": "arn:aws:lambda:us-west-2:123456789012:microvm-image:img",
                "imageVersion": "1",
                "maximumDurationInSeconds": 3900,
                "startedAt": started,
            },
        )
        stub.add_client_error(
            "get_microvm",
            service_error_code="ResourceNotFoundException",
            service_message="MicroVM not found: mvm-1",
            http_status_code=404,
            response_meta={"RequestId": "real-1"},
        )
        stub.add_response("terminate_microvm", {})

        result = run(catching("checking session", client, caught))

        stub.assert_no_pending_responses()
    client.close()
    assert_caught(
        result,
        caught,
        MicrovmNotRunningError,
        f"{DELIVERY}{REMOVED} GetMicrovm request ID: real-1.",
    )


@pytest.mark.parametrize("request_id", ["sts-1", None], ids=["with-id", "without-id"])
def test_names_no_request_id_for_an_sts_error_of_a_real_credential_provider(
    run: Any, request_id: str | None
) -> None:
    """botocore resolves the credentials inside the RunMicrovm call. A
    role-assuming provider calls STS there, and STS denies the role."""
    sts = boto3.client(
        "sts",
        region_name="us-west-2",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
    )
    stub = Stubber(sts)
    stub.add_client_error(
        "assume_role",
        service_error_code="AccessDenied",
        service_message="not authorized to assume role",
        http_status_code=403,
        response_meta={} if request_id is None else {"RequestId": request_id},
    )
    stub.activate()

    def assume_role() -> dict[str, str]:
        sts.assume_role(
            RoleArn="arn:aws:iam::123456789012:role/r", RoleSessionName="job"
        )
        raise AssertionError  # pragma: no cover - STS denies the role

    expired = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=1)

    class AssumeRoleProvider(CredentialProvider):
        """Returns expired credentials. botocore refreshes them with
        ``assume_role`` when it signs the next request."""

        METHOD = "assume-role"

        def load(self) -> RefreshableCredentials:
            return RefreshableCredentials.create_from_metadata(
                metadata={
                    "access_key": "key",
                    "secret_key": "secret",
                    "token": "token",
                    "expiry_time": expired.isoformat(),
                },
                refresh_using=assume_role,
                method=self.METHOD,
            )

    session = botocore.session.get_session()
    session.get_component("credential_provider").insert_before(
        "env", AssumeRoleProvider()
    )
    client = session.create_client("lambda-microvms", region_name="us-west-2")
    launches: list[str] = []
    client.meta.events.register(
        "before-parameter-build.lambda-microvms.RunMicrovm",
        lambda **kwargs: launches.append("RunMicrovm"),
    )
    caught: list[MicrovmError] = []

    result = run(catching("microvm", client, caught))

    stub.assert_no_pending_responses()
    client.close()
    assert_caught(result, caught, MicrovmLaunchError, f"{LAUNCH_FAILED}{STS_DENIED}")
    step_error = result.get_context("job").get_step("job.launch").error
    assert step_error is not None
    assert step_error.type == "botocore.exceptions.ClientError"
    assert step_error.message == STS_DENIED
    # The default retry strategy does not retry AccessDenied.
    assert launches == ["RunMicrovm"]


def throttled(operation: str, *, with_id: bool, count: int = 5) -> list[Answer]:
    """A new ThrottlingException for each call, as botocore raises it."""
    return [
        service_error(
            "ThrottlingException",
            "Rate exceeded",
            operation,
            f"req-{i + 1}" if with_id else None,
            429,
        )
        for i in range(count)
    ]


def _throttle_text(operation: str) -> str:
    return (
        f"An error occurred (ThrottlingException) when calling the {operation} "
        "operation: Rate exceeded"
    )


@pytest.mark.parametrize("with_id", [True, False], ids=["with-id", "without-id"])
def test_retries_a_throttled_auth_token_5_times_and_names_the_last_call(
    run: Any, fake_endpoint: Any, with_id: bool
) -> None:
    endpoint = fake_endpoint()
    tokens = throttled("CreateMicrovmAuthToken", with_id=with_id)
    client = ScriptedClient(endpoint.url, create_microvm_auth_token=tokens)
    caught: list[MicrovmError] = []

    result = run(catching("session", client, caught))

    text = _throttle_text("CreateMicrovmAuthToken")
    message = f"{text}. CreateMicrovmAuthToken request ID: req-5." if with_id else text
    assert_caught(result, caught, MicrovmDeliveryError, f"{DELIVERY}{message}")
    assert client.calls["create_microvm_auth_token"] == 5
    assert endpoint.requests == []


@pytest.mark.parametrize("with_id", [True, False], ids=["with-id", "without-id"])
def test_retries_a_throttled_launch_5_times_and_names_the_last_call(
    run: Any, fake_endpoint: Any, with_id: bool
) -> None:
    launches = throttled("RunMicrovm", with_id=with_id)
    client = ScriptedClient(fake_endpoint().url, run_microvm=launches)
    caught: list[MicrovmError] = []

    result = run(catching("microvm", client, caught))

    text = _throttle_text("RunMicrovm")
    message = f"{text}. RunMicrovm request ID: req-5." if with_id else text
    assert_caught(result, caught, MicrovmLaunchError, f"{LAUNCH_FAILED}{message}")
    assert client.calls["run_microvm"] == 5


def test_names_the_request_id_once_for_one_error_object_raised_by_every_call(
    run: Any, fake_endpoint: Any
) -> None:
    error = service_error("ThrottlingException", "Rate exceeded", "RunMicrovm")
    client = ScriptedClient(fake_endpoint().url, run_microvm=[error] * 5)
    caught: list[MicrovmError] = []

    result = run(catching("microvm", client, caught))

    text = _throttle_text("RunMicrovm")
    assert_caught(
        result,
        caught,
        MicrovmLaunchError,
        f"{LAUNCH_FAILED}{text}. RunMicrovm request ID: req-1.",
    )
    assert client.calls["run_microvm"] == 5
    # The error that the client raised keeps its message.
    assert str(error) == text


def test_names_only_the_last_request_id_of_one_error_object(
    run: Any, fake_endpoint: Any
) -> None:
    error = service_error("ThrottlingException", "Rate exceeded", "RunMicrovm", None)
    calls = iter(range(1, 6))

    def raise_with_new_id() -> Any:
        error.response["ResponseMetadata"]["RequestId"] = f"r-{next(calls)}"
        raise error

    client = ScriptedClient(fake_endpoint().url, run_microvm=[raise_with_new_id] * 5)
    caught: list[MicrovmError] = []

    result = run(catching("microvm", client, caught))

    message = f"{_throttle_text('RunMicrovm')}. RunMicrovm request ID: r-5."
    assert_caught(result, caught, MicrovmLaunchError, f"{LAUNCH_FAILED}{message}")
    step_error = result.get_context("job").get_step("job.launch").error
    assert step_error is not None
    assert step_error.message == message
    assert client.calls["run_microvm"] == 5


def test_gives_a_custom_strategy_the_error_that_botocore_raised(
    run: Any, fake_endpoint: Any
) -> None:
    tokens = throttled("CreateMicrovmAuthToken", with_id=True)
    client = ScriptedClient(fake_endpoint().url, create_microvm_auth_token=tokens)
    seen: list[tuple[Exception, str]] = []

    def strategy(error: Exception, attempt: int) -> RetryDecision:
        seen.append((error, str(error)))
        if attempt < 2:
            return RetryDecision.retry(Duration.from_seconds(1))
        return RetryDecision.no_retry()

    caught: list[MicrovmError] = []

    result = run(catching("session", client, caught, strategy))

    # The launch succeeded, so the strategy saw only the two token errors.
    text = _throttle_text("CreateMicrovmAuthToken")
    assert [error for error, _ in seen] == tokens[:2]
    for (error, message), raised in zip(seen, tokens, strict=False):
        assert error is raised
        assert message == text
    assert_caught(
        result,
        caught,
        MicrovmDeliveryError,
        f"{DELIVERY}{text}. CreateMicrovmAuthToken request ID: req-2.",
    )


def _fail_five_tokens_of_a_session_job(
    endpoint_url: str, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[weakref.ref[ClientError]], list[weakref.ref[ClientError]]]:
    """Run a session job whose 5 ``CreateMicrovmAuthToken`` calls are
    throttled. The handler catches the error and keeps no reference to it.

    Return weak references to the copies and to the originals.

    1. The core SDK calls the step retry strategy with the copy.
    2. :func:`decide_on_original_error` passes the original on to the
       strategy in the config.
    3. So the test wraps :func:`decide_on_original_error` where the
       operation calls it, and records each copy that it receives.
    """
    tokens = throttled("CreateMicrovmAuthToken", with_id=True)
    originals: list[weakref.ref[ClientError]] = [
        weakref.ref(error) for error in tokens if isinstance(error, ClientError)
    ]
    client = ScriptedClient(endpoint_url, create_microvm_auth_token=tokens)
    del tokens
    copies: list[weakref.ref[ClientError]] = []
    threaded: list[bool] = []

    def recording(strategy: Any) -> Any:
        decide = aws_errors.decide_on_original_error(strategy)

        def record(error: Exception, attempt: int) -> RetryDecision:
            if isinstance(error, ClientError) and "_original_error" in vars(error):
                copies.append(weakref.ref(error))
                threaded.append("_run_with_limit" in _frame_names(error.__traceback__))
            return decide(error, attempt)

        return record

    monkeypatch.setattr(operation_module, "decide_on_original_error", recording)
    fields = base_fields(client, None)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        try:
            microvm_session(
                context,
                "pipeline",
                config=MicrovmSessionConfig(**fields, timeout=Duration.from_hours(1)),
                handler=lambda vm, ctx: vm.invoke(
                    ctx,
                    "job",
                    job_input=1,
                    options=MicrovmInvokeOptions(
                        path="/job", timeout=Duration.from_minutes(10)
                    ),
                ),
            )
        except MicrovmError:
            return "caught"
        return "unexpected"  # pragma: no cover - the job fails

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    try:
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
    finally:
        runner.close()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.result == '"caught"'
    assert client.calls["create_microvm_auth_token"] == 5
    assert len(copies) == 5
    # The local test runner gives the invocation a remaining time. So each
    # call ran in the daemon thread of _run_with_limit.
    assert threaded == [True] * 5
    return copies, originals


def _frame_names(traceback: Any) -> list[str]:
    names = []
    while traceback is not None:
        names.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    return names


def test_keeps_no_error_of_a_throttled_token_call_after_the_execution(
    fake_endpoint: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """1. The traceback of each original holds the frame of the thread
       function of ``_run_with_limit``. That frame holds the copy.
    2. The copy holds the original in ``_original_error``. So the copy and
       the original refer to each other.
    3. No module state holds either error. So after the execution, the
       garbage collector must free the copies and the originals.

    The core SDK logs each failed step attempt with the error. pytest keeps
    each log record until the test ends, and a record holds its error. So
    the test turns off the step logger.
    """
    step_logger = logging.getLogger("aws_durable_execution_sdk_python.operation.step")
    monkeypatch.setattr(step_logger, "disabled", True)
    copies, originals = _fail_five_tokens_of_a_session_job(
        fake_endpoint().url, monkeypatch
    )

    gc.collect()

    assert [copy() for copy in copies] == [None] * 5
    assert [original() for original in originals] == [None] * 5


@pytest.mark.parametrize(
    ("request_id", "message"),
    [
        (
            "req-1",
            "An error occurred (AccessDeniedException) when calling the "
            "TerminateMicrovm operation: denied. TerminateMicrovm request ID: req-1.",
        ),
        (
            None,
            "An error occurred (AccessDeniedException) when calling the "
            "TerminateMicrovm operation: denied",
        ),
    ],
    ids=["with-id", "without-id"],
)
def test_keeps_the_job_result_and_records_the_terminate_message(
    run: Any, fake_endpoint: Any, request_id: str | None, message: str
) -> None:
    denied = service_error(
        "AccessDeniedException", "denied", "TerminateMicrovm", request_id, 403
    )
    client = ScriptedClient(fake_endpoint().url, terminate_microvm=[denied])

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return microvm(
            context,
            "build",
            job_input=1,
            config=MicrovmConfig(
                **base_fields(client, None), timeout=Duration.from_minutes(10)
            ),
        )

    result = run(handler, ("build.callback",))

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.result == '"ok"'
    assert client.calls["terminate_microvm"] == 1
    step_error = result.get_context("build").get_step("build.terminate").error
    assert step_error is not None
    assert step_error.type == "botocore.errorfactory.AccessDeniedException"
    assert step_error.message == message


def test_names_no_terminate_request_id_for_an_error_of_another_operation(
    run: Any, fake_endpoint: Any
) -> None:
    """A custom client raises one error object from CreateMicrovmAuthToken
    and then from TerminateMicrovm. The error names CreateMicrovmAuthToken
    as its operation. So the terminate record names no request ID: it would
    be the ID of another call."""
    shared = service_error(
        "AccessDeniedException", "denied", "CreateMicrovmAuthToken", "shared-1", 403
    )
    client = ScriptedClient(
        fake_endpoint().url,
        create_microvm_auth_token=[shared],
        terminate_microvm=[shared],
    )
    caught: list[MicrovmError] = []

    result = run(catching("session", client, caught))

    text = (
        "An error occurred (AccessDeniedException) when calling the "
        "CreateMicrovmAuthToken operation: denied"
    )
    assert_caught(
        result,
        caught,
        MicrovmDeliveryError,
        f"{DELIVERY}{text}. CreateMicrovmAuthToken request ID: shared-1.",
    )
    terminate = result.get_context("pipeline").get_step("pipeline.terminate").error
    assert terminate is not None
    assert terminate.message == text
    assert str(shared) == text


@pytest.mark.parametrize(
    ("call", "kind", "pattern", "calls"),
    [
        (call, kind, pattern, calls)
        for call, kind in [
            ("run_microvm", "microvm"),
            ("create_microvm_auth_token", "session"),
            ("terminate_microvm", "terminate"),
            ("get_microvm", "checking session"),
        ]
        for pattern, calls in [
            (re.compile(r": Rate exceeded$"), 3),
            (re.compile(r"request", re.IGNORECASE), 1),
        ]
    ],
    ids=lambda value: value.pattern if isinstance(value, re.Pattern) else str(value),
)
def test_a_message_based_strategy_decides_on_the_message_of_the_service(
    run: Any,
    fake_endpoint: Any,
    call: str,
    kind: str,
    pattern: re.Pattern[str],
    calls: int,
) -> None:
    """``create_retry_strategy`` matches its patterns against ``str(error)``.

    1. The anchored pattern matches the botocore message. So a throttled call
       is retried until the third attempt.
    2. The loose pattern matches no botocore message of a throttled call. So
       the call is not retried.
    3. The step message has the request ID. A strategy that saw it would
       decide the other way in both cases.
    """
    operation = {
        "run_microvm": "RunMicrovm",
        "create_microvm_auth_token": "CreateMicrovmAuthToken",
        "terminate_microvm": "TerminateMicrovm",
        "get_microvm": "GetMicrovm",
    }[call]
    client = ScriptedClient(
        fake_endpoint().url, **{call: throttled(operation, with_id=True, count=3)}
    )
    strategy = create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=3,
            initial_delay=Duration.from_seconds(1),
            retryable_errors=[pattern],
        )
    )
    caught: list[MicrovmError] = []
    if kind == "terminate":

        @durable_execution
        def handler(event: Any, context: DurableContext) -> Any:
            return microvm(
                context,
                "job",
                job_input=1,
                config=MicrovmConfig(
                    **base_fields(client, strategy), timeout=Duration.from_minutes(10)
                ),
            )

        result = run(handler, ("job.callback",))
    else:
        result = run(catching(kind, client, caught, strategy))

    # The decision first. It is the same as before the request ID existed.
    assert client.calls[call] == calls
    message = f"{_throttle_text(operation)}. {operation} request ID: req-{calls}."
    if kind == "terminate":
        assert result.status is InvocationStatus.SUCCEEDED, result.error
        assert result.result == '"ok"'
        record = result.get_context("job").get_step("job.terminate").error
        assert record is not None
        assert record.message == message
    else:
        prefix = LAUNCH_FAILED if call == "run_microvm" else DELIVERY
        error_class = (
            MicrovmLaunchError if call == "run_microvm" else MicrovmDeliveryError
        )
        assert_caught(result, caught, error_class, f"{prefix}{message}")
