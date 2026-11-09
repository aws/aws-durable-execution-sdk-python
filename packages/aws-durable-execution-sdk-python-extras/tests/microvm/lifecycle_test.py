# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the state check that finds a MicroVM that has ended."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmEndpointUnavailableError,
)
from aws_durable_execution_sdk_python_extras.microvm.lifecycle import (
    MicrovmPermissionDeniedError,
    MicrovmStateCheckError,
    MicrovmStateError,
    check_not_ended,
)
from aws_durable_execution_sdk_python_extras.microvm.request import (
    INVOCATION_RESERVE_SECONDS,
)


def check_with(
    client: Any, *, remaining: Callable[[], float | None] = lambda: None
) -> str | None:
    return check_not_ended(client, "mvm-1", remaining)


def calls(client: Any) -> list[str]:
    return [name for name, _ in client.calls]


def service_error(operation: str, code: str, status: int = 400) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status, "RequestId": "req-1"},
    }
    return ClientError(response, operation)


@pytest.mark.parametrize("state", ["RUNNING", "PENDING", "SUSPENDING", "SUSPENDED"])
def test_check_returns_the_endpoint_of_a_microvm_that_has_not_ended(
    fake_client: Any, state: str
) -> None:
    # The service resumes a suspending or suspended MicroVM on a request,
    # when its idle policy sets autoResumeEnabled. So the check makes one
    # GetMicrovm call and no ResumeMicrovm call, and the caller keeps sending
    # the job.
    client = fake_client(states=[state], endpoint="mvm-1.new.example")

    assert check_with(client) == "mvm-1.new.example"
    assert calls(client) == ["get_microvm"]


@pytest.mark.parametrize("state", ["TERMINATING", "TERMINATED"])
def test_check_raises_the_state_error_for_a_microvm_that_ends(
    fake_client: Any, state: str
) -> None:
    client = fake_client(states=[state])

    with pytest.raises(MicrovmStateError) as raised:
        check_with(client)

    assert raised.value.state == state
    assert str(raised.value) == f"MicroVM mvm-1 is {state}."


def test_check_includes_the_state_reason_without_its_period(fake_client: Any) -> None:
    client = fake_client()
    client.get_microvm = lambda **_: {
        "state": "TERMINATED",
        "stateReason": "Maximum duration reached.",
    }

    with pytest.raises(MicrovmStateError) as raised:
        check_with(client)

    assert str(raised.value) == (
        "MicroVM mvm-1 is TERMINATED: Maximum duration reached."
    )


def test_check_raises_the_state_error_for_a_removed_microvm(
    fake_client: Any,
) -> None:
    error = service_error("GetMicrovm", "ResourceNotFoundException", 404)
    client = fake_client(states=[error])

    with pytest.raises(MicrovmStateError) as raised:
        check_with(client)

    assert raised.value.state is None
    assert "no longer exists" in str(raised.value)
    assert str(raised.value).endswith("GetMicrovm request ID: req-1.")
    assert raised.value.__cause__ is error


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("ThrottlingException", 429),
        ("ServiceException", 500),
        ("UnrecognizedClientException", 403),
    ],
)
def test_check_reports_a_failed_get_microvm_without_failing_the_job(
    fake_client: Any, code: str, status: int
) -> None:
    error = service_error("GetMicrovm", code, status)
    client = fake_client(states=[error])

    with pytest.raises(MicrovmStateCheckError) as raised:
        check_with(client)

    assert f"({code})" in str(raised.value)
    assert str(raised.value).endswith(f"{code} message. GetMicrovm request ID: req-1.")
    assert raised.value.__cause__ is error


def test_check_reports_a_get_microvm_connection_failure(fake_client: Any) -> None:
    error = EndpointConnectionError(endpoint_url="https://lambda.example")
    client = fake_client(states=[error])

    with pytest.raises(MicrovmStateCheckError) as raised:
        check_with(client)

    assert raised.value.__cause__ is error


def test_check_fails_at_once_when_get_microvm_is_denied(fake_client: Any) -> None:
    error = service_error("GetMicrovm", "AccessDeniedException", 403)
    client = fake_client(states=[error])

    with pytest.raises(MicrovmPermissionDeniedError) as raised:
        check_with(client)

    assert str(raised.value) == (
        "MicroVM mvm-1 gave a sign that it does not run. GetMicrovm was "
        "denied, so the operation cannot check its state. The function's "
        "execution role needs lambda:GetMicrovm. GetMicrovm request ID: req-1."
    )
    assert raised.value.__cause__ is error
    assert calls(client) == ["get_microvm"]


def test_check_names_no_get_request_id_for_an_error_of_another_call(
    fake_client: Any,
) -> None:
    # An STS error inside the call carries the request ID of STS.
    client = fake_client(
        states=[service_error("AssumeRole", "AccessDeniedException", 403)]
    )

    with pytest.raises(MicrovmPermissionDeniedError) as raised:
        check_with(client)

    assert str(raised.value).endswith("needs lambda:GetMicrovm.")


def test_check_starts_no_call_when_less_than_1_second_remains_before_the_reserve(
    fake_client: Any,
) -> None:
    client = fake_client()

    with pytest.raises(MicrovmEndpointUnavailableError):
        check_with(client, remaining=lambda: INVOCATION_RESERVE_SECONDS + 0.5)

    assert calls(client) == []


def test_check_stops_a_stalled_get_microvm_at_the_reserve(fake_client: Any) -> None:
    release = threading.Event()
    client = fake_client()
    client.get_microvm = lambda **_: release.wait(30)

    try:
        with pytest.raises(
            MicrovmEndpointUnavailableError,
            match=r"GetMicrovm for MicroVM mvm-1 did not end before the "
            r"invocation reserve, after \d+ ms\.",
        ):
            check_with(client, remaining=lambda: INVOCATION_RESERVE_SECONDS + 1.2)
    finally:
        release.set()
