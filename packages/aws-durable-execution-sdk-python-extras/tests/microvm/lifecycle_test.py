# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the state check that resumes a suspended session MicroVM."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmEndpointUnavailableError,
)
from aws_durable_execution_sdk_python_extras.microvm.lifecycle import (
    EnsureRunningOptions,
    MicrovmResumeDeniedError,
    MicrovmStateCheckError,
    MicrovmStateError,
    ensure_running,
    resume_if_suspended,
)
from aws_durable_execution_sdk_python_extras.microvm.request import (
    INVOCATION_RESERVE_SECONDS,
)


def run_with(
    client: Any,
    *,
    max_wait_seconds: float = 30,
    remaining: Callable[[], float | None] = lambda: None,
    sleeps: list[float] | None = None,
) -> str | None:
    recorded = [] if sleeps is None else sleeps
    return ensure_running(
        EnsureRunningOptions(
            client=client,
            microvm_id="mvm-1",
            max_wait_seconds=max_wait_seconds,
            remaining_seconds=remaining,
            logger=logging.getLogger("test"),
            sleep=recorded.append,
        )
    )


def calls(client: Any) -> list[str]:
    return [name for name, _ in client.calls]


def test_returns_the_current_endpoint_of_a_running_microvm(fake_client: Any) -> None:
    client = fake_client(endpoint="mvm-1.new.example")

    assert run_with(client) == "mvm-1.new.example"
    assert calls(client) == ["get_microvm"]


def test_waits_for_a_suspend_in_progress_then_resumes_once(fake_client: Any) -> None:
    client = fake_client(states=["SUSPENDING", "SUSPENDED", "SUSPENDED", "RUNNING"])
    sleeps: list[float] = []

    assert run_with(client, sleeps=sleeps) == "mvm-1.microvm.example"

    # The MicroVM stays SUSPENDED while its resume hook runs. So one
    # ResumeMicrovm call is enough.
    assert calls(client) == [
        "get_microvm",
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "get_microvm",
    ]
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
    assert sleeps == [0.25, 0.5, 1.0]


def test_keeps_polling_when_resume_reports_a_transition_in_progress(
    fake_client: Any, make_error: Any
) -> None:
    # An auto-resume is in progress. Each SUSPENDED answer sends
    # ResumeMicrovm again, and the service answers ConflictException. A
    # PENDING answer after a resume is a step of the resume, not a boot.
    client = fake_client(
        states=["SUSPENDED", "SUSPENDED", "PENDING", "RUNNING"],
        resumes=[make_error("ConflictException", 409)] * 2,
    )

    assert run_with(client) == "mvm-1.microvm.example"
    assert calls(client) == [
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "get_microvm",
    ]


def test_resumes_again_when_a_resume_conflicts_with_the_suspend(
    fake_client: Any, make_error: Any
) -> None:
    # The service rejects the first ResumeMicrovm with ConflictException.
    # The MicroVM stays SUSPENDED until a resume succeeds. So the next
    # SUSPENDED answer sends ResumeMicrovm again.
    client = fake_client(
        states=["SUSPENDING", "SUSPENDED"],
        resumes=[make_error("ConflictException", 409)],
        suspended_until_resumed=True,
    )
    sleeps: list[float] = []

    assert run_with(client, max_wait_seconds=3, sleeps=sleeps) == (
        "mvm-1.microvm.example"
    )
    assert calls(client) == [
        "get_microvm",
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "resume_microvm",
        "get_microvm",
    ]
    assert sleeps == [0.25, 0.5, 1.0]


def test_gives_up_after_the_wait_limit_when_each_resume_conflicts(
    fake_client: Any, make_error: Any
) -> None:
    client = fake_client(
        resumes=[make_error("ConflictException", 409)] * 10,
        suspended_until_resumed=True,
    )

    with pytest.raises(MicrovmEndpointUnavailableError, match="Last state: SUSPENDED"):
        run_with(client, max_wait_seconds=0.5)

    # One ResumeMicrovm call per GetMicrovm call. The wait of 0.5 seconds
    # after the second GetMicrovm call does not fit in the limit.
    assert calls(client) == [
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "resume_microvm",
    ]


def test_raises_a_resume_error_that_is_not_a_conflict(
    fake_client: Any, make_error: Any
) -> None:
    denied = make_error("AccessDeniedException", 403)
    client = fake_client(states=["SUSPENDED"], resumes=[denied])

    with pytest.raises(type(denied)) as raised:
        run_with(client)

    assert raised.value is denied


def test_returns_at_once_for_a_new_microvm_that_is_still_booting(
    fake_client: Any,
) -> None:
    client = fake_client(states=["PENDING"])

    assert run_with(client) == "mvm-1.microvm.example"
    assert calls(client) == ["get_microvm"]


def test_waits_for_running_after_a_resume_that_reports_pending(
    fake_client: Any,
) -> None:
    client = fake_client(states=["SUSPENDED", "PENDING", "RUNNING"])

    assert run_with(client) == "mvm-1.microvm.example"
    assert calls(client).count("get_microvm") == 3


@pytest.mark.parametrize("state", ["TERMINATING", "TERMINATED"])
def test_raises_the_state_error_for_a_microvm_that_ends(
    fake_client: Any, state: str
) -> None:
    client = fake_client(states=[state])

    with pytest.raises(MicrovmStateError) as raised:
        run_with(client)

    assert raised.value.state == state
    assert str(raised.value) == f"MicroVM mvm-1 is {state}."


def test_includes_the_state_reason(fake_client: Any) -> None:
    client = fake_client()
    client.get_microvm = lambda **_: {
        "state": "TERMINATED",
        "stateReason": "Maximum duration reached",
    }

    with pytest.raises(MicrovmStateError, match="is TERMINATED: Maximum duration"):
        run_with(client)


def test_raises_the_state_error_when_the_microvm_no_longer_exists(
    fake_client: Any, make_error: Any
) -> None:
    client = fake_client(states=[make_error("ResourceNotFoundException", 404)])

    with pytest.raises(MicrovmStateError) as raised:
        run_with(client)

    assert raised.value.state is None
    assert "no longer exists" in str(raised.value)


def test_raises_another_get_microvm_error(fake_client: Any, make_error: Any) -> None:
    throttled = make_error("ThrottlingException", 429)
    client = fake_client(states=[throttled])

    with pytest.raises(type(throttled)) as raised:
        run_with(client)

    assert raised.value is throttled


def test_gives_up_after_the_wait_limit(fake_client: Any) -> None:
    client = fake_client(states=["SUSPENDING"] * 10)

    with pytest.raises(MicrovmEndpointUnavailableError, match="Last state: SUSPENDING"):
        run_with(client, max_wait_seconds=0.5)

    # 0.25 fits in the limit, and the next 0.5 does not.
    assert calls(client) == ["get_microvm", "get_microvm"]


def test_gives_up_before_the_invocation_reserve(fake_client: Any) -> None:
    client = fake_client(states=["SUSPENDING"] * 10)

    with pytest.raises(MicrovmEndpointUnavailableError):
        run_with(
            client,
            # The reserve starts in 1.3 seconds. The fake sleep takes no time,
            # so the waits of 0.25, 0.5, and 1 second fit, and 2 seconds
            # does not.
            remaining=lambda: INVOCATION_RESERVE_SECONDS + 1.3,
        )

    assert calls(client) == ["get_microvm"] * 4


def test_starts_no_call_when_less_than_1_second_remains_before_the_reserve(
    fake_client: Any,
) -> None:
    client = fake_client()

    with pytest.raises(MicrovmEndpointUnavailableError):
        run_with(client, remaining=lambda: INVOCATION_RESERVE_SECONDS + 0.5)

    assert calls(client) == []


def test_stops_a_stalled_get_microvm_at_the_reserve(fake_client: Any) -> None:
    release = threading.Event()
    client = fake_client()
    client.get_microvm = lambda **_: release.wait(30)

    try:
        with pytest.raises(
            MicrovmEndpointUnavailableError, match="Last state: unknown"
        ):
            run_with(client, remaining=lambda: INVOCATION_RESERVE_SECONDS + 1.2)
    finally:
        release.set()


def test_raises_the_state_error_when_the_microvm_is_removed_before_the_resume(
    fake_client: Any, make_error: Any
) -> None:
    client = fake_client(
        states=["SUSPENDED"], resumes=[make_error("ResourceNotFoundException", 404)]
    )

    with pytest.raises(MicrovmStateError, match="no longer exists"):
        run_with(client)


# ---------------------------------------------------------------------------
# resume_if_suspended: the check after a sign of a suspended MicroVM
# ---------------------------------------------------------------------------


def check_with(
    client: Any,
    *,
    max_wait_seconds: float = 30,
    remaining: Callable[[], float | None] = lambda: None,
    sleeps: list[float] | None = None,
) -> str | None:
    recorded = [] if sleeps is None else sleeps
    return resume_if_suspended(
        EnsureRunningOptions(
            client=client,
            microvm_id="mvm-1",
            max_wait_seconds=max_wait_seconds,
            remaining_seconds=remaining,
            logger=logging.getLogger("test"),
            sleep=recorded.append,
        )
    )


def service_error(operation: str, code: str, status: int = 400) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status, "RequestId": "req-1"},
    }
    return ClientError(response, operation)


@pytest.mark.parametrize("state", ["RUNNING", "PENDING"])
def test_check_returns_at_once_for_a_microvm_that_runs_or_boots(
    fake_client: Any, state: str
) -> None:
    client = fake_client(states=[state], endpoint="mvm-1.new.example")

    assert check_with(client) == "mvm-1.new.example"
    assert calls(client) == ["get_microvm"]


@pytest.mark.parametrize("state", ["SUSPENDING", "SUSPENDED"])
def test_check_resumes_a_suspending_or_suspended_microvm_once(
    fake_client: Any, state: str
) -> None:
    # The service honors a resume during SUSPENDING. So the check does not
    # wait for SUSPENDED.
    client = fake_client(states=[state, "SUSPENDED", "PENDING", "RUNNING"])
    sleeps: list[float] = []

    assert check_with(client, sleeps=sleeps) == "mvm-1.microvm.example"
    assert calls(client) == [
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "get_microvm",
        "get_microvm",
    ]
    assert sleeps == [0.25, 0.5]


def test_check_keeps_polling_after_a_resume_conflict(
    fake_client: Any, make_error: Any
) -> None:
    client = fake_client(
        states=["SUSPENDED", "RUNNING"],
        resumes=[make_error("ConflictException", 409)],
    )

    assert check_with(client) == "mvm-1.microvm.example"
    assert calls(client) == ["get_microvm", "resume_microvm", "get_microvm"]


def test_check_resumes_again_after_a_conflict_during_the_suspend(
    fake_client: Any, make_error: Any
) -> None:
    # The service rejects the resume during SUSPENDING with
    # ConflictException. The suspend then ends, and the MicroVM stays
    # SUSPENDED until a resume succeeds. So the first SUSPENDED answer sends
    # ResumeMicrovm again.
    client = fake_client(
        states=["SUSPENDING", "SUSPENDING"],
        resumes=[make_error("ConflictException", 409)],
        suspended_until_resumed=True,
    )
    sleeps: list[float] = []

    assert check_with(client, max_wait_seconds=3, sleeps=sleeps) == (
        "mvm-1.microvm.example"
    )
    assert calls(client) == [
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "get_microvm",
        "resume_microvm",
        "get_microvm",
    ]
    assert sleeps == [0.25, 0.5]


def test_check_gives_up_after_the_wait_limit_when_each_resume_conflicts(
    fake_client: Any, make_error: Any
) -> None:
    client = fake_client(
        states=["SUSPENDING"],
        resumes=[make_error("ConflictException", 409)] * 10,
        suspended_until_resumed=True,
    )

    with pytest.raises(MicrovmEndpointUnavailableError, match="Last state: SUSPENDED"):
        check_with(client, max_wait_seconds=0.5)

    # One ResumeMicrovm call per GetMicrovm call. The wait of 0.5 seconds
    # after the third GetMicrovm call does not fit in the limit.
    assert calls(client) == [
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "resume_microvm",
        "get_microvm",
        "resume_microvm",
    ]


@pytest.mark.parametrize("state", ["TERMINATING", "TERMINATED"])
def test_check_raises_the_state_error_for_a_microvm_that_ends(
    fake_client: Any, state: str
) -> None:
    client = fake_client(states=[state])

    with pytest.raises(MicrovmStateError, match=f"MicroVM mvm-1 is {state}") as raised:
        check_with(client)

    assert raised.value.state == state


@pytest.mark.parametrize("operation", ["GetMicrovm", "ResumeMicrovm"])
def test_check_raises_the_state_error_for_a_removed_microvm(
    fake_client: Any, operation: str
) -> None:
    error = service_error(operation, "ResourceNotFoundException", 404)
    client = (
        fake_client(states=[error])
        if operation == "GetMicrovm"
        else fake_client(states=["SUSPENDED"], resumes=[error])
    )

    with pytest.raises(MicrovmStateError) as raised:
        check_with(client)

    assert str(raised.value).endswith(f"{operation} request ID: req-1.")
    assert raised.value.__cause__ is error


@pytest.mark.parametrize(
    ("code", "denied"),
    [("AccessDeniedException", True), ("ThrottlingException", False)],
)
def test_check_reports_a_failed_get_microvm_without_failing_the_job(
    fake_client: Any, code: str, denied: bool
) -> None:
    error = service_error("GetMicrovm", code)
    client = fake_client(states=[error])

    with pytest.raises(MicrovmStateCheckError) as raised:
        check_with(client)

    assert raised.value.denied is denied
    assert f"({code})" in str(raised.value)
    assert str(raised.value).endswith(f"{code} message. GetMicrovm request ID: req-1.")
    assert raised.value.__cause__ is error


def test_check_reports_a_get_microvm_connection_failure(fake_client: Any) -> None:
    error = EndpointConnectionError(endpoint_url="https://lambda.example")
    client = fake_client(states=[error])

    with pytest.raises(MicrovmStateCheckError) as raised:
        check_with(client)

    assert raised.value.denied is False
    assert raised.value.__cause__ is error


@pytest.mark.parametrize("state", ["SUSPENDING", "SUSPENDED"])
def test_check_fails_at_once_when_resume_microvm_is_denied(
    fake_client: Any, state: str
) -> None:
    error = service_error("ResumeMicrovm", "AccessDeniedException", 403)
    client = fake_client(states=[state], resumes=[error])

    with pytest.raises(MicrovmResumeDeniedError) as raised:
        check_with(client)

    assert str(raised.value) == (
        f"MicroVM mvm-1 is {state}. ResumeMicrovm was denied, so the operation "
        "cannot resume it. The function's execution role needs "
        "lambda:ResumeMicrovm. ResumeMicrovm request ID: req-1."
    )
    assert raised.value.__cause__ is error
    assert calls(client) == ["get_microvm", "resume_microvm"]


def test_check_names_no_request_id_for_an_error_of_another_call(
    fake_client: Any,
) -> None:
    # An STS error inside the call carries the request ID of STS.
    client = fake_client(
        states=["SUSPENDED"],
        resumes=[service_error("AssumeRole", "AccessDeniedException", 403)],
    )

    with pytest.raises(MicrovmResumeDeniedError) as raised:
        check_with(client)

    assert str(raised.value).endswith("needs lambda:ResumeMicrovm.")


def test_check_raises_another_resume_error_with_its_request_id(
    fake_client: Any,
) -> None:
    client = fake_client(
        states=["SUSPENDED"],
        resumes=[service_error("ResumeMicrovm", "ThrottlingException", 429)],
    )

    with pytest.raises(ClientError) as raised:
        check_with(client)

    assert str(raised.value).endswith("ResumeMicrovm request ID: req-1.")


def test_check_gives_up_when_the_resume_does_not_end_in_time(
    fake_client: Any,
) -> None:
    client = fake_client(states=["SUSPENDED"] * 10)

    with pytest.raises(MicrovmEndpointUnavailableError, match="Last state: SUSPENDED"):
        check_with(client, max_wait_seconds=0.1)


def test_check_stops_a_stalled_get_microvm_at_the_reserve(fake_client: Any) -> None:
    release = threading.Event()
    client = fake_client()
    client.get_microvm = lambda **_: release.wait(30)

    try:
        with pytest.raises(
            MicrovmEndpointUnavailableError, match="Last state: unknown"
        ):
            check_with(client, remaining=lambda: INVOCATION_RESERVE_SECONDS + 1.2)
    finally:
        release.set()
