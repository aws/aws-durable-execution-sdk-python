# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the callback reporter."""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from typing import Any

import pytest
from botocore.exceptions import NoCredentialsError
from botocore.stub import Stubber

from aws_durable_execution_sdk_python_microvm_worker import callback_reporter
from aws_durable_execution_sdk_python_microvm_worker.callback_reporter import (
    CLOSED_CALLBACK_CODE,
    MAX_CALLBACK_RESULT_BYTES,
    CallbackReporter,
    CallCancelledError,
    CancelScope,
    ResultSerializationError,
    ResultTooLargeError,
    is_permanent_error,
    is_terminal_callback_error,
)


def reporter_for(
    client: Any, sleeps: list[float] | None = None, logger: Any = None
) -> CallbackReporter:
    return CallbackReporter(
        "cb-1",
        client,
        sleep=(sleeps.append if sleeps is not None else lambda _s: None),
        logger=logger,
    )


# region results


@pytest.mark.parametrize(
    ("result", "encoded"),
    [
        ({"ok": True, "n": [1, 2]}, b'{"ok":true,"n":[1,2]}'),
        (None, b"null"),
        ("text", b'"text"'),
        (3, b"3"),
        ("日", '"日"'.encode()),
        # A lone surrogate cannot be UTF-8, so it is escaped.
        ("\ud800", b'"\\ud800"'),
    ],
)
def test_succeed_sends_json(fake_client, result, encoded):
    client = fake_client()
    reporter_for(client).succeed(result)
    assert client.calls == [("success", {"CallbackId": "cb-1", "Result": encoded})]


def test_succeed_rejects_result_over_256_kb(fake_client):
    client = fake_client()
    # The quotes make the serialized result 2 bytes longer than the string.
    with pytest.raises(ResultTooLargeError) as raised:
        reporter_for(client).succeed("x" * (MAX_CALLBACK_RESULT_BYTES - 1))
    assert raised.value.size == MAX_CALLBACK_RESULT_BYTES + 1
    assert client.calls == []
    # Exactly 256 KB is accepted.
    reporter_for(client).succeed("x" * (MAX_CALLBACK_RESULT_BYTES - 2))
    assert len(client.calls) == 1


def test_succeed_counts_utf8_bytes(fake_client):
    client = fake_client()
    # "日" is 3 bytes in UTF-8. With the 2 quotes, this result is 1 byte over.
    count = (MAX_CALLBACK_RESULT_BYTES - 2) // 3 + 1
    with pytest.raises(ResultTooLargeError):
        reporter_for(client).succeed("日" * count)
    reporter_for(client).succeed("日" * (count - 1))
    assert len(client.calls) == 1


def cyclic() -> list[Any]:
    value: list[Any] = []
    value.append(value)
    return value


@pytest.mark.parametrize("result", [{1, 2}, object(), math.nan, math.inf, cyclic()])
def test_succeed_rejects_non_json_result(fake_client, result):
    client = fake_client()
    with pytest.raises(ResultSerializationError, match="not JSON-serializable"):
        reporter_for(client).succeed(result)
    assert client.calls == []


# endregion results

# region errors


class CustomJobError(Exception):
    pass


class UnprintableError(Exception):
    def __str__(self) -> str:
        msg = "no text"
        raise RuntimeError(msg)


def test_fail_sends_class_name_and_message(fake_client):
    client = fake_client()
    reporter_for(client).fail(CustomJobError("build broke"))
    assert client.calls == [
        (
            "failure",
            {
                "CallbackId": "cb-1",
                "Error": {"ErrorType": "CustomJobError", "ErrorMessage": "build broke"},
            },
        )
    ]


def test_fail_cuts_long_type_and_message(fake_client):
    client = fake_client()
    long_type = type("E" * 300, (Exception,), {})
    reporter_for(client).fail(long_type("m" * 10_000))
    error = client.calls[0][1]["Error"]
    assert len(error["ErrorType"]) == 256
    assert error["ErrorType"].endswith("...")
    assert len(error["ErrorMessage"]) == 8 * 1024
    assert error["ErrorMessage"].endswith("...")


@pytest.mark.parametrize(
    ("value", "error_type", "message"),
    [
        (UnprintableError(), "UnprintableError", "unknown error"),
        ("plain text", "Error", "plain text"),
        (CustomJobError(), "CustomJobError", ""),
    ],
)
def test_fail_reports_unusual_values(fake_client, value, error_type, message):
    client = fake_client()
    reporter_for(client).fail(value)
    assert client.calls[0][1]["Error"] == {
        "ErrorType": error_type,
        "ErrorMessage": message,
    }


# endregion errors


# region retries


def test_completion_retries_transient_errors_five_times(fake_client, make_error):
    errors = [make_error("ServiceException", 500) for _ in range(5)]
    client = fake_client(*errors)
    sleeps: list[float] = []
    with pytest.raises(Exception, match="ServiceException"):
        reporter_for(client, sleeps).succeed(1)
    assert client.names() == ["success"] * 5
    assert sleeps == [1.0, 2.0, 4.0, 8.0]


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("TooManyRequestsException", 429),
        ("ThrottlingException", 400),
        ("ExpiredTokenException", 403),
    ],
)
def test_completion_retries_transient_4xx(fake_client, make_error, code, status):
    client = fake_client(make_error(code, status))
    reporter_for(client).succeed(1)
    assert client.names() == ["success", "success"]


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("AccessDeniedException", 403),
        ("CallbackTimeoutException", 400),
        ("ResourceNotFoundException", 404),
    ],
)
def test_completion_does_not_retry_permanent_errors(
    fake_client, make_error, code, status
):
    client = fake_client(make_error(code, status))
    with pytest.raises(Exception, match=code):
        reporter_for(client).fail(CustomJobError("x"))
    assert client.names() == ["failure"]


def test_closed_callback_on_first_attempt_raises(fake_client, make_error):
    client = fake_client(make_error(CLOSED_CALLBACK_CODE))
    with pytest.raises(Exception, match=CLOSED_CALLBACK_CODE):
        reporter_for(client).succeed(1)


@pytest.mark.parametrize(
    "first",
    [
        "timeout",
        "server",
        "sdk-retried",
    ],
)
def test_closed_callback_after_uncertain_attempt_is_delivered(
    fake_client, make_error, logger, first
):
    """An earlier attempt with an unknown outcome probably delivered the result.

    The service answers CallbackTimeoutException for an already-completed
    callback, measured against the real service.
    """
    responses = {
        "timeout": [TimeoutError("slow"), make_error(CLOSED_CALLBACK_CODE)],
        "server": [
            make_error("ServiceException", 500),
            make_error(CLOSED_CALLBACK_CODE),
        ],
        # botocore retried inside the first attempt, so an earlier try may
        # have reached the service.
        "sdk-retried": [make_error(CLOSED_CALLBACK_CODE, retries=2)],
    }[first]
    client = fake_client(*responses)
    reporter_for(client, logger=logger).succeed(1)
    assert len(logger.lines) == 1
    level, message, extra = logger.lines[0]
    assert level == "warning"
    assert "already complete or timed out" in message
    assert extra["callbackId"] == "cb-1"


def test_invalid_callback_id_after_uncertain_attempt_raises(fake_client, make_error):
    """InvalidParameterValueException means an invalid ID, not "already complete"."""
    client = fake_client(
        TimeoutError("slow"), make_error("InvalidParameterValueException")
    )
    with pytest.raises(Exception, match="InvalidParameterValueException"):
        reporter_for(client).succeed(1)
    assert client.names() == ["success", "success"]


def test_closed_callback_warning_keeps_its_fields_in_a_stdlib_logger(
    fake_client, make_error, caplog
):
    """The fields go in extra, so a logging.Logger keeps them on the record."""
    client = fake_client(TimeoutError("slow"), make_error(CLOSED_CALLBACK_CODE))
    stdlib = logging.getLogger("microvm-worker-test")
    with caplog.at_level(logging.WARNING, logger="microvm-worker-test"):
        reporter_for(client, logger=stdlib).succeed(1)
    [record] = caplog.records
    assert record.callbackId == "cb-1"
    assert record.attempt == 2


def test_closed_callback_after_missing_credentials_raises(fake_client, make_error):
    """Missing credentials send no request, so the first attempt delivered nothing."""
    client = fake_client(NoCredentialsError(), make_error(CLOSED_CALLBACK_CODE))
    with pytest.raises(Exception, match=CLOSED_CALLBACK_CODE):
        reporter_for(client).succeed(1)
    assert client.names() == ["success", "success"]


@pytest.mark.parametrize(
    ("code", "status", "permanent", "terminal"),
    [
        ("AccessDeniedException", 403, True, False),
        ("ValidationException", 400, True, False),
        ("CallbackTimeoutException", 400, True, True),
        ("InvalidParameterValueException", 400, True, True),
        ("ResourceNotFoundException", 404, True, True),
        ("TooManyRequestsException", 429, False, False),
        ("RequestTimeout", 408, False, False),
        ("ConflictException", 409, False, False),
        ("ThrottlingException", 400, False, False),
        ("RequestTimeTooSkewed", 403, False, False),
        ("ServiceException", 500, False, False),
    ],
)
def test_error_classification(make_error, code, status, permanent, terminal):
    error = make_error(code, status)
    assert is_permanent_error(error) is permanent
    assert is_terminal_callback_error(error) is terminal


@pytest.mark.parametrize(
    "error", [TimeoutError(), NoCredentialsError(), RuntimeError()]
)
def test_errors_without_status_are_transient(error):
    assert is_permanent_error(error) is False
    assert is_terminal_callback_error(error) is False


# endregion retries


# region bounded calls


def blocking(release: threading.Event) -> Any:
    """An answer that blocks until ``release`` is set, at most 10 seconds."""
    return lambda: release.wait(10)


def test_heartbeat_times_out(fake_client):
    release = threading.Event()
    client = fake_client(blocking(release))
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="heartbeat call took longer"):
        reporter_for(client).heartbeat(timeout=0.05)
    assert time.monotonic() - started < 2
    release.set()


def test_heartbeat_cancel_ends_a_call_in_flight(fake_client):
    release = threading.Event()
    client = fake_client(blocking(release))
    scope = CancelScope()
    threading.Timer(0.05, scope.cancel).start()
    started = time.monotonic()
    with pytest.raises(CallCancelledError):
        reporter_for(client).heartbeat(timeout=10, cancel=scope)
    assert time.monotonic() - started < 2
    release.set()


def test_heartbeat_with_a_cancelled_scope_makes_no_call(fake_client):
    client = fake_client()
    scope = CancelScope()
    scope.cancel()
    with pytest.raises(CallCancelledError):
        reporter_for(client).heartbeat(timeout=10, cancel=scope)
    assert client.calls == []


def test_wake_sets_the_waiter_on_cancel_and_releases_it_after():
    scope = CancelScope()
    waiter = threading.Event()
    with scope.wake(waiter):
        scope.cancel()
        assert waiter.is_set()
    later = threading.Event()
    with pytest.raises(CallCancelledError):
        with scope.wake(later):
            pass
    assert not later.is_set()


def test_bounded_call_returns_its_error(fake_client, make_error):
    client = fake_client(make_error("CallbackTimeoutException"))
    with pytest.raises(Exception, match="CallbackTimeoutException"):
        reporter_for(client).heartbeat(timeout=5)


def test_stalled_completion_attempt_is_retried(fake_client, monkeypatch):
    """Each completion attempt ends after its timeout, and the next one runs."""
    monkeypatch.setattr(callback_reporter, "COMPLETION_CALL_TIMEOUT_SECONDS", 0.05)
    release = threading.Event()
    client = fake_client(blocking(release))
    reporter_for(client).succeed("done")
    assert client.names() == ["success", "success"]
    release.set()


def test_unbounded_heartbeat_runs_on_the_caller_thread(fake_client):
    seen: list[threading.Thread] = []
    client = fake_client(lambda: seen.append(threading.current_thread()))
    reporter_for(client).heartbeat()
    assert seen == [threading.current_thread()]


# endregion bounded calls

# region client


class FalsyClient:
    """A client whose truth value is False, such as some test doubles."""

    def __init__(self) -> None:
        self.calls = 0

    def __bool__(self) -> bool:
        return False

    def send_durable_execution_callback_heartbeat(self, **_kwargs: Any) -> dict:
        self.calls += 1
        return {}


def test_the_given_client_is_used_and_no_client_is_created(monkeypatch):
    """__init__ only assigns. A falsy test double is used as given."""

    def fail_create(_region: str) -> Any:
        msg = "the reporter created its own client"
        raise AssertionError(msg)

    monkeypatch.setattr(callback_reporter, "_create_client", fail_create)
    client = FalsyClient()
    CallbackReporter("cb-1", client).heartbeat()  # type: ignore[arg-type]
    assert client.calls == 1


def test_reporter_closes_only_its_own_client(fake_client, monkeypatch):
    passed = fake_client()
    reporter_for(passed).close()
    assert passed.closed is False

    created = fake_client()
    regions: list[str] = []

    def create(region: str) -> Any:
        regions.append(region)
        return created

    monkeypatch.setattr(callback_reporter, "_create_client", create)
    owned = CallbackReporter.create("cb-1", "eu-west-1")
    owned.close()
    assert regions == ["eu-west-1"]
    assert created.closed is True


def test_create_raises_when_boto3_cannot_build_the_client(monkeypatch):
    monkeypatch.setenv("AWS_PROFILE", "otelbb-no-such-profile")
    with pytest.raises(Exception, match="otelbb-no-such-profile"):
        CallbackReporter.create("cb-1", "us-east-1")


def test_requests_match_the_lambda_api_model():
    """botocore validates each request against the Lambda service model."""
    client = callback_reporter._create_client("us-east-1")  # noqa: SLF001
    with Stubber(client) as stub:
        stub.add_response(
            "send_durable_execution_callback_success",
            {},
            {"CallbackId": "cb-1", "Result": json.dumps([1]).encode()},
        )
        stub.add_response(
            "send_durable_execution_callback_failure",
            {},
            {
                "CallbackId": "cb-1",
                "Error": {"ErrorType": "ValueError", "ErrorMessage": "bad"},
            },
        )
        stub.add_response(
            "send_durable_execution_callback_heartbeat", {}, {"CallbackId": "cb-1"}
        )
        reporter = CallbackReporter("cb-1", client)
        reporter.succeed([1])
        reporter.fail(ValueError("bad"))
        reporter.heartbeat()
        stub.assert_no_pending_responses()
    client.close()


# endregion client
