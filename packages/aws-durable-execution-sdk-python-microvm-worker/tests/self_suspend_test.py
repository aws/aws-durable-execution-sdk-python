# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the worker's own SuspendMicrovm call."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)

from aws_durable_execution_sdk_python_microvm_worker.self_suspend import (
    SUSPEND_CALL_TIMEOUT_SECONDS,
    create_microvms_client,
    is_rejected_call,
    suspend_microvm,
)


def suspend_error(code: str, status: int, retries: int = 0) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status, "RetryAttempts": retries},
    }
    return ClientError(response, "SuspendMicrovm")


@pytest.mark.parametrize(
    "error",
    [
        suspend_error("AccessDeniedException", 403),
        suspend_error("ValidationException", 400),
        suspend_error("ResourceNotFoundException", 404),
        suspend_error("SomeServiceError", 403),
        # The rejection codes count whatever their status.
        suspend_error("AccessDeniedException", 500),
        NoCredentialsError(),
    ],
)
def test_counts_a_rejection_on_the_first_attempt_as_rejected(
    error: BaseException,
) -> None:
    assert is_rejected_call(error)


@pytest.mark.parametrize(
    "error",
    [
        # An earlier attempt may have suspended the MicroVM.
        suspend_error("ValidationException", 400, retries=1),
        suspend_error("AccessDeniedException", 403, retries=2),
        suspend_error("ThrottlingException", 429),
        # Some services answer a throttle with HTTP 400.
        suspend_error("ThrottlingException", 400),
        suspend_error("ExpiredTokenException", 403),
        suspend_error("ConflictException", 409),
        suspend_error("ConflictException", 400),
        suspend_error("RequestTimeoutException", 408),
        suspend_error("InternalServerException", 500),
        TimeoutError("timed out"),
        EndpointConnectionError(endpoint_url="https://example.com"),
        RuntimeError("boom"),
    ],
)
def test_counts_any_other_error_as_an_unknown_outcome(error: BaseException) -> None:
    assert not is_rejected_call(error)


def test_reads_a_client_error_without_metadata_as_an_unknown_outcome() -> None:
    error = ClientError({"Error": {"Code": "SomeError"}}, "SuspendMicrovm")  # type: ignore[typeddict-item]

    assert not is_rejected_call(error)


class _Client:
    def __init__(self, answer: Any = None, delay: float = 0.0) -> None:
        self.calls: list[dict[str, Any]] = []
        self._answer = answer
        self._delay = delay
        self.release = threading.Event()

    def suspend_microvm(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._delay:
            self.release.wait(self._delay)
        if isinstance(self._answer, BaseException):
            raise self._answer
        return {}


def test_calls_suspend_microvm_with_the_microvm_id() -> None:
    client: Any = _Client()

    suspend_microvm(client, "mvm-1", 5.0)

    assert client.calls == [{"microvmIdentifier": "mvm-1"}]


def test_raises_the_call_s_own_error() -> None:
    error = suspend_error("AccessDeniedException", 403)

    client: Any = _Client(error)

    with pytest.raises(ClientError) as raised:
        suspend_microvm(client, "mvm-1", 5.0)

    assert raised.value is error


def test_raises_the_call_s_own_timeout_error() -> None:
    client: Any = _Client(TimeoutError("read timed out"))

    with pytest.raises(TimeoutError, match="^read timed out$"):
        suspend_microvm(client, "mvm-1", 5.0)


def test_stops_waiting_for_a_stalled_call_after_the_timeout() -> None:
    client: Any = _Client(delay=10.0)
    started = time.monotonic()

    with pytest.raises(TimeoutError, match="longer than 0.05 seconds"):
        suspend_microvm(client, "mvm-1", 0.05)

    assert time.monotonic() - started < 2.0
    client.release.set()


def test_the_default_client_ends_a_read_at_the_call_timeout() -> None:
    client = create_microvms_client("us-west-2")
    try:
        config: Any = client.meta.config
        assert client.meta.region_name == "us-west-2"
        assert config.read_timeout == SUSPEND_CALL_TIMEOUT_SECONDS
        assert config.connect_timeout == 5
        assert config.retries == {"mode": "standard", "total_max_attempts": 3}
    finally:
        client.close()
