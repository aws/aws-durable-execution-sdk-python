# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the default MicroVM retry strategy."""

from __future__ import annotations

from typing import Any

import pytest
from botocore.exceptions import (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ProxyConnectionError,
    ReadTimeoutError,
    ResponseStreamingError,
    SSLError,
)

from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmEndpointUnavailableError,
    MicrovmRequestRejectedError,
    MicrovmRequestUnauthorizedError,
    default_microvm_retry_strategy,
    retry,
)


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("ThrottlingException", 400),
        ("TooManyRequestsException", 400),
        ("InternalServerException", 500),
        ("ServiceException", 500),
        ("ServiceUnavailableException", 503),
        ("ConflictException", 409),
        ("SomethingNew", 503),
        ("SomethingNew", 429),
        ("RequestTimeout", 408),
        ("RequestTimeoutException", 408),
        ("PriorRequestNotComplete", 400),
    ],
)
def test_retries_a_transient_service_error(
    make_error: Any, code: str, status: int
) -> None:
    decision = default_microvm_retry_strategy(make_error(code, status), 1)

    assert decision.should_retry
    assert decision.delay_seconds == 2


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("slow"),
        ConnectionResetError("reset"),
        EndpointConnectionError(endpoint_url="https://example"),
        ConnectTimeoutError(endpoint_url="https://example"),
        ReadTimeoutError(endpoint_url="https://example"),
        ConnectionClosedError(endpoint_url="https://example"),
        ProxyConnectionError(proxy_url="https://proxy", error="refused"),
        SSLError(endpoint_url="https://example", error=OSError("handshake")),
        ResponseStreamingError(error="truncated"),
        HTTPClientError(error="reset"),
    ],
)
def test_retries_a_connection_failure_or_a_timeout(error: Exception) -> None:
    assert default_microvm_retry_strategy(error, 1).should_retry


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("ValidationException", 400),
        ("AccessDeniedException", 403),
        ("ResourceNotFoundException", 404),
        # The service answers it with HTTP 500, so the code check must run
        # before the status check.
        ("InsufficientCapacityException", 500),
    ],
)
def test_does_not_retry_a_permanent_service_error(
    make_error: Any, code: str, status: int
) -> None:
    assert not default_microvm_retry_strategy(make_error(code, status), 1).should_retry


@pytest.mark.parametrize(
    ("error", "retried"),
    [
        (MicrovmEndpointUnavailableError("tier ended"), True),
        (MicrovmRequestUnauthorizedError("refused", 401), True),
        (MicrovmRequestRejectedError("not found", 404), False),
    ],
)
def test_decides_on_the_end_of_the_request_tier(
    error: Exception, retried: bool
) -> None:
    assert default_microvm_retry_strategy(error, 1).should_retry is retried


def test_does_not_retry_another_error() -> None:
    assert not default_microvm_retry_strategy(RuntimeError("bug"), 1).should_retry


def test_stops_after_5_attempts_and_caps_the_delay_at_60_seconds(
    make_error: Any,
) -> None:
    error = make_error("ThrottlingException", 400)
    delays = [
        default_microvm_retry_strategy(error, attempt).delay_seconds
        for attempt in range(1, 5)
    ]

    assert delays == [2, 4, 8, 16]
    assert not default_microvm_retry_strategy(error, 5).should_retry


def test_caps_the_delay_at_60_seconds(make_error: Any, monkeypatch: Any) -> None:
    monkeypatch.setattr(retry, "MAX_ATTEMPTS", 10)

    decision = default_microvm_retry_strategy(make_error("ThrottlingException"), 7)

    assert decision.delay_seconds == 60
