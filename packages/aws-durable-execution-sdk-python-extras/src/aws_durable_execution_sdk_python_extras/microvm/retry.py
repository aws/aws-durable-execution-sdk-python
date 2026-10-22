# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The default step retry strategy of the MicroVM operations."""

from __future__ import annotations

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotocoreConnectionError

from aws_durable_execution_sdk_python_extras.microvm.request import (
    MicrovmEndpointUnavailableError,
    MicrovmRequestUnauthorizedError,
)


MAX_ATTEMPTS = 5
"""Attempts of one step, the first one included."""

MAX_DELAY_SECONDS = 60
"""The longest delay between two attempts."""

_RETRYABLE_ERROR_CODES = frozenset(
    {
        "ThrottlingException",
        "TooManyRequestsException",
        "InternalServerException",
        "ServiceException",
        "ServiceUnavailableException",
        # Another transition of the same MicroVM is in progress.
        "ConflictException",
        # The transient codes of botocore's standard retry mode.
        "RequestTimeout",
        "RequestTimeoutException",
        "PriorRequestNotComplete",
    }
)

_NON_RETRYABLE_ERROR_CODES = frozenset(
    {
        # HTTP 500, but the Region has no capacity for the MicroVM. A retry
        # seconds later fails the same way, and the JavaScript SDK does not
        # retry it either. The check runs before the status check.
        "InsufficientCapacityException",
    }
)

_RETRYABLE_ERROR_TYPES: tuple[type[Exception], ...] = (
    TimeoutError,
    ConnectionError,
    # botocore's connection and HTTP errors do not subclass the builtin
    # ConnectionError. These two bases cover EndpointConnectionError,
    # ConnectTimeoutError, ProxyConnectionError, SSLError, ReadTimeoutError,
    # ConnectionClosedError, and ResponseStreamingError. botocore's standard
    # retry mode treats the same two bases as transient.
    BotocoreConnectionError,
    HTTPClientError,
    # The request step's first retry tier ended. A later attempt runs after
    # a longer delay, outside the invocation.
    MicrovmEndpointUnavailableError,
    MicrovmRequestUnauthorizedError,
)


def default_microvm_retry_strategy(error: Exception, attempt: int) -> RetryDecision:
    """Retry a step that failed for a reason that a later attempt can fix.

    The launch, request, and terminate steps use this strategy unless the
    config sets ``retry_strategy``. It is the second retry tier. Inside each
    attempt, the boto3 client already retried throttling and server errors,
    and the request step already retried the POST.

    An error is retryable when it is one of these:

    1. A service error whose code shows a transient failure, such as
       ``ThrottlingException`` or ``InternalServerException``.
    2. A service error with HTTP status 429, or 500 and above, except
       ``InsufficientCapacityException``.
    3. A connection failure or a timeout, including botocore's
       ``HTTPClientError`` family.
    4. The end of the request step's first retry tier:
       ``MicrovmEndpointUnavailableError``, or
       ``MicrovmRequestUnauthorizedError`` after a refused new token.

    ``MicrovmRequestRejectedError`` is not retried. The route rejected the
    job with a 4xx status or a redirect, and the same job gets the same
    answer.

    Every other error fails the step at once. A ``ValidationException``, for
    example, fails the same way on every attempt.

    Args:
        error: The exception that the step raised.
        attempt: The number of the attempt that failed, starting at 1.

    Returns:
        A retry after 2 to the power ``attempt`` seconds, at most 60 seconds,
        for at most 5 attempts in all. Otherwise no retry.
    """
    if attempt >= MAX_ATTEMPTS or not _is_retryable(error):
        return RetryDecision.no_retry()
    return RetryDecision.retry(
        Duration.from_seconds(min(2**attempt, MAX_DELAY_SECONDS))
    )


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, _RETRYABLE_ERROR_TYPES):
        return True
    if not isinstance(error, ClientError):
        return False
    response = error.response
    code = response.get("Error", {}).get("Code")
    if code in _RETRYABLE_ERROR_CODES:
        return True
    if code in _NON_RETRYABLE_ERROR_CODES:
        return False
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return isinstance(status, int) and (status == 429 or status >= 500)
