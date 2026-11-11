# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The default step retry strategy of the ``agentcore`` operation."""

from __future__ import annotations

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotocoreConnectionError


MAX_ATTEMPTS = 5
"""Attempts of one step, the first one included."""

MAX_DELAY_SECONDS = 60
"""The longest delay between two attempts."""

_RETRYABLE_ERROR_CODES = frozenset(
    {
        # The error codes of the bedrock-agentcore model that a later
        # attempt can fix.
        "ThrottlingException",
        "InternalServerException",
        "RetryableConflictException",
        # The transient codes of botocore's standard retry mode.
        "RequestTimeout",
        "RequestTimeoutException",
        "PriorRequestNotComplete",
    }
)

_RETRYABLE_ERROR_TYPES: tuple[type[Exception], ...] = (
    TimeoutError,
    ConnectionError,
    # botocore's connection and HTTP errors do not subclass the builtin
    # ConnectionError. botocore's standard retry mode treats these two bases
    # as transient.
    BotocoreConnectionError,
    HTTPClientError,
)


def default_agentcore_retry_strategy(error: Exception, attempt: int) -> RetryDecision:
    """Retry a step that failed for a reason that a later attempt can fix.

    The request and stop steps use this strategy unless the config sets
    ``retry_strategy``. Inside each attempt, the boto3 client already retried
    throttling and server errors.

    An error is retryable when it is one of these:

    1. A service error whose code shows a transient failure, such as
       ``ThrottlingException`` or ``RetryableConflictException``.
    2. A service error with HTTP status 429, or 500 and above.
    3. A connection failure or a timeout.

    Every other error fails the step at once. A ``ValidationException`` or
    an ``AccessDeniedException``, for example, fails the same way on every
    attempt.

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
    if response.get("Error", {}).get("Code") in _RETRYABLE_ERROR_CODES:
        return True
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return isinstance(status, int) and (status == 429 or status >= 500)
