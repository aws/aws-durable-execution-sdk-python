# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The default step retry strategy of the ``decider`` and ``llm`` operations."""

from __future__ import annotations

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotocoreConnectionError

from aws_durable_execution_sdk_python_extras.ai.errors import (
    AiTimeoutError,
    DeciderError,
    LlmStoppedError,
)


MAX_ATTEMPTS = 5
"""Attempts of one step, the first one included."""

MAX_DELAY_SECONDS = 60
"""The longest delay between two attempts."""

_RETRYABLE_ERROR_CODES = frozenset(
    {
        "ThrottlingException",
        "ServiceUnavailableException",
        "InternalServerException",
        "ModelTimeoutException",
        "ModelNotReadyException",
        "RequestTimeout",
        "RequestTimeoutException",
        # AWS Lambda Invoke.
        "TooManyRequestsException",
        "ServiceException",
        # Amazon SageMaker InvokeEndpoint.
        "InternalFailure",
        "ServiceUnavailable",
        "InternalDependencyException",
    }
)

_RETRYABLE_STOP_REASONS = frozenset({"malformed_model_output", "malformed_tool_use"})

_RETRYABLE_ERROR_TYPES: tuple[type[Exception], ...] = (
    AiTimeoutError,
    TimeoutError,
    ConnectionError,
    # botocore's connection and HTTP errors do not subclass the builtin
    # ConnectionError. These two bases cover its connection failures and
    # timeouts.
    BotocoreConnectionError,
    HTTPClientError,
)


def default_ai_retry_strategy(error: Exception, attempt: int) -> RetryDecision:
    """Retry a step that failed for a reason that a later attempt can fix.

    Experimental. It can change or be removed in any release.

    The ``decider`` and ``llm`` operations use this strategy unless the caller
    passes ``retry_strategy``. An error is retryable when it is one of these:

    1. :class:`AiTimeoutError`, a connection failure, or a timeout.
    2. :class:`DeciderError` with no status, or with status 408, 429, or 5xx.
    3. :class:`LlmStoppedError` for ``malformed_model_output`` or
       ``malformed_tool_use``.
    4. An AWS service error whose code shows a transient failure, such as
       ``ThrottlingException``, or whose HTTP status is 429 or 5xx.

    Every other error fails the step at once, because it repeats. Examples are
    :class:`DeciderInvalidResponseError`, :class:`LlmMaxTokensError`, a
    :class:`DeciderError` with status 422, and an access-denied or validation
    error.

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
    if isinstance(error, DeciderError):
        status = error.status
        return status is None or status in {408, 429} or status >= 500
    if isinstance(error, LlmStoppedError):
        return error.stop_reason in _RETRYABLE_STOP_REASONS
    if not isinstance(error, ClientError):
        return False
    code = error.response.get("Error", {}).get("Code")
    if code in _RETRYABLE_ERROR_CODES:
        return True
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return isinstance(status, int) and (status == 429 or status >= 500)
