# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Helpers that the ``decider`` and ``llm`` operations share."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

import boto3
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import StepError
from aws_durable_execution_sdk_python.serdes import JsonSerDes

from aws_durable_execution_sdk_python_extras.ai.errors import (
    AiError,
    AiTimeoutError,
    DeciderInvalidResponseError,
)


_T = TypeVar("_T")

INVOCATION_RESERVE_SECONDS = 10
"""Time that a request leaves for the step to record its outcome."""

MAX_CLASSIFY_OPTIONS = 255
"""The most options of ``classify``.

Experimental. It can change or be removed in any release.

A Strands Decider ``choice`` question accepts at most 255.
"""

MAX_SCORE_LEVELS = 10
"""The most entries of a ``score`` scale.

Experimental. It can change or be removed in any release.

A Strands Decider ``score`` question accepts at most 10 levels.
"""


def serialize_input(value: Any) -> str:
    """Serialize an input for the model.

    A string stays as it is, and ``None`` becomes an empty string. Any other
    value is encoded with ``json.dumps``.

    Raises:
        TypeError: When ``json.dumps`` cannot encode the value.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        msg = "The input cannot be encoded as JSON."
        raise TypeError(msg) from error


def _normalize(value: str) -> str:
    return value.strip().casefold()


def validate_question(question: Any) -> str:
    """Check a question.

    Raises:
        TypeError: When the question is not a non-empty string.
    """
    if not isinstance(question, str) or not question.strip():
        msg = "The question must be a non-empty string."
        raise TypeError(msg)
    return question


def validate_options(options: Any, maximum: float = float("inf")) -> list[str]:
    """Check the options of ``classify`` or ``label``.

    The options are at least two non-empty strings that differ after trimming
    whitespace and ignoring case, and at most ``maximum``.

    Raises:
        TypeError: When the options are not a sequence of strings, or an option
            is empty or repeated.
        ValueError: When there are fewer than two options, or more than
            ``maximum``.
    """
    if isinstance(options, str) or not isinstance(options, Sequence):
        msg = "The options must be a list of strings."
        raise TypeError(msg)
    if len(options) < 2:
        msg = "The options must have at least two entries."
        raise ValueError(msg)
    if len(options) > maximum:
        msg = f"The options must have at most {maximum} entries."
        raise ValueError(msg)
    seen: set[str] = set()
    for option in options:
        if not isinstance(option, str) or not option.strip():
            msg = "Every option must be a non-empty string."
            raise TypeError(msg)
        key = _normalize(option)
        if key in seen:
            msg = (
                "The options must be unique, ignoring case and surrounding "
                f'whitespace. "{option}" is repeated.'
            )
            raise TypeError(msg)
        seen.add(key)
    return list(options)


def validate_scale(scale: Any) -> list[str]:
    """Check the scale of ``score``: 2 to :data:`MAX_SCORE_LEVELS` entries.

    Raises:
        TypeError: When the scale is not a sequence of non-empty strings.
        ValueError: When there are fewer than 2 entries or more than
            :data:`MAX_SCORE_LEVELS`.
    """
    if isinstance(scale, str) or not isinstance(scale, Sequence):
        msg = "The scale must be a list of strings."
        raise TypeError(msg)
    if len(scale) < 2:
        msg = "The scale must have at least two entries."
        raise ValueError(msg)
    if len(scale) > MAX_SCORE_LEVELS:
        msg = f"The scale must have at most {MAX_SCORE_LEVELS} entries."
        raise ValueError(msg)
    for entry in scale:
        if not isinstance(entry, str) or not entry.strip():
            msg = "Every scale entry must be a non-empty string."
            raise TypeError(msg)
    return list(scale)


def match_option(options: Sequence[str], reply: Any) -> str | None:
    """Return the option that a reply names, in the caller's spelling.

    The match trims whitespace and ignores case.
    """
    if not isinstance(reply, str):
        return None
    key = _normalize(reply)
    return next((option for option in options if _normalize(option) == key), None)


def to_unit_score(position: float, levels: int) -> float:
    """Map a 0-based position on a scale of ``levels`` entries to 0 to 1."""
    return min(1.0, max(0.0, position / (levels - 1)))


def invalid(message: str) -> DeciderInvalidResponseError:
    """Return the error for a reply that is not usable."""
    return DeciderInvalidResponseError(message=message)


def parse_reply(body: str | bytes) -> Any:
    """Parse a reply body as JSON.

    Raises:
        DeciderInvalidResponseError: When the body is not JSON.
    """
    try:
        return json.loads(body)
    except ValueError as error:
        raise invalid("The decision model's reply is not JSON.") from error


def remaining_seconds(context: DurableContext) -> float | None:
    """Return the invocation's remaining time, in seconds.

    ``None`` means that the compute reports no deadline.
    """
    lambda_context = context.lambda_context
    if lambda_context is None:
        return None
    read = getattr(lambda_context, "get_remaining_time_in_millis", None)
    if not callable(read):
        return None
    remaining = read()
    if not isinstance(remaining, int | float):
        return None
    return remaining / 1000


def call_before_deadline(call: Callable[[], _T], remaining: float | None) -> _T:
    """Run one call that must end :data:`INVOCATION_RESERVE_SECONDS` before
    the invocation ends.

    1. Without a deadline, the call runs as it is.
    2. With a deadline, the call runs in a daemon thread, and this function
       waits for it until the reserve starts. boto3 and ``urllib`` cannot
       cancel a call.
    3. A call that has not ended by then raises :class:`AiTimeoutError`. That
       error is retryable, so the step retries in a later invocation. The
       thread keeps running until the call returns. Its result is discarded.

    Raises:
        BaseException: The exception that the call raised, or
            :class:`AiTimeoutError`.
    """
    if remaining is None:
        return call()
    budget = remaining - INVOCATION_RESERVE_SECONDS
    timeout = AiTimeoutError(
        message=(
            "The request was stopped 10 seconds before the invocation runs out "
            "of time."
        )
    )
    if budget <= 0:
        raise timeout
    outcome: dict[str, Any] = {}
    done = threading.Event()

    def run() -> None:
        try:
            outcome["value"] = call()
        except BaseException as error:  # noqa: BLE001 - re-raised below
            outcome["error"] = error
        finally:
            done.set()

    threading.Thread(target=run, name="ai-call", daemon=True).start()
    if not done.wait(budget):
        raise timeout
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def run_step(
    context: DurableContext,
    name: str,
    sub_type: str,
    retry_strategy: Callable[[Exception, int], Any],
    body: Callable[[float | None], _T],
) -> _T:
    """Run ``body`` in one durable step, and raise an :class:`AiError` as its
    own class.

    ``body`` receives the invocation's remaining time when the attempt starts.
    The step checkpoints plain JSON, as the JavaScript SDK does.

    When the step fails with an :class:`AiError`, the SDK raises ``StepError``
    with that error rebuilt as ``__cause__``. This function raises a copy of
    ``__cause__`` from the ``StepError``. A copy is needed: raising
    ``__cause__`` itself would make each error the ``__cause__`` of the other.
    Any other step failure stays a ``StepError``.
    """
    try:
        return context.step(
            lambda _: body(remaining_seconds(context)),
            name=name,
            config=StepConfig(
                retry_strategy=retry_strategy,
                serdes=JsonSerDes(),
                sub_type=sub_type,
            ),
        )
    except StepError as error:
        cause = error.__cause__
        if isinstance(cause, AiError):
            raise type(cause)(
                message=cause.message,
                error_type=cause.error_type,
                data=cause.data,
                stack_trace=cause.stack_trace,
            ) from error
        raise


class LazyClient:
    """Creates a boto3 client on first use, with the function's Region and
    credentials. A client passed by the caller is used as it is."""

    def __init__(self, service: str, client: Any) -> None:
        self._service = service
        self._client = client
        self._lock = threading.Lock()

    def get(self) -> Any:
        with self._lock:
            if self._client is None:
                self._client = boto3.client(self._service)  # type: ignore[call-overload]
            return self._client
