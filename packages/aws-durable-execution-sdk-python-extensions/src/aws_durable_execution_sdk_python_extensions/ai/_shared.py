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
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from botocore.config import Config

from aws_durable_execution_sdk_python_extensions.__about__ import __version__
from aws_durable_execution_sdk_python_extensions.ai.errors import (
    AiError,
    DeciderInvalidResponseError,
)


_T = TypeVar("_T")


def serialize_input(value: Any) -> str:
    """Serialize an input for the model.

    A string stays as it is, and ``None`` becomes an empty string. Any other
    value is encoded with ``json.dumps``. A value that ``json.dumps`` cannot
    encode raises its ``TypeError`` or ``ValueError`` inside the step.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def _normalize(value: str) -> str:
    return value.strip().casefold()


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


def run_step(
    context: DurableContext,
    name: str,
    sub_type: str,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None,
    body: Callable[[], _T],
) -> _T:
    """Run ``body`` in one durable step, and raise an :class:`AiError` as its
    own class.

    The step checkpoints plain JSON, as the JavaScript SDK does. When
    ``retry_strategy`` is ``None``, the step uses the SDK's default strategy.

    When the step fails with an :class:`AiError`, the SDK raises ``StepError``
    with that error rebuilt as ``__cause__``. This function raises a copy of
    ``__cause__`` from the ``StepError``. A copy is needed: raising
    ``__cause__`` itself would make each error the ``__cause__`` of the other.
    Any other step failure stays a ``StepError``.
    """
    try:
        return context.step(
            lambda _: body(),
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
                self._client = boto3.client(  # type: ignore[call-overload]
                    self._service,
                    config=Config(
                        user_agent_extra=(
                            f"aws-durable-execution-sdk-python-extensions/{__version__} "
                            "md/ai"
                        )
                    ),
                )
            return self._client
