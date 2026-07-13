"""Lifecycle effects produced by applying checkpoint updates.

Applying a batch of operation updates can imply actions on the execution
beyond the state change itself: the execution completed, it failed, or a
callback was created and now needs a timeout. These are returned as data
so the caller can act on them after the write finishes, rather than
triggering them in the middle of it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import (
        CallbackOptions,
        ErrorObject,
    )

    from aws_durable_execution_sdk_python_testing.token import CallbackToken


@dataclass(frozen=True)
class Completed:
    """The execution completed successfully with ``result``."""

    execution_arn: str
    result: str | None = None


@dataclass(frozen=True)
class Failed:
    """The execution failed with ``error``."""

    execution_arn: str
    error: ErrorObject


@dataclass(frozen=True)
class CallbackCreated:
    """A callback was created and its timeout (if any) must be scheduled."""

    execution_arn: str
    operation_id: str
    callback_options: CallbackOptions | None
    callback_token: CallbackToken


CheckpointEffect = Completed | Failed | CallbackCreated
