"""Execution lifecycle effects: collection and application.

While a batch of checkpoint updates is applied, the per-type processors
record lifecycle effects (completion, failure, callback creation) on an
:class:`ExecutionNotifier`. The checkpoint orchestrator then hands the
collected effects to :func:`apply_effects`, which drives them onto an
:class:`ExecutionObserver` after the write completes. Collecting first
and applying afterwards keeps effect handling out of the write itself.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
    CallbackCreated,
    Completed,
    Failed,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from aws_durable_execution_sdk_python.lambda_service import (
        CallbackOptions,
        ErrorObject,
    )

    from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
        CheckpointEffect,
    )
    from aws_durable_execution_sdk_python_testing.token import CallbackToken


class ExecutionObserver(ABC):
    """Receives execution lifecycle events."""

    @abstractmethod
    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Called when execution completes successfully."""

    @abstractmethod
    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Called when execution fails."""

    @abstractmethod
    def on_timed_out(self, execution_arn: str, error: ErrorObject) -> None:
        """Called when execution times out."""

    @abstractmethod
    def on_stopped(self, execution_arn: str, error: ErrorObject) -> None:
        """Called when execution is stopped."""

    @abstractmethod
    def on_callback_created(
        self,
        execution_arn: str,
        operation_id: str,
        callback_options: CallbackOptions | None,
        callback_token: CallbackToken,
    ) -> None:
        """Called when a callback is created."""


class ExecutionNotifier:
    """Collects lifecycle effects raised while applying checkpoint updates.

    Processors record an effect here instead of acting on it; the
    orchestrator reads :attr:`effects` once the updates are applied.
    """

    def __init__(self) -> None:
        self.effects: list[CheckpointEffect] = []

    def notify_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Record that the execution completed successfully."""
        self.effects.append(Completed(execution_arn=execution_arn, result=result))

    def notify_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Record that the execution failed."""
        self.effects.append(Failed(execution_arn=execution_arn, error=error))

    def notify_callback_created(
        self,
        execution_arn: str,
        operation_id: str,
        callback_options: CallbackOptions | None,
        callback_token: CallbackToken,
    ) -> None:
        """Record that a callback was created."""
        self.effects.append(
            CallbackCreated(
                execution_arn=execution_arn,
                operation_id=operation_id,
                callback_options=callback_options,
                callback_token=callback_token,
            )
        )


def apply_effects(
    effects: Iterable[CheckpointEffect], observer: ExecutionObserver
) -> None:
    """Drive each collected effect onto ``observer``."""
    for effect in effects:
        if isinstance(effect, Completed):
            observer.on_completed(effect.execution_arn, effect.result)
        elif isinstance(effect, Failed):
            observer.on_failed(effect.execution_arn, effect.error)
        elif isinstance(effect, CallbackCreated):
            observer.on_callback_created(
                effect.execution_arn,
                effect.operation_id,
                effect.callback_options,
                effect.callback_token,
            )
