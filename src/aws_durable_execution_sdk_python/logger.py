"""Custom logging."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.types import LoggerInterface

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, MutableMapping

    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import ExecutionState


class _LogInfoStateAdapter:
    """Minimal adapter wrapping ExecutionState to satisfy LogInfo.context during transition.

    This allows LogInfo to accept an ExecutionState for backward compatibility
    with callers that haven't been updated to pass a DurableContext yet.
    The adapter exposes the same interface that LogInfo and Logger expect from
    a context: `.state` for accessing the ExecutionState and `.is_replaying()`
    for replay status checks.

    Since replay status has been moved to DurableContext, this adapter always
    returns False (not replaying), meaning logs will not be suppressed. This is
    the safe default when no real context is available.

    Will be removed once all callers pass context directly.
    """

    def __init__(self, execution_state: ExecutionState) -> None:
        self.state: ExecutionState = execution_state

    def is_replaying(self) -> bool:
        """Always return False since replay status is now per-context.

        Without a real DurableContext, we cannot determine replay status.
        Returning False means logs will not be suppressed, which is the safe default.
        """
        return False


def _wrap_context(context_or_state: Any) -> Any:
    """Wrap an ExecutionState in a _LogInfoStateAdapter if needed.

    During the transition period, LogInfo accepts either a DurableContext
    (preferred) or an ExecutionState (deprecated). This helper detects
    ExecutionState instances and wraps them in an adapter so that
    LogInfo.context always exposes `.state` and `.is_replaying()`.

    Args:
        context_or_state: Either a DurableContext or an ExecutionState.

    Returns:
        The original DurableContext, or a _LogInfoStateAdapter wrapping the ExecutionState.
    """
    # Import here to avoid circular imports at module level
    from aws_durable_execution_sdk_python.state import ExecutionState

    if isinstance(context_or_state, ExecutionState):
        return _LogInfoStateAdapter(context_or_state)
    return context_or_state


@dataclass(frozen=True)
class LogInfo:
    """Carries context and metadata for Logger construction.

    The `context` field holds a reference to the owning DurableContext (preferred)
    or a _LogInfoStateAdapter wrapping an ExecutionState (for backward compatibility
    during the transition period). Access `durable_execution_arn` via
    `context.state.durable_execution_arn`.
    """

    context: DurableContext
    parent_id: str | None = None
    operation_id: str | None = None
    name: str | None = None
    attempt: int | None = None

    def __init__(
        self,
        context: DurableContext | ExecutionState,
        parent_id: str | None = None,
        operation_id: str | None = None,
        name: str | None = None,
        attempt: int | None = None,
    ) -> None:
        """Initialize LogInfo, wrapping ExecutionState in an adapter if needed.

        During the transition period, the first positional argument may be either
        a DurableContext (preferred) or an ExecutionState (deprecated). If an
        ExecutionState is passed, it is wrapped in a _LogInfoStateAdapter so that
        `.context.state` and `.context.is_replaying()` work uniformly.
        """
        # Use object.__setattr__ because the dataclass is frozen
        object.__setattr__(self, "context", _wrap_context(context))
        object.__setattr__(self, "parent_id", parent_id)
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "attempt", attempt)

    @property
    def execution_state(self) -> ExecutionState:
        """Backward-compatible access to execution_state via context.state."""
        return self.context.state

    @classmethod
    def from_operation_identifier(
        cls,
        context: DurableContext | ExecutionState | None = None,
        op_id: OperationIdentifier | None = None,
        attempt: int | None = None,
        *,
        execution_state: ExecutionState | None = None,
    ) -> LogInfo:
        """Create new log info from a DurableContext, OperationIdentifier and attempt.

        During transition, accepts either `context` (preferred) or `execution_state`
        (deprecated, for backward compatibility with operation executors that haven't
        been updated yet). Both DurableContext and ExecutionState are accepted as
        the `context` parameter.
        """
        if op_id is None:
            msg = "op_id is required"
            raise ValueError(msg)

        # Resolve context: prefer explicit context, fall back to execution_state
        resolved_context: Any = context
        if resolved_context is None and execution_state is not None:
            resolved_context = execution_state
        if resolved_context is None:
            msg = "Either context or execution_state must be provided"
            raise ValueError(msg)

        return cls(
            context=resolved_context,
            parent_id=op_id.parent_id,
            operation_id=op_id.operation_id,
            name=op_id.name,
            attempt=attempt,
        )

    def with_parent_id(self, parent_id: str) -> LogInfo:
        """Clone the log info with a new parent id."""
        return LogInfo(
            context=self.context,
            parent_id=parent_id,
            operation_id=self.operation_id,
            name=self.name,
            attempt=self.attempt,
        )


class Logger(LoggerInterface):
    def __init__(
        self,
        logger: LoggerInterface,
        default_extra: Mapping[str, object],
        context: DurableContext | None = None,
        *,
        execution_state: ExecutionState | None = None,
    ) -> None:
        self._logger = logger
        self._default_extra = default_extra
        # Prefer context; fall back to execution_state for backward compatibility
        if context is not None:
            self._context = context
        elif execution_state is not None:
            # Wrap execution_state in an adapter so _should_log() works uniformly
            self._context = _LogInfoStateAdapter(execution_state)  # type: ignore[assignment]
        else:
            msg = "Either context or execution_state must be provided to Logger"
            raise ValueError(msg)

    @classmethod
    def from_log_info(cls, logger: LoggerInterface, info: LogInfo) -> Logger:
        """Create a new logger with the given LogInfo."""
        extra: MutableMapping[str, object] = {
            "executionArn": info.context.state.durable_execution_arn
        }
        if info.parent_id:
            extra["parentId"] = info.parent_id
        if info.name:
            # Use 'operation_name' instead of 'name' as key because the stdlib LogRecord internally reserved 'name' parameter
            extra["operationName"] = info.name
        if info.attempt is not None:
            extra["attempt"] = info.attempt
        if info.operation_id:
            extra["operationId"] = info.operation_id
        return cls(
            logger=logger, default_extra=extra, context=info.context
        )

    def with_log_info(self, info: LogInfo) -> Logger:
        """Clone the existing logger with new LogInfo."""
        return Logger.from_log_info(
            logger=self._logger,
            info=info,
        )

    def get_logger(self) -> LoggerInterface:
        """Get the underlying logger."""
        return self._logger

    def debug(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._log(self._logger.debug, msg, *args, extra=extra)

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._log(self._logger.info, msg, *args, extra=extra)

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._log(self._logger.warning, msg, *args, extra=extra)

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._log(self._logger.error, msg, *args, extra=extra)

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._log(self._logger.exception, msg, *args, extra=extra)

    def _log(
        self,
        log_func: Callable,
        msg: object,
        *args: object,
        extra: Mapping[str, object] | None = None,
    ):
        if not self._should_log():
            return
        merged_extra = {**self._default_extra, **(extra or {})}
        log_func(msg, *args, extra=merged_extra)

    def _should_log(self) -> bool:
        return not self._context.is_replaying()
