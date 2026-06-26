"""Custom logging."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.types import LoggerInterface


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, MutableMapping

    from aws_durable_execution_sdk_python.context import ExecutionState
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier


@dataclass(frozen=True)
class LogInfo:
    execution_state: ExecutionState
    parent_id: str | None = None
    operation_id: str | None = None
    name: str | None = None
    attempt: int | None = None

    @classmethod
    def from_operation_identifier(
        cls,
        execution_state: ExecutionState,
        op_id: OperationIdentifier,
        attempt: int | None = None,
    ) -> LogInfo:
        """Create new log info from an execution arn, OperationIdentifier and attempt."""
        return cls(
            execution_state=execution_state,
            parent_id=op_id.parent_id,
            operation_id=op_id.operation_id,
            name=op_id.name,
            attempt=attempt,
        )

    def with_parent_id(self, parent_id: str) -> LogInfo:
        """Clone the log info with a new parent id."""
        return LogInfo(
            execution_state=self.execution_state,
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
        is_replaying: Callable[[], bool] = lambda: False,
    ) -> None:
        self._logger = logger
        self._default_extra = default_extra
        # Replay status is owned by the DurableContext this logger belongs to,
        # not by the execution as a whole. This callable reads that context's
        # current replay status so log de-duplication is decided per-context.
        # The default (never replaying) suits a standalone logger with no
        # owning context, so it always logs.
        self._is_replaying = is_replaying

    @classmethod
    def from_log_info(
        cls,
        logger: LoggerInterface,
        info: LogInfo,
        is_replaying: Callable[[], bool] = lambda: False,
    ) -> Logger:
        """Create a new logger with the given LogInfo and replay-status source."""
        extra: MutableMapping[str, object] = {
            "executionArn": info.execution_state.durable_execution_arn
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
        return cls(logger=logger, default_extra=extra, is_replaying=is_replaying)

    def with_log_info(self, info: LogInfo) -> Logger:
        """Clone the existing logger with new LogInfo, preserving the replay-status source."""
        return Logger.from_log_info(
            logger=self._logger,
            info=info,
            is_replaying=self._is_replaying,
        )

    def with_is_replaying(self, is_replaying: Callable[[], bool]) -> Logger:
        """Clone the logger, rebinding it to a new replay-status source.

        Used when a child context inherits a parent's underlying logger but must
        report its own (the child's) replay status rather than the parent's.
        """
        return Logger(
            logger=self._logger,
            default_extra=self._default_extra,
            is_replaying=is_replaying,
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
        return not self._is_replaying()
