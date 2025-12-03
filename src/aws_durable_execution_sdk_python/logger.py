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
        execution_state: ExecutionState,
    ) -> None:
        self._logger = logger
        self._default_extra = default_extra
        self._execution_state = execution_state

    @classmethod
    def from_log_info(cls, logger: LoggerInterface, info: LogInfo) -> Logger:
        """Create a new logger with the given LogInfo."""
        extra: MutableMapping[str, object] = {
            "executionArn": info.execution_state.durable_execution_arn
        }
        if info.parent_id:
            extra["parentId"] = info.parent_id
        if info.name:
            # Use 'operation_name' instead of 'name' as key because the stdlib LogRecord internally reserved 'name' parameter
            extra["operationName"] = info.name
        if info.attempt is not None:
            extra["attempt"] = info.attempt + 1
        if info.operation_id:
            extra["operationId"] = info.operation_id
        return cls(
            logger=logger, default_extra=extra, execution_state=info.execution_state
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
        return not self._execution_state.is_replaying()
