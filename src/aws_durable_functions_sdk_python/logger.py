"""Custom logging."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from aws_durable_functions_sdk_python.types import LoggerInterface

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping

    from aws_durable_functions_sdk_python.identifier import OperationIdentifier


@dataclass(frozen=True)
class LogInfo:
    execution_arn: str
    parent_id: str | None = None
    name: str | None = None
    attempt: int | None = None

    @classmethod
    def from_operation_identifier(
        cls, execution_arn: str, op_id: OperationIdentifier, attempt: int | None = None
    ) -> LogInfo:
        """Create new log info from an execution arn, OperationIdentifier and attempt."""
        return cls(
            execution_arn=execution_arn,
            parent_id=op_id.parent_id,
            name=op_id.name,
            attempt=attempt,
        )

    def with_parent_id(self, parent_id: str) -> LogInfo:
        """Clone the log info with a new parent id."""
        return LogInfo(
            execution_arn=self.execution_arn,
            parent_id=parent_id,
            name=self.name,
            attempt=self.attempt,
        )


class Logger(LoggerInterface):
    def __init__(
        self, logger: LoggerInterface, default_extra: Mapping[str, object]
    ) -> None:
        self._logger = logger
        self._default_extra = default_extra

    @classmethod
    def from_log_info(cls, logger: LoggerInterface, info: LogInfo) -> Logger:
        """Create a new logger with the given LogInfo."""
        extra: MutableMapping[str, object] = {"execution_arn": info.execution_arn}
        if info.parent_id:
            extra["parent_id"] = info.parent_id
        if info.name:
            extra["name"] = info.name
        if info.attempt:
            extra["attempt"] = info.attempt
        return cls(logger, extra)

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
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.debug(msg, *args, extra=merged_extra)

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.info(msg, *args, extra=merged_extra)

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.warning(msg, *args, extra=merged_extra)

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.error(msg, *args, extra=merged_extra)

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.exception(msg, *args, extra=merged_extra)
