"""Custom logging."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import OperationType
from aws_durable_execution_sdk_python.types import LoggerInterface

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import ExecutionState


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
        self,
        logger: LoggerInterface,
        default_extra: Mapping[str, object],
        execution_state: ExecutionState | None = None,
        visited_operations: set[str] | None = None,
    ) -> None:
        self._logger = logger
        self._default_extra = default_extra
        self._execution_state = execution_state
        self._visited_operations = visited_operations or set()

    @classmethod
    def from_log_info(
        cls,
        logger: LoggerInterface,
        info: LogInfo,
        execution_state: ExecutionState | None = None,
        visited_operations: set[str] | None = None,
    ) -> Logger:
        """Create a new logger with the given LogInfo."""
        extra: MutableMapping[str, object] = {"execution_arn": info.execution_arn}
        if info.parent_id:
            extra["parent_id"] = info.parent_id
        if info.name:
            extra["name"] = info.name
        if info.attempt:
            extra["attempt"] = info.attempt
        return cls(logger, extra, execution_state, visited_operations)

    def with_log_info(self, info: LogInfo) -> Logger:
        """Clone the existing logger with new LogInfo."""
        return Logger.from_log_info(
            logger=self._logger,
            info=info,
            execution_state=self._execution_state,
            visited_operations=self._visited_operations,
        )

    def get_logger(self) -> LoggerInterface:
        """Get the underlying logger."""
        return self._logger

    def is_replay(self) -> bool:
        """Check if we are currently in replay mode.

        Returns True if there are operations in the execution state that haven't been visited yet.
        This indicates we are replaying previously executed operations.
        """
        if not self._execution_state:
            return False

        # If there are no operations, we're not in replay
        if not self._execution_state.operations:
            return False

        # Check if there are any operations in the execution state that we haven't visited
        # Only consider operations that are not EXECUTION type (which are system operations)
        for operation_id, operation in self._execution_state.operations.items():
            # Skip EXECUTION operations as they are system operations, not user operations
            if operation.operation_type == OperationType.EXECUTION:
                continue
            if operation_id not in self._visited_operations:
                return True
        return False

    def mark_operation_visited(self, operation_id: str) -> None:
        """Mark an operation as visited."""
        self._visited_operations.add(operation_id)

    def _should_log(self) -> bool:
        """Determine if logging should occur based on replay state."""
        # For the default logger, only log when not in replay
        return not self.is_replay()

    def debug(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        if not self._should_log():
            return
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.debug(msg, *args, extra=merged_extra)

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        if not self._should_log():
            return
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.info(msg, *args, extra=merged_extra)

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        if not self._should_log():
            return
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.warning(msg, *args, extra=merged_extra)

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        if not self._should_log():
            return
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.error(msg, *args, extra=merged_extra)

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        if not self._should_log():
            return
        merged_extra = {**self._default_extra, **(extra or {})}
        self._logger.exception(msg, *args, extra=merged_extra)

    @property
    def visited_operations(self):
        return self._visited_operations


class ReplayAwareLogger(Logger):
    """A logger that provides custom replay behavior for advanced users.

    This logger allows users to customize logging behavior during replay by overriding
    the _should_log method. By default, it behaves the same as the base Logger.
    """

    def _should_log(self) -> bool:
        """Override this method to customize replay logging behavior.

        Returns:
            bool: True if logging should occur, False otherwise.

        Example:
            def _should_log(self) -> bool:
                # Always log, even during replay
                return True

            def _should_log(self) -> bool:
                # Only log errors during replay
                return not self.is_replay() or self._current_log_level == 'error'
        """
        return super()._should_log()
