"""Base classes for operation executors with checkpoint response handling."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.exceptions import InvalidStateError

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.state import CheckpointedResult

T = TypeVar("T")


@dataclass(frozen=True)
class CheckResult(Generic[T]):
    """Result of checking operation checkpoint status.

    Encapsulates the outcome of checking an operation's status and determines
    the next action in the operation execution flow.

    IMPORTANT: Do not construct directly. Use factory methods:
    - create_is_ready_to_execute(checkpoint) - operation ready to execute
    - create_started() - checkpoint created, check status again
    - create_completed(result) - terminal result available

    Attributes:
        is_ready_to_execute: True if the operation is ready to execute its logic
        has_checkpointed_result: True if a terminal result is already available
        checkpointed_result: Checkpoint data for execute()
        deserialized_result: Final result when operation completed
    """

    is_ready_to_execute: bool
    has_checkpointed_result: bool
    checkpointed_result: CheckpointedResult | None = None
    deserialized_result: T | None = None

    @classmethod
    def create_is_ready_to_execute(
        cls, checkpoint: CheckpointedResult
    ) -> CheckResult[T]:
        """Create a CheckResult indicating the operation is ready to execute.

        Args:
            checkpoint: The checkpoint data to pass to execute()

        Returns:
            CheckResult with is_ready_to_execute=True
        """
        return cls(
            is_ready_to_execute=True,
            has_checkpointed_result=False,
            checkpointed_result=checkpoint,
        )

    @classmethod
    def create_started(cls) -> CheckResult[T]:
        """Create a CheckResult signaling that a checkpoint was created.

        Signals that process() should verify checkpoint status again to detect
        if the operation completed already during checkpoint creation.

        Returns:
            CheckResult indicating process() should check status again
        """
        return cls(is_ready_to_execute=False, has_checkpointed_result=False)

    @classmethod
    def create_completed(cls, result: T) -> CheckResult[T]:
        """Create a CheckResult with a terminal result already deserialized.

        Args:
            result: The final deserialized result

        Returns:
            CheckResult with has_checkpointed_result=True and deserialized_result set
        """
        return cls(
            is_ready_to_execute=False,
            has_checkpointed_result=True,
            deserialized_result=result,
        )


class OperationExecutor(ABC, Generic[T]):
    """Base class for durable operations with checkpoint response handling.

    Provides a framework for implementing operations that check status after
    creating START checkpoints to handle synchronous completion, avoiding
    unnecessary execution or suspension.

    The common pattern:
    1. Check operation status
    2. Create START checkpoint if needed
    3. Check status again (detects synchronous completion)
    4. Execute operation logic when ready

    Subclasses must implement:
    - check_result_status(): Check status, create checkpoint if needed, return next action
    - execute(): Execute the operation logic with checkpoint data
    """

    @abstractmethod
    def check_result_status(self) -> CheckResult[T]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        This method should:
        1. Get the current checkpoint result
        2. Check for terminal statuses (SUCCEEDED, FAILED, etc.) and handle them
        3. Check for pending statuses and suspend if needed
        4. Create a START checkpoint if the operation hasn't started
        5. Return a CheckResult indicating the next action

        Returns:
            CheckResult indicating whether to:
            - Return a terminal result (has_checkpointed_result=True)
            - Execute operation logic (is_ready_to_execute=True)
            - Check status again (neither flag set - checkpoint was just created)

        Raises:
            Operation-specific exceptions for terminal failure states
            SuspendExecution for pending states
        """
        ...  # pragma: no cover

    @abstractmethod
    def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute operation logic with checkpoint data.

        This method is called when the operation is ready to execute its core logic.
        It receives the checkpoint data that was returned by check_result_status().

        Args:
            checkpointed_result: The checkpoint data containing operation state

        Returns:
            The result of executing the operation

        Raises:
            May raise operation-specific errors during execution
        """
        ...  # pragma: no cover

    def process(self) -> T:
        """Process operation with checkpoint response handling.

        Orchestrates the double-check pattern:
        1. Check status (handles replay and existing checkpoints)
        2. If checkpoint was just created, check status again (detects synchronous completion)
        3. Return terminal result if available
        4. Execute operation logic if ready
        5. Raise error for invalid states

        Returns:
            The final result of the operation

        Raises:
            InvalidStateError: If the check result is in an invalid state
            May raise operation-specific errors from check_result_status() or execute()
        """
        # Check 1: Entry (handles replay and existing checkpoints)
        result = self.check_result_status()

        # If checkpoint was created, verify checkpoint response for immediate status change
        if not result.is_ready_to_execute and not result.has_checkpointed_result:
            result = self.check_result_status()

        # Return terminal result if available (can be None for operations that return None)
        if result.has_checkpointed_result:
            return result.deserialized_result  # type: ignore[return-value]

        # Execute operation logic
        if result.is_ready_to_execute:
            if result.checkpointed_result is None:
                msg = "CheckResult is marked ready to execute but checkpointed result is not set."
                raise InvalidStateError(msg)
            return self.execute(result.checkpointed_result)

        # Invalid state - neither terminal nor ready to execute
        msg = "Invalid CheckResult state: neither terminal nor ready to execute"
        raise InvalidStateError(msg)
