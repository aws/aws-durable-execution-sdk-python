"""Model for execution state."""

from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING

import aws_durable_execution_sdk_python.logger as logging
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    CallableRuntimeError,
    DurableExecutionsError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    DurableServiceClient,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StateOutput,
)
from aws_durable_execution_sdk_python.threading import CompletionEvent, OrderedLock

if TYPE_CHECKING:
    import datetime
    from collections.abc import MutableMapping

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckpointBatcherConfig:
    """Configuration for checkpoint batching behavior.

    Attributes:
        max_batch_size_bytes: Maximum batch size in bytes (default: 750KB)
        max_batch_time_seconds: Maximum time to wait before flushing batch (default: 1.0 second)
        max_batch_operations: Maximum number of operations per batch (default: unlimited)
    """

    max_batch_size_bytes: int = 750 * 1024  # 750KB - private readonly MAX_PAYLOAD_SIZE
    max_batch_time_seconds: float = 1.0  # 1 second default
    max_batch_operations: int | float = float("inf")  # No operation limit by default


@dataclass(frozen=True)
class QueuedOperation:
    """Wrapper for operations in the checkpoint queue.

    Attributes:
        operation_update: The operation update to be checkpointed, or None for empty checkpoints
        completion_event: CompletionEvent for synchronous operations, or None for async operations
    """

    operation_update: OperationUpdate | None
    completion_event: CompletionEvent | None = None


@dataclass(frozen=True)
class CheckpointedResult:
    """Result of a checkpointed operation.

    Set by ExecutionState.get_checkpoint_result. This is a convenience wrapper around
    Operation.

    Attributes:
        operation (Operation): The wrapped operation for the checkpoint result.
        status (OperationStatus): The status of the operation.
        result (str): the result of the operation.
        error (ErrorObject): the error of the operation.
    """

    operation: Operation | None = None
    status: OperationStatus | None = None
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def create_from_operation(cls, operation: Operation) -> CheckpointedResult:
        """Create a result from an operation."""
        result: str | None = None
        error: ErrorObject | None = None
        match operation.operation_type:
            case OperationType.STEP:
                step_details = operation.step_details
                result = step_details.result if step_details else None
                error = step_details.error if step_details else None

            case OperationType.CALLBACK:
                callback_details = operation.callback_details
                result = callback_details.result if callback_details else None
                error = callback_details.error if callback_details else None

            case OperationType.CHAINED_INVOKE:
                invoke_details = operation.chained_invoke_details
                result = invoke_details.result if invoke_details else None
                error = invoke_details.error if invoke_details else None

            case OperationType.CONTEXT:
                context_details = operation.context_details
                result = context_details.result if context_details else None
                error = context_details.error if context_details else None

        return cls(
            operation=operation, status=operation.status, result=result, error=error
        )

    @classmethod
    def create_not_found(cls) -> CheckpointedResult:
        """Create a result when the checkpoint was not found."""
        return cls(operation=None)

    def is_existent(self) -> bool:
        """Return true if a checkpoint of any type exists."""
        return self.operation is not None

    def is_succeeded(self) -> bool:
        """Return True if the checkpointed operation is SUCCEEDED."""
        op = self.operation
        if not op:
            return False

        return op.status is OperationStatus.SUCCEEDED

    def is_cancelled(self) -> bool:
        if op := self.operation:
            return op.status is OperationStatus.CANCELLED
        return False

    def is_failed(self) -> bool:
        """Return True if the checkpointed operation is FAILED."""
        op = self.operation
        if not op:
            return False

        return op.status is OperationStatus.FAILED

    def is_stopped(self) -> bool:
        """Return True if the checkpointed operation is STOPPED"""
        op = self.operation
        if not op:
            return False

        return op.status is OperationStatus.STOPPED

    def is_started(self) -> bool:
        """Return True if the checkpointed operation is STARTED."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.STARTED

    def is_started_or_ready(self) -> bool:
        """Return True if the checkpointed operation is STARTED or READY."""
        op = self.operation
        if not op:
            return False
        return op.status in {OperationStatus.STARTED, OperationStatus.READY}

    def is_pending(self) -> bool:
        """Return True if the checkpointed operation is PENDING."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.PENDING

    def is_timed_out(self) -> bool:
        """Return True if the checkpointed operation is TIMED_OUT."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.TIMED_OUT

    def is_replay_children(self) -> bool:
        op = self.operation
        if not op:
            return False
        return op.context_details.replay_children if op.context_details else False

    def raise_callable_error(self, msg: str | None = None) -> None:
        if self.error is None:
            err_msg = (
                msg
                or "Unknown error. No ErrorObject exists on the Checkpoint Operation."
            )
            raise CallableRuntimeError(
                message=err_msg,
                error_type=None,
                data=None,
                stack_trace=None,
            )

        raise self.error.to_callable_runtime_error()

    def get_next_attempt_timestamp(self) -> datetime.datetime | None:
        if self.operation and self.operation.step_details:
            return self.operation.step_details.next_attempt_timestamp
        return None


# shared so don't need to create an instance for each not found check
CHECKPOINT_NOT_FOUND = CheckpointedResult.create_not_found()


class ExecutionState:
    """Get, set and maintain execution state. This is mutable. Create and check checkpoints."""

    def __init__(
        self,
        durable_execution_arn: str,
        initial_checkpoint_token: str,
        operations: MutableMapping[str, Operation],
        service_client: DurableServiceClient,
        batcher_config: CheckpointBatcherConfig | None = None,
    ):
        self.durable_execution_arn: str = durable_execution_arn
        self._current_checkpoint_token: str = initial_checkpoint_token
        self.operations: MutableMapping[str, Operation] = operations
        self._service_client: DurableServiceClient = service_client
        self._ordered_checkpoint_lock: OrderedLock = OrderedLock()
        self._operations_lock: Lock = Lock()

        # Checkpoint batching configuration
        self._batcher_config: CheckpointBatcherConfig = (
            batcher_config or CheckpointBatcherConfig()
        )

        # Checkpoint batching components
        self._checkpoint_queue: queue.Queue[QueuedOperation] = queue.Queue()
        self._overflow_queue: queue.Queue[QueuedOperation] = queue.Queue()
        self._checkpointing_stopped: threading.Event = threading.Event()
        self._checkpointing_failed: CompletionEvent = CompletionEvent()

        # Concurrency management for parallel operations: parent_id -> {child_operation_ids}
        self._parent_to_children: dict[str, set[str]] = {}

        # Operations whose parent has completed
        self._parent_done: set[str] = set()

        # Protects parent_to_children and parent_done
        self._parent_done_lock: Lock = Lock()

    def fetch_paginated_operations(
        self,
        initial_operations: list[Operation],
        checkpoint_token: str,
        next_marker: str | None,
    ) -> None:
        """Add initial operations and fetch all paginated operations from the Durable Functions API. This method is thread_safe.

        The checkpoint_token is passed explicitly as a parameter rather than using the instance variable to ensure thread safety.

        Args:
            initial_operations: initial operations to be added to ExecutionState
            checkpoint_token: checkpoint token used to call Durable Functions API.
            next_marker: a marker indicates that there are paginated operations.
        """
        all_operations: list[Operation] = (
            initial_operations.copy() if initial_operations else []
        )
        while next_marker:
            output: StateOutput = self._service_client.get_execution_state(
                durable_execution_arn=self.durable_execution_arn,
                checkpoint_token=checkpoint_token,
                next_marker=next_marker,
            )
            all_operations.extend(output.operations)
            next_marker = output.next_marker
        with self._operations_lock:
            self.operations.update({op.operation_id: op for op in all_operations})

    def get_checkpoint_result(self, checkpoint_id: str) -> CheckpointedResult:
        """Get checkpoint result.

        Note this does not invoke the Durable Functions API. It only checks
        against the checkpoints currently saved in ExecutionState. The current
        saved checkpoints are from InitialExecutionState as retrieved
        at the start of the current execution/replay (see execution.durable_execution),
        and from each create_checkpoint response.

        Args:
            checkpoint_id: str - id for checkpoint to retrieve.

        Returns:
            CheckpointedResult with is_succeeded True if the checkpoint exists and its
                status is SUCCEEDED. If the checkpoint exists but its status is not
                SUCCEEDED, or if the checkpoint doesn't exist, then return
                CheckpointedResult with is_succeeded=False,result=None.
        """
        # checking status are deliberately under a lighter non-serialized lock
        with self._operations_lock:
            if checkpoint := self.operations.get(checkpoint_id):
                return CheckpointedResult.create_from_operation(checkpoint)

        return CHECKPOINT_NOT_FOUND

    def create_checkpoint(
        self,
        operation_update: OperationUpdate | None = None,
        is_sync: bool = True,  # noqa: FBT001, FBT002
    ) -> None:
        """Create a checkpoint with optional synchronous behavior.

        This method enqueues a checkpoint operation for processing by the background
        batching thread. By default, the operation is synchronous (blocking) to ensure
        the checkpoint is persisted before continuing. For performance-critical paths
        where immediate confirmation is not required, set is_sync=False.

        Synchronous checkpoints (is_sync=True, default):
        - Block the caller until the checkpoint is processed by the background thread
        - Ensure the checkpoint is persisted before continuing
        - Safe default for correctness
        - Use cases: Most operations requiring confirmation before proceeding

        Asynchronous checkpoints (is_sync=False, opt-in):
        - Return immediately without waiting for the checkpoint to complete
        - Performance optimization for specific use cases
        - Use cases: observability checkpoints, fire-and-forget operations

        When to use synchronous checkpoints (is_sync=True, default):
        1. Step START with AtMostOncePerRetry semantics - prevents duplicate execution
        2. Operation completion (SUCCEED/FAIL) - ensures state persisted before returning
        3. Retry operations - ensures retry state recorded before continuing
        4. Callback START - must wait for API to generate callback ID
        5. Invoke START - ensures chained invoke recorded before proceeding
        6. Child context results - ensures results persisted before returning
        7. Large results - ensures results saved before returning to caller
        8. Wait for condition completion - ensures state recorded before proceeding
        9. Most operations - safe default

        When to use asynchronous checkpoints (is_sync=False, opt-in):
        1. Step START with AtLeastOncePerRetry semantics - performance optimization
        2. Child context START - fire-and-forget for performance
        3. Wait for condition START - observability only, no blocking needed
        4. Any checkpoint where immediate confirmation is not required AND performance matters

        Args:
            operation_update: The checkpoint to create. If None, creates an empty
                            checkpoint to get a fresh checkpoint token and updated
                            operations list.
            is_sync: If True (default), blocks until the checkpoint is processed.
                    If False, returns immediately without blocking for performance.

        Raises:
            Any exception from the background checkpoint processing will propagate
            through the ThreadPoolExecutor to the main thread, terminating the Lambda.

        Examples:
            # Synchronous checkpoint (default, safe)
            execution_state.create_checkpoint(operation_update)

            # Explicit synchronous checkpoint
            execution_state.create_checkpoint(operation_update, is_sync=True)

            # Asynchronous checkpoint (opt-in for performance)
            execution_state.create_checkpoint(operation_update, is_sync=False)

            # Empty checkpoint (sync by default)
            execution_state.create_checkpoint()

            # Empty checkpoint (async for performance)
            execution_state.create_checkpoint(is_sync=False)
        """
        # if this is CONTEXT complete, mark incomplete descendants as orphans so the children can't complete after the parent
        if operation_update is not None:
            # Use single lock to coordinate completion and checkpoint validation
            with self._parent_done_lock:
                # Build parent-to-children map as operations are created
                if operation_update.parent_id:
                    if operation_update.parent_id not in self._parent_to_children:
                        self._parent_to_children[operation_update.parent_id] = set()
                    self._parent_to_children[operation_update.parent_id].add(
                        operation_update.operation_id
                    )

                # Handle CONTEXT completion - mark descendants while holding lock
                if (
                    operation_update.operation_type == OperationType.CONTEXT
                    and operation_update.action
                    in {OperationAction.SUCCEED, OperationAction.FAIL}
                ):
                    self._mark_orphans(operation_update.operation_id)

                # Check if this operation's parent is done
                if operation_update.operation_id in self._parent_done:
                    logger.debug(
                        "Rejecting checkpoint for operation %s - parent is done",
                        operation_update.operation_id,
                    )
                    return

        # Check if background checkpointing has failed
        if self._checkpointing_failed.is_set():
            # This will raise the stored BackgroundThreadError
            self._checkpointing_failed.wait()

        # Conditionally create completion event based on is_sync parameter
        completion_event: CompletionEvent | None = (
            CompletionEvent() if is_sync else None
        )

        # Create wrapper object for queue
        queued_op = QueuedOperation(operation_update, completion_event)

        # Enqueue the wrapper object (operation_update can be None for empty checkpoints)
        self._checkpoint_queue.put(queued_op)

        # Conditionally wait for completion based on is_sync parameter
        if is_sync:
            logger.debug("Enqueued checkpoint operation for synchronous processing")
            if completion_event is None:  # pragma: no cover
                # this shouldn't ever be possible
                msg: str = "completion_event must be set for synchronous execution"
                raise DurableExecutionsError(msg)

            # Wait for completion - will raise BackgroundThreadError if background thread fails
            completion_event.wait()
        else:
            logger.debug("Enqueued checkpoint operation for asynchronous processing")

    def create_checkpoint_sync(
        self,
        operation_update: OperationUpdate | None = None,
    ) -> None:
        """Create a synchronous checkpoint that raises original errors instead of BackgroundThreadError.

        This method is identical to create_checkpoint(is_sync=True) except that if the background
        checkpoint processing fails, it raises the original exception directly instead of wrapping
        it in a BackgroundThreadError.

        This is useful in execution contexts where you want the original checkpoint error to
        propagate (e.g., CheckpointError, RuntimeError) rather than the wrapped BackgroundThreadError.
        The method always blocks until the checkpoint is processed.

        Args:
            operation_update: The checkpoint to create. If None, creates an empty checkpoint.

        Raises:
            The original exception from the background checkpoint processing if it fails,
            unwrapped from BackgroundThreadError (e.g., CheckpointError, RuntimeError).

        Example:
            # Instead of getting BackgroundThreadError wrapping a CheckpointError:
            execution_state.create_checkpoint_sync(operation_update)
            # Raises CheckpointError directly
        """
        try:
            self.create_checkpoint(operation_update, is_sync=True)
        except BackgroundThreadError as bg_error:
            # Background checkpoint system failed - unwrap the original error
            logger.exception("Checkpoint processing failed - unwrapping original error")
            self.stop_checkpointing()
            # Raise the original exception unwrapped
            raise bg_error.source_exception from bg_error

    def _mark_orphans(self, context_id: str) -> None:
        """Mark all descendants (direct and transitive) as orphaned.

        This method uses BFS (Breadth-First Search) to recursively collect all
        descendants of the given context operation and marks them as orphaned.
        Once marked, these operations will be rejected if they attempt to checkpoint.

        Must be called while holding _parent_done_lock.

        Args:
            context_id: The operation ID of the CONTEXT that has completed
        """
        # Collect all descendants recursively using BFS
        all_descendants = set()
        # Start with root
        to_process: set[str] = {context_id}

        while to_process:
            current_id = to_process.pop()

            # Skip if already processed (avoid cycles, though shouldn't happen)
            if current_id in all_descendants:
                continue

            all_descendants.add(current_id)

            # Add all direct children to processing queue
            direct_children = self._parent_to_children.get(current_id, set())
            to_process.update(direct_children)

        # Remove the root itself (we only want descendants)
        all_descendants.discard(context_id)

        # Mark all descendants as orphaned
        self._parent_done.update(all_descendants)
        logger.debug(
            "Marked %d descendants as parent-done for context %s",
            len(all_descendants),
            context_id,
        )

    def checkpoint_batches_forever(self) -> None:
        """Single background thread that batches operations and processes results.

        Runs until shutdown is signaled. This method processes checkpoint operations
        in batches, makes API calls to persist them, and updates the execution state
        with the results.

        The method maintains the checkpoint token locally and updates it after each
        successful batch processing. It continues running until stop_checkpointing()
        is called.

        Note: When shutdown is signaled, only non-essential async checkpoints may remain
        in the queue. All critical synchronous checkpoints (SUCCEED, FAIL, etc.) will
        have already completed because the main thread blocks on them. Therefore, we
        don't need to drain the queue - the Lambda timeout will handle cleanup.

        Raises:
            Any exception from the service client checkpoint call will propagate naturally,
            terminating the background thread and signaling an error to the main thread.
        """
        # Keep checkpoint token as local variable in the loop
        current_checkpoint_token: str = self._current_checkpoint_token

        while not self._checkpointing_stopped.is_set():
            # Collect operations into a batch
            batch: list[QueuedOperation] = self._collect_checkpoint_batch()

            if batch:
                # Extract OperationUpdates from QueuedOperations for API call
                updates: list[OperationUpdate] = [
                    q.operation_update for q in batch if q.operation_update is not None
                ]

                logger.debug(
                    "Processing checkpoint batch with %d operations (%d non-empty)",
                    len(batch),
                    len(updates),
                )

                try:
                    # Make API call with batched operations
                    output: CheckpointOutput = self._service_client.checkpoint(
                        durable_execution_arn=self.durable_execution_arn,
                        checkpoint_token=current_checkpoint_token,
                        updates=updates,
                        client_token=None,
                    )

                    logger.debug("Checkpoint batch processed successfully")

                    # Signal completion for any synchronous operations
                    for queued_op in batch:
                        if queued_op.completion_event is not None:
                            queued_op.completion_event.set()

                    # Update local token for next iteration
                    current_checkpoint_token = output.checkpoint_token

                    # Fetch new operations from the API
                    self.fetch_paginated_operations(
                        output.new_execution_state.operations,
                        output.checkpoint_token,
                        output.new_execution_state.next_marker,
                    )
                except Exception as e:
                    # Checkpoint failed - wake all blocked threads so they can raise error
                    # Drain both queues and signal all completion events
                    logger.exception("Checkpoint batch processing failed")
                    bg_error: BackgroundThreadError = BackgroundThreadError(
                        "Checkpoint creation failed", e
                    )

                    # FIFO: although at this point order not really import any anymore
                    # Signal completion events for the failed batch
                    for queued_op in batch:
                        if queued_op.completion_event is not None:
                            queued_op.completion_event.set(bg_error)

                    # overflow 1st: although at this point order not really import any anymore
                    while not self._overflow_queue.empty():
                        try:
                            item = self._overflow_queue.get_nowait()
                            if item.completion_event:
                                item.completion_event.set(bg_error)
                        except queue.Empty:
                            break

                    # finally Wake all blocked threads in main queue
                    while not self._checkpoint_queue.empty():
                        try:
                            item = self._checkpoint_queue.get_nowait()
                            if item.completion_event:
                                item.completion_event.set(bg_error)
                        except queue.Empty:
                            break

                    # Set the failure event so future checkpoint attempts fail immediately
                    self._checkpointing_failed.set(bg_error)

                    # Exit the loop - error has been signaled to main thread via completion events
                    break

        logger.debug("Background checkpoint processing stopped")

    def stop_checkpointing(self) -> None:
        """Signal background thread to stop checkpointing.

        This method sets the checkpointing stopped event, which signals the background
        thread to exit. Any remaining async checkpoints in the queue are non-essential
        (observability only) and will be abandoned. All critical synchronous checkpoints
        will have already completed before this is called.
        """
        logger.debug("Signaling background thread to stop checkpointing")
        self._checkpointing_stopped.set()

    def _collect_checkpoint_batch(self) -> list[QueuedOperation]:
        """Collect multiple checkpoint operations into a batch for API efficiency.

        Processes overflow queue first to maintain FIFO order, then collects from main queue.
        Respects configured size, time, and operation count limits. Blocks for the first
        operation if queues are empty, then collects additional operations within the time
        window.

        Returns:
            List of QueuedOperation objects ready for batch processing. Returns empty list
            if no operations are available.
        """
        batch: list[QueuedOperation] = []
        total_size = 0

        # First, drain overflow queue (FIFO order preserved)
        try:
            while len(batch) < self._batcher_config.max_batch_operations:
                overflow_op = self._overflow_queue.get_nowait()
                op_size = self._calculate_operation_size(overflow_op)

                if total_size + op_size > self._batcher_config.max_batch_size_bytes:
                    # Put back and stop
                    self._overflow_queue.put(overflow_op)
                    break

                batch.append(overflow_op)
                total_size += op_size
        except queue.Empty:
            pass

        # If batch is empty, get first operation from main queue
        if not batch:
            # Block for first operation, checking stop signal periodically
            while not self._checkpointing_stopped.is_set():
                try:
                    first_op = self._checkpoint_queue.get(
                        timeout=0.1
                    )  # Check stop signal every 100ms
                    self._checkpoint_queue.task_done()
                    batch.append(first_op)
                    total_size += self._calculate_operation_size(first_op)
                    break
                except queue.Empty:
                    continue

            # If stopped and no operation retrieved, return empty batch
            if not batch:
                return batch

        # Start batching window using configured time
        batch_deadline = time.time() + self._batcher_config.max_batch_time_seconds

        # Collect additional operations within the time window
        while (
            time.time() < batch_deadline
            and len(batch) < self._batcher_config.max_batch_operations
            and not self._checkpointing_stopped.is_set()
        ):
            remaining_time = min(
                batch_deadline - time.time(),
                0.1,  # Check stop signal every 100ms
            )

            if remaining_time <= 0:
                break

            try:
                additional_op = self._checkpoint_queue.get(timeout=remaining_time)
                self._checkpoint_queue.task_done()
                op_size = self._calculate_operation_size(additional_op)

                # Check if adding this operation would exceed size limit
                if total_size + op_size > self._batcher_config.max_batch_size_bytes:
                    # Put in overflow queue for next batch
                    self._overflow_queue.put(additional_op)
                    logger.debug(
                        "Batch size limit reached, moving operation to overflow queue"
                    )
                    break

                batch.append(additional_op)
                total_size += op_size

            except queue.Empty:
                break

        logger.debug(
            "Collected batch of %d operations, total size: %d bytes",
            len(batch),
            total_size,
        )
        return batch

    @staticmethod
    def _calculate_operation_size(queued_op: QueuedOperation) -> int:
        """Calculate the serialized size of a queued operation for batching limits.

        Uses JSON serialization to estimate the size of the operation update. Empty
        checkpoints (None operation_update) have zero size.

        Args:
            queued_op: The queued operation to calculate size for

        Returns:
            Size in bytes of the serialized operation, or 0 for empty checkpoints
        """
        # Empty checkpoints have no size
        if queued_op.operation_update is None:
            return 0

        # Use JSON serialization to estimate size
        serialized = json.dumps(queued_op.operation_update.to_dict()).encode("utf-8")
        return len(serialized)
