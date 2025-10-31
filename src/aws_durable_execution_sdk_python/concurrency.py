"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import heapq
import threading
import time
from abc import ABC, abstractmethod
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Generic, Self, TypeVar

import aws_durable_execution_sdk_python.logger as logging
from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import (
    InvalidStateError,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.child import child_handler
from aws_durable_execution_sdk_python.types import BatchResult as BatchResultProtocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.config import CompletionConfig
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import ExecutionState
    from aws_durable_execution_sdk_python.types import SummaryGenerator


logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

CallableType = TypeVar("CallableType")
ResultType = TypeVar("ResultType")


# region Result models
class BatchItemStatus(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class CompletionReason(Enum):
    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"


@dataclass(frozen=True)
class SuspendResult:
    should_suspend: bool
    exception: SuspendExecution | None = None

    @staticmethod
    def do_not_suspend() -> SuspendResult:
        return SuspendResult(should_suspend=False)

    @staticmethod
    def suspend(exception: SuspendExecution) -> SuspendResult:
        return SuspendResult(should_suspend=True, exception=exception)


@dataclass(frozen=True)
class BatchItem(Generic[R]):
    index: int
    status: BatchItemStatus
    result: R | None = None
    error: ErrorObject | None = None

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "status": self.status.value,
            "result": self.result,
            "error": self.error.to_dict() if self.error else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchItem[R]:
        return cls(
            index=data["index"],
            status=BatchItemStatus(data["status"]),
            result=data.get("result"),
            error=ErrorObject.from_dict(data["error"]) if data.get("error") else None,
        )


@dataclass(frozen=True)
class BatchResult(Generic[R], BatchResultProtocol[R]):  # noqa: PYI059
    all: list[BatchItem[R]]
    completion_reason: CompletionReason

    @classmethod
    def from_dict(
        cls, data: dict, completion_config: CompletionConfig | None = None
    ) -> BatchResult[R]:
        batch_items: list[BatchItem[R]] = [
            BatchItem.from_dict(item) for item in data["all"]
        ]

        completion_reason_value = data.get("completionReason")
        if completion_reason_value is None:
            # Infer completion reason from batch item statuses and completion config
            # This aligns with the TypeScript implementation that uses completion config
            # to accurately reconstruct the completion reason during replay
            result = cls.from_items(batch_items, completion_config)
            logger.warning(
                "Missing completionReason in BatchResult deserialization, "
                "inferred '%s' from batch item statuses. "
                "This may indicate incomplete serialization data.",
                result.completion_reason.value,
            )
            return result

        completion_reason = CompletionReason(completion_reason_value)
        return cls(batch_items, completion_reason)

    @classmethod
    def from_items(
        cls,
        items: list[BatchItem[R]],
        completion_config: CompletionConfig | None = None,
    ):
        """
        Infer completion reason based on batch item statuses and completion config.

        This follows the same logic as the TypeScript implementation:
        - If all items completed: ALL_COMPLETED
        - If minSuccessful threshold met and not all completed: MIN_SUCCESSFUL_REACHED
        - Otherwise: FAILURE_TOLERANCE_EXCEEDED
        """

        statuses = (item.status for item in items)
        counts = Counter(statuses)
        succeeded_count = counts.get(BatchItemStatus.SUCCEEDED, 0)
        failed_count = counts.get(BatchItemStatus.FAILED, 0)
        started_count = counts.get(BatchItemStatus.STARTED, 0)

        completed_count = succeeded_count + failed_count
        total_count = started_count + completed_count

        # If all items completed (no started items), it's ALL_COMPLETED
        if completed_count == total_count:
            completion_reason = CompletionReason.ALL_COMPLETED
        elif (  # If we have completion config and minSuccessful threshold is met
            completion_config
            and (min_successful := completion_config.min_successful) is not None
            and succeeded_count >= min_successful
        ):
            completion_reason = CompletionReason.MIN_SUCCESSFUL_REACHED
        else:
            # Otherwise, assume failure tolerance was exceeded
            completion_reason = CompletionReason.FAILURE_TOLERANCE_EXCEEDED

        return cls(items, completion_reason)

    def to_dict(self) -> dict:
        return {
            "all": [item.to_dict() for item in self.all],
            "completionReason": self.completion_reason.value,
        }

    def succeeded(self) -> list[BatchItem[R]]:
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def failed(self) -> list[BatchItem[R]]:
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    def started(self) -> list[BatchItem[R]]:
        return [item for item in self.all if item.status is BatchItemStatus.STARTED]

    @property
    def status(self) -> BatchItemStatus:
        return BatchItemStatus.FAILED if self.has_failure else BatchItemStatus.SUCCEEDED

    @property
    def has_failure(self) -> bool:
        return any(item.status is BatchItemStatus.FAILED for item in self.all)

    def throw_if_error(self) -> None:
        first_error = next(
            (item.error for item in self.all if item.status is BatchItemStatus.FAILED),
            None,
        )
        if first_error:
            raise first_error.to_callable_runtime_error()

    def get_results(self) -> list[R]:
        return [
            item.result
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def get_errors(self) -> list[ErrorObject]:
        return [
            item.error
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    @property
    def success_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.SUCCEEDED)

    @property
    def failure_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.FAILED)

    @property
    def started_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.STARTED)

    @property
    def total_count(self) -> int:
        return len(self.all)


# endregion Result models


# region concurrency models
@dataclass(frozen=True)
class Executable(Generic[CallableType]):
    index: int
    func: CallableType


class BranchStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    SUSPENDED_WITH_TIMEOUT = "suspended_with_timeout"
    FAILED = "failed"


class ExecutableWithState(Generic[CallableType, ResultType]):
    """Manages the execution state and lifecycle of an executable."""

    def __init__(self, executable: Executable[CallableType]):
        self.executable = executable
        self._status = BranchStatus.PENDING
        self._future: Future | None = None
        self._suspend_until: float | None = None
        self._result: ResultType = None  # type: ignore[assignment]
        self._is_result_set: bool = False
        self._error: Exception | None = None

    @property
    def future(self) -> Future:
        """Get the future, raising error if not available."""
        if self._future is None:
            msg = f"ExecutableWithState was never started. {self.executable.index}"
            raise InvalidStateError(msg)
        return self._future

    @property
    def status(self) -> BranchStatus:
        """Get current status."""
        return self._status

    @property
    def result(self) -> ResultType:
        """Get result if completed."""
        if not self._is_result_set or self._status != BranchStatus.COMPLETED:
            msg = f"result not available in status {self._status}"
            raise InvalidStateError(msg)
        return self._result

    @property
    def error(self) -> Exception:
        """Get error if failed."""
        if self._error is None or self._status != BranchStatus.FAILED:
            msg = f"error not available in status {self._status}"
            raise InvalidStateError(msg)
        return self._error

    @property
    def suspend_until(self) -> float | None:
        """Get suspend timestamp."""
        return self._suspend_until

    @property
    def is_running(self) -> bool:
        """Check if currently running."""
        return self._status is BranchStatus.RUNNING

    @property
    def can_resume(self) -> bool:
        """Check if can resume from suspension."""
        return self._status is BranchStatus.SUSPENDED or (
            self._status is BranchStatus.SUSPENDED_WITH_TIMEOUT
            and self._suspend_until is not None
            and time.time() >= self._suspend_until
        )

    @property
    def index(self) -> int:
        return self.executable.index

    @property
    def callable(self) -> CallableType:
        return self.executable.func

    # region State transitions
    def run(self, future: Future) -> None:
        """Transition to RUNNING state with a future."""
        if self._status != BranchStatus.PENDING:
            msg = f"Cannot start running from {self._status}"
            raise InvalidStateError(msg)
        self._status = BranchStatus.RUNNING
        self._future = future

    def suspend(self) -> None:
        """Transition to SUSPENDED state (indefinite)."""
        self._status = BranchStatus.SUSPENDED
        self._suspend_until = None

    def suspend_with_timeout(self, timestamp: float) -> None:
        """Transition to SUSPENDED_WITH_TIMEOUT state."""
        self._status = BranchStatus.SUSPENDED_WITH_TIMEOUT
        self._suspend_until = timestamp

    def complete(self, result: ResultType) -> None:
        """Transition to COMPLETED state."""
        self._status = BranchStatus.COMPLETED
        self._result = result
        self._is_result_set = True

    def fail(self, error: Exception) -> None:
        """Transition to FAILED state."""
        self._status = BranchStatus.FAILED
        self._error = error

    def reset_to_pending(self) -> None:
        """Reset to PENDING state for resubmission."""
        self._status = BranchStatus.PENDING
        self._future = None
        self._suspend_until = None

    # endregion State transitions


class ExecutionCounters:
    """Thread-safe counters for tracking execution state."""

    def __init__(
        self,
        total_tasks: int,
        min_successful: int,
        tolerated_failure_count: int | None,
        tolerated_failure_percentage: float | None,
    ):
        self.total_tasks: int = total_tasks
        self.min_successful: int = min_successful
        self.tolerated_failure_count: int | None = tolerated_failure_count
        self.tolerated_failure_percentage: float | None = tolerated_failure_percentage
        self.success_count: int = 0
        self.failure_count: int = 0
        self._lock = threading.Lock()

    def complete_task(self) -> None:
        """Task completed successfully."""
        with self._lock:
            self.success_count += 1

    def fail_task(self) -> None:
        """Task failed."""
        with self._lock:
            self.failure_count += 1

    def should_continue(self) -> bool:
        """
        Check if we should continue starting new tasks (based on failure tolerance).
        Matches TypeScript shouldContinue() logic.
        """
        with self._lock:
            # If no completion config, only continue if no failures
            if (
                self.tolerated_failure_count is None
                and self.tolerated_failure_percentage is None
            ):
                return self.failure_count == 0

            # Check failure count tolerance
            if (
                self.tolerated_failure_count is not None
                and self.failure_count > self.tolerated_failure_count
            ):
                return False

            # Check failure percentage tolerance
            if self.tolerated_failure_percentage is not None and self.total_tasks > 0:
                failure_percentage = (self.failure_count / self.total_tasks) * 100
                if failure_percentage > self.tolerated_failure_percentage:
                    return False

            return True

    def is_complete(self) -> bool:
        """
        Check if execution should complete (based on completion criteria).
        Matches TypeScript isComplete() logic.
        """
        with self._lock:
            completed_count = self.success_count + self.failure_count

            # All tasks completed
            if completed_count == self.total_tasks:
                # Complete if no failure tolerance OR no failures OR min successful reached
                return (
                    (
                        self.tolerated_failure_count is None
                        and self.tolerated_failure_percentage is None
                    )
                    or self.failure_count == 0
                    or self.success_count >= self.min_successful
                )

            # when we breach min successful, we've completed
            return self.success_count >= self.min_successful

    def should_complete(self) -> bool:
        """
        Check if execution should complete.
        Combines TypeScript shouldContinue() and isComplete() logic.
        """
        return self.is_complete() or not self.should_continue()

    def is_all_completed(self) -> bool:
        """True if all tasks completed successfully."""
        with self._lock:
            return self.success_count == self.total_tasks

    def is_min_successful_reached(self) -> bool:
        """True if minimum successful tasks reached."""
        with self._lock:
            return self.success_count >= self.min_successful

    def is_failure_tolerance_exceeded(self) -> bool:
        """True if failure tolerance was exceeded."""
        with self._lock:
            return self._is_failure_condition_reached(
                tolerated_count=self.tolerated_failure_count,
                tolerated_percentage=self.tolerated_failure_percentage,
                failure_count=self.failure_count,
            )

    def _is_failure_condition_reached(
        self,
        tolerated_count: int | None,
        tolerated_percentage: float | None,
        failure_count: int,
    ) -> bool:
        """True if failure conditions are reached (no locking - caller must lock)."""
        # Failure count condition
        if tolerated_count is not None and failure_count > tolerated_count:
            return True

        # Failure percentage condition
        if tolerated_percentage is not None and self.total_tasks > 0:
            failure_percentage = (failure_count / self.total_tasks) * 100
            if failure_percentage > tolerated_percentage:
                return True

        return False


# endegion concurrency models


# region concurrency logic
class TimerScheduler:
    """Manage timed suspend tasks with a background timer thread."""

    def __init__(
        self, resubmit_callback: Callable[[ExecutableWithState], None]
    ) -> None:
        self.resubmit_callback = resubmit_callback
        self._pending_resumes: list[tuple[float, ExecutableWithState]] = []
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer_thread.start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()

    def schedule_resume(
        self, exe_state: ExecutableWithState, resume_time: float
    ) -> None:
        """Schedule a task to resume at the specified time."""
        with self._lock:
            heapq.heappush(self._pending_resumes, (resume_time, exe_state))

    def shutdown(self) -> None:
        """Shutdown the timer thread and cancel all pending resumes."""
        self._shutdown.set()
        self._timer_thread.join(timeout=1.0)
        with self._lock:
            self._pending_resumes.clear()

    def _timer_loop(self) -> None:
        """Background thread that processes timed resumes."""
        while not self._shutdown.is_set():
            next_resume_time = None

            with self._lock:
                if self._pending_resumes:
                    next_resume_time = self._pending_resumes[0][0]

            if next_resume_time is None:
                # No pending resumes, wait a bit and check again
                self._shutdown.wait(timeout=0.1)
                continue

            current_time = time.time()
            if current_time >= next_resume_time:
                # Time to resume
                with self._lock:
                    # no branch cover because hard to test reliably - this is a double-safety check if heap mutated
                    # since the first peek on next_resume_time further up
                    if (  # pragma: no branch
                        self._pending_resumes
                        and self._pending_resumes[0][0] <= current_time
                    ):
                        _, exe_state = heapq.heappop(self._pending_resumes)
                        if exe_state.can_resume:
                            exe_state.reset_to_pending()
                            self.resubmit_callback(exe_state)
            else:
                # Wait until next resume time
                wait_time = min(next_resume_time - current_time, 0.1)
                self._shutdown.wait(timeout=wait_time)


class ConcurrentExecutor(ABC, Generic[CallableType, ResultType]):
    """Execute durable operations concurrently. This contains the execution logic for Map and Parallel."""

    def __init__(
        self,
        executables: list[Executable[CallableType]],
        max_concurrency: int | None,
        completion_config: CompletionConfig,
        sub_type_top: OperationSubType,
        sub_type_iteration: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        item_serdes: SerDes | None = None,
        summary_generator: SummaryGenerator | None = None,
    ):
        """Initialize ConcurrentExecutor.

        Args:
            summary_generator: Optional function to generate compact summaries for large results.
                When the serialized result exceeds 256KB, this generator creates a JSON summary
                instead of checkpointing the full result. Used by map/parallel operations to
                handle large BatchResult payloads efficiently. Matches TypeScript behavior in
                run-in-child-context-handler.ts.
        """
        self.executables = executables
        self.max_concurrency = max_concurrency
        self.completion_config = completion_config
        self.sub_type_top = sub_type_top
        self.sub_type_iteration = sub_type_iteration
        self.name_prefix = name_prefix
        self.summary_generator = summary_generator

        # Event-driven state tracking for when the executor is done
        self._completion_event = threading.Event()
        self._suspend_exception: SuspendExecution | None = None

        # ExecutionCounters will keep track of completion criteria and on-going counters
        min_successful = self.completion_config.min_successful or len(self.executables)
        tolerated_failure_count = self.completion_config.tolerated_failure_count
        tolerated_failure_percentage = (
            self.completion_config.tolerated_failure_percentage
        )

        self.counters: ExecutionCounters = ExecutionCounters(
            len(executables),
            min_successful,
            tolerated_failure_count,
            tolerated_failure_percentage,
        )
        self.executables_with_state: list[ExecutableWithState] = []
        self.serdes = serdes
        self.item_serdes = item_serdes

    @abstractmethod
    def execute_item(
        self, child_context: DurableContext, executable: Executable[CallableType]
    ) -> ResultType:
        """Execute a single executable in a child context and return the result."""
        raise NotImplementedError

    def execute(
        self, execution_state: ExecutionState, executor_context: DurableContext
    ) -> BatchResult[ResultType]:
        """Execute items concurrently with event-driven state management."""
        logger.debug(
            "▶️ Executing concurrent operation, items: %d", len(self.executables)
        )

        max_workers = self.max_concurrency or len(self.executables)

        self.executables_with_state = [
            ExecutableWithState(executable=exe) for exe in self.executables
        ]
        self._completion_event.clear()
        self._suspend_exception = None

        def resubmitter(executable_with_state: ExecutableWithState) -> None:
            """Resubmit a timed suspended task."""
            execution_state.create_checkpoint()
            submit_task(executable_with_state)

        with (
            TimerScheduler(resubmitter) as scheduler,
            ThreadPoolExecutor(max_workers=max_workers) as thread_executor,
        ):

            def submit_task(executable_with_state: ExecutableWithState) -> None:
                """Submit task to the thread executor and mark its state as started."""
                future = thread_executor.submit(
                    self._execute_item_in_child_context,
                    executor_context,
                    executable_with_state.executable,
                )
                executable_with_state.run(future)

                def on_done(future: Future) -> None:
                    self._on_task_complete(executable_with_state, future, scheduler)

                future.add_done_callback(on_done)

            # Submit initial tasks
            for exe_state in self.executables_with_state:
                submit_task(exe_state)

            # Wait for completion
            self._completion_event.wait()

            # Suspend execution if everything done and at least one of the tasks raised a suspend exception.
            if self._suspend_exception:
                raise self._suspend_exception

        # Build final result
        return self._create_result()

    def should_execution_suspend(self) -> SuspendResult:
        """Check if execution should suspend."""
        earliest_timestamp: float = float("inf")
        indefinite_suspend_task: (
            ExecutableWithState[CallableType, ResultType] | None
        ) = None

        for exe_state in self.executables_with_state:
            if exe_state.status in {BranchStatus.PENDING, BranchStatus.RUNNING}:
                # Exit here! Still have tasks that can make progress, don't suspend.
                return SuspendResult.do_not_suspend()
            if exe_state.status is BranchStatus.SUSPENDED_WITH_TIMEOUT:
                if (
                    exe_state.suspend_until
                    and exe_state.suspend_until < earliest_timestamp
                ):
                    earliest_timestamp = exe_state.suspend_until
            elif exe_state.status is BranchStatus.SUSPENDED:
                indefinite_suspend_task = exe_state

        # All tasks are in final states and at least one of them is a suspend.
        if earliest_timestamp != float("inf"):
            return SuspendResult.suspend(
                TimedSuspendExecution(
                    "All concurrent work complete or suspended pending retry.",
                    earliest_timestamp,
                )
            )
        if indefinite_suspend_task:
            return SuspendResult.suspend(
                SuspendExecution(
                    "All concurrent work complete or suspended and pending external callback."
                )
            )

        return SuspendResult.do_not_suspend()

    def _on_task_complete(
        self,
        exe_state: ExecutableWithState,
        future: Future,
        scheduler: TimerScheduler,
    ) -> None:
        """Handle task completion, suspension, or failure."""
        try:
            result = future.result()
            exe_state.complete(result)
            self.counters.complete_task()
        except TimedSuspendExecution as tse:
            exe_state.suspend_with_timeout(tse.scheduled_timestamp)
            scheduler.schedule_resume(exe_state, tse.scheduled_timestamp)
        except SuspendExecution:
            exe_state.suspend()
            # For indefinite suspend, don't schedule resume
        except Exception as e:  # noqa: BLE001
            exe_state.fail(e)
            self.counters.fail_task()

        # Check if execution should complete or suspend
        if self.counters.should_complete():
            self._completion_event.set()
        else:
            suspend_result = self.should_execution_suspend()
            if suspend_result.should_suspend:
                self._suspend_exception = suspend_result.exception
                self._completion_event.set()

    def _create_result(self) -> BatchResult[ResultType]:
        """
        Build the final BatchResult.

        When this function executes, we've terminated the upper/parent context for whatever reason.
        It follows that our items can be only in 3 states, Completed, Failed and Started (in all of the possible forms).
        We tag each branch based on its observed value at the time of completion of the parent / upper context, and pass the
        results to BatchResult.

        Any inference wrt completion reason is left up to BatchResult, keeping the logic inference isolated.
        """
        batch_items: list[BatchItem[ResultType]] = []
        for executable in self.executables_with_state:
            match executable.status:
                case BranchStatus.COMPLETED:
                    batch_items.append(
                        BatchItem(
                            executable.index,
                            BatchItemStatus.SUCCEEDED,
                            executable.result,
                        )
                    )
                case BranchStatus.FAILED:
                    batch_items.append(
                        BatchItem(
                            executable.index,
                            BatchItemStatus.FAILED,
                            error=ErrorObject.from_exception(executable.error),
                        )
                    )
                case (
                    BranchStatus.PENDING
                    | BranchStatus.RUNNING
                    | BranchStatus.SUSPENDED
                    | BranchStatus.SUSPENDED_WITH_TIMEOUT
                ):
                    batch_items.append(
                        BatchItem(executable.index, BatchItemStatus.STARTED)
                    )

        return BatchResult.from_items(batch_items, self.completion_config)

    def _execute_item_in_child_context(
        self,
        executor_context: DurableContext,
        executable: Executable[CallableType],
    ) -> ResultType:
        """
        Execute a single item in a derived child context.

        instead of relying on `executor_context.run_in_child_context`
        we generate an operation_id for the child, and then call `child_handler`
        directly. This avoids the hidden mutation of the context's internal counter.
        we can do this because we explicitly control the generation of step_id and do it
        using executable.index.


        invariant: `operation_id` for a given executable is deterministic,
            and execution order invariant.
        """

        operation_id = executor_context._create_step_id_for_logical_step(  # noqa: SLF001
            executable.index
        )
        name = f"{self.name_prefix}{executable.index}"
        child_context = executor_context.create_child_context(operation_id)
        operation_identifier = OperationIdentifier(
            operation_id,
            executor_context._parent_id,  # noqa: SLF001
            name,
        )

        def run_in_child_handler():
            return self.execute_item(child_context, executable)

        return child_handler(
            run_in_child_handler,
            child_context.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                serdes=self.item_serdes or self.serdes,
                sub_type=self.sub_type_iteration,
                summary_generator=self.summary_generator,
            ),
        )

    def replay(self, execution_state: ExecutionState, executor_context: DurableContext):
        """
        Replay rather than re-run children.

        if we are here, then we are in replay_children.
        This will pre-generate all the operation ids for the children and collect the checkpointed
        results.
        """
        items: list[BatchItem[ResultType]] = []
        for executable in self.executables:
            operation_id = executor_context._create_step_id_for_logical_step(  # noqa: SLF001
                executable.index
            )
            checkpoint = execution_state.get_checkpoint_result(operation_id)

            result: ResultType | None = None
            error = None
            status: BatchItemStatus
            if checkpoint.is_succeeded():
                status = BatchItemStatus.SUCCEEDED
                result = self._execute_item_in_child_context(
                    executor_context, executable
                )

            elif checkpoint.is_failed():
                error = checkpoint.error
                status = BatchItemStatus.FAILED
            else:
                status = BatchItemStatus.STARTED

            batch_item = BatchItem(executable.index, status, result=result, error=error)
            items.append(batch_item)
        return BatchResult.from_items(items, self.completion_config)


# endregion concurrency logic
