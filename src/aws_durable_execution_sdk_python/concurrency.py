"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import heapq
import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Generic, Self, TypeVar

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import (
    InvalidStateError,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.types import BatchResult as BatchResultProtocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.config import CompletionConfig
    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import ExecutionState
    from aws_durable_execution_sdk_python.types import DurableContext


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
    def from_dict(cls, data: dict) -> BatchResult[R]:
        batch_items: list[BatchItem[R]] = [
            BatchItem.from_dict(item) for item in data["all"]
        ]

        completion_reason_value = data.get("completionReason")
        if completion_reason_value is None:
            # Mix of success and failure, but all completed
            # By default TS when not given assumes all_completed
            # assignment stacktrace
            # 3. https://github.com/aws/aws-durable-execution-sdk-js/blob/ff8b72ef888dd47a840f36d4eb0ee84dd3b55a30/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/batch-result.ts#L34-L41
            # 2. https://github.com/aws/aws-durable-execution-sdk-js/blob/ff8b72ef888dd47a840f36d4eb0ee84dd3b55a30/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/batch-result.ts#L146-L149
            # 1. https://github.com/aws/aws-durable-execution-sdk-js/blob/ff8b72ef888dd47a840f36d4eb0ee84dd3b55a30/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/batch-result.ts#L127
            # 0. https://github.com/aws/aws-durable-execution-sdk-js/blob/ff8b72ef888dd47a840f36d4eb0ee84dd3b55a30/packages/aws-durable-execution-sdk-js/src/handlers/concurrent-execution-handler/concurrent-execution-handler.ts#L518-L528
            # When deserializing objects, we branch on 0, where we assume object has the field if it was not serialized
            # properly.
            completion_reason = cls._infer_completion_reason(batch_items)
            logger.warning(
                "Missing completionReason in BatchResult deserialization, "
                "inferred '%s' from batch item statuses. "
                "This may indicate incomplete serialization data.",
                completion_reason.value,
            )
        else:
            completion_reason = CompletionReason(completion_reason_value)

        return cls(batch_items, completion_reason)

    @classmethod
    def _infer_completion_reason(
        cls, batch_items: list[BatchItem[R]]
    ) -> CompletionReason:
        """Infer completion reason based on batch item statuses."""
        if not batch_items:
            return CompletionReason.ALL_COMPLETED

        succeeded_count = sum(
            1 for item in batch_items if item.status == BatchItemStatus.SUCCEEDED
        )
        failed_count = sum(
            1 for item in batch_items if item.status == BatchItemStatus.FAILED
        )
        started_count = sum(
            1 for item in batch_items if item.status == BatchItemStatus.STARTED
        )

        # If there are started items, execution was terminated early
        if started_count > 0:
            return CompletionReason.MIN_SUCCESSFUL_REACHED

        # All items completed (succeeded or failed)
        if failed_count == 0:
            # All succeeded
            return CompletionReason.ALL_COMPLETED
        if succeeded_count == 0:
            # All failed - likely hit failure tolerance
            return CompletionReason.FAILURE_TOLERANCE_EXCEEDED

        return CompletionReason.ALL_COMPLETED

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

    def should_complete(self) -> bool:
        """Check if execution should complete."""
        with self._lock:
            # Success condition
            if self.success_count >= self.min_successful:
                return True

            # Failure conditions
            if self._is_failure_condition_reached(
                tolerated_count=self.tolerated_failure_count,
                tolerated_percentage=self.tolerated_failure_percentage,
                failure_count=self.failure_count,
            ):
                return True

            # Impossible to succeed condition
            # TODO: should this keep running? TS doesn't currently handle this either.
            remaining_tasks = self.total_tasks - self.success_count - self.failure_count
            return self.success_count + remaining_tasks < self.min_successful

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
    ):
        self.executables = executables
        self.max_concurrency = max_concurrency
        self.completion_config = completion_config
        self.sub_type_top = sub_type_top
        self.sub_type_iteration = sub_type_iteration
        self.name_prefix = name_prefix

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

    @abstractmethod
    def execute_item(
        self, child_context: DurableContext, executable: Executable[CallableType]
    ) -> ResultType:
        """Execute a single executable in a child context and return the result."""
        raise NotImplementedError

    def execute(
        self,
        execution_state: ExecutionState,
        run_in_child_context: Callable[
            [Callable[[DurableContext], ResultType], str | None, ChildConfig | None],
            ResultType,
        ],
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
                    run_in_child_context,
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
        """Build the final BatchResult."""
        batch_items: list[BatchItem[ResultType]] = []
        completed_branches: list[ExecutableWithState] = []
        failed_branches: list[ExecutableWithState] = []

        for executable in self.executables_with_state:
            if executable.status is BranchStatus.COMPLETED:
                completed_branches.append(executable)
                batch_items.append(
                    BatchItem(
                        executable.index, BatchItemStatus.SUCCEEDED, executable.result
                    )
                )
            elif executable.status is BranchStatus.FAILED:
                failed_branches.append(executable)
                batch_items.append(
                    BatchItem(
                        executable.index,
                        BatchItemStatus.FAILED,
                        error=ErrorObject.from_exception(executable.error),
                    )
                )

        completion_reason: CompletionReason = (
            CompletionReason.ALL_COMPLETED
            if self.counters.is_all_completed()
            else (
                CompletionReason.MIN_SUCCESSFUL_REACHED
                if self.counters.is_min_successful_reached()
                else CompletionReason.FAILURE_TOLERANCE_EXCEEDED
            )
        )

        return BatchResult(batch_items, completion_reason)

    def _execute_item_in_child_context(
        self,
        run_in_child_context: Callable[
            [Callable[[DurableContext], ResultType], str | None, ChildConfig | None],
            ResultType,
        ],
        executable: Executable[CallableType],
    ) -> ResultType:
        """Execute a single item in a child context."""

        def execute_in_child_context(child_context: DurableContext) -> ResultType:
            return self.execute_item(child_context, executable)

        return run_in_child_context(
            execute_in_child_context,
            f"{self.name_prefix}{executable.index}",
            ChildConfig(serdes=self.serdes, sub_type=self.sub_type_iteration),
        )


# endregion concurrency logic
