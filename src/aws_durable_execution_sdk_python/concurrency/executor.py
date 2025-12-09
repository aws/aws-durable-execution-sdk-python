"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import heapq
import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Generic, Self, TypeVar

from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    BranchStatus,
    Executable,
    ExecutableWithState,
    ExecutionCounters,
    SuspendResult,
)
from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import (
    OrphanedChildException,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.child import child_handler

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

        thread_executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            with TimerScheduler(resubmitter) as scheduler:

                def submit_task(executable_with_state: ExecutableWithState) -> Future:
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
                    return future

                # Submit initial tasks
                futures = [
                    submit_task(exe_state) for exe_state in self.executables_with_state
                ]

                # Wait for completion
                self._completion_event.wait()

                # Cancel futures that haven't started yet
                for future in futures:
                    future.cancel()

                # Suspend execution if everything done and at least one of the tasks raised a suspend exception.
                if self._suspend_exception:
                    raise self._suspend_exception

        finally:
            # Shutdown without waiting for running threads for early return when
            # completion criteria are met (e.g., min_successful).
            # Running threads will continue in background but they raise OrphanedChildException
            # on the next attempt to checkpoint.
            thread_executor.shutdown(wait=False, cancel_futures=True)

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

        if future.cancelled():
            exe_state.suspend()
            return

        try:
            result = future.result()
            exe_state.complete(result)
            self.counters.complete_task()
        except OrphanedChildException:
            # Parent already completed and returned.
            # State is already RUNNING, which _create_result() marked as STARTED
            # Just log and exit - no state change needed
            logger.debug(
                "Terminating orphaned branch %s without error because parent has completed already",
                exe_state.index,
            )
            return
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

        result: ResultType = child_handler(
            run_in_child_handler,
            child_context.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                serdes=self.item_serdes or self.serdes,
                sub_type=self.sub_type_iteration,
                summary_generator=self.summary_generator,
            ),
        )
        child_context.state.track_replay(operation_id=operation_id)
        return result

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
