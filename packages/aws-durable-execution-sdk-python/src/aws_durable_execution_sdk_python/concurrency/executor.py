"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import heapq
import logging
import queue
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    Branch,
    BranchEvent,
    BranchEventKind,
    BranchStatus,
    CompletionPolicy,
    CompletionReason,
    CompletionRecord,
    Executable,
)
from aws_durable_execution_sdk_python.config import (
    ChildConfig,
    NestingType,
)
from aws_durable_execution_sdk_python.exceptions import (
    DurableOperationError,
    InvalidStateError,
    OrphanedChildException,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import (
    OperationIdentifier,
    OperationIdNamespace,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.child import child_handler


if TYPE_CHECKING:
    from collections.abc import Callable
    from aws_durable_execution_sdk_python.config import CompletionConfig
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )


logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

CallableType = TypeVar("CallableType")
ResultType = TypeVar("ResultType")


def _branch_error_object(err: Exception) -> ErrorObject:
    """Convert a failed branch's error for the batch result.

    Records the raw escaping type so live results, branch FAIL checkpoints,
    and replay reconstruction all carry the same discriminator;
    ``from_exception`` on the ChildContextError wrapper would instead
    record the wrapper class name.
    """
    if isinstance(err, DurableOperationError):
        return ErrorObject(
            message=err.message,
            type=err.error_type,
            data=err.data,
            stack_trace=err.stack_trace,
        )
    return ErrorObject.from_exception(err)


class ConcurrentExecutor(Generic[CallableType, ResultType]):
    """Execute durable operations concurrently. This contains the execution logic for Map and Parallel.

    Scheduling model: a single coordinator loop runs on the calling thread
    and owns all branch state. Worker threads run branches and report each
    outcome as a :class:`BranchEvent` on a queue; they never mutate shared
    state, so the module needs no locks.

    ``max_concurrency`` bounds in-flight branches, not threads. A branch
    that suspends (e.g. awaiting an invoke result or callback) keeps its
    concurrency slot until it reaches a terminal state. New branches start
    only when a slot frees up. When every in-flight branch is suspended and
    no slot is available, the parent suspends too: with the earliest resume
    timestamp when one exists, indefinitely otherwise.

    Branches are always started in index order and operation ids derive
    from the branch index, so scheduling is deterministic across
    invocations. On re-invocation the previously started branches are
    admitted first and replay from their checkpoints.
    """

    def __init__(
        self,
        executables: list[Executable[CallableType]],
        max_concurrency: int | None,
        completion_config: CompletionConfig,
        sub_type_top: OperationSubType,
        sub_type_iteration: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        operation_id_namespace: OperationIdNamespace,
        item_serdes: SerDes | None = None,
        nesting_type: NestingType = NestingType.NESTED,
    ):
        self.executables = executables
        self.operation_id_namespace = operation_id_namespace
        self.max_concurrency = max_concurrency
        self.completion_config = completion_config
        self.sub_type_top = sub_type_top
        self.sub_type_iteration = sub_type_iteration
        self.name_prefix = name_prefix
        self.nesting_type = nesting_type
        self.serdes = serdes
        self.item_serdes = item_serdes

        self.policy: CompletionPolicy = CompletionPolicy.from_config(
            len(executables), completion_config
        )
        self.branches: list[Branch[CallableType, ResultType]] = []

    def execute_item(  # noqa: PLR6301
        self, child_context: DurableContext, executable: Executable[CallableType]
    ) -> ResultType:
        """Execute a single executable in a child context and return the result."""
        logger.debug("▶️ Processing branch: %s", executable.index)
        func = cast("Callable[[DurableContext], ResultType]", executable.func)
        result: ResultType = func(child_context)
        logger.debug("✅ Processed branch: %s", executable.index)
        return result

    def get_iteration_name(self, index: int) -> str:
        """Get the display name for an iteration/branch at the given index.

        Returns the Executable's bound name when present, else
        "{name_prefix}{index}". An explicitly provided empty name is
        preserved.
        """
        name: str | None = self.executables[index].name
        return name if name is not None else f"{self.name_prefix}{index}"

    def execute(
        self, execution_state: ExecutionState, executor_context: DurableContext
    ) -> BatchResult[ResultType]:
        """Run the coordinator loop until the batch completes or suspends."""
        logger.debug(
            "▶️ Executing concurrent operation, items: %d", len(self.executables)
        )

        if not self.executables:
            logger.debug("No items to execute, returning empty result")
            return self._create_result()

        max_in_flight: int = self.max_concurrency or len(self.executables)
        self.branches = [Branch(executable=exe) for exe in self.executables]

        events: queue.Queue[BranchEvent[ResultType]] = queue.Queue()
        pending: deque[Branch[CallableType, ResultType]] = deque(self.branches)
        timed_resumes: list[tuple[float, int]] = []
        branch_by_index: dict[int, Branch[CallableType, ResultType]] = {
            branch.index: branch for branch in self.branches
        }

        in_flight: int = 0
        running: int = 0
        succeeded: int = 0
        failed: int = 0

        pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_in_flight)
        # Registered so ExecutionState.close() joins any branches still
        # running after early completion before the invocation returns.
        execution_state.register_branch_pool(pool)

        def submit(branch: Branch[CallableType, ResultType]) -> None:
            branch.start()
            pool.submit(
                self._branch_worker, executor_context, events, branch.executable
            )

        try:
            while True:
                if self.policy.is_complete(
                    succeeded, failed
                ) or not self.policy.should_continue(failed):
                    break

                # Start branches in index order up to the in-flight limit.
                # Suspended branches keep their slot: in_flight only
                # decreases on terminal events.
                while (
                    pending
                    and in_flight < max_in_flight
                    and self.policy.should_continue(failed)
                ):
                    submit(pending.popleft())
                    in_flight += 1
                    running += 1

                # Resume due timed suspends in-process. One checkpoint
                # refresh serves the whole due wave; a failure is terminal
                # for the execution and propagates from this thread.
                now: float = time.time()
                due: list[Branch[CallableType, ResultType]] = []
                while timed_resumes and timed_resumes[0][0] <= now:
                    _, index = heapq.heappop(timed_resumes)
                    due.append(branch_by_index[index])
                if due:
                    execution_state.create_checkpoint()
                    for branch in due:
                        submit(branch)
                        running += 1
                    continue

                if running == 0:
                    # Every in-flight branch is suspended and no slot is
                    # free (or no work remains): suspend the parent.
                    if timed_resumes:
                        raise TimedSuspendExecution(
                            "All concurrent work complete or suspended pending retry.",
                            timed_resumes[0][0],
                        )
                    raise SuspendExecution(
                        "All concurrent work complete or suspended and pending external callback."
                    )

                timeout: float | None = None
                if timed_resumes:
                    timeout = max(timed_resumes[0][0] - time.time(), 0)
                try:
                    event: BranchEvent[ResultType] = events.get(timeout=timeout)
                except queue.Empty:
                    # A timed resume came due while branches were running.
                    continue

                applied: Branch[CallableType, ResultType] = branch_by_index[event.index]
                match event.kind:
                    case BranchEventKind.COMPLETED:
                        applied.complete(event.result)
                        succeeded += 1
                        running -= 1
                        in_flight -= 1
                    case BranchEventKind.FAILED if event.error is not None:
                        applied.fail(event.error)
                        failed += 1
                        running -= 1
                        in_flight -= 1
                    case BranchEventKind.SUSPENDED:
                        applied.suspend()
                        running -= 1
                    case BranchEventKind.SUSPENDED_UNTIL if event.resume_at is not None:
                        applied.suspend_until(event.resume_at)
                        heapq.heappush(timed_resumes, (event.resume_at, event.index))
                        running -= 1
                    case BranchEventKind.ORPHANED:
                        # An ancestor context already checkpointed terminal, so
                        # every further checkpoint under it is rejected. Stop
                        # scheduling: the result of this batch is discarded
                        # upstream by the same orphan mechanism.
                        break
                    case BranchEventKind.FATAL if event.fatal_error is not None:
                        # System-level failure: propagate immediately without
                        # counting a branch failure or checkpointing further.
                        raise event.fatal_error
                    case _:
                        # A dropped event would leave the counters stale and
                        # hang the coordinator, so fail loudly instead.
                        msg = f"Unhandled branch event: {event}"
                        raise InvalidStateError(msg)
        finally:
            # Shutdown without waiting for running threads for early return
            # when completion criteria are met (e.g., min_successful).
            # Running threads continue in the background of this invocation
            # and raise OrphanedChildException on their next attempt to
            # checkpoint. ExecutionState.close() joins them before the
            # invocation returns, so no branch thread outlives the
            # invocation.
            pool.shutdown(wait=False, cancel_futures=True)

        # The decision that ended the loop determines the reason. Captured
        # before the drain so raced terminal events update item statuses
        # without flipping the reason (and the recorded summary) to a
        # decision that never fired.
        completion_reason: CompletionReason = self.policy.reason(succeeded, failed)

        # Apply terminal events that raced the completion decision, so a
        # branch that finished just before the batch completed is reported
        # with its true status instead of STARTED. Best effort: events from
        # still-running branches that arrive later are not waited for.
        while True:
            try:
                raced: BranchEvent[ResultType] = events.get_nowait()
            except queue.Empty:
                break
            raced_branch: Branch[CallableType, ResultType] = branch_by_index[
                raced.index
            ]
            if raced.kind is BranchEventKind.COMPLETED:
                raced_branch.complete(raced.result)
            elif raced.kind is BranchEventKind.FAILED and raced.error is not None:
                raced_branch.fail(raced.error)
            elif raced.kind is BranchEventKind.FATAL and raced.fatal_error is not None:
                # A straggler hit a system-level failure after the completion
                # decision. The same failure would reject the parent's own
                # checkpoint, so propagate instead of returning a result.
                raise raced.fatal_error

        return self._create_result(completion_reason)

    def _create_result(
        self, completion_reason: CompletionReason | None = None
    ) -> BatchResult[ResultType]:
        """Build the final BatchResult from branch states.

        Branches map to batch items by status: COMPLETED and FAILED map to
        their terminal statuses, anything started but not terminal maps to
        STARTED. Never-started branches (still PENDING) are omitted,
        matching the TypeScript implementation.

        The completion reason is the one captured when the completion
        decision fired. When absent it is computed from the branch states
        against the true batch total.
        """
        succeeded: int = 0
        failed: int = 0
        batch_items: list[BatchItem[ResultType]] = []
        for branch in self.branches:
            match branch.status:
                case BranchStatus.COMPLETED:
                    succeeded += 1
                    batch_items.append(
                        BatchItem(
                            branch.index,
                            BatchItemStatus.SUCCEEDED,
                            branch.result,
                        )
                    )
                case BranchStatus.FAILED if branch.error is not None:
                    failed += 1
                    batch_items.append(
                        BatchItem(
                            branch.index,
                            BatchItemStatus.FAILED,
                            error=_branch_error_object(branch.error),
                        )
                    )
                case (
                    BranchStatus.RUNNING
                    | BranchStatus.SUSPENDED
                    | BranchStatus.SUSPENDED_WITH_TIMEOUT
                ):
                    batch_items.append(BatchItem(branch.index, BatchItemStatus.STARTED))
                case BranchStatus.PENDING:
                    pass
                case _:
                    # A silently skipped branch would shrink a
                    # customer-visible result, so fail loudly instead.
                    msg = f"Branch {branch.index} in unexpected state {branch.status}"
                    raise InvalidStateError(msg)

        if completion_reason is None:
            completion_reason = self.policy.reason(succeeded, failed)
        return BatchResult(batch_items, completion_reason)

    def _branch_worker(
        self,
        executor_context: DurableContext,
        events: queue.Queue[BranchEvent[ResultType]],
        executable: Executable[CallableType],
    ) -> None:
        """Worker-thread body: run one branch and report its outcome.

        Converts every outcome into a :class:`BranchEvent` on the queue and
        never raises into the pool. The coordinator loop is the sole
        consumer of the events.
        """
        try:
            result: ResultType = self._execute_item_in_child_context(
                executor_context, executable
            )
        except TimedSuspendExecution as tse:
            events.put(
                BranchEvent.suspended_until(executable.index, tse.scheduled_timestamp)
            )
        except SuspendExecution:
            events.put(BranchEvent.suspended(executable.index))
        except OrphanedChildException:
            # Parent already completed and returned; the branch stays
            # RUNNING and is reported as STARTED.
            logger.debug(
                "Terminating orphaned branch %s without error because parent has completed already",
                executable.index,
            )
            events.put(BranchEvent.orphaned(executable.index))
        except Exception as e:  # noqa: BLE001
            events.put(BranchEvent.failed(executable.index, e))
        except BaseException as e:
            # System-level failure (background checkpoint failure, SystemExit).
            # Post a fatal event so the coordinator re-raises it on the
            # calling thread instead of blocking forever on the queue, then
            # let the exception propagate to the worker thread.
            events.put(BranchEvent.fatal(executable.index, e))
            raise
        else:
            events.put(BranchEvent.completed(executable.index, result))

    def _execute_item_in_child_context(
        self,
        executor_context: DurableContext,
        executable: Executable[CallableType],
    ) -> ResultType:
        """
        Execute a single item in a derived child context.

        Instead of relying on `executor_context.run_in_child_context` we
        generate an operation_id for the child, then call `child_handler`
        directly. This avoids the hidden mutation of the context's
        internal counter. We explicitly derive the child's operation_id
        from `executable.index` so that the same input always produces
        the same id regardless of the order branches actually run in.

        Invariant: `operation_id` for a given executable is deterministic
        and execution-order invariant.
        """

        operation_id: str = self.operation_id_namespace.create_id_for_step(
            executable.index
        )
        name: str = self.get_iteration_name(executable.index)
        is_virtual: bool = self.nesting_type is NestingType.FLAT

        child_context: DurableContext = executor_context.create_child_context(
            operation_id, is_virtual=is_virtual
        )
        # For NESTED this is for branch's START/SUCCEED/FAIL checkpoints (not the children of the branch).
        # For FLAT `child_handler` skips checkpoints, so not used.
        # Construct it unconditionally to keep the call simple.
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=self.sub_type_iteration,
            parent_id=executor_context._parent_id,  # noqa: SLF001
            name=name,
        )

        # The branch/iteration container op is resolved here via child_handler,
        # bypassing context.run_in_child_context and therefore the parent's
        # `_replay_aware`. Replicate the two things `_replay_aware` would have
        # done for this container op while replaying:
        #   1. Existence flip: a brand-new branch (no checkpoint) is new work,
        #      so the child must start in NEW rather than inheriting REPLAY —
        #      otherwise logs before the branch's first inner op are wrongly
        #      de-duplicated during a map/parallel replay.
        #   2. Replay hook: a branch that already has a checkpoint was observed
        #      in a prior invocation, so emit the plugin replay hook (once).
        # Virtual (FLAT) branches do not checkpoint themselves, so neither
        # applies; their inner operations still self-correct via `_replay_aware`.
        if not is_virtual and child_context.is_replaying():
            branch_checkpoint = child_context.state.get_checkpoint_result(operation_id)
            if not branch_checkpoint.is_existent():
                child_context._set_replay_status_new()  # noqa: SLF001
            elif branch_checkpoint.operation is not None:
                child_context.state.emit_operation_replay_hook(
                    branch_checkpoint.operation
                )

        def run_in_child_handler() -> ResultType:
            return self.execute_item(child_context, executable)

        result: ResultType = child_handler(
            run_in_child_handler,
            child_context.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                serdes=self.item_serdes or self.serdes,
                sub_type=self.sub_type_iteration,
                is_virtual=is_virtual,
            ),
        )
        return result

    def replay(
        self,
        execution_state: ExecutionState,
        executor_context: DurableContext,
        checkpointed_result: CheckpointedResult | None = None,
    ) -> BatchResult[ResultType]:
        """Reconstruct the batch result while in replay_children mode.

        When the operation's summary carries a recorded completion decision,
        the reconstruction obeys it so the result matches the live result
        exactly: branches recorded STARTED are reported STARTED without
        consulting child checkpoints, branches past the started prefix are
        omitted, terminal branches are re-derived, and the completion
        reason is the recorded value verbatim.

        Summaries without a record (checkpoints written before the record
        existed) fall back to deriving every branch from its child
        checkpoint.
        """
        record: CompletionRecord | None = CompletionRecord.from_summary_payload(
            checkpointed_result.result if checkpointed_result else None
        )
        if record is None:
            return self._replay_from_checkpoints(execution_state, executor_context)

        items: list[BatchItem[ResultType]] = []
        for executable in self.executables:
            if executable.index >= record.started_total:
                continue
            if executable.index in record.started_indexes:
                items.append(BatchItem(executable.index, BatchItemStatus.STARTED))
                continue
            items.append(
                self._replay_terminal_item(
                    execution_state, executor_context, executable
                )
            )
        return BatchResult(items, record.completion_reason)

    def _replay_terminal_item(
        self,
        execution_state: ExecutionState,
        executor_context: DurableContext,
        executable: Executable[CallableType],
    ) -> BatchItem[ResultType]:
        """Re-derive one branch recorded as terminal.

        Non-virtual branches have a terminal checkpoint: terminal branch
        events are only emitted after the synchronous SUCCEED/FAIL
        checkpoint call returned. Virtual (FLAT) branches never checkpoint
        themselves, so re-executing the branch body over its inner
        operations' checkpoints discriminates success from failure.
        """
        operation_id: str = self.operation_id_namespace.create_id_for_step(
            executable.index
        )
        checkpoint: CheckpointedResult = execution_state.get_checkpoint_result(
            operation_id
        )
        if checkpoint.is_succeeded():
            result: ResultType = self._execute_item_in_child_context(
                executor_context, executable
            )
            return BatchItem(executable.index, BatchItemStatus.SUCCEEDED, result)
        if checkpoint.is_failed():
            return BatchItem(
                executable.index, BatchItemStatus.FAILED, error=checkpoint.error
            )
        if self.nesting_type is NestingType.FLAT:
            try:
                flat_result: ResultType = self._execute_item_in_child_context(
                    executor_context, executable
                )
            except Exception as e:  # noqa: BLE001
                return BatchItem(
                    executable.index,
                    BatchItemStatus.FAILED,
                    error=_branch_error_object(e),
                )
            return BatchItem(executable.index, BatchItemStatus.SUCCEEDED, flat_result)
        return BatchItem(executable.index, BatchItemStatus.STARTED)

    def _replay_from_checkpoints(
        self, execution_state: ExecutionState, executor_context: DurableContext
    ) -> BatchResult[ResultType]:
        """Derive every branch from its child checkpoint.

        Fallback reconstruction for summaries that carry no completion
        record. Every executable is represented, matching the live results
        that predate the recorded decision.
        """
        items: list[BatchItem[ResultType]] = []
        for executable in self.executables:
            operation_id = self.operation_id_namespace.create_id_for_step(
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
