"""Execution life-cycle logic."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationUpdate,
    OperationStatus,
    OperationType,
    CallbackOptions,
    CallbackTimeoutType,
    StepDetails,
    StepOptions,
)

from aws_durable_execution_sdk_python_testing.checkpoint.core import CheckpointCore
from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    DEFAULT_MAX_INVOCATION_PAGE_BYTES,
)
from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (
    CheckpointRequestDispatcher,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.checkpoint import (
    CheckpointValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    ExecutionAlreadyStartedException,
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import (
    CheckpointIdempotencyRecord,
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.model import (
    CheckpointDurableExecutionResponse,
    CheckpointUpdatedExecutionState,
    EventCreationContext,
    EventType,
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionResponse,
    GetDurableExecutionStateResponse,
    ListDurableExecutionsByFunctionResponse,
    ListDurableExecutionsResponse,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    StopDurableExecutionResponse,
    TERMINAL_STATUSES,
)
from aws_durable_execution_sdk_python_testing.model import (
    Event as HistoryEvent,
)
from aws_durable_execution_sdk_python_testing.model import (
    Execution as ExecutionSummary,
)
from aws_durable_execution_sdk_python_testing.observer import (
    ExecutionNotifier,
    ExecutionObserver,
)
from aws_durable_execution_sdk_python_testing.token import (
    CallbackToken,
    CheckpointToken,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
        CheckpointProcessor,
    )
    from aws_durable_execution_sdk_python_testing.invoker import Invoker
    from aws_durable_execution_sdk_python_testing.scheduler import Event, Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore

logger = logging.getLogger(__name__)


class InvocationState(Enum):
    """Per-ARN handler-invocation state machine.

    Lives on :class:`Executor` keyed by execution ARN, never
    persisted: a crashed ``INVOKING`` state would strand the execution
    forever on restart because the gate would read "someone is
    invoking" while nobody is.

    Transitions:

    * ``PRE_INVOKE`` → ``INVOKING`` — when ``_invoke_execution``
      dispatches a handler call.
    * ``INVOKING`` → ``PRE_INVOKE`` — when a handler returns
      ``PENDING`` or fails (retry will re-enter ``PRE_INVOKE`` →
      ``INVOKING``).
    * ``INVOKING`` → ``COMPLETED`` — when the execution reaches a
      terminal status (handler returned ``SUCCEEDED`` / ``FAILED``,
      stop requested, timeout).
    """

    PRE_INVOKE = "PRE_INVOKE"
    INVOKING = "INVOKING"
    COMPLETED = "COMPLETED"


class Executor(ExecutionObserver):
    MAX_CONSECUTIVE_FAILED_ATTEMPTS: int = 5
    RETRY_BACKOFF_SECONDS: int = 5

    def __init__(
        self,
        store: ExecutionStore,
        scheduler: Scheduler,
        invoker: Invoker,
        checkpoint_processor: CheckpointProcessor,
        max_invocation_page_bytes: int | None = None,
        invocation_timeout_seconds: int = 900,
    ):
        self._store = store
        self._scheduler = scheduler
        self._invoker = invoker
        self._checkpoint_processor = checkpoint_processor
        self._invocation_timeout_seconds = invocation_timeout_seconds
        self._dispatcher = CheckpointRequestDispatcher()
        self._notifier = ExecutionNotifier()
        # Self-observe so processors called via our own dispatcher can
        # schedule wait / retry / callback timers through the normal
        # observer path.
        self._notifier.add_observer(self)
        self._max_invocation_page_bytes = (
            max_invocation_page_bytes
            if max_invocation_page_bytes is not None
            else DEFAULT_MAX_INVOCATION_PAGE_BYTES
        )
        # Per-ARN lock infrastructure. Locks live on the
        # Executor, not the Execution, because the SQLite / Filesystem
        # stores return a freshly deserialised Execution on every
        # load — an instance-level lock would protect nothing across
        # the load/save window. Entries are deliberately leaked on
        # execution termination; removing them would create a
        # lock-identity race.
        self._arn_locks: dict[str, threading.Lock] = {}
        self._arn_locks_supervisor: threading.Lock = threading.Lock()
        # Per-ARN handler-invocation state machine. Gates
        # _invoke_execution so at most one handler invocation is in
        # flight per execution. Never persisted: an absent
        # entry is PRE_INVOKE by convention.
        self._invocation_state: dict[str, InvocationState] = {}
        # Single earliest-pending wake-up timer per execution.
        # Replaces the per-op call_later pattern for wait timers and
        # step retries so concurrent async completions all fire on
        # their earliest aggregate moment rather than chaining.
        # Entries cancelled on terminal transitions. Never persisted.
        self._pending_wakeup: dict[str, Future] = {}
        self._completion_events: dict[str, Event] = {}
        self._callback_timeouts: dict[str, Future] = {}
        self._callback_heartbeats: dict[str, Future] = {}
        self._execution_timeout: Future | None = None

    def start_execution(
        self,
        input: StartDurableExecutionInput,  # noqa: A002
    ) -> StartDurableExecutionOutput:
        # Generate invocation_id if not provided
        if input.invocation_id is None:
            input = StartDurableExecutionInput(
                account_id=input.account_id,
                function_name=input.function_name,
                function_qualifier=input.function_qualifier,
                execution_name=input.execution_name,
                execution_timeout_seconds=input.execution_timeout_seconds,
                execution_retention_period_days=input.execution_retention_period_days,
                invocation_id=str(uuid.uuid4()),
                trace_fields=input.trace_fields,
                tenant_id=input.tenant_id,
                input=input.input,
                lambda_endpoint=input.lambda_endpoint,
            )

        execution = Execution.new(input=input)
        execution.start()
        self._store.save(execution)
        logger.debug("Created execution with ARN: %s", execution.durable_execution_arn)

        completion_event = self._scheduler.create_event()
        self._completion_events[execution.durable_execution_arn] = completion_event

        # Schedule execution timeout
        if input.execution_timeout_seconds > 0:

            def timeout_handler():
                error = ErrorObject.from_message(
                    f"Execution timed out after {input.execution_timeout_seconds} seconds."
                )
                self.on_timed_out(execution.durable_execution_arn, error)

            self._execution_timeout = self._scheduler.call_later(
                timeout_handler,
                delay=input.execution_timeout_seconds,
                completion_event=completion_event,
            )

        # Schedule initial invocation to run immediately
        self._invoke_execution(execution.durable_execution_arn)

        return StartDurableExecutionOutput(
            execution_arn=execution.durable_execution_arn
        )

    def get_execution(self, execution_arn: str) -> Execution:
        """Get execution by ARN.

        Args:
            execution_arn: The execution ARN to retrieve

        Returns:
            Execution: The execution object

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        try:
            return self._store.load(execution_arn)
        except KeyError as e:
            msg: str = f"Execution {execution_arn} not found"
            raise ResourceNotFoundException(msg) from e

    def get_execution_details(self, execution_arn: str) -> GetDurableExecutionResponse:
        """Get detailed execution information for web API response.

        Args:
            execution_arn: The execution ARN to retrieve

        Returns:
            GetDurableExecutionResponse: Detailed execution information

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        execution = self.get_execution(execution_arn)

        # Extract execution details from the first operation (EXECUTION type)
        execution_op = execution.get_operation_execution_started()
        status = execution.current_status().value

        # Extract result and error from execution result
        result = None
        error = None
        if execution.result:
            if execution.result.status == InvocationStatus.SUCCEEDED:
                result = execution.result.result
            elif execution.result.status == InvocationStatus.FAILED:
                error = execution.result.error

        return GetDurableExecutionResponse(
            durable_execution_arn=execution.durable_execution_arn,
            durable_execution_name=execution.start_input.execution_name,
            function_arn=f"arn:aws:lambda:us-east-1:123456789012:function:{execution.start_input.function_name}",
            status=status,
            start_timestamp=execution_op.start_timestamp
            if execution_op.start_timestamp
            else datetime.now(UTC),
            input_payload=execution_op.execution_details.input_payload
            if execution_op.execution_details
            else None,
            result=result,
            error=error,
            end_timestamp=execution_op.end_timestamp
            if execution_op.end_timestamp
            else None,
            version="1.0",
        )

    def list_executions(
        self,
        function_name: str | None = None,
        function_version: str | None = None,  # noqa: ARG002
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> ListDurableExecutionsResponse:
        """List executions with filtering and pagination.

        Args:
            function_name: Filter by function name
            function_version: Filter by function version
            execution_name: Filter by execution name
            status_filter: Filter by status (RUNNING, SUCCEEDED, FAILED)
            started_after: Filter executions started after this time
            started_before: Filter executions started before this time
            marker: Pagination marker
            max_items: Maximum items to return (default 50)
            reverse_order: Return results in reverse chronological order

        Returns:
            ListDurableExecutionsResponse: List of executions with pagination
        """
        # Convert marker to offset
        offset: int = 0
        if marker:
            try:
                offset = int(marker)
            except ValueError:
                offset = 0

        # Query store directly with parameters
        executions, next_marker = self._store.query(
            function_name=function_name,
            execution_name=execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            limit=max_items or 50,
            offset=offset,
            reverse_order=reverse_order,
        )

        # Convert to ExecutionSummary objects
        execution_summaries: list[ExecutionSummary] = [
            ExecutionSummary.from_execution(execution, execution.current_status().value)
            for execution in executions
        ]

        return ListDurableExecutionsResponse(
            durable_executions=execution_summaries, next_marker=next_marker
        )

    def list_executions_by_function(
        self,
        function_name: str,
        qualifier: str | None = None,  # noqa: ARG002
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> ListDurableExecutionsByFunctionResponse:
        """List executions for a specific function.

        Args:
            function_name: The function name to filter by
            qualifier: Function qualifier/version
            execution_name: Filter by execution name
            status_filter: Filter by status (RUNNING, SUCCEEDED, FAILED)
            started_after: Filter executions started after this time
            started_before: Filter executions started before this time
            marker: Pagination marker
            max_items: Maximum items to return (default 50)
            reverse_order: Return results in reverse chronological order

        Returns:
            ListDurableExecutionsByFunctionResponse: List of executions for the function
        """
        # Use the general list_executions method with function_name filter
        list_response = self.list_executions(
            function_name=function_name,
            execution_name=execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            marker=marker,
            max_items=max_items,
            reverse_order=reverse_order,
        )

        return ListDurableExecutionsByFunctionResponse(
            durable_executions=list_response.durable_executions,
            next_marker=list_response.next_marker,
        )

    def stop_execution(
        self, execution_arn: str, error: ErrorObject | None = None
    ) -> StopDurableExecutionResponse:
        """Stop a running execution.

        Args:
            execution_arn: The execution ARN to stop
            error: Optional error to use when stopping the execution

        Returns:
            StopDurableExecutionResponse: Response containing end timestamp

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        with self._lock_for(execution_arn):
            execution = self.get_execution(execution_arn)

            if execution.is_complete:
                # Idempotent: return the existing stop timestamp
                execution_op = execution.get_operation_execution_started()
                stop_timestamp = execution_op.end_timestamp or datetime.now(UTC)
                return StopDurableExecutionResponse(stop_timestamp=stop_timestamp)

            # Use provided error or create a default one
            stop_error = error or ErrorObject.from_message(
                "Execution stopped by user request"
            )

            # Stop sets TERMINATED close status (different from fail)
            logger.exception("[%s] Stopping execution.", execution_arn)
            execution.complete_stopped(error=stop_error)  # Sets CloseStatus.TERMINATED
            self._store.update(execution)
            self._complete_events(execution_arn=execution_arn)

        return StopDurableExecutionResponse(stop_timestamp=datetime.now(UTC))

    def get_execution_state(
        self,
        execution_arn: str,
        checkpoint_token: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,  # noqa: ARG002 — kept for API compat; page is byte-bounded
    ) -> GetDurableExecutionStateResponse:
        """Return a page of operations from the pinned snapshot.

        Valid only while the execution is ``INVOKING``. The
        call is a pure read: no ``handler_seen_seq`` advance, no
        ``token_sequence`` bump, no idempotency mutation.

        Raises:
            ResourceNotFoundException: execution does not exist.
            InvalidParameterValueException: when the invocation gate
                is not ``INVOKING``, the token is stale, or the
                marker does not resolve against the pinned sequence.
        """
        with self._lock_for(execution_arn):
            execution = self.get_execution(execution_arn)

            # Invocation gate: reads are valid only while INVOKING.
            if (
                self._invocation_state.get(execution_arn, InvocationState.PRE_INVOKE)
                is not InvocationState.INVOKING
            ):
                msg = "Invalid checkpoint token"
                raise InvalidParameterValueException(msg)

            # Token check: reject a stale or superseded checkpoint token.
            if not self._is_current_token(execution, checkpoint_token):
                msg = "Invalid checkpoint token"
                raise InvalidParameterValueException(msg)

            paginator = OperationPaginatorState.pin(execution)
            ops, next_marker = paginator.page(marker, self._max_invocation_page_bytes)

        return GetDurableExecutionStateResponse(operations=ops, next_marker=next_marker)

    def get_execution_history(
        self,
        execution_arn: str,
        include_execution_data: bool = False,  # noqa: FBT001, FBT002
        reverse_order: bool = False,  # noqa: FBT001, FBT002
        marker: str | None = None,
        max_items: int | None = None,
    ) -> GetDurableExecutionHistoryResponse:
        """Get execution history with events.

        Args:
            execution_arn: The execution ARN
            include_execution_data: Whether to include execution data in events
            reverse_order: Return events in reverse chronological order
            marker: Pagination marker (event_id)
            max_items: Maximum items to return

        Returns:
            GetDurableExecutionHistoryResponse: Execution history with events

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        execution: Execution = self.get_execution(execution_arn)

        # Generate events
        all_events: list[HistoryEvent] = []
        durable_execution_arn: str = execution.durable_execution_arn

        # Add InvocationCompleted events
        for completion in execution.invocation_completions:
            invocation_event = HistoryEvent.create_invocation_completed(
                event_id=0,  # Temporary, will be reassigned
                event_timestamp=completion.end_timestamp,
                start_timestamp=completion.start_timestamp,
                end_timestamp=completion.end_timestamp,
                request_id=completion.request_id,
            )
            all_events.append(invocation_event)

        # Generate events from update history (one event per update).
        # Each checkpoint update increments the history event counter,
        # so every recorded transition becomes a separate history event.
        updates: list[OperationUpdate] = execution.updates
        timestamps: list[datetime] = execution.update_timestamps
        # Track cumulative step_details per operation for retry details
        op_step_attempt: dict[str, int] = {}

        if updates:
            # Build set of operation IDs covered by updates
            update_op_ids: set[str] = {u.operation_id for u in updates}

            # For operations NOT covered by updates (e.g., EXECUTION
            # created by start_execution, WAITs completed via
            # complete_wait, callbacks completed async), generate events
            # from the final operation snapshot.
            for op in execution.operations:
                if op.operation_id in update_op_ids:
                    continue
                if op.start_timestamp is not None:
                    context = EventCreationContext(
                        op,
                        0,
                        durable_execution_arn,
                        execution.start_input,
                        execution.result,
                        None,
                        include_execution_data,
                    )
                    all_events.append(HistoryEvent.create_event_started(context))
                if op.end_timestamp is not None and op.status in TERMINAL_STATUSES:
                    context = EventCreationContext(
                        op,
                        0,
                        durable_execution_arn,
                        execution.start_input,
                        execution.result,
                        None,
                        include_execution_data,
                    )
                    all_events.append(HistoryEvent.create_event_terminated(context))

            # For operations WITH updates, generate one event per update
            # (captures retry cycles faithfully). Use the real operation
            # (which has all details like callback_id, wait_details, etc.)
            # but override status/timestamps per update action.
            ops_by_id: dict[str, Operation] = {
                op.operation_id: op for op in execution.operations
            }
            for idx, update in enumerate(updates):
                ts: datetime = (
                    timestamps[idx] if idx < len(timestamps) else datetime.now(UTC)
                )

                real_op: Operation | None = ops_by_id.get(update.operation_id)
                if real_op is None:
                    continue

                status: OperationStatus
                start_ts: datetime | None = real_op.start_timestamp or ts
                end_ts: datetime | None = None
                step_details: StepDetails | None = real_op.step_details

                match update.action:
                    case OperationAction.START:
                        status = OperationStatus.STARTED
                        start_ts = ts
                        op_step_attempt.setdefault(update.operation_id, 0)
                        op_step_attempt[update.operation_id] += 1
                    case OperationAction.RETRY:
                        # Match JS getRetryHistoryEventDetail: RETRY with
                        # error → StepFailed, without error → StepSucceeded
                        status = (
                            OperationStatus.FAILED
                            if update.error
                            else OperationStatus.SUCCEEDED
                        )
                        end_ts = ts
                        attempt: int = op_step_attempt.get(update.operation_id, 1)
                        step_details = StepDetails(
                            attempt=attempt,
                            error=update.error,
                        )
                    case OperationAction.SUCCEED:
                        status = OperationStatus.SUCCEEDED
                        end_ts = ts
                    case OperationAction.FAIL:
                        status = OperationStatus.FAILED
                        end_ts = ts
                        if update.operation_type == OperationType.STEP:
                            attempt = op_step_attempt.get(update.operation_id, 1)
                            step_details = StepDetails(
                                attempt=attempt,
                                error=update.error,
                            )
                    case _:
                        continue

                # Create a copy of the real operation with overridden
                # status and timestamps for this specific transition.
                event_op: Operation = Operation(
                    operation_id=real_op.operation_id,
                    operation_type=real_op.operation_type,
                    status=status,
                    parent_id=real_op.parent_id,
                    name=real_op.name,
                    start_timestamp=start_ts,
                    end_timestamp=end_ts,
                    sub_type=real_op.sub_type,
                    step_details=step_details,
                    callback_details=real_op.callback_details,
                    wait_details=real_op.wait_details,
                    context_details=real_op.context_details,
                    execution_details=real_op.execution_details,
                    chained_invoke_details=real_op.chained_invoke_details,
                )

                op_update_ref: OperationUpdate | None = (
                    update
                    if update.action in (OperationAction.RETRY, OperationAction.FAIL)
                    else None
                )

                context = EventCreationContext(
                    event_op,
                    0,
                    durable_execution_arn,
                    execution.start_input,
                    execution.result,
                    op_update_ref,
                    include_execution_data,
                )

                if update.action == OperationAction.START:
                    if update.operation_type == OperationType.CHAINED_INVOKE:
                        all_events.append(
                            HistoryEvent.create_chained_invoke_event_pending(context)
                        )
                    else:
                        all_events.append(HistoryEvent.create_event_started(context))
                else:
                    all_events.append(HistoryEvent.create_event_terminated(context))

            # Operations started via checkpoint but completed async (WAITs
            # by timer, CALLBACKs by external call) have their terminal
            # transition only in the operation state, not in updates.
            last_update_action: dict[str, OperationAction] = {}
            for u in updates:
                last_update_action[u.operation_id] = u.action
            for op in execution.operations:
                if op.operation_id not in update_op_ids:
                    continue
                if (
                    last_update_action.get(op.operation_id)
                    not in (OperationAction.SUCCEED, OperationAction.FAIL)
                    and op.end_timestamp is not None
                    and op.status in TERMINAL_STATUSES
                ):
                    context = EventCreationContext(
                        op,
                        0,
                        durable_execution_arn,
                        execution.start_input,
                        execution.result,
                        None,
                        include_execution_data,
                    )
                    all_events.append(HistoryEvent.create_event_terminated(context))
        else:
            # Fallback: generate events from final operation state (legacy
            # path for tests that set up operations directly without going
            # through the checkpoint pipeline).
            ops: list[Operation] = execution.operations
            for op in ops:
                if op.status is OperationStatus.PENDING:
                    if (
                        op.operation_type is not OperationType.CHAINED_INVOKE
                        or op.start_timestamp is None
                    ):
                        continue
                    context = EventCreationContext(
                        op,
                        0,
                        durable_execution_arn,
                        execution.start_input,
                        execution.result,
                        None,
                        include_execution_data,
                    )
                    all_events.append(
                        HistoryEvent.create_chained_invoke_event_pending(context)
                    )
                if op.start_timestamp is not None:
                    context = EventCreationContext(
                        op,
                        0,
                        durable_execution_arn,
                        execution.start_input,
                        execution.result,
                        None,
                        include_execution_data,
                    )
                    all_events.append(HistoryEvent.create_event_started(context))
                if op.end_timestamp is not None and op.status in TERMINAL_STATUSES:
                    context = EventCreationContext(
                        op,
                        0,
                        durable_execution_arn,
                        execution.start_input,
                        execution.result,
                        None,
                        include_execution_data,
                    )
                    all_events.append(HistoryEvent.create_event_terminated(context))

        # Sort events by timestamp to get correct chronological order
        all_events.sort(key=lambda event: event.event_timestamp)

        # Reassign event IDs based on chronological order
        all_events = [
            HistoryEvent.from_event_with_id(event, i)
            for i, event in enumerate(all_events, 1)
        ]

        # Apply cursor-based pagination
        if max_items is None:
            max_items = 100

        # Handle pagination marker
        if reverse_order:
            all_events.reverse()
        start_index: int = 0
        if marker:
            try:
                marker_event_id: int = int(marker)
                # Find the index of the first event with event_id >= marker
                start_index = len(all_events)
                for i, e in enumerate(all_events):
                    is_valid_page_start: bool = (
                        e.event_id < marker_event_id
                        if reverse_order
                        else e.event_id >= marker_event_id
                    )
                    if is_valid_page_start:
                        start_index = i
                        break
            except ValueError:
                start_index = 0

        # Get paginated events
        end_index: int = start_index + max_items
        paginated_events: list[HistoryEvent] = all_events[start_index:end_index]

        # Generate next marker
        next_marker: str | None = None
        if end_index < len(all_events):
            if reverse_order:
                # Next marker is the event_id of the last returned event
                next_marker = (
                    str(paginated_events[-1].event_id) if paginated_events else None
                )
            else:
                # Next marker is the event_id of the next event after the last returned
                next_marker = (
                    str(all_events[end_index].event_id)
                    if end_index < len(all_events)
                    else None
                )

        return GetDurableExecutionHistoryResponse(
            events=paginated_events, next_marker=next_marker
        )

    def checkpoint_execution(
        self,
        execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate] | None = None,
        client_token: str | None = None,
    ) -> CheckpointDurableExecutionResponse:
        """Process a checkpoint request for an execution.

        Applies ``updates`` in place, advances ``token_sequence`` once,
        computes the delta of operations the handler has not yet seen,
        truncates to one page, and advances ``handler_seen_seq`` only
        for the ops actually returned.

        The full load-mutate-save sequence runs under the per-ARN
        lock so concurrent callers against the same execution
        can never read a half-applied mutation.

        Raises:
            ResourceNotFoundException: If the execution does not exist.
            InvalidParameterValueException: If the checkpoint token is
                invalid (wrong ``token_sequence`` or the execution is
                already complete).
        """
        with self._lock_for(execution_arn):
            execution = self.get_execution(execution_arn)

            # Idempotency first. A caller retrying a previously-
            # successful checkpoint is entitled to the cached response
            # even if the execution has since moved on (though in
            # practice it won't — the SDK waits for a response before
            # advancing). Idempotency check runs before the token check.
            cached = self._maybe_replay_cached(
                execution, checkpoint_token, client_token
            )
            if cached is not None:
                return cached

            # Invocation-state gate. Rejects stale checkpoints
            # from handler processes that died or timed out and came
            # back after we tore down.
            if (
                self._invocation_state.get(execution_arn, InvocationState.PRE_INVOKE)
                is not InvocationState.INVOKING
            ):
                msg = "Invalid checkpoint token"
                raise InvalidParameterValueException(msg)

            if not self._is_current_token(execution, checkpoint_token):
                msg = "Invalid checkpoint token"
                raise InvalidParameterValueException(msg)

            if updates:
                CheckpointValidator.validate_input(updates, execution)
                self._dispatcher.apply_updates(
                    execution=execution,
                    updates=updates,
                    client_token=client_token,
                    notifier=self._notifier,
                    touch=execution.touch_operation,
                )

            new_token_sequence = execution.advance_token_sequence()

            paginator = OperationPaginatorState.pin(execution)
            # The checkpoint response returns the full unseen delta in a
            # single response. Advance handler_seen_seq to cover every
            # returned op so the next delta carries only operations
            # touched after this response.
            response_ops = paginator.unseen_operations()
            if response_ops:
                highest_delivered_seq = max(
                    execution.operation_last_touched_seq[op.operation_id]
                    for op in response_ops
                )
                paginator.advance_handler_seen(highest_delivered_seq)

            new_token = CheckpointToken(
                execution_arn=execution.durable_execution_arn,
                token_sequence=new_token_sequence,
                invocation_id=execution.current_invocation_id,
            ).to_str()

            # Cache the response so a retried call with the
            # same (client_token, inbound_checkpoint_token) gets a
            # byte-identical replay.
            execution.last_checkpoint = CheckpointIdempotencyRecord(
                client_token=client_token or "",
                inbound_checkpoint_token=checkpoint_token,
                outbound_checkpoint_token=new_token,
                operations=list(response_ops),
                next_marker=None,
            )

            self._store.update(execution)

            response = CheckpointDurableExecutionResponse(
                checkpoint_token=new_token,
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=response_ops,
                    next_marker=None,
                ),
            )

        # Re-arm the earliest-pending wake-up outside the
        # lock so the next scheduler-driven completion fires at the
        # minimum pending timestamp across Wait / Step ops.
        self._schedule_earliest_pending(execution_arn)

        return response

    def _maybe_replay_cached(
        self,
        execution: Execution,
        checkpoint_token: str,
        client_token: str | None,
    ) -> CheckpointDurableExecutionResponse | None:
        """Replay the cached response for a retried checkpoint call.

        Returns ``None`` when there is no idempotency match.
        """
        cached = CheckpointCore.match_cached(execution, checkpoint_token, client_token)
        if cached is None:
            return None
        return CheckpointDurableExecutionResponse(
            checkpoint_token=cached.outbound_checkpoint_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=list(cached.operations),
                next_marker=cached.next_marker,
            ),
        )

    def _is_current_token(
        self,
        execution: Execution,
        checkpoint_token: str | None,
    ) -> bool:
        """Check that ``checkpoint_token`` parses cleanly and matches the
        execution's current ``token_sequence`` and invocation identity.
        Terminal executions reject all tokens."""
        if checkpoint_token is None:
            return False
        try:
            parsed = CheckpointToken.from_str(checkpoint_token)
        except (ValueError, KeyError):
            return False
        return (
            not execution.is_complete
            and parsed.token_sequence == execution.token_sequence
            and parsed.invocation_id == execution.current_invocation_id
        )

    def _lock_for(self, arn: str) -> threading.Lock:
        """Return the per-ARN ``threading.Lock`` for an execution.

        Lazily created on first access. Entries are deliberately
        leaked — see :class:`Executor` docstring and the design
        for why removal would introduce a lock-identity race.
        """
        with self._arn_locks_supervisor:
            lock = self._arn_locks.get(arn)
            if lock is None:
                lock = threading.Lock()
                self._arn_locks[arn] = lock
            return lock

    def _release_gate(self, arn: str, to: InvocationState) -> None:
        """Transition ``_invocation_state[arn]`` out of ``INVOKING``.

        Called from ``_on_invoke_finished_ok`` (PENDING → PRE_INVOKE
        or terminal → COMPLETED) and ``_handle_invoke_failure``
        (FAILED → PRE_INVOKE so a retry can re-enter). Caller MUST
        hold ``_lock_for(arn)`` around the state read that justified
        the transition.
        """
        self._invocation_state[arn] = to

    def _cleanup_execution_state(self, arn: str) -> None:
        """Tear down per-execution Executor-side state when an
        execution reaches a terminal status. Removes entries from
        ``_callback_timeouts`` and ``_callback_heartbeats`` and cancels
        pending futures. **Does NOT remove ``_arn_locks[arn]``**:
        removal would let two threads acquire different lock objects
        for the same ARN across cleanup boundaries, breaking the
        serialisation guarantee.
        """
        # Cancel and drop callback timers.
        for callback_id in list(self._callback_timeouts):
            future = self._callback_timeouts.get(callback_id)
            if future is not None and not future.done():
                future.cancel()
        for callback_id in list(self._callback_heartbeats):
            future = self._callback_heartbeats.get(callback_id)
            if future is not None and not future.done():
                future.cancel()
        self._completion_events.pop(arn, None)
        self._invocation_state.pop(arn, None)
        # Cancel the earliest-pending wake-up timer on terminal
        # transitions — no further re-invokes needed.
        pending = self._pending_wakeup.pop(arn, None)
        if pending is not None and not pending.done():
            pending.cancel()

    @staticmethod
    def _earliest_pending_timestamp(execution: Execution) -> datetime | None:
        """Minimum wake-up timestamp across scheduler-driven pending
        completions. Returns None if none exist.

        Sources:

        * ``wait.scheduled_end_timestamp`` for ``STARTED`` Wait ops.
        * ``step.next_attempt_timestamp`` for ``PENDING`` Step ops.

        Callback timeouts are deliberately excluded: they
        keep their existing per-callback timers and would
        double-fire if folded in here.
        """
        candidates: list[datetime] = []
        for op in execution.operations:
            if (
                op.operation_type == OperationType.WAIT
                and op.status == OperationStatus.STARTED
                and op.wait_details is not None
                and op.wait_details.scheduled_end_timestamp is not None
            ):
                candidates.append(op.wait_details.scheduled_end_timestamp)
            elif (
                op.operation_type == OperationType.STEP
                and op.status == OperationStatus.PENDING
                and op.step_details is not None
                and op.step_details.next_attempt_timestamp is not None
            ):
                candidates.append(op.step_details.next_attempt_timestamp)
        if not candidates:
            return None
        return min(candidates)

    def _schedule_earliest_pending(self, execution_arn: str) -> None:
        """Arm a single wake-up timer at the earliest pending
        completion moment for ``execution_arn``. Cancels any
        previously armed wake-up first, so each re-invocation or
        checkpoint commit re-computes the horizon fresh.

        Thread-safe: the pop+scheduler-call+put sequence runs under
        the supervisor lock so two concurrent callers (e.g. HTTP
        thread post-checkpoint + scheduler thread post-invoke) can't
        both leak a Future into the dict.
        """
        try:
            execution = self._store.load(execution_arn)
        except Exception:  # noqa: BLE001 — defensive; store may be torn down
            return

        if execution.is_complete:
            # Drop any stale wake-up without re-arming.
            with self._arn_locks_supervisor:
                existing = self._pending_wakeup.pop(execution_arn, None)
            if existing is not None and not existing.done():
                existing.cancel()
            return

        earliest = self._earliest_pending_timestamp(execution)
        if earliest is None:
            with self._arn_locks_supervisor:
                existing = self._pending_wakeup.pop(execution_arn, None)
            if existing is not None and not existing.done():
                existing.cancel()
            return

        now = datetime.now(UTC)
        delay = max((earliest - now).total_seconds(), 0.0)

        completion_event = self._completion_events.get(execution_arn)
        # Cancel-then-arm atomically under the supervisor lock so
        # concurrent callers can't leave orphan Futures in the dict.
        with self._arn_locks_supervisor:
            existing = self._pending_wakeup.pop(execution_arn, None)
            future = self._scheduler.call_later(
                self._fire_due_and_invoke_handler(execution_arn),
                delay=delay,
                completion_event=completion_event,
            )
            self._pending_wakeup[execution_arn] = future
        if existing is not None and not existing.done():
            existing.cancel()

    def _fire_due_and_invoke_handler(
        self, execution_arn: str
    ) -> Callable[[], Awaitable[None]]:
        """Build the coroutine armed by ``_schedule_earliest_pending``.

        Walks every Wait/Step op whose scheduled moment has passed
        by ``now``, transitions them via
        :meth:`Execution.complete_wait` / :meth:`complete_retry`
        (both of which call ``touch_operation``), then routes through
        the normal ``_invoke_execution`` path so the same
        invariants apply to the resulting handler call.
        """

        async def fire_due() -> None:
            with self._lock_for(execution_arn):
                try:
                    execution = self._store.load(execution_arn)
                except Exception:  # noqa: BLE001
                    return
                if execution.is_complete:
                    return

                now = datetime.now(UTC)
                completed_any = False
                for op in list(execution.operations):
                    if (
                        op.operation_type == OperationType.WAIT
                        and op.status == OperationStatus.STARTED
                        and op.wait_details is not None
                        and op.wait_details.scheduled_end_timestamp is not None
                        and op.wait_details.scheduled_end_timestamp <= now
                    ):
                        try:
                            execution.complete_wait(op.operation_id)
                            completed_any = True
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "[%s] earliest-pending: complete_wait failed for %s",
                                execution_arn,
                                op.operation_id,
                            )
                    elif (
                        op.operation_type == OperationType.STEP
                        and op.status == OperationStatus.PENDING
                        and op.step_details is not None
                        and op.step_details.next_attempt_timestamp is not None
                        and op.step_details.next_attempt_timestamp <= now
                    ):
                        try:
                            execution.complete_retry(op.operation_id)
                            completed_any = True
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "[%s] earliest-pending: complete_retry failed for %s",
                                execution_arn,
                                op.operation_id,
                            )
                if completed_any:
                    self._store.update(execution)

            # Outside the lock: invoke and arm the next wake-up.
            if completed_any:
                self._invoke_execution(execution_arn)
            self._schedule_earliest_pending(execution_arn)

        return fire_due

    def send_callback_success(
        self,
        callback_id: str,
        result: bytes | None = None,
    ) -> SendDurableExecutionCallbackSuccessResponse:
        """Send callback success response.

        Args:
            callback_id: The callback ID to respond to
            result: Optional result data for the callback

        Returns:
            SendDurableExecutionCallbackSuccessResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        try:
            callback_token = CallbackToken.from_str(callback_id)
            with self._lock_for(callback_token.execution_arn):
                execution = self.get_execution(callback_token.execution_arn)
                execution.complete_callback_success(callback_id, result)
                self._store.update(execution)
                self._cleanup_callback_timeouts(callback_id)
            self._invoke_execution(callback_token.execution_arn)
            logger.info("Callback success completed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback success: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackSuccessResponse()

    def send_callback_failure(
        self,
        callback_id: str,
        error: ErrorObject | None = None,
    ) -> SendDurableExecutionCallbackFailureResponse:
        """Send callback failure response.

        Args:
            callback_id: The callback ID to respond to
            error: Optional error object for the callback failure

        Returns:
            SendDurableExecutionCallbackFailureResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        # A callback failure sent with no error payload must leave the
        # error fields absent, not synthesize an empty ErrorMessage.
        # ErrorObject.to_dict() omits None fields, so an all-None
        # error serializes to {} and the SDK surfaces undefined fields.
        callback_error: ErrorObject = error or ErrorObject(
            message=None, type=None, data=None, stack_trace=None
        )

        try:
            callback_token: CallbackToken = CallbackToken.from_str(callback_id)
            with self._lock_for(callback_token.execution_arn):
                execution: Execution = self.get_execution(callback_token.execution_arn)
                execution.complete_callback_failure(callback_id, callback_error)
                self._store.update(execution)
                self._cleanup_callback_timeouts(callback_id)
            self._invoke_execution(callback_token.execution_arn)
            logger.info("Callback failure completed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback failure: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackFailureResponse()

    def send_callback_heartbeat(
        self, callback_id: str
    ) -> SendDurableExecutionCallbackHeartbeatResponse:
        """Send callback heartbeat to keep callback alive.

        Args:
            callback_id: The callback ID to send heartbeat for

        Returns:
            SendDurableExecutionCallbackHeartbeatResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        try:
            callback_token: CallbackToken = CallbackToken.from_str(callback_id)
            with self._lock_for(callback_token.execution_arn):
                execution: Execution = self.get_execution(callback_token.execution_arn)

                # Find callback operation to verify it exists and is active
                _, operation = execution.find_callback_operation(callback_id)
                if operation.status != OperationStatus.STARTED:
                    msg = f"Callback {callback_id} is not active"
                    raise ResourceNotFoundException(msg)

                # Reset heartbeat timeout if configured
                self._reset_callback_heartbeat_timeout(
                    callback_id, execution.durable_execution_arn
                )
            logger.info("Callback heartbeat processed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback heartbeat: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackHeartbeatResponse()

    def _validate_invocation_response_and_store(
        self,
        execution_arn: str,
        response: DurableExecutionInvocationOutput,
        execution: Execution,
    ):
        """Validate response status and save it to the store if fine.

        Raises:
            InvalidParameterValueException: If the response status is invalid.
            IllegalStateException: If the response status is valid but the execution is already completed.
        """
        if execution.is_complete:
            msg_already_complete: str = "Execution already completed, ignoring result"

            raise IllegalStateException(msg_already_complete)

        if response.status is None:
            msg_status_required: str = "Response status is required"

            raise InvalidParameterValueException(msg_status_required)

        match response.status:
            case InvocationStatus.FAILED:
                if response.result is not None:
                    msg_failed_result: str = (
                        "Cannot provide a Result for FAILED status."
                    )
                    raise InvalidParameterValueException(msg_failed_result)
                logger.info("[%s] Execution failed", execution_arn)
                self._complete_workflow(
                    execution_arn, result=None, error=response.error
                )

            case InvocationStatus.SUCCEEDED:
                if response.error is not None:
                    msg_success_error: str = (
                        "Cannot provide an Error for SUCCEEDED status."
                    )
                    raise InvalidParameterValueException(msg_success_error)
                logger.info("[%s] Execution succeeded", execution_arn)
                self._complete_workflow(
                    execution_arn, result=response.result, error=None
                )

            case InvocationStatus.PENDING:
                if not execution.has_pending_operations(execution):
                    msg_pending_ops: str = (
                        "Cannot return PENDING status with no pending operations."
                    )
                    raise InvalidParameterValueException(msg_pending_ops)
                logger.info("[%s] Execution pending async work", execution_arn)

            case _:
                msg_unexpected_status: str = (
                    f"Unexpected invocation status: {response.status}"
                )
                raise IllegalStateException(msg_unexpected_status)

    def _invoke_handler(self, execution_arn: str) -> Callable[[], Awaitable[None]]:
        """Create a parameterless callable that captures execution arn for the scheduler."""

        async def invoke() -> None:
            # Under the per-ARN lock, claim the invocation gate and
            # snapshot the execution for input construction. Release
            # the lock BEFORE the blocking Lambda invoke — otherwise
            # the customer handler's HTTP callbacks (which land on
            # other threads and also acquire this lock) would block
            # forever, deadlocking the execution.
            execution: Execution
            invocation_input: DurableExecutionInvocationInput
            try:
                with self._lock_for(execution_arn):
                    execution = self._store.load(execution_arn)

                    if execution.is_complete:
                        logger.info(
                            "[%s] Execution already completed, ignoring invoke",
                            execution_arn,
                        )
                        self._invocation_state[execution_arn] = (
                            InvocationState.COMPLETED
                        )
                        return

                    current_state = self._invocation_state.get(
                        execution_arn, InvocationState.PRE_INVOKE
                    )
                    if current_state is InvocationState.COMPLETED:
                        logger.info(
                            "[%s] Invocation state already COMPLETED, not re-invoking",
                            execution_arn,
                        )
                        return
                    if current_state is InvocationState.INVOKING:
                        # Another invocation is already in flight
                        # . Record that we wanted to re-invoke;
                        # the in-flight handler's post-invoke hook will
                        # consult ``needs_reinvoke`` and schedule a
                        # follow-up if set.
                        execution.needs_reinvoke = True
                        self._store.save(execution)
                        logger.info(
                            "[%s] Handler already INVOKING; deferring re-invoke",
                            execution_arn,
                        )
                        return

                    # Claim the gate: at most one handler
                    # invocation per execution in flight.
                    self._invocation_state[execution_arn] = InvocationState.INVOKING
                    execution.begin_new_invocation()

                    invocation_input = self._invoker.create_invocation_input(
                        execution=execution
                    )
                    self._store.save(execution)

                # Lock released. Blocking Lambda call happens here so
                # HTTP callback threads can acquire the lock for their
                # own load-mutate-save windows.
                invocation_start = datetime.now(UTC)
                invoke_response = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._invoker.invoke,
                        execution.start_input.function_name,
                        invocation_input,
                        execution.start_input.lambda_endpoint,
                    ),
                    timeout=self._invocation_timeout_seconds,
                )
                invocation_end = datetime.now(UTC)

                # Re-acquire the lock for post-invoke state updates.
                # While we were blocked, the Execution may have been
                # mutated by HTTP callback threads; reload fresh.
                with self._lock_for(execution_arn):
                    execution = self._store.load(execution_arn)

                    execution.record_invocation_completion(
                        invocation_start,
                        invocation_end,
                        invoke_response.request_id,
                    )
                    self._store.save(execution)

                    if execution.is_complete:
                        # A stop_execution / timeout landed while we
                        # were invoking — the terminal state is
                        # authoritative; don't let the handler's
                        # response overwrite it.
                        logger.info(
                            "[%s] Execution completed during invocation, ignoring result",
                            execution_arn,
                        )
                        self._invocation_state[execution_arn] = (
                            InvocationState.COMPLETED
                        )
                        return

                response = invoke_response.invocation_output
                try:
                    self._validate_invocation_response_and_store(
                        execution_arn, response, execution
                    )
                except (InvalidParameterValueException, IllegalStateException) as e:
                    logger.warning(
                        "[%s] Lambda output validation failure: %s",
                        execution_arn,
                        e,
                    )
                    error_obj = ErrorObject.from_exception(e)
                    # Release gate before retry scheduling so the
                    # retry's _invoke_execution call finds PRE_INVOKE.
                    # Under the per-ARN lock to block concurrent
                    # triggers.
                    with self._lock_for(execution_arn):
                        self._invocation_state[execution_arn] = (
                            InvocationState.PRE_INVOKE
                        )
                    self._retry_invocation(execution, error_obj)
                    return

                # A clean invocation (no validation failure,
                # no exception) resets the retry counter to zero. The
                # prior implementation only ever grew this counter.
                with self._lock_for(execution_arn):
                    reset_target = self._store.load(execution_arn)
                    reset_target.consecutive_failed_invocation_attempts = 0
                    self._store.save(reset_target)

                # Clean return from handler. If the response was
                # terminal, _complete_workflow has already set
                # is_complete and the next _invoke_execution call
                # (if any) will see COMPLETED. If PENDING, release
                # the gate to PRE_INVOKE so the next trigger can
                # start a fresh invocation.
                with self._lock_for(execution_arn):
                    reloaded = self._store.load(execution_arn)
                    if reloaded.is_complete:
                        self._invocation_state[execution_arn] = (
                            InvocationState.COMPLETED
                        )
                        should_reinvoke = False
                    else:
                        self._invocation_state[execution_arn] = (
                            InvocationState.PRE_INVOKE
                        )
                        # Re-invoke if another trigger
                        # deferred during this invocation. Clear the
                        # flag before scheduling so a concurrent
                        # trigger doesn't set it a second time after
                        # we've already picked it up.
                        should_reinvoke = reloaded.needs_reinvoke
                        if should_reinvoke:
                            reloaded.needs_reinvoke = False
                            self._store.save(reloaded)

                if should_reinvoke:
                    self._invoke_execution(execution_arn)
                else:
                    # Arm the earliest-pending wake-up so any
                    # scheduler-driven completion (wait timer, step
                    # retry) fires on its earliest aggregate moment.
                    self._schedule_earliest_pending(execution_arn)

            except ResourceNotFoundException:
                logger.warning("[%s] Function No longer exists", execution_arn)
                error_obj = ErrorObject.from_message(message="Function not found")
                # Release gate — _fail_workflow will set COMPLETED
                # after writing the terminal state. Transition under
                # the lock so a concurrent _invoke_execution can't
                # see PRE_INVOKE and race us.
                with self._lock_for(execution_arn):
                    self._invocation_state[execution_arn] = InvocationState.PRE_INVOKE
                self._fail_workflow(execution_arn, error_obj)

            except asyncio.TimeoutError:
                # Invocation killed by Lambda timeout. Step operations
                # stay in their current state (STARTED) — no checkpoint
                # was sent. Record the failed invocation and re-invoke.
                invocation_end = datetime.now(UTC)
                logger.warning(
                    "[%s] Invocation timed out after %ds",
                    execution_arn,
                    self._invocation_timeout_seconds,
                )
                with self._lock_for(execution_arn):
                    execution = self._store.load(execution_arn)
                    execution.record_invocation_completion(
                        invocation_start,
                        invocation_end,
                        str(uuid.uuid4()),
                    )
                    self._store.save(execution)
                    self._invocation_state[execution_arn] = InvocationState.PRE_INVOKE
                error_obj = ErrorObject.from_message(
                    message=f"Function timed out after {self._invocation_timeout_seconds} seconds"
                )
                self._retry_invocation(execution, error_obj)

            except Exception as e:  # noqa: BLE001
                # Handle invocation errors (network, function not found, etc.)
                logger.warning("[%s] Invocation failed: %s", execution_arn, e)
                error_obj = ErrorObject.from_exception(e)
                # Transition gate + reload execution under a single
                # lock acquisition so callers never see a half-updated
                # state.
                with self._lock_for(execution_arn):
                    self._invocation_state[execution_arn] = InvocationState.PRE_INVOKE
                    try:
                        execution = self._store.load(execution_arn)
                    except Exception:  # noqa: BLE001
                        return
                self._retry_invocation(execution, error_obj)

        return invoke

    def _invoke_execution(self, execution_arn: str, delay: float = 0) -> None:
        """Invoke execution after delay in seconds."""
        completion_event = self._completion_events.get(execution_arn)
        self._scheduler.call_later(
            self._invoke_handler(execution_arn),
            delay=delay,
            completion_event=completion_event,
        )

    def _complete_workflow(
        self, execution_arn: str, result: str | None, error: ErrorObject | None
    ):
        """Complete workflow - handles both success and failure with terminal state validation."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateException(msg)

        if error is not None:
            self.fail_execution(execution_arn, error)
        else:
            self.complete_execution(execution_arn, result)

    def _fail_workflow(self, execution_arn: str, error: ErrorObject):
        """Fail workflow with terminal state validation."""
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateException(msg)

        self.fail_execution(execution_arn, error)

    def _retry_invocation(self, execution: Execution, error: ErrorObject):
        """Handle retry logic or fail execution if retries exhausted.

        Budget: ``MAX_CONSECUTIVE_FAILED_ATTEMPTS`` attempts
        per ``RETRY_BACKOFF_SECONDS`` backoff. Increments the counter
        BEFORE the threshold check so an attempt-N failure that
        reaches the ceiling fails the execution rather than scheduling
        a pointless retry.
        """
        execution.consecutive_failed_invocation_attempts += 1
        self._store.save(execution)

        if (
            execution.consecutive_failed_invocation_attempts
            >= self.MAX_CONSECUTIVE_FAILED_ATTEMPTS
        ):
            # Budget exhausted — fail the execution with the last
            # observed error.
            self._fail_workflow(
                execution_arn=execution.durable_execution_arn, error=error
            )
            return

        # Schedule retry with backoff via the same _invoke_execution
        # entry point, so the at-most-one-invocation guarantee still
        # applies.
        self._invoke_execution(
            execution_arn=execution.durable_execution_arn,
            delay=self.RETRY_BACKOFF_SECONDS,
        )

    def _complete_events(self, execution_arn: str):
        # complete doesn't actually checkpoint explicitly
        if event := self._completion_events.get(execution_arn):
            event.set()
        if self._execution_timeout:
            self._execution_timeout.cancel()
            self._execution_timeout = None

    def wait_until_complete(
        self, execution_arn: str, timeout: float | None = None
    ) -> bool:
        """Block until execution completion. Don't do this unless you actually want to block.

        Args
            timeout (int|float|None): Wait for event to set until this timeout.

        Returns:
            True when set. False if the event timed out without being set.
        """
        if event := self._completion_events.get(execution_arn):
            return event.wait(timeout)

        # this really shouldn't happen - implies execution timed out?
        msg: str = "execution does not exist."

        raise ResourceNotFoundException(msg)

    def complete_execution(self, execution_arn: str, result: str | None = None) -> None:
        """Complete execution successfully (COMPLETE_WORKFLOW_EXECUTION decision)."""
        logger.debug("[%s] Completing execution with result: %s", execution_arn, result)
        with self._lock_for(execution_arn):
            execution: Execution = self._store.load(execution_arn=execution_arn)
            execution.complete_success(result=result)  # Sets CloseStatus.COMPLETED
            self._store.update(execution)
            if execution.result is None:
                msg: str = "Execution result is required"
                raise IllegalStateException(msg)
        self._complete_events(execution_arn=execution_arn)

    def fail_execution(self, execution_arn: str, error: ErrorObject) -> None:
        """Fail execution with error (FAIL_WORKFLOW_EXECUTION decision)."""
        logger.error("[%s] Completing execution with error: %s", execution_arn, error)
        with self._lock_for(execution_arn):
            execution: Execution = self._store.load(execution_arn=execution_arn)
            execution.complete_fail(error=error)  # Sets CloseStatus.FAILED
            self._store.update(execution)
            # set by complete_fail
            if execution.result is None:
                msg: str = "Execution result is required"
                raise IllegalStateException(msg)
        self._complete_events(execution_arn=execution_arn)

    # region ExecutionObserver
    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Complete execution successfully. Observer method triggered by notifier."""
        self.complete_execution(execution_arn, result)

    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Fail execution. Observer method triggered by notifier."""
        self.fail_execution(execution_arn, error)

    def on_timed_out(self, execution_arn: str, error: ErrorObject) -> None:
        """Handle execution timeout (workflow timeout). Observer method triggered by notifier."""
        logger.exception("[%s] Execution timed out.", execution_arn)
        with self._lock_for(execution_arn):
            execution: Execution = self._store.load(execution_arn=execution_arn)
            execution.complete_timeout(error=error)  # Sets CloseStatus.TIMED_OUT
            self._store.update(execution)
        self._complete_events(execution_arn=execution_arn)

    def on_stopped(self, execution_arn: str, error: ErrorObject) -> None:
        """Handle execution stop. Observer method triggered by notifier."""
        # This should not be called directly - stop_execution handles termination
        self.fail_execution(execution_arn, error)

    def on_callback_created(
        self,
        execution_arn: str,
        operation_id: str,
        callback_options: CallbackOptions | None,
        callback_token: CallbackToken,
    ) -> None:
        """Handle callback creation. Observer method triggered by notifier."""
        callback_id = callback_token.to_str()
        logger.debug(
            "[%s] Callback created for operation %s with callback_id: %s",
            execution_arn,
            operation_id,
            callback_id,
        )

        # Schedule callback timeouts if configured
        self._schedule_callback_timeouts(execution_arn, callback_options, callback_id)

    # endregion ExecutionObserver

    # region Callback Timeouts
    def _schedule_callback_timeouts(
        self,
        execution_arn: str,
        callback_options: CallbackOptions | None,
        callback_id: str,
    ) -> None:
        """Schedule callback timeout and heartbeat timeout if configured."""
        try:
            if not callback_options:
                return

            completion_event = self._completion_events.get(execution_arn)

            # Schedule main timeout if configured
            if callback_options.timeout_seconds > 0:

                def timeout_handler():
                    self._on_callback_timeout(execution_arn, callback_id)

                timeout_future = self._scheduler.call_later(
                    timeout_handler,
                    delay=callback_options.timeout_seconds,
                    completion_event=completion_event,
                )
                self._callback_timeouts[callback_id] = timeout_future

            # Schedule heartbeat timeout if configured
            if callback_options.heartbeat_timeout_seconds > 0:

                def heartbeat_timeout_handler():
                    self._on_callback_heartbeat_timeout(execution_arn, callback_id)

                heartbeat_future = self._scheduler.call_later(
                    heartbeat_timeout_handler,
                    delay=callback_options.heartbeat_timeout_seconds,
                    completion_event=completion_event,
                )
                self._callback_heartbeats[callback_id] = heartbeat_future

        except Exception:
            logger.exception(
                "[%s] Error scheduling callback timeouts for %s",
                execution_arn,
                callback_id,
            )

    def _reset_callback_heartbeat_timeout(
        self, callback_id: str, execution_arn: str
    ) -> None:
        """Reset the heartbeat timeout for a callback."""
        # Cancel existing heartbeat timeout
        if heartbeat_future := self._callback_heartbeats.pop(callback_id, None):
            heartbeat_future.cancel()

        # Find callback options to reschedule heartbeat timeout
        try:
            callback_token = CallbackToken.from_str(callback_id)
            execution = self.get_execution(callback_token.execution_arn)

            callback_options = None
            for update in execution.updates:
                if (
                    update.operation_id == callback_token.operation_id
                    and update.callback_options
                    and update.action.value == "START"
                ):
                    callback_options = update.callback_options
                    break

            if callback_options and callback_options.heartbeat_timeout_seconds > 0:

                def heartbeat_timeout_handler():
                    self._on_callback_heartbeat_timeout(execution_arn, callback_id)

                completion_event = self._completion_events.get(execution_arn)

                heartbeat_future = self._scheduler.call_later(
                    heartbeat_timeout_handler,
                    delay=callback_options.heartbeat_timeout_seconds,
                    completion_event=completion_event,
                )
                self._callback_heartbeats[callback_id] = heartbeat_future

        except Exception:
            logger.exception(
                "[%s] Error resetting callback heartbeat timeout for %s",
                execution_arn,
                callback_id,
            )

    def _cleanup_callback_timeouts(self, callback_id: str) -> None:
        """Clean up timeout events for a completed callback."""
        # Clean up main timeout
        if timeout_future := self._callback_timeouts.pop(callback_id, None):
            timeout_future.cancel()

        # Clean up heartbeat timeout
        if heartbeat_future := self._callback_heartbeats.pop(callback_id, None):
            heartbeat_future.cancel()

    def _on_callback_timeout(self, execution_arn: str, callback_id: str) -> None:
        """Handle callback timeout."""
        try:
            callback_token = CallbackToken.from_str(callback_id)
            with self._lock_for(callback_token.execution_arn):
                execution = self.get_execution(callback_token.execution_arn)

                if execution.is_complete:
                    return

                # Fail the callback with timeout error
                timeout_error = ErrorObject(
                    message="Callback timed out",
                    type=CallbackTimeoutType.TIMEOUT.value,
                    data=None,
                    stack_trace=None,
                )
                execution.complete_callback_timeout(callback_id, timeout_error)
                self._store.update(execution)
                logger.warning("[%s] Callback %s timed out", execution_arn, callback_id)
            self._invoke_execution(callback_token.execution_arn)
        except Exception:
            logger.exception(
                "[%s] Error processing callback timeout for %s",
                execution_arn,
                callback_id,
            )

    def _on_callback_heartbeat_timeout(
        self, execution_arn: str, callback_id: str
    ) -> None:
        """Handle callback heartbeat timeout."""
        try:
            callback_token = CallbackToken.from_str(callback_id)
            with self._lock_for(callback_token.execution_arn):
                execution = self.get_execution(callback_token.execution_arn)

                if execution.is_complete:
                    return

                # Fail the callback with heartbeat timeout error

                heartbeat_error = ErrorObject(
                    message="Callback timed out on heartbeat",
                    type=CallbackTimeoutType.HEARTBEAT.value,
                    data=None,
                    stack_trace=None,
                )
                execution.complete_callback_timeout(callback_id, heartbeat_error)
                self._store.update(execution)
                logger.warning(
                    "[%s] Callback %s heartbeat timed out", execution_arn, callback_id
                )
            self._invoke_execution(callback_token.execution_arn)
        except Exception:
            logger.exception(
                "[%s] Error processing callback heartbeat timeout for %s",
                execution_arn,
                callback_id,
            )

    # endregion Callback Timeouts
