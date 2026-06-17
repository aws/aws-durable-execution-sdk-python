"""Execution life-cycle logic."""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
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
    ExecutionObserver,
    apply_effects,
)
from aws_durable_execution_sdk_python_testing.token import (
    CallbackToken,
    CheckpointToken,
)
from aws_durable_execution_sdk_python_testing.worker.checkpoint_tasks import (
    CallableTask,
)
from aws_durable_execution_sdk_python_testing.worker.registry import ExecutionRegistry
from aws_durable_execution_sdk_python_testing.worker.status import InvocationState


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
        CheckpointEffect,
    )
    from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
        CheckpointProcessor,
    )
    from aws_durable_execution_sdk_python_testing.invoker import (
        Invoker,
        InvokeResponse,
    )
    from aws_durable_execution_sdk_python_testing.scheduler import Event, Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore

logger = logging.getLogger(__name__)


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
        registry: ExecutionRegistry | None = None,
    ):
        self._store = store
        self._scheduler = scheduler
        self._invoker = invoker
        self._checkpoint_processor = checkpoint_processor
        self._registry = (
            registry if registry is not None else ExecutionRegistry(store, scheduler)
        )
        self._invocation_timeout_seconds = invocation_timeout_seconds
        self._dispatcher = CheckpointRequestDispatcher()
        self._max_invocation_page_bytes = (
            max_invocation_page_bytes
            if max_invocation_page_bytes is not None
            else DEFAULT_MAX_INVOCATION_PAGE_BYTES
        )
        # Guards the pending-wakeup map below so concurrent callers
        # re-arming the earliest-pending timer can't leak a Future
        # into the dict.
        self._pending_wakeup_lock: threading.Lock = threading.Lock()
        # Single earliest-pending wake-up timer per execution: wait
        # timers and step retries share one timer that fires at their
        # earliest aggregate moment. Entries are cancelled on terminal
        # transitions and never persisted.
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
        return (
            self._registry.get_or_create(execution_arn)
            .submit(CallableTask(lambda: self._apply_stop(execution_arn, error)))
            .result()
        )

    def _apply_stop(
        self, execution_arn: str, error: ErrorObject | None
    ) -> StopDurableExecutionResponse:
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
        max_items: int | None = None,
    ) -> GetDurableExecutionStateResponse:
        """Return a page of operations, serialized on the execution's worker."""
        worker = self._registry.get_or_create(execution_arn)
        return (
            self._registry.get_or_create(execution_arn)
            .submit(
                CallableTask(
                    lambda: self._get_execution_state(
                        execution_arn, checkpoint_token, marker, max_items
                    )
                )
            )
            .result()
        )

    def _get_execution_state(
        self,
        execution_arn: str,
        checkpoint_token: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,  # noqa: ARG002 — kept for API compat; page is byte-bounded
    ) -> GetDurableExecutionStateResponse:
        """Return a page of operations from the pinned snapshot.

        Valid only while the execution is ``INVOKING``. The
        call is a pure read: no ``handler_seen_seq`` advance,
        no ``token_sequence`` bump, no idempotency mutation.

        Raises:
            ResourceNotFoundException: execution does not exist.
            InvalidParameterValueException: when the invocation gate
                is not ``INVOKING``, the token is stale, or the
                marker does not resolve against the pinned sequence.
        """
        execution = self.get_execution(execution_arn)

        # Invocation gate: reads are valid only while INVOKING.
        if self._invocation_gate(execution_arn) is not InvocationState.INVOKING:
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
        """Process a checkpoint, serializing it on the execution's worker.

        Routes through the per-execution worker so checkpoints for one
        execution never overlap.
        """
        worker = self._registry.get_or_create(execution_arn)
        return worker.submit(
            CallableTask(
                lambda: self._checkpoint_execution(
                    execution_arn, checkpoint_token, updates, client_token
                )
            )
        ).result()

    def _checkpoint_execution(
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

        Runs on the execution's worker lane, so concurrent callers
        against the same execution can never read a half-applied
        mutation.

        Raises:
            ResourceNotFoundException: If the execution does not exist.
            InvalidParameterValueException: If the checkpoint token is
                invalid (wrong ``token_sequence`` or the execution is
                already complete).
        """
        execution = self.get_execution(execution_arn)

        # Idempotency first. A caller retrying a previously-
        # successful checkpoint is entitled to the cached response
        # even if the execution has since moved on (though in
        # practice it won't — the SDK waits for a response before
        # advancing). Idempotency check runs before the token check.
        cached = self._maybe_replay_cached(execution, checkpoint_token, client_token)
        if cached is not None:
            return cached

        # Invocation-state gate: reject stale checkpoints from
        # handler processes that died or timed out and came back after
        # the worker tore down.
        if self._invocation_gate(execution_arn) is not InvocationState.INVOKING:
            msg = "Invalid checkpoint token"
            raise InvalidParameterValueException(msg)

        if not self._is_current_token(execution, checkpoint_token):
            msg = "Invalid checkpoint token"
            raise InvalidParameterValueException(msg)

        result = CheckpointCore.apply(
            execution,
            checkpoint_token,
            updates or [],
            client_token,
            self._dispatcher,
        )

        self._store.update(execution)

        response = CheckpointDurableExecutionResponse(
            checkpoint_token=result.checkpoint_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=result.operations,
                next_marker=None,
            ),
        )

        # Apply lifecycle effects after the write, not mid-apply:
        # completion and failure submit follow-up work onto this
        # execution's lane, which a running lane task cannot re-enter.
        apply_effects(result.effects, self)

        # Re-arm the earliest-pending wake-up so the next
        # scheduler-driven completion fires at the minimum pending
        # timestamp across Wait / Step ops.
        self._schedule_earliest_pending(execution_arn)

        return response

    def _maybe_replay_cached(
        self,
        execution: Execution,
        checkpoint_token: str,
        client_token: str | None,
    ) -> CheckpointDurableExecutionResponse | None:
        """Replay the cached response for a retried checkpoint call.

        Returns ``None`` unless the last checkpoint matches the incoming
        ``(client_token, checkpoint_token)`` pair. A match is a
        byte-identical replay; no state is mutated.
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

    def _invocation_gate(self, arn: str) -> InvocationState:
        """Read the invocation gate from the execution's worker.

        Absent worker means no invocation has started, i.e. PRE_INVOKE.
        """
        worker = self._registry.get(arn)
        return worker.status if worker is not None else InvocationState.PRE_INVOKE

    def _set_invocation_gate(self, arn: str, status: InvocationState) -> None:
        """Set the invocation gate on the execution's worker."""
        self._registry.get_or_create(arn).set_status(status)

    def _release_gate(self, arn: str, to: InvocationState) -> None:
        """Transition the invocation gate out of ``INVOKING``.

        Called from ``_on_invoke_finished_ok`` (PENDING → PRE_INVOKE
        or terminal → COMPLETED) and ``_handle_invoke_failure``
        (FAILED → PRE_INVOKE so a retry can re-enter).
        """
        self._set_invocation_gate(arn, to)

    def _cleanup_execution_state(self, arn: str) -> None:
        """Tear down per-execution Executor-side state when an
        execution reaches a terminal status. Removes entries from
        ``_callback_timeouts`` and ``_callback_heartbeats`` and cancels
        pending futures.
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

        * ``wait.scheduled_end_timestamp`` for ``STARTED`` Wait ops

        * ``step.next_attempt_timestamp`` for ``PENDING`` Step ops


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
            with self._pending_wakeup_lock:
                existing = self._pending_wakeup.pop(execution_arn, None)
            if existing is not None and not existing.done():
                existing.cancel()
            return

        earliest = self._earliest_pending_timestamp(execution)
        if earliest is None:
            with self._pending_wakeup_lock:
                existing = self._pending_wakeup.pop(execution_arn, None)
            if existing is not None and not existing.done():
                existing.cancel()
            return

        now = datetime.now(UTC)
        delay = max((earliest - now).total_seconds(), 0.0)

        completion_event = self._completion_events.get(execution_arn)
        # Cancel-then-arm atomically under the supervisor lock so
        # concurrent callers can't leave orphan Futures in the dict.
        with self._pending_wakeup_lock:
            existing = self._pending_wakeup.pop(execution_arn, None)
            future = self._scheduler.call_later(
                self._fire_due_and_invoke_handler(execution_arn),
                delay=delay,
                completion_event=completion_event,
            )
            self._pending_wakeup[execution_arn] = future
        if existing is not None and not existing.done():
            existing.cancel()

    def _fire_due_operations(self, execution_arn: str) -> bool:
        """Complete any Wait/Step ops whose scheduled moment has passed.

        Returns True if at least one operation completed (so the caller
        re-invokes the handler).
        """
        try:
            execution = self._store.load(execution_arn)
        except Exception:  # noqa: BLE001
            return False

        if execution.is_complete:
            return False

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
        return completed_any

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
            # Await the lane future on the scheduler loop via wrap_future
            # rather than asyncio.to_thread(...result()). The scheduler's
            # executor thread is occupied by the in-flight blocking invoke,
            # and an invocation that force-checkpoints depends on this timer
            # to complete the wait or retry it is blocked on. Bridging
            # through the executor would deadlock.
            future = self._registry.get_or_create(execution_arn).submit(
                CallableTask(lambda: self._fire_due_operations(execution_arn))
            )
            completed_any = await asyncio.wrap_future(future)
            # Outside the lane: invoke and arm the next wake-up.
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
            arn = callback_token.execution_arn
            self._registry.get_or_create(arn).submit(
                CallableTask(
                    lambda: self._complete_callback_success(callback_id, result)
                )
            ).result()
            self._invoke_execution(arn)
            logger.info("Callback success completed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback success: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackSuccessResponse()

    def _complete_callback_success(
        self, callback_id: str, result: bytes | None
    ) -> None:
        callback_token = CallbackToken.from_str(callback_id)
        execution = self.get_execution(callback_token.execution_arn)
        execution.complete_callback_success(callback_id, result)
        self._store.update(execution)
        self._cleanup_callback_timeouts(callback_id)

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
        # error fields absent, not synthesize an empty
        # ErrorMessage. ErrorObject.to_dict() omits None fields, so an all-None
        # error serializes to {} and the SDK surfaces undefined fields.
        callback_error: ErrorObject = error or ErrorObject(
            message=None, type=None, data=None, stack_trace=None
        )

        try:
            callback_token: CallbackToken = CallbackToken.from_str(callback_id)
            arn = callback_token.execution_arn
            self._registry.get_or_create(arn).submit(
                CallableTask(
                    lambda: self._complete_callback_failure(callback_id, callback_error)
                )
            ).result()
            self._invoke_execution(arn)
            logger.info("Callback failure completed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback failure: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackFailureResponse()

    def _complete_callback_failure(
        self, callback_id: str, callback_error: ErrorObject
    ) -> None:
        callback_token = CallbackToken.from_str(callback_id)
        execution = self.get_execution(callback_token.execution_arn)
        execution.complete_callback_failure(callback_id, callback_error)
        self._store.update(execution)
        self._cleanup_callback_timeouts(callback_id)

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
            arn = callback_token.execution_arn
            self._registry.get_or_create(arn).submit(
                CallableTask(lambda: self._process_callback_heartbeat(callback_id))
            ).result()
            logger.info("Callback heartbeat processed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback heartbeat: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackHeartbeatResponse()

    def _process_callback_heartbeat(self, callback_id: str) -> None:
        callback_token = CallbackToken.from_str(callback_id)
        execution = self.get_execution(callback_token.execution_arn)

        # Verify the callback operation exists and is active.
        _, operation = execution.find_callback_operation(callback_id)
        if operation.status != OperationStatus.STARTED:
            msg = f"Callback {callback_id} is not active"
            raise ResourceNotFoundException(msg)

        self._reset_callback_heartbeat_timeout(
            callback_id, execution.durable_execution_arn
        )

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

    def _begin_invocation(
        self, execution_arn: str
    ) -> tuple[Execution, DurableExecutionInvocationInput] | None:
        """Claim the invocation gate and build the handler input.

        Returns the execution and its invocation input when this call
        claims the gate (PRE_INVOKE -> INVOKING); returns None when the
        execution is already complete, the gate is COMPLETED, or another
        invocation is in flight (recording a deferred re-invoke).
        """
        execution = self._store.load(execution_arn)

        if execution.is_complete:
            logger.info(
                "[%s] Execution already completed, ignoring invoke",
                execution_arn,
            )
            self._set_invocation_gate(execution_arn, InvocationState.COMPLETED)
            return None

        current_state = self._invocation_gate(execution_arn)
        if current_state is InvocationState.COMPLETED:
            logger.info(
                "[%s] Invocation state already COMPLETED, not re-invoking",
                execution_arn,
            )
            return None
        if current_state is InvocationState.INVOKING:
            # Another invocation is in flight. Record the
            # re-invoke intent; the in-flight handler's post-invoke
            # hook consults needs_reinvoke and schedules a follow-up.
            execution.needs_reinvoke = True
            self._store.save(execution)
            logger.info(
                "[%s] Handler already INVOKING; deferring re-invoke",
                execution_arn,
            )
            return None

        # Claim the gate: at most one handler invocation per
        # execution in flight.
        self._set_invocation_gate(execution_arn, InvocationState.INVOKING)
        execution.begin_new_invocation()
        invocation_input = self._invoker.create_invocation_input(execution=execution)
        self._store.save(execution)
        return execution, invocation_input

    def _finish_invocation(
        self,
        execution_arn: str,
        invoke_response: InvokeResponse,
        invocation_start: datetime,
        invocation_end: datetime,
    ) -> None:
        """Apply a completed handler invocation.

        Records the attempt, validates the response (completing or
        retrying), resets the retry counter on a clean run, and releases
        the gate to PRE_INVOKE (re-invoking if a trigger was deferred) or
        COMPLETED.
        """
        execution = self._store.load(execution_arn)

        execution.record_invocation_completion(
            invocation_start,
            invocation_end,
            invoke_response.request_id,
        )
        self._store.save(execution)

        if execution.is_complete:
            # A stop / timeout landed while we were invoking — the
            # terminal state is authoritative; the handler's
            # response does not overwrite it.
            logger.info(
                "[%s] Execution completed during invocation, ignoring result",
                execution_arn,
            )
            self._set_invocation_gate(execution_arn, InvocationState.COMPLETED)
            return

        response = invoke_response.invocation_output
        try:
            self._validate_invocation_response_and_store(
                execution_arn, response, execution
            )
        except (InvalidParameterValueException, IllegalStateException) as e:
            logger.warning(
                "[%s] Lambda output validation failure: %s", execution_arn, e
            )
            error_obj = ErrorObject.from_exception(e)
            # Release the gate before retry scheduling so the retry's
            # invoke finds PRE_INVOKE.
            self._set_invocation_gate(execution_arn, InvocationState.PRE_INVOKE)
            self._retry_invocation(execution, error_obj)
            return

        # A clean invocation resets the retry counter.
        reset_target = self._store.load(execution_arn)
        reset_target.consecutive_failed_invocation_attempts = 0
        self._store.save(reset_target)

        # Release the gate: terminal -> COMPLETED, otherwise PRE_INVOKE
        # (re-invoking if a trigger was deferred mid-invocation).
        reloaded = self._store.load(execution_arn)
        if reloaded.is_complete:
            self._set_invocation_gate(execution_arn, InvocationState.COMPLETED)
            should_reinvoke = False
        else:
            self._set_invocation_gate(execution_arn, InvocationState.PRE_INVOKE)
            should_reinvoke = reloaded.needs_reinvoke
            if should_reinvoke:
                reloaded.needs_reinvoke = False
                self._store.save(reloaded)

        if should_reinvoke:
            self._invoke_execution(execution_arn)
        else:
            # Arm the earliest-pending wake-up.
            self._schedule_earliest_pending(execution_arn)

    def _fail_invocation_not_found(
        self, execution_arn: str, error: ErrorObject
    ) -> None:
        self._set_invocation_gate(execution_arn, InvocationState.PRE_INVOKE)
        self._fail_workflow(execution_arn, error)

    def _retry_after_timeout(
        self,
        execution_arn: str,
        error: ErrorObject,
        invocation_start: datetime,
        invocation_end: datetime,
    ) -> None:
        execution = self._store.load(execution_arn)
        execution.record_invocation_completion(
            invocation_start, invocation_end, str(uuid.uuid4())
        )
        self._store.save(execution)
        self._set_invocation_gate(execution_arn, InvocationState.PRE_INVOKE)
        self._retry_invocation(execution, error)

    def _retry_after_error(self, execution_arn: str, error: ErrorObject) -> None:
        self._set_invocation_gate(execution_arn, InvocationState.PRE_INVOKE)
        try:
            execution = self._store.load(execution_arn)
        except Exception:  # noqa: BLE001
            return
        self._retry_invocation(execution, error)

    def _invoke_handler(self, execution_arn: str) -> Callable[[], Awaitable[None]]:
        """Create a parameterless callable that captures execution arn for the scheduler."""

        async def invoke() -> None:
            # The gate claim, the blocking Lambda invoke, and the
            # post-invoke processing run as separate steps: the two
            # state-mutation windows run on the execution's worker lane
            # (serialized with checkpoints), while the blocking invoke
            # runs off the lane so the handler's checkpoints can run on
            # it.
            try:
                claim = await asyncio.to_thread(
                    lambda: self._registry.get_or_create(execution_arn)
                    .submit(CallableTask(lambda: self._begin_invocation(execution_arn)))
                    .result()
                )
                if claim is None:
                    return
                execution, invocation_input = claim

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
                await asyncio.to_thread(
                    lambda: self._registry.get_or_create(execution_arn)
                    .submit(
                        CallableTask(
                            lambda: self._finish_invocation(
                                execution_arn,
                                invoke_response,
                                invocation_start,
                                invocation_end,
                            )
                        )
                    )
                    .result()
                )

            except ResourceNotFoundException:
                logger.warning("[%s] Function No longer exists", execution_arn)
                error_obj = ErrorObject.from_message(message="Function not found")
                await asyncio.to_thread(
                    lambda: self._registry.get_or_create(execution_arn)
                    .submit(
                        CallableTask(
                            lambda: self._fail_invocation_not_found(
                                execution_arn, error_obj
                            )
                        )
                    )
                    .result()
                )

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
                error_obj = ErrorObject.from_message(
                    message=f"Function timed out after {self._invocation_timeout_seconds} seconds"
                )
                await asyncio.to_thread(
                    lambda: self._registry.get_or_create(execution_arn)
                    .submit(
                        CallableTask(
                            lambda: self._retry_after_timeout(
                                execution_arn,
                                error_obj,
                                invocation_start,
                                invocation_end,
                            )
                        )
                    )
                    .result()
                )

            except Exception as e:  # noqa: BLE001
                # Handle invocation errors (network, function not found, etc.)
                logger.warning("[%s] Invocation failed: %s", execution_arn, e)
                error_obj = ErrorObject.from_exception(e)
                await asyncio.to_thread(
                    lambda: self._registry.get_or_create(execution_arn)
                    .submit(
                        CallableTask(
                            lambda: self._retry_after_error(execution_arn, error_obj)
                        )
                    )
                    .result()
                )

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
        """Handle execution timeout (workflow timeout)."""
        logger.exception("[%s] Execution timed out.", execution_arn)
        self._registry.get_or_create(execution_arn).submit(
            CallableTask(lambda: self._apply_timeout(execution_arn, error))
        ).result()
        self._complete_events(execution_arn=execution_arn)

    def _apply_timeout(self, execution_arn: str, error: ErrorObject) -> None:
        execution = self._store.load(execution_arn=execution_arn)
        execution.complete_timeout(error=error)  # Sets CloseStatus.TIMED_OUT
        self._store.update(execution)

    def on_stopped(self, execution_arn: str, error: ErrorObject) -> None:
        """Handle execution stop."""
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

    def _apply_callback_timeout(self, callback_id: str, error: ErrorObject) -> None:
        callback_token = CallbackToken.from_str(callback_id)
        execution = self.get_execution(callback_token.execution_arn)
        if execution.is_complete:
            return
        execution.complete_callback_timeout(callback_id, error)
        self._store.update(execution)

    def _on_callback_timeout(self, execution_arn: str, callback_id: str) -> None:
        """Handle callback timeout."""
        try:
            callback_token = CallbackToken.from_str(callback_id)
            arn = callback_token.execution_arn
            timeout_error = ErrorObject(
                message="Callback timed out",
                type=CallbackTimeoutType.TIMEOUT.value,
                data=None,
                stack_trace=None,
            )
            self._registry.get_or_create(arn).submit(
                CallableTask(
                    lambda: self._apply_callback_timeout(callback_id, timeout_error)
                )
            ).result()
            logger.warning("[%s] Callback %s timed out", execution_arn, callback_id)
            self._invoke_execution(arn)
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
            arn = callback_token.execution_arn
            heartbeat_error = ErrorObject(
                message="Callback timed out on heartbeat",
                type=CallbackTimeoutType.HEARTBEAT.value,
                data=None,
                stack_trace=None,
            )
            self._registry.get_or_create(arn).submit(
                CallableTask(
                    lambda: self._apply_callback_timeout(callback_id, heartbeat_error)
                )
            ).result()
            logger.warning(
                "[%s] Callback %s heartbeat timed out", execution_arn, callback_id
            )
            self._invoke_execution(arn)
        except Exception:
            logger.exception(
                "[%s] Error processing callback heartbeat timeout for %s",
                execution_arn,
                callback_id,
            )

    # endregion Callback Timeouts
