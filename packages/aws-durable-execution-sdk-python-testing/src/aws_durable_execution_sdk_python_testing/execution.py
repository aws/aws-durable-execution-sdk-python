from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from threading import Lock
from typing import Any
from uuid import uuid4

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    IllegalStateException,
    InvalidParameterValueException,
)

# Import AWS exceptions
from aws_durable_execution_sdk_python_testing.model import (
    InvocationCompletedDetails,
    StartDurableExecutionInput,
)
from aws_durable_execution_sdk_python_testing.token import (
    CheckpointToken,
)


class ExecutionStatus(Enum):
    """Execution status for API responses."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"
    TIMED_OUT = "TIMED_OUT"


@dataclass(frozen=True)
class CheckpointIdempotencyRecord:
    """Single-slot cache of the most recent accepted checkpoint response.

    A retried checkpoint call with the same
    ``(client_token, inbound_checkpoint_token)`` pair is entitled to a
    byte-identical response. This record is what we compare against and
    replay from.
    """

    client_token: str
    inbound_checkpoint_token: str
    outbound_checkpoint_token: str
    operations: list[Operation]
    next_marker: str | None

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict for persistence."""
        return {
            "ClientToken": self.client_token,
            "InboundCheckpointToken": self.inbound_checkpoint_token,
            "OutboundCheckpointToken": self.outbound_checkpoint_token,
            "Operations": [op.to_json_dict() for op in self.operations],
            "NextMarker": self.next_marker,
        }

    @classmethod
    def from_json_dict(cls, data: dict[str, Any]) -> CheckpointIdempotencyRecord:
        """Reconstruct from a to_json_dict output."""
        return cls(
            client_token=data["ClientToken"],
            inbound_checkpoint_token=data["InboundCheckpointToken"],
            outbound_checkpoint_token=data["OutboundCheckpointToken"],
            operations=[
                Operation.from_json_dict(op_data) for op_data in data["Operations"]
            ],
            next_marker=data.get("NextMarker"),
        )


class Execution:
    """Execution state."""

    def __init__(
        self,
        durable_execution_arn: str,
        start_input: StartDurableExecutionInput,
        operations: list[Operation],
    ):
        self.durable_execution_arn: str = durable_execution_arn
        # operation is frozen, it won't mutate - no need to clone/deep-copy
        self.start_input: StartDurableExecutionInput = start_input
        self.operations: list[Operation] = operations
        self.updates: list[OperationUpdate] = []
        self.update_timestamps: list[datetime] = []
        self.invocation_completions: list[InvocationCompletedDetails] = []
        self.updated_operation_ids: list[str] = []
        # Two monotonic counters. `token_sequence` is the checkpoint-response
        # version carried on the wire; `seq_counter` is the internal
        # event counter bumped on every state-affecting mutation.
        self.token_sequence: int = 0
        self.seq_counter: int = 0
        # Watermark of the highest `seq_counter` the handler has been told
        # about (via InitialExecutionState or a successful checkpoint
        # response). Filters the "unseen" delta on the next checkpoint.
        self.handler_seen_seq: int = 0
        # Per-op "last touched at `seq_counter`" bookkeeping. Sparse:
        # ops not in the dict are treated as touch-seq 0.
        self.operation_last_touched_seq: dict[str, int] = {}
        # Per-op payload size, tracked as a sidecar dict because
        # ``Operation`` is frozen upstream.
        self.operation_size_bytes: dict[str, int] = {}
        # Set when a trigger arrived while an invocation was already
        # in flight; the gate-release path consults it to decide whether
        # to schedule another invocation.
        self.needs_reinvoke: bool = False
        # Single-slot cache for idempotent checkpoint replays.
        self.last_checkpoint: CheckpointIdempotencyRecord | None = None
        # Identity of the handler invocation currently allowed to
        # checkpoint. Regenerated each time a new invocation is
        # dispatched, so a checkpoint carrying a superseded invocation's
        # token is rejected. Held in memory semantics: it defaults empty
        # for executions that never went through an invocation.
        self.current_invocation_id: str = ""
        self._state_lock: Lock = Lock()
        self.is_complete: bool = False
        self.result: DurableExecutionInvocationOutput | None = None
        self.consecutive_failed_invocation_attempts: int = 0
        self.close_status: ExecutionStatus | None = None

    def touch_operation(self, operation_id: str) -> None:
        """Record a state-affecting event on an operation.

        Bumps ``seq_counter`` by 1 and records the new value as the
        operation's "last touched" sequence. Called from the checkpoint
        dispatcher (once per accepted update) and from every async
        completion path (``complete_wait``, ``complete_retry``,
        ``complete_callback_*``, terminal transitions).
        """
        self.seq_counter += 1
        self.operation_last_touched_seq[operation_id] = self.seq_counter

    def advance_token_sequence(self) -> int:
        """Bump the checkpoint-response version counter.

        Called exactly once per accepted non-idempotent checkpoint call.
        Idempotent replays and async completions do not call this.
        Returns the new value.
        """
        self.token_sequence += 1
        return self.token_sequence

    def begin_new_invocation(self) -> str:
        """Assign a fresh identity to the next handler invocation and
        return it. A checkpoint token minted for a prior invocation no
        longer matches once this is called, so a superseded invocation's
        checkpoints are rejected.
        """
        self.current_invocation_id = str(uuid4())
        return self.current_invocation_id

    def current_status(self) -> ExecutionStatus:
        """Get execution status."""
        if not self.is_complete:
            return ExecutionStatus.RUNNING

        if not self.close_status:
            msg: str = "close_status must be set when execution is complete"
            raise IllegalStateException(msg)

        return self.close_status

    @staticmethod
    def new(input: StartDurableExecutionInput) -> Execution:  # noqa: A002
        # make a nicer arn
        # Pattern: arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}(-gov)?-[a-z]+-\d{1}:\d{12}:durable-execution:[a-zA-Z0-9-_\.]+:[a-zA-Z0-9-_\.]+:[a-zA-Z0-9-_\.]+
        # Example: arn:aws:lambda:us-east-1:123456789012:durable-execution:myDurableFunction:myDurableExecutionName:ce67da72-3701-4f83-9174-f4189d27b0a5
        return Execution(
            durable_execution_arn=str(uuid4())
            + "/"
            + (input.invocation_id or str(uuid4())),
            start_input=input,
            operations=[],
        )

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize execution to JSON-serializable dictionary"""
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "StartInput": self.start_input.to_dict(),
            "Operations": [op.to_json_dict() for op in self.operations],
            "Updates": [update.to_dict() for update in self.updates],
            "UpdateTimestamps": [ts.isoformat() for ts in self.update_timestamps],
            "InvocationCompletions": [
                completion.to_json_dict() for completion in self.invocation_completions
            ],
            "UpdatedOperationIds": self.updated_operation_ids,
            "TokenSequence": self.token_sequence,
            "SeqCounter": self.seq_counter,
            "HandlerSeenSeq": self.handler_seen_seq,
            "OperationLastTouchedSeq": dict(self.operation_last_touched_seq),
            "OperationSizeBytes": dict(self.operation_size_bytes),
            "NeedsReinvoke": self.needs_reinvoke,
            "LastCheckpoint": (
                self.last_checkpoint.to_json_dict() if self.last_checkpoint else None
            ),
            "IsComplete": self.is_complete,
            "Result": self.result.to_dict() if self.result else None,
            "ConsecutiveFailedInvocationAttempts": self.consecutive_failed_invocation_attempts,
            "CloseStatus": self.close_status.value if self.close_status else None,
            "CurrentInvocationId": self.current_invocation_id,
        }

    @classmethod
    def from_json_dict(cls, data: dict[str, Any]) -> Execution:
        """Deserialize execution from dictionary."""
        # Reconstruct start_input
        start_input = StartDurableExecutionInput.from_dict(data["StartInput"])

        # Reconstruct operations
        operations = [
            Operation.from_json_dict(op_data) for op_data in data["Operations"]
        ]

        # Create execution
        execution = cls(
            durable_execution_arn=data["DurableExecutionArn"],
            start_input=start_input,
            operations=operations,
        )

        # Set additional fields
        execution.updates = [
            OperationUpdate.from_dict(update_data) for update_data in data["Updates"]
        ]
        execution.update_timestamps = [
            datetime.fromisoformat(ts) for ts in data.get("UpdateTimestamps", [])
        ]
        execution.invocation_completions = [
            InvocationCompletedDetails.from_json_dict(item)
            for item in data.get("InvocationCompletions", [])
        ]
        execution.updated_operation_ids = list(data.get("UpdatedOperationIds", []))
        # NOTE: prior format included a "UsedTokens" set. Current
        # removed it (the Executor no longer consults it — token
        # validity is via _is_current_token against token_sequence).
        # Old-format dicts are loaded by ignoring "UsedTokens".
        execution.token_sequence = data["TokenSequence"]
        # Safe defaults for fields added after the original schema.
        execution.seq_counter = data.get("SeqCounter", 0)
        execution.handler_seen_seq = data.get("HandlerSeenSeq", 0)
        execution.operation_last_touched_seq = dict(
            data.get("OperationLastTouchedSeq", {})
        )
        execution.operation_size_bytes = dict(data.get("OperationSizeBytes", {}))
        execution.needs_reinvoke = data.get("NeedsReinvoke", False)
        execution.current_invocation_id = data.get("CurrentInvocationId", "")
        last_checkpoint_data = data.get("LastCheckpoint")
        execution.last_checkpoint = (
            CheckpointIdempotencyRecord.from_json_dict(last_checkpoint_data)
            if last_checkpoint_data
            else None
        )
        execution.is_complete = data["IsComplete"]
        execution.result = (
            DurableExecutionInvocationOutput.from_dict(data["Result"])
            if data["Result"]
            else None
        )
        execution.consecutive_failed_invocation_attempts = data[
            "ConsecutiveFailedInvocationAttempts"
        ]
        close_status_str = data.get("CloseStatus")
        execution.close_status = (
            ExecutionStatus(close_status_str) if close_status_str else None
        )

        return execution

    def start(self) -> None:
        if self.start_input.invocation_id is None:
            msg: str = "invocation_id is required"
            raise InvalidParameterValueException(msg)
        with self._state_lock:
            self.operations.append(
                Operation(
                    operation_id=self.start_input.invocation_id,
                    parent_id=None,
                    name=self.start_input.execution_name,
                    start_timestamp=datetime.now(UTC),
                    operation_type=OperationType.EXECUTION,
                    status=OperationStatus.STARTED,
                    execution_details=ExecutionDetails(
                        input_payload=self.start_input.get_normalized_input()
                    ),
                )
            )

    def get_operation_execution_started(self) -> Operation:
        if not self.operations:
            msg: str = "execution not started."

            raise IllegalStateException(msg)

        return self.operations[0]

    def get_new_checkpoint_token(self) -> str:
        """Serialise the current ``token_sequence`` as a checkpoint token.

        Does NOT bump ``token_sequence``. Only
        ``Executor.checkpoint_execution`` (via
        :meth:`advance_token_sequence`) is allowed to advance the
        counter. This method retains its name for backward
        compatibility but is now semantically
        "get_current_checkpoint_token".
        """
        with self._state_lock:
            token = CheckpointToken(
                execution_arn=self.durable_execution_arn,
                token_sequence=self.token_sequence,
                invocation_id=self.current_invocation_id,
            )
            return token.to_str()

    def get_navigable_operations(self) -> list[Operation]:
        """Return a snapshot copy of the operation list.

        The copy is taken under ``_state_lock`` so callers iterating the
        result are not exposed to concurrent operation mutations. Operations
        are frozen dataclasses, so a shallow copy is a stable snapshot.
        """
        with self._state_lock:
            return list(self.operations)

    def get_assertable_operations(self) -> list[Operation]:
        """Get list of operations, but exclude the EXECUTION operations"""
        # TODO: this excludes EXECUTION at start, but can there be an EXECUTION at the end if there was a checkpoint with large payload?
        return self.operations[1:]

    def has_pending_operations(self, execution: Execution) -> bool:
        """True if execution has pending operations."""

        for operation in execution.operations:
            if (
                operation.operation_type == OperationType.STEP
                and operation.status == OperationStatus.PENDING
            ) or (
                operation.operation_type
                in [
                    OperationType.WAIT,
                    OperationType.CALLBACK,
                    OperationType.CHAINED_INVOKE,
                ]
                and operation.status == OperationStatus.STARTED
            ):
                return True
        return False

    def record_invocation_completion(
        self, start_timestamp: datetime, end_timestamp: datetime, request_id: str
    ) -> None:
        """Record an invocation completion event."""
        self.invocation_completions.append(
            InvocationCompletedDetails(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                request_id=request_id,
            )
        )
        self.updated_operation_ids = []

    def _record_updated_operation(self, operation_id: str) -> None:
        """Remember an operation changed outside the last invocation."""
        if operation_id not in self.updated_operation_ids:
            self.updated_operation_ids.append(operation_id)

    def complete_success(self, result: str | None) -> None:
        """Complete execution successfully (DecisionType.COMPLETE_WORKFLOW_EXECUTION)."""
        self.result = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=result
        )
        self.is_complete = True
        self.close_status = ExecutionStatus.SUCCEEDED
        self._end_execution(OperationStatus.SUCCEEDED)

    def complete_fail(self, error: ErrorObject) -> None:
        """Complete execution with failure (DecisionType.FAIL_WORKFLOW_EXECUTION)."""
        self.result = DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED, error=error
        )
        self.is_complete = True
        self.close_status = ExecutionStatus.FAILED
        self._end_execution(OperationStatus.FAILED)

    def complete_timeout(self, error: ErrorObject) -> None:
        """Complete execution with timeout."""
        self.result = DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED, error=error
        )
        self.is_complete = True
        self.close_status = ExecutionStatus.TIMED_OUT
        self._end_execution(OperationStatus.TIMED_OUT)

    def complete_stopped(self, error: ErrorObject) -> None:
        """Complete execution as terminated (TerminateWorkflowExecutionV2Request)."""
        self.result = DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED, error=error
        )
        self.is_complete = True
        self.close_status = ExecutionStatus.STOPPED
        self._end_execution(OperationStatus.STOPPED)

    def find_operation(self, operation_id: str) -> tuple[int, Operation]:
        """Find operation by ID, return index and operation."""
        for i, operation in enumerate(self.operations):
            if operation.operation_id == operation_id:
                return i, operation
        msg: str = f"Attempting to update state of an Operation [{operation_id}] that doesn't exist"
        raise IllegalStateException(msg)

    def find_callback_operation(self, callback_id: str) -> tuple[int, Operation]:
        """Find callback operation by callback_id, return index and operation."""
        for i, operation in enumerate(self.operations):
            if (
                operation.operation_type == OperationType.CALLBACK
                and operation.callback_details
                and operation.callback_details.callback_id == callback_id
            ):
                return i, operation
        msg: str = f"Callback operation with callback_id [{callback_id}] not found"
        raise IllegalStateException(msg)

    def complete_wait(self, operation_id: str) -> Operation:
        """Complete WAIT operation when timer fires."""
        index, operation = self.find_operation(operation_id)

        # Validate
        if operation.status != OperationStatus.STARTED:
            msg_wait_not_started: str = f"Attempting to transition a Wait Operation[{operation_id}] to SUCCEEDED when it's not STARTED"
            raise IllegalStateException(msg_wait_not_started)
        if operation.operation_type != OperationType.WAIT:
            msg_not_wait: str = (
                f"Expected WAIT operation, got {operation.operation_type}"
            )
            raise IllegalStateException(msg_not_wait)

        # Thread-safe increment sequence and operation update
        with self._state_lock:
            self.touch_operation(operation_id)
            # Build and assign updated operation
            self.operations[index] = replace(
                operation,
                status=OperationStatus.SUCCEEDED,
                end_timestamp=datetime.now(UTC),
            )
            self._record_updated_operation(operation_id)
            return self.operations[index]

    def complete_retry(self, operation_id: str) -> Operation:
        """Complete STEP retry when timer fires."""
        index, operation = self.find_operation(operation_id)

        # Validate
        if operation.status != OperationStatus.PENDING:
            msg_step_not_pending: str = f"Attempting to transition a Step Operation[{operation_id}] to READY when it's not PENDING"
            raise IllegalStateException(msg_step_not_pending)
        if operation.operation_type != OperationType.STEP:
            msg_not_step: str = (
                f"Expected STEP operation, got {operation.operation_type}"
            )
            raise IllegalStateException(msg_not_step)

        # Thread-safe increment sequence and operation update
        with self._state_lock:
            self.touch_operation(operation_id)
            # Build updated step_details with cleared next_attempt_timestamp
            new_step_details = None
            if operation.step_details:
                new_step_details = replace(
                    operation.step_details, next_attempt_timestamp=None
                )

            # Build updated operation
            updated_operation = replace(
                operation, status=OperationStatus.READY, step_details=new_step_details
            )

            # Assign
            self.operations[index] = updated_operation
            self._record_updated_operation(operation_id)
            return updated_operation

    def complete_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> Operation:
        """Complete CALLBACK operation with success."""
        index, operation = self.find_callback_operation(callback_id)
        if operation.status != OperationStatus.STARTED:
            msg: str = f"Callback operation [{callback_id}] is not in STARTED state"
            raise IllegalStateException(msg)

        with self._state_lock:
            self.touch_operation(operation.operation_id)
            updated_callback_details = None
            if operation.callback_details:
                updated_callback_details = replace(
                    operation.callback_details,
                    result=result.decode() if result else None,
                )

            self.operations[index] = replace(
                operation,
                status=OperationStatus.SUCCEEDED,
                end_timestamp=datetime.now(UTC),
                callback_details=updated_callback_details,
            )
            return self.operations[index]

    def complete_callback_failure(
        self, callback_id: str, error: ErrorObject
    ) -> Operation:
        """Complete CALLBACK operation with failure."""
        index, operation = self.find_callback_operation(callback_id)

        if operation.status != OperationStatus.STARTED:
            msg: str = f"Callback operation [{callback_id}] is not in STARTED state"
            raise IllegalStateException(msg)

        with self._state_lock:
            self.touch_operation(operation.operation_id)
            updated_callback_details = None
            if operation.callback_details:
                updated_callback_details = replace(
                    operation.callback_details, error=error
                )

            self.operations[index] = replace(
                operation,
                status=OperationStatus.FAILED,
                end_timestamp=datetime.now(UTC),
                callback_details=updated_callback_details,
            )
            return self.operations[index]

    def complete_callback_timeout(
        self, callback_id: str, error: ErrorObject
    ) -> Operation:
        """Complete CALLBACK operation with timeout."""
        index, operation = self.find_callback_operation(callback_id)

        if operation.status != OperationStatus.STARTED:
            msg: str = f"Callback operation [{callback_id}] is not in STARTED state"
            raise IllegalStateException(msg)

        with self._state_lock:
            self.touch_operation(operation.operation_id)
            updated_callback_details = None
            if operation.callback_details:
                updated_callback_details = replace(
                    operation.callback_details, error=error
                )

            self.operations[index] = replace(
                operation,
                status=OperationStatus.TIMED_OUT,
                end_timestamp=datetime.now(UTC),
                callback_details=updated_callback_details,
            )
            return self.operations[index]

    def _end_execution(self, status: OperationStatus) -> None:
        """Set the end_timestamp on the main EXECUTION operation when execution completes."""
        execution_op: Operation = self.get_operation_execution_started()
        if execution_op.operation_type == OperationType.EXECUTION:
            with self._state_lock:
                # Terminal transition of the EXECUTION op is a real
                # state change — record it via touch_operation so
                # introspection / GetDurableExecutionState reflects
                # it. The handler has returned by this point, so the
                # touch is not load-bearing for a checkpoint delta.
                self.touch_operation(execution_op.operation_id)
                self.operations[0] = replace(
                    execution_op,
                    status=status,
                    end_timestamp=datetime.now(UTC),
                )


@dataclass(frozen=True)
class OperationPaginatorState:
    """Pinned context for serving operation state to the handler.

    One class does three jobs that share a pinned snapshot of the
    execution's operation graph:

    * :meth:`page` — bytes-bounded pagination across the snapshot,
      used by invocation-input construction and
      ``GetDurableExecutionState``.
    * :meth:`unseen_operations` — strict ``> handler_seen_seq``
      delta filter, used by ``checkpoint_execution`` to decide which
      ops go back to the handler.
    * :meth:`advance_handler_seen` — monotonic forward update of the
      delivery watermark on the wrapped :class:`Execution`, called
      after a checkpoint response is constructed and its ops are
      known.

    Pinning ``token_sequence`` and ``operations`` at construction
    guarantees that paginated reads and delta computation see the same
    snapshot, even if the underlying :class:`Execution` is mutated
    between method calls.
    """

    execution: Execution
    pinned_token_sequence: int
    snapshot_operations: list[Operation]

    @classmethod
    def pin(cls, execution: Execution) -> OperationPaginatorState:
        """Capture the execution's current token_sequence and operation
        list. Subsequent mutations on the Execution do not affect this
        instance's ``page`` / ``unseen_operations`` results."""
        return cls(
            execution=execution,
            pinned_token_sequence=execution.token_sequence,
            snapshot_operations=list(execution.operations),
        )

    def page(
        self,
        marker: str | None,
        max_size_bytes: int,
    ) -> tuple[list[Operation], str | None]:
        """Return a page of ops starting after ``marker``, bounded by
        ``max_size_bytes``. Second element of the tuple is a marker for
        the next page, or ``None`` when the page fits everything."""
        start_idx = self._resolve_marker(marker)
        return self._walk_page(self.snapshot_operations, start_idx, max_size_bytes)

    def unseen_operations(self) -> list[Operation]:
        """Operations whose ``operation_last_touched_seq`` is strictly
        greater than the wrapped execution's ``handler_seen_seq``,
        in creation order.

        Ops absent from ``operation_last_touched_seq`` are treated as
        touch-seq 0 and never appear in a delta once the
        watermark has moved.
        """
        touched = self.execution.operation_last_touched_seq
        cutoff = self.execution.handler_seen_seq
        return [
            op
            for op in self.snapshot_operations
            if touched.get(op.operation_id, 0) > cutoff
        ]

    def advance_handler_seen(self, seq: int) -> None:
        """Advance the wrapped execution's ``handler_seen_seq`` to
        ``seq`` if that represents forward progress. Monotonic:
        smaller or equal values are ignored."""
        if seq > self.execution.handler_seen_seq:
            self.execution.handler_seen_seq = seq

    # --- internals -------------------------------------------------

    def _walk_page(
        self,
        ops: list[Operation],
        start_idx: int,
        max_size_bytes: int,
    ) -> tuple[list[Operation], str | None]:
        selected: list[Operation] = []
        total = 0
        for i in range(start_idx, len(ops)):
            op = ops[i]
            size = self._size_for(op)
            if selected and total + size > max_size_bytes:
                return selected, self._encode_marker(i)
            selected.append(op)
            total += size
        return selected, None

    def _size_for(self, op: Operation) -> int:
        # Zero-size ops are possible (no payload / no error / no input);
        # floor at 1 to guarantee pagination always advances.
        return max(self.execution.operation_size_bytes.get(op.operation_id, 0), 1)

    def _resolve_marker(self, marker: str | None) -> int:
        if marker is None:
            return 0
        seq, idx = self._decode_marker(marker)
        if seq != self.pinned_token_sequence:
            msg = "Invalid marker"
            raise InvalidParameterValueException(msg)
        if idx < 0 or idx > len(self.snapshot_operations):
            msg = "Invalid marker"
            raise InvalidParameterValueException(msg)
        return idx

    def _encode_marker(self, next_idx: int) -> str:
        return f"{self.pinned_token_sequence}:{next_idx}"

    @staticmethod
    def _decode_marker(marker: str) -> tuple[int, int]:
        try:
            seq_part, idx_part = marker.split(":", 1)
            return int(seq_part), int(idx_part)
        except (ValueError, AttributeError) as exc:
            msg = "Invalid marker"
            raise InvalidParameterValueException(msg) from exc
