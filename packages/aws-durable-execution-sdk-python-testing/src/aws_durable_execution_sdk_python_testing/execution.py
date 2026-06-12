from __future__ import annotations

from dataclasses import replace
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
        self.invocation_completions: list[InvocationCompletedDetails] = []
        self.used_tokens: set[str] = set()
        # TODO: this will need to persist/rehydrate depending on inmemory vs sqllite store
        self._token_sequence: int = 0
        self._state_lock: Lock = Lock()
        self.is_complete: bool = False
        self.result: DurableExecutionInvocationOutput | None = None
        self.consecutive_failed_invocation_attempts: int = 0
        self.close_status: ExecutionStatus | None = None

    @property
    def token_sequence(self) -> int:
        """Get current token sequence value."""
        return self._token_sequence

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
            "InvocationCompletions": [
                completion.to_json_dict() for completion in self.invocation_completions
            ],
            "UsedTokens": list(self.used_tokens),
            "TokenSequence": self._token_sequence,
            "IsComplete": self.is_complete,
            "Result": self.result.to_dict() if self.result else None,
            "ConsecutiveFailedInvocationAttempts": self.consecutive_failed_invocation_attempts,
            "CloseStatus": self.close_status.value if self.close_status else None,
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
        execution.invocation_completions = [
            InvocationCompletedDetails.from_json_dict(item)
            for item in data.get("InvocationCompletions", [])
        ]
        execution.used_tokens = set(data["UsedTokens"])
        execution._token_sequence = data["TokenSequence"]  # noqa: SLF001
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
        """Generate a new checkpoint token with incremented sequence"""
        with self._state_lock:
            self._token_sequence += 1
            new_token_sequence = self._token_sequence
            token = CheckpointToken(
                execution_arn=self.durable_execution_arn,
                token_sequence=new_token_sequence,
            )
            token_str = token.to_str()
            self.used_tokens.add(token_str)
            return token_str

    def get_navigable_operations(self) -> list[Operation]:
        """Get list of operations, but exclude child operations where the parent has already completed."""
        return self.operations

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
            self._token_sequence += 1
            # Build and assign updated operation
            self.operations[index] = replace(
                operation,
                status=OperationStatus.SUCCEEDED,
                end_timestamp=datetime.now(UTC),
            )
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
            self._token_sequence += 1
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
            self._token_sequence += 1
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
            self._token_sequence += 1
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
            self._token_sequence += 1
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
                self.operations[0] = replace(
                    execution_op,
                    status=status,
                    end_timestamp=datetime.now(UTC),
                )
