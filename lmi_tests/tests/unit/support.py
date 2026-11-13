"""Assert fixed behavior, not the bug. All blocking fixture work has a final release."""

import json
import time
from unittest.mock import Mock


from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInputWithClient,
    InitialExecutionState,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
)


class MemoryClient:
    def checkpoint(self, *, updates, **_kwargs):
        changed = [
            Operation(
                operation_id=u.operation_id,
                operation_type=u.operation_type,
                status={
                    "START": OperationStatus.STARTED,
                    "SUCCEED": OperationStatus.SUCCEEDED,
                    "FAIL": OperationStatus.FAILED,
                }[u.action.value],
                name=u.name,
                parent_id=u.parent_id,
                sub_type=u.sub_type,
                step_details=StepDetails(result=u.payload, error=u.error)
                if u.operation_type == OperationType.STEP
                else None,
                context_details=ContextDetails(result=u.payload, error=u.error)
                if u.operation_type == OperationType.CONTEXT
                else None,
            )
            for u in updates
        ]
        return CheckpointOutput(
            "next-token", CheckpointUpdatedExecutionState(operations=changed)
        )


def invocation():
    return DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution/local",
        checkpoint_token="token",
        initial_execution_state=InitialExecutionState(
            operations=[
                Operation(
                    operation_id="local",
                    operation_type=OperationType.EXECUTION,
                    status=OperationStatus.STARTED,
                    execution_details=ExecutionDetails(input_payload=json.dumps({})),
                )
            ],
            next_marker="",
        ),
        service_client=MemoryClient(),
    )


def lambda_context(remaining):
    context = Mock()
    context.aws_request_id = "local-request"
    context.client_context = context.identity = context.tenant_id = None
    context.invoked_function_arn = "test-function"
    context._epoch_deadline_time_in_ms = int(time.time() * 1000) + remaining
    context.get_remaining_time_in_millis.side_effect = lambda: (
        context._epoch_deadline_time_in_ms - int(time.time() * 1000)
    )
    return context
