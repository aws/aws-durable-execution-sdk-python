"""Chained invoke through the endpoint dispatcher, with a scripted Lambda endpoint.

The web runner resolves a chained-invoke target from the function
configuration file and invokes handlers at a Lambda endpoint. These tests
replace that endpoint with a scripted :class:`Invoker`, so the executor,
checkpoint processor, and dispatcher run for real while the endpoint's
behavior is fixed by the test.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.child_dispatcher import (
    EndpointChildDispatcher,
    FunctionConfig,
    FunctionConfigs,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.invoker import (
    InvokeResponse,
    LambdaInvoker,
)
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution

PARENT = "parent"
CHILD = "child"


class _ScriptedEndpoint:
    """An :class:`Invoker` that plays a Lambda endpoint.

    ``parent`` is a durable handler that invokes ``child`` once and then
    returns the chained-invoke operation's outcome. ``child`` does not
    exist at the endpoint, so invoking it raises ResourceNotFoundException
    exactly as :class:`LambdaInvoker` does for a 404 from Lambda.
    """

    def __init__(self) -> None:
        self.executor: Executor | None = None
        self._paginator = LambdaInvoker(Mock())
        # child ARN -> parent ARN, as pinned by the executor.
        self.inherited: dict[str, str] = {}

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        return self._paginator.create_invocation_input(execution)

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        msg = f"the scripted endpoint cannot move to {endpoint_url} ({region_name})"
        raise AssertionError(msg)

    def inherit_endpoint(
        self, child_execution_arn: str, parent_execution_arn: str
    ) -> None:
        self.inherited[child_execution_arn] = parent_execution_arn

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,  # noqa: A002
        endpoint_url: str | None = None,  # noqa: ARG002
        tenant_id: str | None = None,  # noqa: ARG002
        account_id: str | None = None,  # noqa: ARG002
        region_name: str | None = None,  # noqa: ARG002
    ) -> InvokeResponse:
        if function_name == CHILD:
            # The executor pins a child to its parent's endpoint before
            # scheduling the child's first invocation.
            assert input.durable_execution_arn in self.inherited
            msg = f"Function not found: {CHILD}"
            raise ResourceNotFoundException(msg)
        assert function_name == PARENT
        assert self.executor is not None
        operations = input.initial_execution_state.operations
        invoke_op = next(
            (
                op
                for op in operations
                if op.operation_type == OperationType.CHAINED_INVOKE
            ),
            None,
        )
        if invoke_op is None:
            self.executor.checkpoint_execution(
                input.durable_execution_arn,
                input.checkpoint_token,
                [
                    OperationUpdate(
                        operation_id="invoke-1",
                        operation_type=OperationType.CHAINED_INVOKE,
                        action=OperationAction.START,
                        name="call-child",
                        payload="{}",
                        chained_invoke_options=ChainedInvokeOptions(
                            function_name=CHILD, tenant_id=None
                        ),
                    )
                ],
            )
            return _output(InvocationStatus.PENDING)
        if invoke_op.status == OperationStatus.STARTED:
            return _output(InvocationStatus.PENDING)
        details = invoke_op.chained_invoke_details
        assert details is not None
        error = details.error
        return _output(
            InvocationStatus.SUCCEEDED,
            result=json.dumps(
                {
                    "status": invoke_op.status.value,
                    "type": error.type if error else None,
                    "message": error.message if error else None,
                }
            ),
        )


def _output(status: InvocationStatus, result: str | None = None) -> InvokeResponse:
    return InvokeResponse(
        invocation_output=DurableExecutionInvocationOutput(
            status=status, result=result
        ),
        request_id="scripted",
    )


def test_configured_durable_target_missing_at_endpoint_keeps_lambda_error_code():
    """The file says ``child`` is durable, but the endpoint has no such function.

    The child execution's first invocation fails with
    ResourceNotFoundException, and the parent's operation carries that
    error code, as it does when the service cannot find the function.
    """
    endpoint = _ScriptedEndpoint()
    store = InMemoryExecutionStore()
    scheduler = Scheduler()
    checkpoint_processor = CheckpointProcessor(store, scheduler)
    executor = Executor(
        store=store,
        scheduler=scheduler,
        invoker=endpoint,
        checkpoint_processor=checkpoint_processor,
        child_dispatcher=EndpointChildDispatcher(
            FunctionConfigs(
                {CHILD: FunctionConfig(is_durable=True, execution_timeout_seconds=30)}
            ),
            client_provider=lambda _arn: Mock(),
            invocation_timeout_seconds=5,
        ),
    )
    checkpoint_processor.add_execution_observer(executor)
    endpoint.executor = executor
    scheduler.start()
    try:
        output = executor.start_execution(
            StartDurableExecutionInput(
                account_id="123456789012",
                function_name=PARENT,
                function_qualifier="$LATEST",
                execution_name="run",
                execution_timeout_seconds=30,
                execution_retention_period_days=1,
                input="{}",
            )
        )
        parent_arn: str = output.execution_arn or ""
        assert executor.wait_until_complete(parent_arn, timeout=20)

        parent = executor.get_execution(parent_arn)
        assert parent.result is not None
        outcome: dict[str, Any] = json.loads(parent.result.result or "{}")
        assert outcome == {
            "status": "FAILED",
            "type": "ResourceNotFoundException",
            "message": "Function not found: child",
        }
        # The child execution exists and failed with the same error.
        child_arn = parent.chained_invoke_children["invoke-1"]
        child = executor.get_execution(child_arn)
        assert child.parent_execution_arn == parent_arn
        assert child.result is not None
        assert child.result.error is not None
        assert child.result.error.type == "ResourceNotFoundException"
        # The executor pinned the child to its parent's endpoint.
        assert endpoint.inherited == {child_arn: parent_arn}
    finally:
        executor.shutdown()
        scheduler.stop()
