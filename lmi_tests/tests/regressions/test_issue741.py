"""Assert fixed behavior, not the bug. All blocking fixture work has a final release."""

from concurrent.futures import ThreadPoolExecutor, wait
import json
import threading
import time
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    MapConfig,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.exceptions import InvocationError
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


def test_expired_invocation_starts_no_step_body():
    effects = []
    handler = durable_execution(
        lambda _, c: c.step(lambda _: effects.append("write"), name="write")
    )
    try:
        handler(invocation(), lambda_context(-1000))
    except InvocationError:
        pass  # invocation interruption is permitted; a user operation failure is not
    assert effects == [], (
        "#741: already-expired invocation still executed user side effect"
    )


@pytest.mark.parametrize("mode", ["parallel", "map", "nested"])
def test_early_result_does_not_pin_wrapper_past_deadline(mode):
    started, release, computed = threading.Event(), threading.Event(), threading.Event()

    def slow(child):
        def io(_):
            started.set()
            assert release.wait(6), "emergency fixture timeout"
            return "loser"

        return child.step(io, name="slow")

    def fast(child):
        def io(_):
            assert started.wait(3)
            return "winner"

        return child.step(io, name="fast")

    def race(context):
        if mode == "map":
            return context.map(
                [0, 1],
                lambda c, item, _idx, _all: slow(c) if item == 0 else fast(c),
                name="race",
                config=MapConfig(
                    max_concurrency=2,
                    completion_config=CompletionConfig(min_successful=1),
                ),
            )
        return context.parallel(
            [slow, fast],
            name="race",
            config=ParallelConfig(
                max_concurrency=2, completion_config=CompletionConfig.first_successful()
            ),
        )

    @durable_execution
    def handler(_, context):
        if mode == "nested":
            context.map(
                [0],
                lambda c, _item, _idx, _all: c.run_in_child_context(race, name="child"),
                name="outer",
            )
        else:
            race(context)
        computed.set()
        return "winner"

    with ThreadPoolExecutor(1) as caller:
        future = caller.submit(handler, invocation(), lambda_context(1000))
        try:
            assert computed.wait(2), "Fixture failed to select a winner"
            done, _ = wait([future], timeout=2)
            assert done, (
                "#741: user result computed but wrapper still joins losing branch past deadline"
            )
        finally:
            release.set()
            future.result(timeout=3)
