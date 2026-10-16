"""Public lifecycle regressions for #741, including non-cooperative limits."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python import deadline as deadline_module
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    Duration,
    ParallelConfig,
    StepConfig,
    StepSemantics,
)
from aws_durable_execution_sdk_python.exceptions import (
    InvocationDeadlineExceeded,
    OrphanedChildException,
    StepError,
)
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
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepDetails,
    StateOutput,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    OperationChangeInfo,
)
from aws_durable_execution_sdk_python.retries import RetryPresets


class RecordingService:
    """A serialized checkpoint service retaining the real SDK's operation IDs."""

    def __init__(self) -> None:
        self.operations: dict[str, Operation] = {}
        self.calls: list[list[OperationUpdate]] = []
        self.block_transport = threading.Event()
        self.transport_entered = threading.Event()
        self.release_transport = threading.Event()

    def checkpoint(
        self, *, updates: list[OperationUpdate], **kwargs: Any
    ) -> CheckpointOutput:
        if self.block_transport.is_set():
            self.transport_entered.set()
            assert self.release_transport.wait(5)
        self.calls.append(updates)
        changed = []
        for update in updates:
            status = {
                OperationAction.START: OperationStatus.STARTED,
                OperationAction.SUCCEED: OperationStatus.SUCCEEDED,
                OperationAction.FAIL: OperationStatus.FAILED,
            }[update.action]
            operation = Operation(
                operation_id=update.operation_id,
                operation_type=update.operation_type,
                status=status,
                parent_id=update.parent_id,
                name=update.name,
                sub_type=update.sub_type,
                step_details=StepDetails(result=update.payload, error=update.error)
                if update.operation_type is OperationType.STEP
                else None,
                context_details=ContextDetails(
                    result=update.payload, error=update.error
                )
                if update.operation_type is OperationType.CONTEXT
                else None,
            )
            self.operations[operation.operation_id] = operation
            changed.append(operation)
        return CheckpointOutput(
            checkpoint_token="next-token",
            new_execution_state=CheckpointUpdatedExecutionState(operations=changed),
        )

    def event(
        self, name: str = "execution"
    ) -> DurableExecutionInvocationInputWithClient:
        initial = Operation(
            operation_id=name,
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
            execution_details=ExecutionDetails(
                input_payload=json.dumps({"name": name})
            ),
        )
        return DurableExecutionInvocationInputWithClient(
            durable_execution_arn=f"arn:test:execution/{name}",
            checkpoint_token="token",
            initial_execution_state=InitialExecutionState(
                operations=[initial, *self.operations.values()], next_marker=""
            ),
            service_client=self,  # type: ignore[arg-type]
        )


def context(remaining_ms: int = 900000) -> Mock:
    value = Mock()
    value.get_remaining_time_in_millis.return_value = remaining_ms
    value.aws_request_id = "request"
    value.client_context = None
    value.identity = None
    value.invoked_function_arn = "test"
    value.tenant_id = None
    return value


def test_expired_invocation_never_enters_handler_or_dispatches_checkpoint() -> None:
    service = RecordingService()
    body = Mock()
    wrapped = durable_execution(body, plugins=[])
    with pytest.raises(InvocationDeadlineExceeded):
        wrapped(service.event(), context(0))
    body.assert_not_called()
    assert not service.calls


def test_deadline_retry_replays_saved_success_and_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    monkeypatch.setattr(
        deadline_module, "time", SimpleNamespace(monotonic=lambda: now[0])
    )
    service = RecordingService()
    effects: list[str] = []
    attempts = 0
    observed_errors: list[tuple[str, str | None]] = []

    @durable_execution(plugins=[])  # type: ignore[call-arg]
    def handler(_event: Any, ctx: DurableContext) -> Any:
        nonlocal attempts
        attempts += 1

        def save(_: Any) -> str:
            effects.append("saved")
            return "saved"

        saved = ctx.step(save, name="saved")

        def fail(_: Any) -> None:
            effects.append("failed")
            raise ValueError("persisted failure")

        try:
            ctx.step(
                fail,
                name="failed",
                config=StepConfig(retry_strategy=RetryPresets.none()),
            )
        except StepError as error:
            observed_errors.append((error.error_type, error.message))
        if attempts == 1:
            now[0] += 1000

        def last(_: Any) -> str:
            effects.append("last")
            return saved

        return ctx.step(last, name="last")

    with pytest.raises(InvocationDeadlineExceeded):
        handler(service.event(), context())
    assert effects == ["saved", "failed"]
    outcome = handler(service.event(), context())
    assert outcome["Status"] == "SUCCEEDED"
    assert json.loads(outcome["Result"]) == "saved"
    assert effects == ["saved", "failed", "last"]
    assert observed_errors == [("ValueError", "persisted failure")] * 2


def test_expiry_inside_step_does_not_call_user_retry_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    monkeypatch.setattr(
        deadline_module, "time", SimpleNamespace(monotonic=lambda: now[0])
    )
    service = RecordingService()
    retry = Mock()

    @durable_execution(plugins=[])  # type: ignore[call-arg]
    def handler(_event: Any, ctx: DurableContext) -> Any:
        def body(_: Any) -> str:
            now[0] += 1000
            return "late"

        return ctx.step(body, name="work", config=StepConfig(retry_strategy=retry))

    with pytest.raises(InvocationDeadlineExceeded):
        handler(service.event(), context())
    retry.assert_not_called()
    assert all(
        update.action is OperationAction.START
        for batch in service.calls
        for update in batch
    )


def test_completed_ancestor_rejects_new_step_before_body_or_checkpoint() -> None:
    service = RecordingService()
    loser_started = threading.Event()
    parent_completed = threading.Event()
    rejected = threading.Event()
    late_body = Mock()

    def loser(child: DurableContext) -> Any:
        def held_io(_: Any) -> None:
            loser_started.set()
            assert parent_completed.wait(5)

        try:
            child.step(held_io, name="held")
        finally:
            try:
                child.step(late_body, name="late")
            except OrphanedChildException:
                rejected.set()
                raise

    def winner(child: DurableContext) -> Any:
        assert loser_started.wait(5)
        return child.step(lambda _: "winner", name="win")

    @durable_execution(plugins=[])  # type: ignore[call-arg]
    def handler(_event: Any, ctx: DurableContext) -> Any:
        result = ctx.parallel(
            [loser, winner],
            name="race",
            config=ParallelConfig(
                max_concurrency=2, completion_config=CompletionConfig.first_successful()
            ),
        )
        parent_completed.set()
        return result.get_results()

    outcome = handler(service.event(), context())
    assert outcome["Status"] == "SUCCEEDED"
    assert rejected.is_set()
    late_body.assert_not_called()
    assert all(update.name != "late" for batch in service.calls for update in batch)


def test_early_completion_joins_orphan_waiter_after_budget_expires() -> None:
    service = RecordingService()
    waiter_started = threading.Event()
    result_computed = threading.Event()
    waiter_finished = threading.Event()

    def loser(child: DurableContext) -> Any:
        # Park in the same SDK refresh wait used by the coordinator. Its refresh
        # is deliberately due after the invocation budget, with no blocked I/O.
        refresh = child.state.schedule_refresh(time.time() + 60)
        waiter_started.set()
        try:
            refresh.wait()
        finally:
            waiter_finished.set()

    def winner(child: DurableContext) -> Any:
        assert waiter_started.wait(5)
        return child.step(lambda _: "winner", name="win")

    @durable_execution(plugins=[])  # type: ignore[call-arg]
    def handler(_event: Any, ctx: DurableContext) -> Any:
        ctx.parallel(
            [loser, winner],
            name="race",
            config=ParallelConfig(
                max_concurrency=2, completion_config=CompletionConfig.first_successful()
            ),
        )
        result_computed.set()
        return "winner"

    with ThreadPoolExecutor(max_workers=1) as callers:
        future = callers.submit(handler, service.event(), context(2000))
        assert result_computed.wait(2)
        with pytest.raises(InvocationDeadlineExceeded):
            future.result(timeout=5)
    assert waiter_finished.is_set()
    assert not any(
        thread.name.startswith("dex-handler") for thread in threading.enumerate()
    )


@pytest.mark.parametrize("blocked_part", ["transport", "plugin", "user"])
def test_noncooperative_work_is_joined_and_other_invocations_stay_healthy(
    blocked_part: str,
) -> None:
    service = RecordingService()
    entered = threading.Event()
    release = threading.Event()

    class HeldUpdate(DurableInstrumentationPlugin):
        def on_operation_change(self, _info: OperationChangeInfo) -> None:
            entered.set()
            assert release.wait(5)

    plugins: list[DurableInstrumentationPlugin] = (
        [HeldUpdate()] if blocked_part == "plugin" else []
    )
    if blocked_part == "transport":
        service.block_transport.set()
        entered = service.transport_entered
        release = service.release_transport

    @durable_execution(plugins=plugins)  # type: ignore[call-arg]
    def handler(_event: Any, ctx: DurableContext) -> Any:
        def body(_: Any) -> str:
            if blocked_part == "user":
                entered.set()
                assert release.wait(5)
            return "done"

        return ctx.step(
            body,
            name="work",
            config=StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY),
        )

    healthy_service = RecordingService()
    healthy = durable_execution(
        lambda _, ctx: ctx.step(lambda _: "healthy", name="healthy"), plugins=[]
    )
    with ThreadPoolExecutor(max_workers=2) as callers:
        expired = callers.submit(handler, service.event(), context(2000))
        try:
            assert entered.wait(2)
            assert not expired.done()
            healthy_result = callers.submit(
                healthy, healthy_service.event(), context()
            ).result(timeout=2)
            assert healthy_result["Status"] == "SUCCEEDED"
            # A blocked transport/hook/user body is explicitly not detached.
            time.sleep(2.0)
            assert not expired.done()
        finally:
            release.set()
        with pytest.raises(InvocationDeadlineExceeded):
            expired.result(timeout=5)
    assert not any(
        update.action is OperationAction.SUCCEED
        for batch in service.calls
        for update in batch
    )


@pytest.mark.parametrize("fail", [False, True])
def test_accepted_terminal_result_survives_slow_delivery_hook(
    monkeypatch: pytest.MonkeyPatch, fail: bool
) -> None:
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.execution.LAMBDA_RESPONSE_SIZE_LIMIT", 20
    )
    service = RecordingService()
    entered = threading.Event()
    release = threading.Event()
    ends: list[InvocationEndInfo] = []

    class HeldUpdate(DurableInstrumentationPlugin):
        def on_operation_change(self, _info: OperationChangeInfo) -> None:
            entered.set()
            assert release.wait(5)

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            ends.append(info)

    @durable_execution(plugins=[HeldUpdate()])  # type: ignore[call-arg]
    def handler(_event: Any, _ctx: DurableContext) -> str:
        if fail:
            raise ValueError("a large persisted failure" * 10)
        return "a large persisted result" * 10

    with ThreadPoolExecutor(max_workers=1) as callers:
        future = callers.submit(handler, service.event(), context(2000))
        try:
            assert entered.wait(2)
            time.sleep(2.0)
            assert not future.done()
        finally:
            release.set()
        result = future.result(timeout=5)
    assert result["Status"] == ("FAILED" if fail else "SUCCEEDED")
    assert len(ends) == 1
    assert ends[0].status.value == result["Status"]
    updates = [update for batch in service.calls for update in batch]
    assert len(updates) == 1
    assert updates[0].operation_type is OperationType.EXECUTION
    assert updates[0].action is (
        OperationAction.FAIL if fail else OperationAction.SUCCEED
    )


@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("expire_after_first_page", [False, True])
def test_accepted_terminal_checkpoint_stops_pagination_at_cutoff(
    monkeypatch: pytest.MonkeyPatch, fail: bool, expire_after_first_page: bool
) -> None:
    now = [100.0]
    monkeypatch.setattr(
        deadline_module, "time", SimpleNamespace(monotonic=lambda: now[0])
    )
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.execution.LAMBDA_RESPONSE_SIZE_LIMIT", 20
    )
    pages: list[str] = []

    class TerminalPagesService(RecordingService):
        def checkpoint(
            self, *, updates: list[OperationUpdate], **kwargs: Any
        ) -> CheckpointOutput:
            response = super().checkpoint(updates=updates, **kwargs)
            if not expire_after_first_page:
                now[0] += 1000
            return CheckpointOutput(
                checkpoint_token=response.checkpoint_token,
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=response.new_execution_state.operations,
                    next_marker="terminal-page",
                ),
            )

        def get_execution_state(
            self, *, next_marker: str, **kwargs: Any
        ) -> StateOutput:
            pages.append(next_marker)
            now[0] += 1000
            return StateOutput(operations=[], next_marker="must-not-fetch")

    def handler(_event: Any, _ctx: DurableContext) -> str:
        if fail:
            raise ValueError("a large persisted failure" * 10)
        return "a large persisted result" * 10

    service = TerminalPagesService()
    wrapped = durable_execution(handler, plugins=[])
    outcome = wrapped(service.event(), context())
    assert outcome["Status"] == ("FAILED" if fail else "SUCCEEDED")
    assert pages == (["terminal-page"] if expire_after_first_page else [])


def test_public_parallel_wait_coordinator_unwinds_at_deadline() -> None:
    service = RecordingService()
    held = threading.Event()
    release = threading.Event()
    root_unwound = threading.Event()

    def waiting(child: DurableContext) -> None:
        child.wait(Duration.from_seconds(60), name="durable-wait")

    def busy(child: DurableContext) -> str:
        def body(_: Any) -> str:
            held.set()
            assert release.wait(8)
            return "late"

        return child.step(body, name="held-step")

    def handler(_event: Any, ctx: DurableContext) -> None:
        try:
            ctx.parallel(
                [waiting, busy],
                name="parallel",
                config=ParallelConfig(max_concurrency=2),
            )
        finally:
            root_unwound.set()

    wrapped = durable_execution(handler, plugins=[])
    with ThreadPoolExecutor(max_workers=1) as callers:
        future = callers.submit(wrapped, service.event(), context(2000))
        try:
            assert held.wait(3)
            # The actual public wait enters a timed resume wave while its peer
            # runs. The coordinator must leave its queue wait at the deadline.
            assert root_unwound.wait(4)
            assert not future.done()  # the uncooperative peer remains confined
        finally:
            release.set()
        with pytest.raises(InvocationDeadlineExceeded):
            future.result(timeout=5)
    assert any(
        update.name == "durable-wait" for batch in service.calls for update in batch
    )
    assert all(
        update.action is OperationAction.START
        for batch in service.calls
        for update in batch
    )
