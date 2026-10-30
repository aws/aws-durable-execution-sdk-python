"""A wait update survives failed delivery and a checkpoint-free resume."""

import json
import time
from pathlib import Path
from typing import Any

import pytest

from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationChangeInfo,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.executor import Executor, InvocationState
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


@pytest.mark.parametrize("filesystem", [False, True])
@pytest.mark.parametrize("failed_delivery", [False, True])
@pytest.mark.parametrize("legacy_format", [False, True])
def test_wait_input_acknowledgment_survives_checkpoint_free_resume(
    filesystem: bool,
    failed_delivery: bool,
    legacy_format: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store: ExecutionStore = (
        FileSystemExecutionStore.create(tmp_path / "store")
        if filesystem
        else InMemoryExecutionStore()
    )
    starts: list[InvocationStartInfo] = []
    ends: list[InvocationEndInfo] = []
    wait_deliveries: list[int] = []
    attempts: list[int] = []
    step_calls: list[str] = []
    legacy_restores: list[dict[str, Any]] = []
    original_begin = Executor._begin_invocation  # noqa: SLF001

    def begin_with_legacy_state(
        executor: Executor, arn: str
    ) -> tuple[Execution, DurableExecutionInvocationInput, int] | None:
        if (
            legacy_format
            and not legacy_restores
            and executor._invocation_gate(arn) is InvocationState.PRE_INVOKE  # noqa: SLF001
        ):
            execution = store.load(arn)
            if execution.updated_operation_ids:
                # Rehydrate an actual wait completion in the older store format.
                snapshot = execution.to_json_dict()
                for field in (
                    "SeqCounter",
                    "HandlerSeenSeq",
                    "OperationLastTouchedSeq",
                    "OperationSizeBytes",
                ):
                    snapshot.pop(field)
                restored = Execution.from_json_dict(snapshot)
                assert restored.seq_counter == restored.handler_seen_seq == 0
                assert restored.updated_operation_ids == execution.updated_operation_ids
                store.save(restored)
                legacy_restores.append(snapshot)
        return original_begin(executor, arn)

    monkeypatch.setattr(Executor, "_begin_invocation", begin_with_legacy_state)

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            starts.append(info)
            if any(
                op.name == "delivered-wait" and op.status.value == "SUCCEEDED"
                for op in info.updated_operations.values()
            ):
                wait_deliveries.append(len(starts))

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            ends.append(info)

        def on_operation_change(self, info: OperationChangeInfo) -> None:
            if any(
                op.name == "delivered-wait" and op.status.value == "SUCCEEDED"
                for op in info.updated_operations.values()
            ):
                wait_deliveries.append(len(starts))

    class ObserveFactory:
        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            return Observe()

    @durable_step
    def marker(_context: Any, value: str) -> str:
        step_calls.append(value)
        return value

    def durable_handler(_event: Any, context: DurableContext) -> list[str]:
        before = context.step(marker("before"), name="before-wait")
        barrier = context.create_callback(
            name="barrier", config=CallbackConfig(serdes=JsonSerDes())
        )
        context.wait(Duration.from_seconds(1), name="delivered-wait")
        value = barrier.result()
        assert isinstance(value, str)
        return [before, context.step(marker(value), name="after-barrier")]

    handler = durable_execution(durable_handler, plugins=[ObserveFactory()])

    def entrypoint(event: Any, context: Any) -> Any:
        attempts.append(1)
        if failed_delivery and len(attempts) == 2:
            raise RuntimeError("delivery failed before the SDK received its input")
        return handler(event, context)

    with DurableFunctionTestRunner(
        handler=entrypoint,
        store=store,
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=25,
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = runner.wait_for_callback(arn, name="barrier", timeout=5)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            events = runner.get_execution_history(arn).events
            if (
                len(ends) >= 2
                and sum(event.event_type == "InvocationCompleted" for event in events)
                >= 2
            ):
                break
            time.sleep(0.01)
        else:
            raise AssertionError("The checkpoint-free resumed invocation did not end")
        saved = store.load(arn)
        wait = next(op for op in saved.operations if op.name == "delivered-wait")
        acknowledged = (
            list(saved.updated_operation_ids),
            saved.handler_seen_seq,
            saved.operation_last_touched_seq.get(wait.operation_id, 0),
        )
        runner.send_callback_success(callback_id, result=b'"after"')
        result = runner.wait_for_result(arn, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    assert json.loads(result.result) == ["before", "after"]
    pending_ids, seen_sequence, wait_sequence = acknowledged
    assert pending_ids == []
    assert seen_sequence >= wait_sequence
    assert [info.status.value for info in ends] == [
        "PENDING",
        "PENDING",
        "SUCCEEDED",
    ]
    assert set(wait_deliveries) == {2}
    assert step_calls == ["before", "after"]
    assert len(attempts) == (4 if failed_delivery else 3)
    assert len(legacy_restores) == int(legacy_format)
