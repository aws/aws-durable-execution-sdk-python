"""An input-only callback resume must not redeliver its terminal update later."""

import json
import time

import pytest
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.executor import Executor, InvocationState
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


@pytest.mark.parametrize("filesystem", [False, True])
@pytest.mark.parametrize("failed_delivery", [False, True])
@pytest.mark.parametrize("legacy_format", [False, True])
def test_callback_input_acknowledgment_survives_checkpoint_free_resume(
    filesystem, failed_delivery, legacy_format, tmp_path, monkeypatch
):
    store = (
        FileSystemExecutionStore.create(tmp_path / "store")
        if filesystem
        else InMemoryExecutionStore()
    )
    starts = []
    invocation_ends = []
    operation_ends = []
    attempts = []
    marker_calls = []
    legacy_restores = []

    original_begin = Executor._begin_invocation  # noqa: SLF001

    def begin_with_old_store(self, arn):
        # Rehydrate a real completed callback using the persisted format that
        # predates sequence accounting. No SDK hook or invocation input is faked.
        if (
            legacy_format
            and not legacy_restores
            and self._invocation_gate(arn) is InvocationState.PRE_INVOKE  # noqa: SLF001
        ):
            execution = store.load(arn)
            if execution.updated_operation_ids:
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
        return original_begin(self, arn)

    monkeypatch.setattr(Executor, "_begin_invocation", begin_with_old_store)

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            starts.append(info)

        def on_invocation_end(self, info):
            invocation_ends.append(info)

        def on_operation_end(self, info):
            if info.name in ("a", "b"):
                operation_ends.append((len(starts), info.name, info.is_replayed))

    @durable_step
    def marker(context, value):
        marker_calls.append(1)
        return value

    @durable_execution(plugins=[Observe()])
    def handler(event, context: DurableContext):
        a = context.create_callback(
            name="a", config=CallbackConfig(serdes=JsonSerDes())
        )
        b = context.create_callback(
            name="b", config=CallbackConfig(serdes=JsonSerDes())
        )
        result = [a.result(), b.result()]
        return context.step(marker(result), name="after-b")

    def entrypoint(event, context):
        attempts.append(1)
        if failed_delivery and len(attempts) == 2:
            raise RuntimeError("invocation failed before the SDK received its input")
        return handler(event, context)

    with DurableFunctionTestRunner(
        handler=entrypoint,
        store=store,
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=25,
    ) as runner:
        arn = runner.run_async(input="{}")
        a_id = runner.wait_for_callback(arn, name="a", timeout=5)
        b_id = runner.wait_for_callback(arn, name="b", timeout=5)
        runner.send_callback_success(a_id, result=b'"A"')
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            events = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
            if (
                len(invocation_ends) >= 2
                and sum(event.event_type == "InvocationCompleted" for event in events)
                >= 2
            ):
                break
            time.sleep(0.01)
        else:
            raise AssertionError("The second SDK invocation did not suspend")
        runner.send_callback_success(b_id, result=b'"B"')
        result = runner.wait_for_result(arn, timeout=10)

    assert result.status.value == "SUCCEEDED"
    assert json.loads(result.result) == ["A", "B"]
    assert [info.status.value for info in invocation_ends] == [
        "PENDING",
        "PENDING",
        "SUCCEEDED",
    ]
    assert operation_ends == [(2, "a", False), (3, "b", False)]
    assert marker_calls == [1]
    assert len(attempts) == (4 if failed_delivery else 3)
    assert len(legacy_restores) == int(legacy_format)
