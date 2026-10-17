"""Real public handlers replay through the service's retained-operation view."""

import json
import os
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.exceptions import ChildContextError
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)


@pytest.mark.parametrize("filesystem", [False, True])
@pytest.mark.parametrize("mode", ["completed", "retained", "nested-retained", "failed"])
def test_public_resume_uses_service_state_but_inspection_remains_complete(
    filesystem, mode, tmp_path
):
    store = (
        FileSystemExecutionStore.create(tmp_path / "state")
        if filesystem
        else InMemoryExecutionStore()
    )
    snapshots = []
    child_calls = []
    step_calls = []

    def save(_step):
        step_calls.append(1)
        return "saved"

    def child(ctx):
        child_calls.append(1)
        saved = ctx.step(save, name="saved-child-step")
        if mode == "failed":
            raise ValueError("stored-child-failure")
        return {
            "saved": saved,
            "large": "x" * (300 * 1024)
            if mode in ["retained", "nested-retained"]
            else "",
        }

    def outer(ctx):
        value = ctx.run_in_child_context(child, name="inner-retained")
        return {"saved": value["saved"], "large": ""}

    def handler(_event, ctx):
        try:
            value = ctx.run_in_child_context(
                outer if mode == "nested-retained" else child, name="outer"
            )
            result = {"saved": value["saved"], "length": len(value["large"])}
        except ChildContextError:
            assert mode == "failed"
            result = {"failed": True}
        gate = ctx.create_callback(
            name="resume", config=CallbackConfig(serdes=JsonSerDes())
        )
        assert gate.result() == "resume"
        return result

    wrapped = durable_execution(handler)

    def entrypoint(event, context):
        initial = event.initial_execution_state.operations
        state = event.service_client.get_execution_state(
            event.durable_execution_arn, event.checkpoint_token, ""
        )
        snapshots.append(
            {
                "input_names": [op.name for op in initial],
                "state_names": [op.name for op in state.operations],
                "input_ids": [op.operation_id for op in initial],
                "state_ids": [op.operation_id for op in state.operations],
                "updated_ids": list(event.updated_operation_ids),
            }
        )
        return wrapped(event, context)

    with DurableFunctionTestRunner(
        handler=entrypoint,
        store=store,
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=20,
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = runner.wait_for_callback(arn, name="resume", timeout=5)
        runner.send_callback_success(callback_id, result=b'"resume"')
        result = runner.wait_for_result(arn, timeout=10)
        history = runner.get_execution_history(arn, include_execution_data=True).events
    assert result.status.value == "SUCCEEDED"
    assert json.loads(result.result) == (
        {"failed": True}
        if mode == "failed"
        else {"saved": "saved", "length": 300 * 1024 if mode == "retained" else 0}
    )
    assert (
        len(snapshots) == 2 and snapshots[0]["input_ids"] == snapshots[0]["state_ids"]
    )
    assert snapshots[1]["input_ids"] == snapshots[1]["state_ids"]
    visible = "saved-child-step" in snapshots[1]["input_names"]
    assert visible is (mode == "retained")
    if mode == "nested-retained":
        assert "inner-retained" not in snapshots[1]["input_names"]
    assert "outer" in snapshots[1]["input_names"]
    assert step_calls == [1] and len(child_calls) == (2 if mode == "retained" else 1)
    raw = store.load(arn)
    assert any(op.name == "saved-child-step" for op in raw.operations)
    inspected = result.get_operation_by_name("outer")
    if mode == "nested-retained":
        inspected = inspected.get_operation_by_name("inner-retained")
    assert (
        inspected.get_operation_by_name("saved-child-step").name == "saved-child-step"
    )
    assert (
        sum(
            e.event_type == "StepSucceeded" and e.name == "saved-child-step"
            for e in history
        )
        == 1
    )
    assert any(e.name == "saved-child-step" for e in history)
    if os.environ.get("SERVICE_PRUNING_PROOF_DIR"):
        out = Path(os.environ["SERVICE_PRUNING_PROOF_DIR"])
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{mode}-fs{filesystem}.json").write_text(
            json.dumps(
                dict(
                    mode=mode,
                    filesystem=filesystem,
                    snapshots=snapshots,
                    result=json.loads(result.result),
                    step_calls=len(step_calls),
                    child_calls=len(child_calls),
                    history_names=[e.name for e in history],
                ),
                indent=2,
            )
            + "\n"
        )


@pytest.mark.parametrize("filesystem", [False, True])
def test_hidden_callback_updated_id_does_not_make_its_metadata_visible(
    filesystem, tmp_path
):
    import time

    store = (
        FileSystemExecutionStore.create(tmp_path / "hidden-state")
        if filesystem
        else InMemoryExecutionStore()
    )
    snapshots = []
    child_calls = []
    saved_calls = []

    def saved(_step):
        saved_calls.append(1)
        return "saved"

    def child(ctx):
        child_calls.append(1)
        callback = ctx.create_callback(
            name="hidden-b", config=CallbackConfig(serdes=JsonSerDes())
        )
        assert ctx.step(saved, name="hidden-saved") == "saved"
        return callback.callback_id

    def handler(_event, ctx):
        a = ctx.create_callback(
            name="root-a", config=CallbackConfig(serdes=JsonSerDes())
        )
        b_id = ctx.run_in_child_context(child, name="closed-parent")
        return {"a": a.result(), "b_id": b_id}

    wrapped = durable_execution(handler)

    def entrypoint(event, context):
        state = event.service_client.get_execution_state(
            event.durable_execution_arn, event.checkpoint_token, ""
        )
        snapshots.append(
            {
                "input_ids": [
                    op.operation_id for op in event.initial_execution_state.operations
                ],
                "state_ids": [op.operation_id for op in state.operations],
                "updated_ids": list(event.updated_operation_ids),
            }
        )
        return wrapped(event, context)

    def completed(runner, arn, count):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            events = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
            if sum(e.event_type == "InvocationCompleted" for e in events) >= count:
                return events
            time.sleep(0.01)
        raise AssertionError("invocation completion not observed")

    with DurableFunctionTestRunner(
        handler=entrypoint,
        store=store,
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=25,
    ) as runner:
        arn = runner.run_async(input="{}")
        a_id = runner.wait_for_callback(arn, name="root-a", timeout=5)
        b_id = runner.wait_for_callback(arn, name="hidden-b", timeout=5)
        first = completed(runner, arn, 1)
        b_operation_id = next(
            e.operation_id
            for e in first
            if e.name == "hidden-b" and e.event_type == "CallbackStarted"
        )
        runner.send_callback_success(b_id, result=b'"B"')
        completed(runner, arn, 2)
        runner.send_callback_success(a_id, result=b'"A"')
        result = runner.wait_for_result(arn, timeout=10)
        history = runner.get_execution_history(arn, include_execution_data=True).events
    assert result.status.value == "SUCCEEDED" and json.loads(result.result) == {
        "a": "A",
        "b_id": b_id,
    }
    assert len(snapshots) == 3 and child_calls == [1] and saved_calls == [1]
    assert b_operation_id in snapshots[1]["updated_ids"]
    assert b_operation_id not in snapshots[1]["input_ids"]
    assert b_operation_id not in snapshots[1]["state_ids"]
    assert any(op.operation_id == b_operation_id for op in store.load(arn).operations)
    assert (
        result.get_operation_by_name("closed-parent")
        .get_operation_by_name("hidden-b")
        .result
        == '"B"'
    )
    assert (
        sum(
            e.event_type == "CallbackSucceeded" and e.operation_id == b_operation_id
            for e in history
        )
        == 1
    )
