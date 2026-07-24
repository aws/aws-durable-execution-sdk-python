"""T8/T9: exercise every DagContext task-kind executor closure end-to-end."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.config import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from aws_durable_execution_sdk_python.dag import DagConfig, TaskStatus
from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from aws_durable_execution_sdk_python.retries import RetryPresets
from tests.dag_support import make_context, make_state

NO_RETRY = RetryPresets.none()


def test_map_task():
    def register(d):
        d.map(
            [1, 2, 3],
            lambda ctx, item, idx, items: ctx.step(lambda sc: item * 2, name=f"m{idx}"),
            name="mymap",
        )

    state, _ = make_state()
    result = make_context(state).dag(register, name="p")
    assert result.get_status("mymap") is TaskStatus.SUCCEEDED
    batch = result.get_result("mymap")
    assert sorted(batch.get_results()) == [2, 4, 6]


def test_parallel_task():
    def register(d):
        d.parallel(
            [
                lambda ctx: ctx.step(lambda sc: "one", name="b1"),
                lambda ctx: ctx.step(lambda sc: "two", name="b2"),
            ],
            name="par",
        )

    state, _ = make_state()
    result = make_context(state).dag(register, name="p")
    assert result.get_status("par") is TaskStatus.SUCCEEDED
    assert sorted(result.get_result("par").get_results()) == ["one", "two"]


def test_wait_for_condition_task_completes():
    def check(deps, state_val, cctx):
        return {"done": True}

    cfg = WaitForConditionConfig(
        wait_strategy=lambda s, attempt: WaitForConditionDecision.stop_polling(),
        initial_state={"done": False},
    )

    def register(d):
        d.wait_for_condition(check, cfg, name="poll")

    state, _ = make_state()
    result = make_context(state).dag(register, name="p")
    assert result.get_status("poll") is TaskStatus.SUCCEEDED
    assert result.get_result("poll") == {"done": True}


def test_invoke_task_suspends():
    def register(d):
        d.invoke("fn:prod", lambda deps: {"x": 1}, name="charge")

    state, _ = make_state()
    with pytest.raises(SuspendExecution):
        make_context(state).dag(register, name="p")


def test_wait_task_suspends():
    def register(d):
        d.wait(30, name="cooldown")

    state, _ = make_state()
    with pytest.raises(SuspendExecution):
        make_context(state).dag(register, name="p")


def test_wait_for_callback_task_suspends():
    def register(d):
        d.wait_for_callback(
            lambda deps, cb_id, ctx: None, name="approval"
        )

    state, _ = make_state()
    with pytest.raises(SuspendExecution):
        make_context(state).dag(register, name="p")


def test_invoke_eager_payload():
    """Non-callable payload is used as-is (covers the eager branch)."""
    def register(d):
        d.invoke("fn:prod", {"eager": True}, name="charge")

    state, _ = make_state()
    with pytest.raises(SuspendExecution):
        make_context(state).dag(register, name="p")


def test_map_inputs_from_deps():
    def register(d):
        src = d.step(lambda deps, sc: [1, 2], name="src")
        d.map(
            lambda deps: deps["src"],
            lambda ctx, item, idx, items: ctx.step(lambda sc: item + 10, name=f"m{idx}"),
            deps=[src],
            name="mymap",
        )

    state, _ = make_state()
    result = make_context(state).dag(register, name="p")
    assert sorted(result.get_result("mymap").get_results()) == [11, 12]
