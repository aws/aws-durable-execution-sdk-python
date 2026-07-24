"""T9: DAG integration tests via context.dag() (local in-memory runner)."""

from __future__ import annotations

import threading

from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagConfig,
    TaskStatus,
    TriggerRule,
)
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.retries import RetryPresets
from tests.dag_support import make_context, make_state

NO_RETRY = RetryPresets.none()


def test_diamond_runs_branches_concurrently_with_native_subtypes():
    barrier = threading.Barrier(2, timeout=3)
    reached = {"ok": False}

    def branch(_deps, _sc):
        barrier.wait()
        reached["ok"] = True
        return "b"

    def register(d):
        a = d.step(lambda deps, sc: "A", name="a")
        b = d.step(branch, deps=[a], name="b")
        c = d.step(branch, deps=[a], name="c")
        # a child-context task (mixed op type)
        d.run_in_child_context(
            lambda deps, child: "child-done", deps=[b, c], name="finish"
        )

    state, client = make_state()
    result = make_context(state).dag(register, name="pipeline")

    assert reached["ok"] is True  # b and c ran concurrently
    assert result.success_count == 4
    # step tasks appear as native STEP subtype under DAG_NODE_T_ ids
    assert client.operations["1-DAG_NODE_T_a"].sub_type is OperationSubType.STEP
    # child-context task appears as its native subtype
    assert (
        client.operations["1-DAG_NODE_T_finish"].sub_type
        is OperationSubType.RUN_IN_CHILD_CONTEXT
    )


def test_compensation_pattern():
    def charge(_deps, _sc):
        raise RuntimeError("charge failed")

    def register(d):
        c = d.step(charge, name="charge")
        d.step(lambda deps, sc: "refunded", deps=[c], name="refund").trigger_rule(
            TriggerRule.ALL_FAILED
        )
        d.step(lambda deps, sc: "fulfilled", deps=[c], name="fulfill")
        d.step(lambda deps, sc: "audited", deps=[c], name="audit").trigger_rule(
            TriggerRule.ALL_DONE
        )

    state, _ = make_state()
    result = make_context(state).dag(
        register, name="saga", config=DagConfig(default_retry_strategy=NO_RETRY)
    )
    assert result.get_status("charge") is TaskStatus.FAILED
    assert result.get_result("refund") == "refunded"  # ALL_FAILED ran
    assert result.get_status("fulfill") is TaskStatus.SKIPPED  # ALL_SUCCESS skipped
    assert result.get_result("audit") == "audited"  # ALL_DONE ran
    assert result.completion_reason is DagCompletionReason.COMPLETED_WITH_FAILURES


def test_run_if_branching():
    def register(d):
        a = d.step(lambda deps, sc: 5, name="a")
        d.step(
            lambda deps, sc: "high",
            deps=[a],
            name="high",
            run_if=lambda deps: deps["a"] > 10,
        )
        d.step(
            lambda deps, sc: "low",
            deps=[a],
            name="low",
            run_if=lambda deps: deps["a"] <= 10,
        )

    state, _ = make_state()
    result = make_context(state).dag(register, name="branch")
    assert result.get_status("high") is TaskStatus.SKIPPED
    assert result.get_result("low") == "low"


def test_nested_dag_isolation_and_subtypes():
    def inner(d):
        d.step(lambda deps, sc: "x", name="x")

    def outer(d):
        d.step(lambda deps, sc: "a", name="a")
        d.dag(inner, name="inner")

    state, client = make_state()
    result = make_context(state).dag(outer, name="outer")
    nested = result.get_result("inner")
    assert nested.get_result("x") == "x"
    # nested container id is name-based and carries the DAG subtype
    assert (
        client.operations["1-DAG_NODE_T_inner"].sub_type is OperationSubType.DAG
    )
    # nested sub-task id is prefixed by the nested container id (per-level)
    assert "1-DAG_NODE_T_inner-DAG_NODE_T_x" in client.operations
