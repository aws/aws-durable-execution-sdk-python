"""T5: DagExecutor scheduler tests."""

from __future__ import annotations

import threading
import time

import pytest

from aws_durable_execution_sdk_python.config import CompletionConfig
from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagConfig,
    SkipReason,
    TaskStatus,
    TriggerRule,
)
from aws_durable_execution_sdk_python.exceptions import (
    DagExecutionError,
    ValidationError,
)
from aws_durable_execution_sdk_python.operation.dag_context import DagContextImpl
from aws_durable_execution_sdk_python.operation.dag_executor import DagExecutor
from aws_durable_execution_sdk_python.operation.dag_validator import validate_dag
from aws_durable_execution_sdk_python.retries import RetryPresets
from tests.dag_support import make_context, make_state

NO_RETRY = RetryPresets.none()


def run_dag(register, config=None, parent_id="dag"):
    config = config or DagConfig()
    state, client = make_state()
    ctx = make_context(state, parent_id=parent_id)
    d = DagContextImpl(ctx, config)
    register(d)
    validate_dag(d)
    result = DagExecutor(ctx, d.get_tasks(), config).run()
    return result, client


def test_diamond_topological_order_and_results():
    order = []
    order_lock = threading.Lock()

    def rec(name):
        with order_lock:
            order.append(name)

    def register(d):
        a = d.step(lambda deps, sc: (rec("a"), "A")[1], name="a")
        b = d.step(lambda deps, sc: (rec("b"), deps["a"] + "B")[1], deps=[a], name="b")
        c = d.step(lambda deps, sc: (rec("c"), deps["a"] + "C")[1], deps=[a], name="c")
        d.step(
            lambda deps, sc: (rec("d"), deps["b"] + deps["c"])[1],
            deps=[b, c],
            name="d",
        )

    result, _ = run_dag(register)
    assert result.get_status("d") is TaskStatus.SUCCEEDED
    assert result.get_result("d") == "ABAC"
    assert result.success_count == 4
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    # a before b,c before d
    assert order[0] == "a"
    assert order[-1] == "d"


def test_branches_run_concurrently():
    barrier = threading.Barrier(2, timeout=3)
    both = {"ok": False}

    def branch(_deps, _sc):
        try:
            barrier.wait()
            both["ok"] = True
        except threading.BrokenBarrierError:  # pragma: no cover
            pass
        return 1

    def register(d):
        a = d.step(lambda deps, sc: 0, name="a")
        d.step(branch, deps=[a], name="b")
        d.step(branch, deps=[a], name="c")

    result, _ = run_dag(register)
    assert both["ok"] is True  # b and c reached the barrier simultaneously
    assert result.success_count == 3


def test_max_concurrency_throttles():
    current = {"n": 0, "max": 0}
    lock = threading.Lock()

    def slow(_deps, _sc):
        with lock:
            current["n"] += 1
            current["max"] = max(current["max"], current["n"])
        time.sleep(0.05)
        with lock:
            current["n"] -= 1
        return 1

    def register(d):
        for i in range(5):
            d.step(slow, name=f"t{i}")

    result, _ = run_dag(register, DagConfig(max_concurrency=2))
    assert result.success_count == 5
    assert current["max"] <= 2


def test_trigger_rule_skip_propagation():
    def boom(_deps, _sc):
        raise ValueError("boom")

    def register(d):
        a = d.step(boom, name="a")
        # default ALL_SUCCESS -> skipped because a FAILED
        d.step(lambda deps, sc: 1, deps=[a], name="b")

    result, _ = run_dag(register, DagConfig(default_retry_strategy=NO_RETRY))
    assert result.get_status("a") is TaskStatus.FAILED
    assert result.get_status("b") is TaskStatus.SKIPPED
    assert result.results["b"].skip_reason is SkipReason.TRIGGER_RULE
    assert result.completion_reason is DagCompletionReason.COMPLETED_WITH_FAILURES


def test_compensation_all_failed_runs_on_failure():
    def charge(_deps, _sc):
        raise RuntimeError("charge failed")

    def register(d):
        c = d.step(charge, name="charge")
        # refund runs when charge FAILED
        d.step(lambda deps, sc: "refunded", deps=[c], name="refund").trigger_rule(
            TriggerRule.ALL_FAILED
        )
        # fulfill only on success -> skipped
        d.step(lambda deps, sc: "fulfilled", deps=[c], name="fulfill")
        # audit always runs
        d.step(lambda deps, sc: "audited", deps=[c], name="audit").trigger_rule(
            TriggerRule.ALL_DONE
        )

    result, _ = run_dag(register, DagConfig(default_retry_strategy=NO_RETRY))
    assert result.get_status("charge") is TaskStatus.FAILED
    assert result.get_result("refund") == "refunded"
    assert result.get_status("fulfill") is TaskStatus.SKIPPED
    assert result.get_result("audit") == "audited"


def test_run_if_skip():
    def register(d):
        a = d.step(lambda deps, sc: 10, name="a")
        d.step(
            lambda deps, sc: "ran",
            deps=[a],
            name="b",
            run_if=lambda deps: deps["a"] > 100,
        )

    result, _ = run_dag(register)
    assert result.get_status("b") is TaskStatus.SKIPPED
    assert result.results["b"].skip_reason is SkipReason.RUN_IF_PREDICATE


def test_min_successful_early_completion():
    def register(d):
        for i in range(4):
            d.step(lambda deps, sc: 1, name=f"t{i}")

    result, _ = run_dag(
        register, DagConfig(completion_config=CompletionConfig(min_successful=2))
    )
    assert result.completion_reason is DagCompletionReason.MIN_SUCCESSFUL_REACHED
    assert result.success_count >= 2


def test_failure_tolerance_exceeded():
    def boom(_deps, _sc):
        raise ValueError("x")

    def register(d):
        for i in range(3):
            d.step(boom, name=f"t{i}")

    result, _ = run_dag(
        register,
        DagConfig(
            default_retry_strategy=NO_RETRY,
            completion_config=CompletionConfig(tolerated_failure_count=0),
        ),
    )
    assert result.completion_reason is DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_default_drains_on_failure_no_fail_fast():
    """A failure does not abort; independent tasks still run (drain)."""
    ran = {"b": False}

    def boom(_deps, _sc):
        raise ValueError("x")

    def register(d):
        d.step(boom, name="a")
        d.step(lambda deps, sc: ran.__setitem__("b", True), name="b")

    result, _ = run_dag(register, DagConfig(default_retry_strategy=NO_RETRY))
    assert ran["b"] is True
    assert result.failure_count == 1
    assert result.success_count == 1


def test_throw_if_error():
    def boom(_deps, _sc):
        raise ValueError("bad")

    def register(d):
        d.step(boom, name="a")

    result, _ = run_dag(register, DagConfig(default_retry_strategy=NO_RETRY))
    with pytest.raises(DagExecutionError):
        result.throw_if_error()


def test_empty_dag():
    result, _ = run_dag(lambda d: None)
    assert result.total_count == 0
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED


def test_invalid_max_concurrency():
    state, _ = make_state()
    ctx = make_context(state, parent_id="dag")
    d = DagContextImpl(ctx, DagConfig())
    d.step(lambda deps, sc: 1, name="a")
    with pytest.raises(ValidationError):
        DagExecutor(ctx, d.get_tasks(), DagConfig(max_concurrency=0))
