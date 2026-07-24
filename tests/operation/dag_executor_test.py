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


def test_failure_tolerance_percentage_exceeded():
    def boom(_deps, _sc):
        raise ValueError("x")

    def register(d):
        d.step(boom, name="a")
        d.step(lambda deps, sc: 1, name="b")

    result, _ = run_dag(
        register,
        DagConfig(
            default_retry_strategy=NO_RETRY,
            completion_config=CompletionConfig(tolerated_failure_percentage=10),
        ),
    )
    assert result.completion_reason is DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED


def test_invalid_max_concurrency():
    state, _ = make_state()
    ctx = make_context(state, parent_id="dag")
    d = DagContextImpl(ctx, DagConfig())
    d.step(lambda deps, sc: 1, name="a")
    with pytest.raises(ValidationError):
        DagExecutor(ctx, d.get_tasks(), DagConfig(max_concurrency=0))


def test_default_trigger_rule_from_config_applies():
    """DagConfig.default_trigger_rule is used when a task sets no explicit rule."""

    def boom(_deps, _sc):
        raise ValueError("x")

    def register(d):
        a = d.step(boom, name="a")
        # no explicit trigger_rule -> inherits config default ALL_DONE, so it
        # runs even though its upstream FAILED.
        d.step(lambda deps, sc: "ran", deps=[a], name="b")

    result, _ = run_dag(
        register,
        DagConfig(
            default_retry_strategy=NO_RETRY,
            default_trigger_rule=TriggerRule.ALL_DONE,
        ),
    )
    assert result.get_status("a") is TaskStatus.FAILED
    assert result.get_status("b") is TaskStatus.SUCCEEDED
    assert result.get_result("b") == "ran"


def test_explicit_trigger_rule_overrides_config_default():
    """An explicit per-task trigger_rule wins over DagConfig.default_trigger_rule."""

    def register(d):
        a = d.step(lambda deps, sc: 1, name="a")
        # config default is ALL_DONE, but explicit ALL_FAILED + a SUCCEEDED => skip
        d.step(
            lambda deps, sc: "ran",
            deps=[a],
            name="b",
            trigger_rule=TriggerRule.ALL_FAILED,
        )

    result, _ = run_dag(register, DagConfig(default_trigger_rule=TriggerRule.ALL_DONE))
    assert result.get_status("b") is TaskStatus.SKIPPED


# region run_if-raises regression (worker-thread callback swallowed exceptions)
import signal  # noqa: E402
from contextlib import contextmanager  # noqa: E402


@contextmanager
def _fail_on_hang(seconds: int = 10):
    """Turn a scheduler hang into an assertion failure instead of blocking the
    whole test session. SIGALRM fires on the main thread (where pytest runs)."""

    def _handler(_signum, _frame):
        raise AssertionError("DagExecutor.run() hung (run_if regression)")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def test_run_if_raises_on_non_root_fails_task_and_drains():
    """A run_if that raises on a downstream task (evaluated inside a worker-thread
    completion callback) must not hang: the task FAILS and the DAG drains."""

    def register(d):
        a = d.step(lambda deps, sc: "A", name="a")
        # run_if dereferences a missing dep -> KeyError, evaluated after `a` done
        d.step(
            lambda deps, sc: "ran",
            deps=[a],
            name="b",
            run_if=lambda deps: deps["missing"] > 0,
        )
        # independent root task must still run (drain, not fail-fast)
        d.step(lambda deps, sc: "c-ran", name="c")

    with _fail_on_hang():
        result, _ = run_dag(register, DagConfig(default_retry_strategy=NO_RETRY))

    assert result.get_status("a") is TaskStatus.SUCCEEDED
    assert result.get_status("b") is TaskStatus.FAILED
    assert result.results["b"].error is not None
    assert result.get_result("c") == "c-ran"
    assert result.failure_count == 1
    assert result.completion_reason is DagCompletionReason.COMPLETED_WITH_FAILURES
    with pytest.raises(DagExecutionError):
        result.throw_if_error()


def test_run_if_raises_on_root_fails_task():
    """A raising run_if on a root task fails that task (consistent with the
    non-root path) rather than aborting the whole DAG."""

    def register(d):
        d.step(
            lambda deps, sc: "ran",
            name="a",
            run_if=lambda deps: 1 // 0 == 0,
        )
        d.step(lambda deps, sc: "b-ran", name="b")

    with _fail_on_hang():
        result, _ = run_dag(register, DagConfig(default_retry_strategy=NO_RETRY))

    assert result.get_status("a") is TaskStatus.FAILED
    assert result.get_result("b") == "b-ran"
    assert result.failure_count == 1


# endregion run_if-raises regression


# region threshold-completion fidelity (mirrors ExecutionCounters.should_complete)
def _threshold_executor(task_count, config):
    state, _ = make_state()
    ctx = make_context(state, parent_id="dag")
    d = DagContextImpl(ctx, config)
    for i in range(task_count):
        d.step(lambda deps, sc: 1, name=f"t{i}")
    return DagExecutor(ctx, d.get_tasks(), config)


def test_threshold_success_checked_before_failure():
    """When both min_successful and failure-tolerance fire, success wins (matches
    batch ExecutionCounters ordering)."""
    ex = _threshold_executor(
        3,
        DagConfig(
            completion_config=CompletionConfig(
                min_successful=2, tolerated_failure_count=0
            )
        ),
    )
    ex._success = 2
    ex._failure = 1
    assert ex._threshold_reason_locked() is DagCompletionReason.MIN_SUCCESSFUL_REACHED


def test_threshold_impossible_to_succeed_stops_early():
    """Once min_successful can no longer be reached, stop (reported as
    FAILURE_TOLERANCE_EXCEEDED, matching batch _create_result)."""
    ex = _threshold_executor(
        3, DagConfig(completion_config=CompletionConfig(min_successful=3))
    )
    ex._failure = 1  # max reachable successes = 3 - 1 = 2 < 3
    assert (
        ex._threshold_reason_locked() is DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED
    )


def test_threshold_percentage_denominator_excludes_skipped():
    """Skipped tasks are excluded from the failure-percentage denominator so
    they do not dilute the ratio."""
    ex = _threshold_executor(
        4, DagConfig(completion_config=CompletionConfig(tolerated_failure_percentage=40))
    )
    ex._skip = 2
    ex._failure = 1
    ex._success = 1
    # denom = 4 - 2 = 2 -> 50% > 40% -> exceeded. (Old denom=4 -> 25%, would NOT.)
    assert (
        ex._threshold_reason_locked() is DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED
    )


# endregion threshold-completion fidelity
