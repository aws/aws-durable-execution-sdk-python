"""T5: DagExecutor scheduler tests."""

from __future__ import annotations

import threading
import time

import pytest

from aws_durable_execution_sdk_python.config import CompletionConfig, StepConfig
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
# Per-task step config that disables retries, so an intentionally failing step
# fails promptly (attempt 1) instead of falling back to RetryPresets.default().
NO_RETRY_CFG = StepConfig(retry_strategy=NO_RETRY)


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
        a = d.step(boom, name="a", config=NO_RETRY_CFG)
        # default ALL_SUCCESS -> skipped because a FAILED
        d.step(lambda deps, sc: 1, deps=[a], name="b")

    result, _ = run_dag(register)
    assert result.get_status("a") is TaskStatus.FAILED
    assert result.get_status("b") is TaskStatus.SKIPPED
    assert result.results["b"].skip_reason is SkipReason.TRIGGER_RULE
    assert result.completion_reason is DagCompletionReason.COMPLETED_WITH_FAILURES


def test_compensation_all_failed_runs_on_failure():
    def charge(_deps, _sc):
        raise RuntimeError("charge failed")

    def register(d):
        c = d.step(charge, name="charge", config=NO_RETRY_CFG)
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

    result, _ = run_dag(register)
    assert result.get_status("charge") is TaskStatus.FAILED
    assert result.get_result("refund") == "refunded"
    assert result.get_status("fulfill") is TaskStatus.SKIPPED
    assert result.get_result("audit") == "audited"


def test_deps_value_is_none_for_failed_upstream_under_all_done():
    """A non-ALL_SUCCESS task (ALL_DONE) may run while an upstream FAILED. Reading
    that dependency's result inside the body yields ``None`` at runtime — the
    long-standing behavior that the ``DepsMap[handle] -> T | None`` type reflects.

    Exercises the ``TaskHandle`` (typed) access path specifically, since that is
    the overload whose return type was corrected from bare ``T`` to ``T | None``.
    Also asserts ``DagResult.get_result(handle)`` returns ``None`` for the same
    failed task (its handle overload has the identical fix).
    """
    seen = {}

    def boom(_deps, _sc):
        raise ValueError("boom")

    def register(d):
        charge = d.step(boom, name="charge", config=NO_RETRY_CFG)

        def audit(deps, _sc):
            # Handle-typed access: value is None because `charge` FAILED, even
            # though this ALL_DONE task legitimately runs. Also confirm the
            # string-keyed access agrees.
            seen["by_handle"] = deps[charge]
            seen["by_name"] = deps["charge"]
            return "audited"

        d.step(audit, deps=[charge], name="audit").trigger_rule(TriggerRule.ALL_DONE)
        # Expose the handle to the assertions below.
        seen["charge_handle"] = charge

    result, _ = run_dag(register)

    assert result.get_status("charge") is TaskStatus.FAILED
    assert result.get_status("audit") is TaskStatus.SUCCEEDED
    assert result.get_result("audit") == "audited"
    # The dependency's value inside the body was None (not the bare result type).
    assert seen["by_handle"] is None
    assert seen["by_name"] is None
    # DagResult.get_result for the failed task is likewise None (its handle
    # overload was corrected to T | None as well).
    assert result.get_result(seen["charge_handle"]) is None
    assert result.get_result("charge") is None


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
            d.step(boom, name=f"t{i}", config=NO_RETRY_CFG)

    result, _ = run_dag(
        register,
        DagConfig(
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
        d.step(boom, name="a", config=NO_RETRY_CFG)
        d.step(lambda deps, sc: ran.__setitem__("b", True), name="b")

    result, _ = run_dag(register)
    assert ran["b"] is True
    assert result.failure_count == 1
    assert result.success_count == 1


def test_throw_if_error():
    def boom(_deps, _sc):
        raise ValueError("bad")

    def register(d):
        d.step(boom, name="a", config=NO_RETRY_CFG)

    result, _ = run_dag(register)
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
        d.step(boom, name="a", config=NO_RETRY_CFG)
        d.step(lambda deps, sc: 1, name="b")

    result, _ = run_dag(
        register,
        DagConfig(
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
        a = d.step(boom, name="a", config=NO_RETRY_CFG)
        # no explicit trigger_rule -> inherits config default ALL_DONE, so it
        # runs even though its upstream FAILED.
        d.step(lambda deps, sc: "ran", deps=[a], name="b")

    result, _ = run_dag(
        register,
        DagConfig(
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


# region run_if-raises abort (a raising predicate ABORTS the DAG)
import signal  # noqa: E402
from contextlib import contextmanager  # noqa: E402

from aws_durable_execution_sdk_python.exceptions import DagPredicateError  # noqa: E402


@contextmanager
def _fail_on_hang(seconds: int = 10):
    """Turn a scheduler hang into an assertion failure instead of blocking the
    whole test session. SIGALRM fires on the main thread (where pytest runs)."""

    def _handler(_signum, _frame):
        raise AssertionError("DagExecutor.run() hung (run_if abort regression)")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def _make_executor(register, config=None, parent_id="dag"):
    """Build a DagExecutor so a test can inspect ``_results`` after ``run()``
    raises (``run_dag`` can't, because the abort means there is no DagResult)."""
    config = config or DagConfig()
    state, _ = make_state()
    ctx = make_context(state, parent_id=parent_id)
    d = DagContextImpl(ctx, config)
    register(d)
    validate_dag(d)
    return DagExecutor(ctx, d.get_tasks(), config)


def test_run_if_raises_on_non_root_aborts_dag():
    """A run_if that raises on a downstream task (evaluated inside a worker-thread
    completion callback) ABORTS the DAG with DagPredicateError: the offending
    task gets no terminal state and a downstream ALL_FAILED compensation task
    never runs."""

    def register(d):
        a = d.step(lambda deps, sc: "A", name="a")
        # run_if dereferences a missing dep -> KeyError, evaluated after `a` done
        b = d.step(
            lambda deps, sc: "ran",
            deps=[a],
            name="b",
            run_if=lambda deps: deps["missing"] > 0,
        )
        # ALL_FAILED compensation on the offending task: MUST NOT run, because a
        # predicate defect must never drive a compensation path.
        d.step(
            lambda deps, sc: "refunded",
            deps=[b],
            name="refund",
            trigger_rule=TriggerRule.ALL_FAILED,
        )

    ex = _make_executor(register)
    with _fail_on_hang(), pytest.raises(DagPredicateError) as ei:
        ex.run()

    assert isinstance(ei.value.__cause__, KeyError)
    assert ei.value.task_name == "b"
    assert "b" in str(ei.value)
    # `a` completed normally; `b` (offending) has NO terminal state; `refund`
    # (downstream ALL_FAILED) never ran.
    assert ex._results["a"].status is TaskStatus.SUCCEEDED  # noqa: SLF001
    assert "b" not in ex._results  # noqa: SLF001
    assert "refund" not in ex._results  # noqa: SLF001


def test_run_if_raises_on_root_aborts_dag():
    """A raising run_if on a root task ABORTS the DAG (propagates out of the very
    first pump on the caller thread) rather than failing that task."""

    def register(d):
        a = d.step(
            lambda deps, sc: "ran",
            name="a",
            run_if=lambda deps: 1 // 0 == 0,
        )
        # ALL_FAILED compensation on the offending root: MUST NOT run.
        d.step(
            lambda deps, sc: "refunded",
            deps=[a],
            name="refund",
            trigger_rule=TriggerRule.ALL_FAILED,
        )

    ex = _make_executor(register)
    with _fail_on_hang(), pytest.raises(DagPredicateError) as ei:
        ex.run()

    assert isinstance(ei.value.__cause__, ZeroDivisionError)
    assert ei.value.task_name == "a"
    assert "a" not in ex._results  # noqa: SLF001 - no terminal state
    assert "refund" not in ex._results  # noqa: SLF001 - compensation did not run


# endregion run_if-raises abort


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


# region multi-suspend precedence (earliest timed wins over indefinite)
from aws_durable_execution_sdk_python.exceptions import (  # noqa: E402
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.operation.dag_context import TaskDef  # noqa: E402


def _suspend_executor(specs):
    """Build a DagExecutor of independent root tasks that each raise `exc`.

    `specs` is a list of (name, exception) pairs; every task is a root
    (empty deps, ALL_SUCCESS) so all are submitted concurrently.
    """
    state, _ = make_state()
    ctx = make_context(state, parent_id="dag")

    def make_executor(exc):
        def executor(_ctx, _deps_map):
            raise exc

        return executor

    tasks = {
        name: TaskDef(
            name=name,
            kind="step",
            inline_deps=[],
            all_deps=[],
            trigger_rule=TriggerRule.ALL_SUCCESS,
            run_if=None,
            config=None,
            executor=make_executor(exc),
        )
        for name, exc in specs
    }
    return DagExecutor(ctx, tasks, DagConfig())


def test_two_concurrent_timed_waits_raise_earliest_timestamp():
    """(a) Two concurrent timed suspends -> the EARLIEST timestamp is raised."""
    now = time.time()
    ex = _suspend_executor(
        [
            ("slow", TimedSuspendExecution("slow", now + 100)),
            ("fast", TimedSuspendExecution("fast", now + 5)),
        ]
    )
    with pytest.raises(TimedSuspendExecution) as ei:
        ex.run()
    assert ei.value.scheduled_timestamp == pytest.approx(now + 5)


def test_timed_suspend_wins_over_indefinite():
    """(b) Timed + indefinite concurrent -> timed wins (timer not dropped)."""
    now = time.time()
    ex = _suspend_executor(
        [
            ("callback", SuspendExecution("waiting for external callback")),
            ("timer", TimedSuspendExecution("timer", now + 7)),
        ]
    )
    with pytest.raises(TimedSuspendExecution) as ei:
        ex.run()
    assert ei.value.scheduled_timestamp == pytest.approx(now + 7)


def test_indefinite_only_raises_indefinite_suspend():
    """No timed suspend pending -> the indefinite SuspendExecution is raised."""
    ex = _suspend_executor([("callback", SuspendExecution("external callback"))])
    with pytest.raises(SuspendExecution) as ei:
        ex.run()
    assert not isinstance(ei.value, TimedSuspendExecution)


# endregion multi-suspend precedence


# region in-process timed resume (map/parallel parity)
def _root_executor(specs, config=None):
    """Build a DagExecutor whose independent root tasks run `specs` concurrently.

    `specs` is a list of (name, func) where func(ctx, deps_map) returns a result
    or raises. Every task is a root (empty deps, ALL_SUCCESS) so all are
    submitted at once. Returns (executor, in_memory_client).
    """
    state, client = make_state()
    ctx = make_context(state, parent_id="dag")
    tasks = {
        name: TaskDef(
            name=name,
            kind="step",
            inline_deps=[],
            all_deps=[],
            trigger_rule=TriggerRule.ALL_SUCCESS,
            run_if=None,
            config=None,
            executor=func,
        )
        for name, func in specs
    }
    return DagExecutor(ctx, tasks, config or DagConfig()), client


def test_timed_wait_resumes_in_process_within_single_invocation():
    """(a) A timed suspend is resumed IN-PROCESS by the DAG-owned TimerScheduler while
    a concurrent task keeps the invocation alive: run() returns a success result
    (no SuspendExecution bubbles to the platform) and the timed task re-runs."""
    calls = {"x": 0, "y": 0}
    x_done = threading.Event()

    def x(_ctx, _deps):
        calls["x"] += 1
        if calls["x"] == 1:
            # First pass suspends with a short timer; the scheduler must re-run
            # this task in-process rather than surfacing a platform suspend.
            raise TimedSuspendExecution("wait", time.time() + 0.05)
        x_done.set()
        return "x-done"

    def y(_ctx, _deps):
        calls["y"] += 1
        # Stay RUNNING until X has resumed + completed, so the DAG never settles
        # into a platform suspend for the pure-timed case.
        assert x_done.wait(timeout=3)
        return "y-done"

    ex, client = _root_executor([("x", x), ("y", y)])
    result = ex.run()  # must NOT raise -> resumed within a single invocation

    assert result.get_status("x") is TaskStatus.SUCCEEDED
    assert result.get_status("y") is TaskStatus.SUCCEEDED
    assert result.get_result("x") == "x-done"
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert calls["x"] == 2  # initial + in-process timed resume
    assert calls["y"] == 1
    # The resume checkpoints before re-running (mirrors ConcurrentExecutor).
    assert client.checkpoint_count >= 1


def test_indefinite_callback_suspends_the_invocation():
    """(b) An indefinite (callback) suspend can only be resolved by the platform:
    run() raises a plain SuspendExecution and the task is never re-run."""
    calls = {"n": 0}

    def approval(_ctx, _deps):
        calls["n"] += 1
        raise SuspendExecution("waiting for external callback")

    ex, _ = _root_executor([("approval", approval)])
    with pytest.raises(SuspendExecution) as ei:
        ex.run()
    assert not isinstance(ei.value, TimedSuspendExecution)
    assert calls["n"] == 1  # no in-process resume for indefinite suspends


def test_mixed_timed_and_indefinite_forces_platform_suspend():
    """(c) Timed + indefinite concurrently: the indefinite one forces a platform
    suspend, and timed-wins precedence still surfaces the EARLIEST timer so the
    platform resumes as soon as possible. Neither task resumes in-process."""
    now = time.time()
    calls = {"cb": 0, "timer": 0}

    def cb(_ctx, _deps):
        calls["cb"] += 1
        raise SuspendExecution("external callback")

    def timer(_ctx, _deps):
        calls["timer"] += 1
        raise TimedSuspendExecution("timer", now + 5)

    ex, _ = _root_executor([("cb", cb), ("timer", timer)])
    with pytest.raises(TimedSuspendExecution) as ei:
        ex.run()
    assert ei.value.scheduled_timestamp == pytest.approx(now + 5)
    # Indefinite forces platform suspend -> no in-process re-run of either task.
    assert calls["cb"] == 1
    assert calls["timer"] == 1


# endregion in-process timed resume


# region no checkpoint after abort (teardown-window regression)
from aws_durable_execution_sdk_python.dag import TaskHandle  # noqa: E402
from aws_durable_execution_sdk_python.operation.dag_executor import (  # noqa: E402
    _TimedResume,
)


def _bare_executor():
    """A DagExecutor with no tasks, wired to an in-memory client so a direct
    ``_resubmit`` call exercises the real ``create_checkpoint`` path (it lands
    on ``InMemoryServiceClient.checkpoint_count``)."""
    state, client = make_state()
    ctx = make_context(state, parent_id="dag")
    return DagExecutor(ctx, {}, DagConfig()), client


def test_resubmit_checkpoints_when_not_aborting_control():
    """Sensitivity anchor. With the abort flag UNSET, a timed resume writes
    exactly one checkpoint before re-running (mirrors
    ``ConcurrentExecutor.resubmitter``) and clears the task's timer bookkeeping.

    This is the checkpoint the abort guard must suppress. Pinning it with the
    *same* setup as the guard test below — the only difference being
    ``_scheduler_exception`` — proves the guard test is not vacuous: the count
    flips from 1 to 0 solely because of the abort flag.
    """
    ex, client = _bare_executor()
    ex._pending_timers.add("t")  # noqa: SLF001
    assert client.checkpoint_count == 0
    ex._resubmit([_TimedResume("t")])
    assert client.checkpoint_count == 1  # resume checkpointed
    assert "t" not in ex._pending_timers  # noqa: SLF001 - would re-run


def test_resubmit_writes_no_checkpoint_after_abort_decision():
    """Regression (direct): once the DAG has decided to abort
    (``_scheduler_exception`` set — e.g. a ``run_if`` predicate raised), a timed
    resume that fires during the teardown/drain window must write NO checkpoint
    and must not touch task state. Identical setup to the control above; only
    the abort flag differs, and it takes the checkpoint count from 1 to 0.
    """
    ex, client = _bare_executor()
    ex._pending_timers.add("t")  # noqa: SLF001
    ex._scheduler_exception = DagPredicateError("aborted", task_name="x")  # noqa: SLF001
    ex._resubmit([_TimedResume("t")])
    assert client.checkpoint_count == 0  # zero checkpoints after the abort decision
    assert "t" in ex._pending_timers  # noqa: SLF001 - guarded path left state intact


def test_no_late_checkpoint_in_abort_drain_window():
    """Regression (end-to-end race). Reproduces the reviewer's observation: the
    DAG-owned ``TimerScheduler`` is the OUTER context manager and the pool the
    INNER one, so the timer thread is still alive while the pool drains. A task
    that timed-suspended has a resume pending; when a *different* task's
    ``run_if`` aborts the DAG, the pending resume can fire ``_resubmit`` during
    the drain window and — before the fix — write a checkpoint AFTER the abort
    decision.

    Topology (raw TaskDefs so the step machinery does not emit its own
    checkpoints; the ONLY checkpoint source is ``_resubmit``):

      * ``seed``    (root) completes immediately -> makes ``gate`` ready on a
                    worker thread, so the abort is captured into
                    ``_scheduler_exception`` (the non-root abort path) rather
                    than raising out of the first pump.
      * ``timer``   (root) timed-suspends with a resume due *now* -> a resume is
                    queued on the scheduler heap and ``timer`` stays in
                    ``_pending_timers`` across the abort.
      * ``gate``    (deps=[seed]) ``run_if`` raises -> DAG aborts. It records
                    ``checkpoint_count`` at that instant.
      * ``blocker`` (root) stays in-flight ~0.5s to hold the pool-drain window
                    open, giving the ~0.1s timer loop several chances to fire
                    the due resume before the scheduler is torn down.

    Sensitivity: with the guard removed I observed exactly one late checkpoint
    (final == at-abort + 1) and this assertion fails; with the guard, the timer
    loop fires ``_resubmit`` in the same window but it early-returns, so the
    count is unchanged.
    """
    from aws_durable_execution_sdk_python.dag import TriggerRule  # noqa: PLC0415
    from aws_durable_execution_sdk_python.operation.dag_context import (  # noqa: PLC0415
        TaskDef,
    )

    state, client = make_state()
    ctx = make_context(state, parent_id="dag")
    at_abort = {"count": None}

    def seed_exec(_ctx, _deps):
        return "seed"

    def timer_exec(_ctx, _deps):
        # Due immediately: the scheduler queues a resume the timer thread will
        # fire on its next (<=0.1s) loop, i.e. squarely inside the drain window.
        raise TimedSuspendExecution("timer", time.time())

    def blocker_exec(_ctx, _deps):
        # Keep the pool draining so the scheduler (outer CM) is not yet torn
        # down while the due resume fires.
        time.sleep(0.5)
        return "blocker"

    def gate_run_if(_deps):
        # The abort decision. Snapshot the checkpoint count at this instant;
        # nothing legitimate may checkpoint afterwards.
        at_abort["count"] = client.checkpoint_count
        raise KeyError("predicate defect")

    seed_ref = TaskHandle(_name="seed", _dag=None)

    def _root(name, executor):
        return TaskDef(
            name=name,
            kind="step",
            inline_deps=[],
            all_deps=[],
            trigger_rule=TriggerRule.ALL_SUCCESS,
            run_if=None,
            config=None,
            executor=executor,
        )

    tasks = {
        "seed": _root("seed", seed_exec),
        "timer": _root("timer", timer_exec),
        "blocker": _root("blocker", blocker_exec),
        "gate": TaskDef(
            name="gate",
            kind="step",
            inline_deps=[],
            all_deps=[seed_ref],
            trigger_rule=TriggerRule.ALL_SUCCESS,
            run_if=gate_run_if,
            config=None,
            executor=seed_exec,
        ),
    }
    ex = DagExecutor(ctx, tasks, DagConfig())

    with _fail_on_hang(), pytest.raises(DagPredicateError):
        ex.run()

    # The abort actually happened via the predicate.
    assert at_abort["count"] is not None
    # Nothing checkpointed before the abort in this DAG, and — the regression —
    # nothing checkpointed after it either, despite the resume firing in-window.
    assert at_abort["count"] == 0
    assert client.checkpoint_count == 0, (
        f"late checkpoint after abort decision: {client.checkpoint_count}"
    )
    # And the aborting predicate's task never got a terminal state.
    assert "gate" not in ex._results  # noqa: SLF001


# endregion no checkpoint after abort (teardown-window regression)


# region default max_concurrency cap (contract: unset -> 40, previously unbounded)
from aws_durable_execution_sdk_python.operation import dag_executor as _dag_executor  # noqa: E402
from aws_durable_execution_sdk_python.operation.dag_executor import (  # noqa: E402
    DEFAULT_DAG_MAX_CONCURRENCY,
)


def test_default_dag_max_concurrency_constant_is_40():
    """Pin the shared cross-language default. The behavioural tests below size
    themselves off this constant, so this guards against a silent retune."""
    assert DEFAULT_DAG_MAX_CONCURRENCY == 40


def _spy_pool(monkeypatch):
    """Record the ``max_workers`` every ThreadPoolExecutor is built with.

    Returns the list the executor's real constructor is still invoked, so the
    DAG runs for real; we only observe the pool size."""
    captured: list[int] = []
    real = _dag_executor.ThreadPoolExecutor

    def spy(*args, **kwargs):
        captured.append(kwargs.get("max_workers", args[0] if args else None))
        return real(*args, **kwargs)

    monkeypatch.setattr(_dag_executor, "ThreadPoolExecutor", spy)
    return captured


def test_default_caps_pool_max_workers_when_unset(monkeypatch):
    """A DAG wider than the default and with NO ``max_concurrency`` must build
    its pool with exactly 40 workers, not one-per-task (the previously unbounded
    behaviour that spawned N OS threads). The pool is the actual resource being
    protected, so assert on the size it was constructed with."""
    captured = _spy_pool(monkeypatch)
    width = DEFAULT_DAG_MAX_CONCURRENCY + 20  # 60: comfortably wider than the cap

    def register(d):
        for i in range(width):
            d.step(lambda deps, sc: 1, name=f"t{i}")

    result, _ = run_dag(register)
    assert result.success_count == width
    assert captured == [DEFAULT_DAG_MAX_CONCURRENCY]
    assert captured[0] <= DEFAULT_DAG_MAX_CONCURRENCY


def test_default_never_exceeds_40_in_flight_when_unset():
    """The sensitive one. A graph wider than 40 with no ``max_concurrency`` must
    never run more than 40 task bodies concurrently. This asserts an OBSERVED
    peak (a lock-guarded counter), not a config value.

    A ``Barrier`` sized to the cap makes the assertion two-sided: every worker
    increments the live counter, then blocks on the barrier, so a full wave of
    exactly 40 bodies is simultaneously in flight before any releases — proving
    the pool genuinely reaches 40 (not merely stays under it). Because only the
    pool's threads ever run a body and each runs one at a time, the counter can
    exceed 40 only if the pool was built with >40 workers. Width is a whole
    multiple of the cap so the barrier drains in exact waves and never deadlocks;
    the same graph under the old unbounded behaviour would put all `width`
    bodies in flight at once, pushing the peak to `width`."""
    width = DEFAULT_DAG_MAX_CONCURRENCY * 2  # 80: two exact waves of 40
    tracker = {"current": 0, "peak": 0}
    lock = threading.Lock()
    barrier = threading.Barrier(DEFAULT_DAG_MAX_CONCURRENCY, timeout=10)

    def body(_deps, _sc):
        with lock:
            tracker["current"] += 1
            tracker["peak"] = max(tracker["peak"], tracker["current"])
        try:
            barrier.wait()
        except threading.BrokenBarrierError:  # pragma: no cover - only on regression
            pass
        finally:
            with lock:
                tracker["current"] -= 1
        return 1

    def register(d):
        for i in range(width):
            d.step(body, name=f"t{i}")

    result, _ = run_dag(register)
    assert result.success_count == width
    # Two-sided: exactly the cap was reached, and it was never exceeded.
    assert tracker["peak"] == DEFAULT_DAG_MAX_CONCURRENCY


def test_explicit_max_concurrency_below_40_wins(monkeypatch):
    """An explicit value below the default still wins: the pool is built with
    that value, not the 40 cap."""
    captured = _spy_pool(monkeypatch)

    def register(d):
        for i in range(60):
            d.step(lambda deps, sc: 1, name=f"t{i}")

    result, _ = run_dag(register, DagConfig(max_concurrency=5))
    assert result.success_count == 60
    assert captured == [5]


def test_explicit_max_concurrency_above_40_wins(monkeypatch):
    """An explicit value ABOVE the default still wins (the cap is only a default,
    never a ceiling): the pool is built with 50 workers for a 60-task graph."""
    captured = _spy_pool(monkeypatch)

    def register(d):
        for i in range(60):
            d.step(lambda deps, sc: 1, name=f"t{i}")

    result, _ = run_dag(register, DagConfig(max_concurrency=50))
    assert result.success_count == 60
    assert captured == [50]


def test_default_cap_does_not_over_allocate_for_small_graphs(monkeypatch):
    """A DAG narrower than the cap and unset ``max_concurrency`` still builds a
    pool sized to the task count (min(total, 40)), preserving the pre-change
    small-graph behaviour rather than always allocating 40."""
    captured = _spy_pool(monkeypatch)

    def register(d):
        for i in range(3):
            d.step(lambda deps, sc: 1, name=f"t{i}")

    result, _ = run_dag(register)
    assert result.success_count == 3
    assert captured == [3]


# endregion default max_concurrency cap
