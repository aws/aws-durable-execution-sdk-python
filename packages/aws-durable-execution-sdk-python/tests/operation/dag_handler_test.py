"""T7: context.dag() wiring, FutureWarning, error unwrapping, nested DAG, exports."""

from __future__ import annotations

import warnings

import pytest

from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagConfig,
    TaskStatus,
)
from aws_durable_execution_sdk_python.exceptions import DagCyclicDependencyError
from aws_durable_execution_sdk_python.retries import RetryPresets
from tests.dag_support import make_context, make_state

NO_RETRY = RetryPresets.none()


def _diamond(d):
    a = d.step(lambda deps, sc: "A", name="a")
    b = d.step(lambda deps, sc: deps["a"] + "B", deps=[a], name="b")
    c = d.step(lambda deps, sc: deps["a"] + "C", deps=[a], name="c")
    d.step(lambda deps, sc: deps["b"] + deps["c"], deps=[b, c], name="d")


def test_context_dag_end_to_end():
    state, _ = make_state()
    ctx = make_context(state)
    result = ctx.dag(_diamond, name="pipeline")
    assert result.get_result("d") == "ABAC"
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert result.success_count == 4


def test_future_warning_emitted_once():
    import aws_durable_execution_sdk_python.operation.dag as dag_mod

    dag_mod._warned = False  # reset for the test
    state, _ = make_state()
    ctx = make_context(state)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ctx.dag(lambda d: d.step(lambda deps, sc: 1, name="a"), name="p1")
        ctx.dag(lambda d: d.step(lambda deps, sc: 1, name="a"), name="p2")
    future_warnings = [w for w in caught if issubclass(w.category, FutureWarning)]
    assert len(future_warnings) == 1


def test_cycle_surfaces_typed_error():
    state, _ = make_state()
    ctx = make_context(state)

    def register(d):
        a = d.step(lambda deps, sc: 1, name="a")
        b = d.step(lambda deps, sc: 2, deps=[a], name="b")
        a.after(b)

    with pytest.raises(DagCyclicDependencyError):
        ctx.dag(register, name="cyclic")


def test_nested_dag_scope_isolation():
    state, _ = make_state()
    ctx = make_context(state)

    def inner(d):
        d.step(lambda deps, sc: "inner-x", name="x")

    def outer(d):
        d.step(lambda deps, sc: "outer-a", name="a")
        d.dag(inner, name="inner")

    result = ctx.dag(outer, name="outer")
    assert result.get_status("a") is TaskStatus.SUCCEEDED
    nested = result.get_result("inner")
    assert nested.get_result("x") == "inner-x"


def test_invalid_max_concurrency_raises_at_handler():
    from aws_durable_execution_sdk_python.exceptions import ValidationError

    state, _ = make_state()
    ctx = make_context(state)
    with pytest.raises(ValidationError):
        ctx.dag(
            lambda d: d.step(lambda deps, sc: 1, name="a"),
            name="p",
            config=DagConfig(max_concurrency=-1),
        )


def test_public_exports():
    import aws_durable_execution_sdk_python as sdk

    for symbol in [
        "DagContext",
        "TaskHandle",
        "DagResult",
        "DagConfig",
        "TriggerRule",
        "TaskStatus",
        "SkipReason",
        "DagCompletionReason",
        "DagExecutionError",
        "DagCyclicDependencyError",
        "DagInvalidTaskNameError",
        "DagDuplicateTaskError",
        "DagInvalidDependencyError",
        "DagPredicateError",
    ]:
        assert hasattr(sdk, symbol), symbol


def test_summary_generator_wired_into_child_config():
    """DagConfig.summary_generator is passed through to the container ChildConfig."""
    from aws_durable_execution_sdk_python.operation.dag import dag_handler

    captured = {}

    def fake_run_in_child_context(body, name, child_config):
        captured["config"] = child_config

    def gen(_result):  # pragma: no cover - not invoked (small payload)
        return "summary"

    state, _ = make_state()
    dag_handler(
        run_in_child_context=fake_run_in_child_context,
        state=state,
        name="p",
        register=lambda d: None,
        config=DagConfig(summary_generator=gen),
    )
    assert captured["config"].summary_generator is gen


def test_nested_dag_summary_generator_wired():
    """run_nested_dag builds a container ChildConfig carrying summary_generator."""
    import aws_durable_execution_sdk_python.operation.dag as dag_mod

    captured = {}

    def fake_child_handler(func, state, operation_identifier, config):
        captured["config"] = config
        return func()

    def gen(_result):  # pragma: no cover - not invoked (small payload)
        return "nested-summary"

    original = dag_mod.child_handler
    dag_mod.child_handler = fake_child_handler  # type: ignore[assignment]
    try:
        state, _ = make_state()
        ctx = make_context(state)
        dag_mod.run_nested_dag(
            ctx,
            "inner",
            lambda d: d.step(lambda deps, sc: 1, name="x"),
            DagConfig(summary_generator=gen),
        )
    finally:
        dag_mod.child_handler = original  # type: ignore[assignment]
    assert captured["config"].summary_generator is gen



def test_unwrap_dag_error_reconstructs_typed_error_on_replay():
    """On replay the checkpointed failure rebuilds a CallableRuntimeError with
    error_type set but __cause__ absent; unwrap must still surface the typed
    Dag* error so replay matches the first run."""
    from aws_durable_execution_sdk_python.exceptions import (
        ChildContextError,
        DagExecutionError,
    )
    from aws_durable_execution_sdk_python.operation.dag import unwrap_dag_error

    exc = ChildContextError(
        message="2 task(s) FAILED",
        error_type="DagExecutionError",
        data=None,
        stack_trace=None,
    )
    assert exc.__cause__ is None
    with pytest.raises(DagExecutionError, match="FAILED"):
        unwrap_dag_error(exc)


def test_unwrap_dag_error_passthrough_for_non_dag_error():
    from aws_durable_execution_sdk_python.exceptions import ChildContextError
    from aws_durable_execution_sdk_python.operation.dag import unwrap_dag_error

    exc = ChildContextError(
        message="boom", error_type="ValueError", data=None, stack_trace=None
    )
    with pytest.raises(ChildContextError):
        unwrap_dag_error(exc)


def test_unwrap_dag_error_preserves_live_cause_when_present():
    """When a live DagPredicateError is the ChildContextError cause (before the
    durable boundary rebuilds it), unwrap surfaces it with its OWN original
    cause and task_name intact, suppressing the ChildContextError wrapper."""
    from aws_durable_execution_sdk_python.exceptions import (
        ChildContextError,
        DagPredicateError,
    )
    from aws_durable_execution_sdk_python.operation.dag import unwrap_dag_error

    original = KeyError("missing")
    predicate_error = DagPredicateError(
        "run_if predicate for DAG task 'b' raised KeyError: 'missing'",
        task_name="b",
    )
    predicate_error.__cause__ = original
    wrapper = ChildContextError(
        message="run_if predicate for DAG task 'b' raised KeyError: 'missing'",
        error_type="DagPredicateError",
        data=None,
        stack_trace=None,
    )
    wrapper.__cause__ = predicate_error

    with pytest.raises(DagPredicateError) as ei:
        unwrap_dag_error(wrapper)

    assert ei.value is predicate_error
    assert ei.value.task_name == "b"
    assert ei.value.__cause__ is original


def test_run_if_raise_aborts_dag_through_context():
    """A raising run_if surfaces DagPredicateError (not a DagResult with a FAILED
    task) out of ctx.dag(). Across the durable child-context boundary the error
    is rebuilt from serialized fields (type name + message) to keep first run and
    replay identical, so ``task_name`` and the live ``__cause__`` are erased here;
    the offending task survives IN the message. The wrapped chain is verified at
    the scheduler level in dag_executor_test.py instead."""
    from aws_durable_execution_sdk_python.dag import TriggerRule
    from aws_durable_execution_sdk_python.exceptions import DagPredicateError

    state, _ = make_state()
    ctx = make_context(state)

    def register(d):
        a = d.step(lambda deps, sc: "A", name="a")
        d.step(
            lambda deps, sc: "b",
            deps=[a],
            name="b",
            run_if=lambda deps: deps["missing"] > 0,  # KeyError
        )
        # ALL_FAILED compensation MUST NOT run.
        d.step(
            lambda deps, sc: pytest.fail("compensation ran on a predicate defect"),
            deps=[a],
            name="refund",
            trigger_rule=TriggerRule.ALL_FAILED,
        )

    with pytest.raises(DagPredicateError) as ei:
        ctx.dag(
            register,
            name="pipeline",
            config=DagConfig(default_retry_strategy=NO_RETRY),
        )

    # Type surfaces cleanly; the offending task is named in the durable message.
    assert "b" in str(ei.value)
    # The durable boundary rebuilds from serialized fields, so task_name and the
    # live cause are not retrievable here (parity with the rest of the Dag*
    # family and identical on replay).
    assert ei.value.task_name is None
    assert ei.value.__cause__ is None


def test_unwrap_dag_error_reconstructs_predicate_error_on_replay():
    """On replay the checkpointed predicate abort rebuilds with error_type set
    but __cause__ absent; unwrap must still surface DagPredicateError (task name
    survives in the message)."""
    from aws_durable_execution_sdk_python.exceptions import (
        ChildContextError,
        DagPredicateError,
    )
    from aws_durable_execution_sdk_python.operation.dag import unwrap_dag_error

    exc = ChildContextError(
        message="run_if predicate for DAG task 'b' raised KeyError: 'missing'",
        error_type="DagPredicateError",
        data=None,
        stack_trace=None,
    )
    assert exc.__cause__ is None
    with pytest.raises(DagPredicateError, match="'b'"):
        unwrap_dag_error(exc)
