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
    ]:
        assert hasattr(sdk, symbol), symbol
