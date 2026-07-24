"""T3: DagContext registration, TaskHandle chaining, DepsMap access."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.dag import (
    DagConfig,
    DepsMap,
    TriggerRule,
)
from aws_durable_execution_sdk_python.exceptions import DagInvalidTaskNameError
from aws_durable_execution_sdk_python.operation.dag_context import DagContextImpl
from tests.dag_support import make_context, make_state


def _impl() -> DagContextImpl:
    state, _ = make_state()
    ctx = make_context(state, parent_id="c")
    return DagContextImpl(ctx, DagConfig())


def test_depsmap_string_and_handle_access():
    d = _impl()
    fetch = d.step(lambda deps, sc: "x", name="fetch")
    dm = DepsMap({"fetch": "result-value"})
    assert dm["fetch"] == "result-value"
    assert dm[fetch] == "result-value"
    assert "fetch" in dm
    assert fetch in dm
    assert len(dm) == 1
    assert list(dm) == ["fetch"]


def test_taskhandle_hash_by_name_and_identity_eq():
    d = _impl()
    a = d.step(lambda deps, sc: 1, name="a")
    b = d.step(lambda deps, sc: 1, name="b")
    # identity equality (eq=False) -> distinct handles are not equal
    assert a is a
    assert a != b
    assert hash(a) == hash("a")


def test_after_adds_ordering_only_dep():
    d = _impl()
    a = d.step(lambda deps, sc: 1, name="a")
    b = d.step(lambda deps, sc: 2, name="b")
    d.step(lambda deps, sc: 3, deps=[a], name="c").after(b)
    tasks = d.get_tasks()
    cdef = tasks["c"]
    assert [h.name for h in cdef.inline_deps] == ["a"]
    assert {h.name for h in cdef.all_deps} == {"a", "b"}


def test_trigger_rule_chaining_mutates_taskdef():
    d = _impl()
    a = d.step(lambda deps, sc: 1, name="a")
    d.step(lambda deps, sc: 2, deps=[a], name="b").trigger_rule(TriggerRule.ALL_DONE)
    assert d.get_tasks()["b"].trigger_rule is TriggerRule.ALL_DONE


def test_name_resolution_from_original_name():
    from aws_durable_execution_sdk_python.context import durable_step

    d = _impl()

    @durable_step
    def my_step(step_ctx):
        return 1

    h = d.step(my_step())
    assert h.name == "my_step"


def test_unresolvable_name_raises():
    d = _impl()
    with pytest.raises(DagInvalidTaskNameError):
        d.step(lambda deps, sc: 1)  # bare lambda, no name


def test_wait_requires_name():
    d = _impl()
    with pytest.raises(DagInvalidTaskNameError):
        d.wait(5)


def test_duplicate_registration_recorded_in_order():
    d = _impl()
    d.step(lambda deps, sc: 1, name="dup")
    d.step(lambda deps, sc: 2, name="dup")
    # dict keeps last; registration order keeps both for the validator
    assert len(d.get_tasks()) == 1
    assert [t.name for t in d.get_registration_order()] == ["dup", "dup"]


def test_invoke_registers_with_deferred_payload():
    d = _impl()
    h = d.invoke("fn:prod", lambda deps: {"a": 1}, name="charge")
    assert h.name == "charge"
    assert d.get_tasks()["charge"].kind == "invoke"
