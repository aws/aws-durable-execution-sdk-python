"""T4: DAG validator tests."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.dag import DagConfig
from aws_durable_execution_sdk_python.exceptions import (
    DagCyclicDependencyError,
    DagDuplicateTaskError,
    DagInvalidDependencyError,
    DagInvalidTaskNameError,
)
from aws_durable_execution_sdk_python.operation.dag_context import DagContextImpl
from aws_durable_execution_sdk_python.operation.dag_validator import validate_dag
from tests.dag_support import make_context, make_state


def _impl() -> DagContextImpl:
    state, _ = make_state()
    return DagContextImpl(make_context(state, parent_id="c"), DagConfig())


def _step(d, name, deps=None):
    return d.step(lambda deps_map, sc: name, deps=deps, name=name)


def test_valid_diamond_passes():
    d = _impl()
    a = _step(d, "a")
    b = _step(d, "b", [a])
    c = _step(d, "c", [a])
    _step(d, "d", [b, c])
    validate_dag(d)  # no raise


def test_self_loop_detected():
    d = _impl()
    a = _step(d, "a")
    a.after(a)  # self dependency
    with pytest.raises(DagCyclicDependencyError):
        validate_dag(d)


def test_two_cycle_detected():
    d = _impl()
    a = _step(d, "a")
    b = _step(d, "b", [a])
    a.after(b)  # a<->b
    with pytest.raises(DagCyclicDependencyError) as exc:
        validate_dag(d)
    assert "a" in str(exc.value) and "b" in str(exc.value)


def test_deep_cycle_detected():
    d = _impl()
    a = _step(d, "a")
    b = _step(d, "b", [a])
    c = _step(d, "c", [b])
    a.after(c)  # a->b->c->a
    with pytest.raises(DagCyclicDependencyError):
        validate_dag(d)


@pytest.mark.parametrize("bad", ["", "a-b", "has space", "x" * 101, "DAG_NODE_T_x"])
def test_invalid_names(bad):
    d = _impl()
    # register with a valid name, then tamper the name to bypass registration guards
    _step(d, "valid")
    d.get_tasks()["valid"].name = bad
    with pytest.raises(DagInvalidTaskNameError):
        validate_dag(d)


def test_duplicate_names():
    d = _impl()
    _step(d, "dup")
    _step(d, "dup")
    with pytest.raises(DagDuplicateTaskError):
        validate_dag(d)


def test_foreign_scope_dep():
    d1 = _impl()
    foreign = _step(d1, "foreign")

    d2 = _impl()
    _step(d2, "a")
    d2.get_tasks()["a"].all_deps.append(foreign)
    with pytest.raises(DagInvalidDependencyError):
        validate_dag(d2)


def test_valid_names_pass():
    d = _impl()
    _step(d, "abc_123")
    _step(d, "A9")
    validate_dag(d)
