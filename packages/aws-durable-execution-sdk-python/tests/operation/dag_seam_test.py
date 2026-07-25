"""T2: name-based entity-id seam tests."""

from __future__ import annotations

import hashlib

from tests.dag_support import make_context, make_state


def _task_digest(prefix: str | None, name: str) -> str:
    """Expected DAG task id: the name-based pre-image, blake2b-bounded to 64 hex.

    Mirrors ``DurableContext._create_task_id`` / the core's
    ``_create_step_id_for_logical_step`` bounding so the backend operation id
    stays within the 64-char ``updates[].id`` limit.
    """
    logical_id = f"{prefix}-DAG_NODE_T_{name}" if prefix else f"DAG_NODE_T_{name}"
    return hashlib.blake2b(logical_id.encode()).hexdigest()[:64]


def test_create_task_id_unprefixed():
    state, _ = make_state()
    ctx = make_context(state, parent_id=None)
    task_id = ctx._create_task_id("fetch")
    assert task_id == _task_digest(None, "fetch")
    assert len(task_id) <= 64


def test_create_task_id_prefixed():
    state, _ = make_state()
    ctx = make_context(state, parent_id="container")
    task_id = ctx._create_task_id("fetch")
    assert task_id == _task_digest("container", "fetch")
    assert len(task_id) <= 64


def test_create_task_id_does_not_touch_counter():
    state, _ = make_state()
    ctx = make_context(state, parent_id="c")
    before = ctx._step_counter.get_current()
    ctx._create_task_id("a")
    ctx._create_task_id("b")
    assert ctx._step_counter.get_current() == before


def test_no_collision_with_counter_ids():
    """Counter pre-images are {prefix}-{int}; task pre-images are
    {prefix}-DAG_NODE_T_{name}. Both are blake2b-bounded, so the digests differ
    because the pre-images differ (the reserved token guarantees disjointness).
    """
    state, _ = make_state()
    ctx = make_context(state, parent_id="c")
    counter_id = ctx._create_step_id()
    task_id = ctx._create_task_id("1")
    assert counter_id != task_id
    assert task_id == _task_digest("c", "1")


def test_seam_checkpoints_under_task_id_and_fast_path_on_replay():
    """Drive one explicit-id step through the seam; confirm checkpoint id and fast path."""
    state, client = make_state()
    ctx = make_context(state, parent_id="dagc")

    calls = {"n": 0}

    def body(_step_ctx):
        calls["n"] += 1
        return "value"

    # first call runs and checkpoints under the name-based id
    result = ctx._run_step_with_task_id("mytask", body)
    assert result == "value"
    assert calls["n"] == 1
    assert _task_digest("dagc", "mytask") in client.operations

    # second call (simulated replay) hits the checkpoint fast path, no re-exec
    result2 = ctx._run_step_with_task_id("mytask", body)
    assert result2 == "value"
    assert calls["n"] == 1  # not re-executed


def test_per_level_hashing_no_multi_level_preimage():
    """A nested DAG container id becomes the child's parent_id, so sub-task ids
    are blake2b({container}-DAG_NODE_T_{name})[:64] per level — the container's
    already-hashed digest is the prefix, never a single raw multi-level string
    like ...DAG_NODE_T_validation-DAG_NODE_T_rule_a hashed once at one level."""
    state, _ = make_state()
    outer = make_context(state, parent_id="root")
    container_id = outer._create_task_id("validation")
    assert container_id == _task_digest("root", "validation")
    assert len(container_id) <= 64
    # nested scope: the container digest is the child's parent prefix
    nested = make_context(state, parent_id=container_id)
    sub_id = nested._create_task_id("rule_a")
    # sub-task pre-image uses the container DIGEST as prefix (re-hashed per level)
    assert sub_id == _task_digest(container_id, "rule_a")
    assert len(sub_id) <= 64
    # it is NOT the single raw multi-level pre-image hashed at one level
    assert sub_id != _task_digest("root", "validation-DAG_NODE_T_rule_a")
