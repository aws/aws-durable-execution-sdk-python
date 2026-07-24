"""T2: name-based entity-id seam tests."""

from __future__ import annotations

from tests.dag_support import make_context, make_state


def test_create_task_id_unprefixed():
    state, _ = make_state()
    ctx = make_context(state, parent_id=None)
    assert ctx._create_task_id("fetch") == "DAG_NODE_T_fetch"


def test_create_task_id_prefixed():
    state, _ = make_state()
    ctx = make_context(state, parent_id="container")
    assert (
        ctx._create_task_id("fetch") == "container-DAG_NODE_T_fetch"
    )


def test_create_task_id_does_not_touch_counter():
    state, _ = make_state()
    ctx = make_context(state, parent_id="c")
    before = ctx._step_counter.get_current()
    ctx._create_task_id("a")
    ctx._create_task_id("b")
    assert ctx._step_counter.get_current() == before


def test_no_collision_with_counter_ids():
    """Counter ids are {prefix}-{int}; task ids are {prefix}-DAG_NODE_T_{name}."""
    state, _ = make_state()
    ctx = make_context(state, parent_id="c")
    counter_id = ctx._create_step_id()
    task_id = ctx._create_task_id("1")
    assert counter_id != task_id
    assert task_id == "c-DAG_NODE_T_1"


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
    assert "dagc-DAG_NODE_T_mytask" in client.operations

    # second call (simulated replay) hits the checkpoint fast path, no re-exec
    result2 = ctx._run_step_with_task_id("mytask", body)
    assert result2 == "value"
    assert calls["n"] == 1  # not re-executed


def test_per_level_hashing_no_multi_level_preimage():
    """A nested DAG container id becomes the child's parent_id, so sub-task ids are
    {container}-DAG_NODE_T_{name} per level, never a single raw multi-level string
    like ...DAG_NODE_T_validation-DAG_NODE_T_rule_a composed at one level."""
    state, _ = make_state()
    outer = make_context(state, parent_id="root")
    container_id = outer._create_task_id("validation")
    assert container_id == "root-DAG_NODE_T_validation"
    # nested scope: the container id is the child's parent prefix
    nested = make_context(state, parent_id=container_id)
    sub_id = nested._create_task_id("rule_a")
    assert sub_id == "root-DAG_NODE_T_validation-DAG_NODE_T_rule_a"
    # the sub-task prefix is exactly the container id (composed per level)
    assert sub_id.startswith(container_id + "-DAG_NODE_T_")
