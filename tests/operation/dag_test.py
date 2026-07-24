"""T9: DAG replay / interruption / large-payload re-execute tests."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagConfig,
    TaskStatus,
)
from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from tests.dag_support import make_context, make_state


def test_container_checkpointed_with_dag_subtype_and_serialized_result():
    """The DAG container is checkpointed under sub_type=DAG and its DagResult
    serializes into the container payload (round-trips via the DAG serdes).

    Note: small-payload child-context *replay* short-circuit returns None in this
    base-SDK snapshot because ``CheckpointedResult`` does not read back CONTEXT
    results (a pre-existing limitation, unrelated to the DAG). The DAG's
    re-execute model (spec §8.2) is exercised by the interrupt-resume and
    large-payload tests below.
    """
    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
    from aws_durable_execution_sdk_python.operation.dag_result import (
        create_dag_result_serdes,
    )

    side = {"a": 0}

    def register(d):
        d.step(lambda deps, sc: side.__setitem__("a", side["a"] + 1) or "A", name="a")

    state, client = make_state()
    r1 = make_context(state).dag(register, name="p")
    assert side["a"] == 1
    assert r1.get_result("a") == "A"

    container = client.operations["1"]
    assert container.sub_type is OperationSubType.DAG
    # step task checkpointed under a DAG_NODE_T_ id
    assert "1-DAG_NODE_T_a" in client.operations
    # the serialized container payload round-trips to an equal DagResult
    payload = container.context_details.result
    restored = create_dag_result_serdes().deserialize(payload, None)
    assert restored.get_result("a") == "A"
    assert restored.completion_reason is DagCompletionReason.ALL_COMPLETED


def test_interrupt_and_resume():
    """Interrupt via a gate that suspends on run 1; resume completes remaining once."""
    side = {"a": 0, "work": 0}
    control = {"suspend": True}

    def register(d):
        a = d.step(
            lambda deps, sc: side.__setitem__("a", side["a"] + 1) or "A", name="a"
        )

        def gate(deps, sc):
            if control["suspend"]:
                raise SuspendExecution("gate not ready")
            return "open"

        g = d.step(gate, deps=[a], name="gate")
        d.step(
            lambda deps, sc: side.__setitem__("work", side["work"] + 1) or "done",
            deps=[g],
            name="work",
        )
        # a run_if-skipped task that must stay skipped across replay
        d.step(
            lambda deps, sc: "never",
            deps=[a],
            name="skipme",
            run_if=lambda deps: False,
        )

    state, client = make_state()

    # run 1: gate suspends -> whole DAG suspends
    with pytest.raises(SuspendExecution):
        make_context(state).dag(register, name="p")
    assert side["a"] == 1  # a completed and checkpointed
    assert side["work"] == 0  # work never scheduled (gate not terminal)
    # skipped task minted no checkpoint
    assert "1-DAG_NODE_T_skipme" not in client.operations

    # run 2: gate now succeeds
    control["suspend"] = False
    result = make_context(state).dag(register, name="p")
    assert side["a"] == 1  # a hit the checkpoint fast path, not re-executed
    assert side["work"] == 1  # remaining task ran exactly once
    assert result.get_result("work") == "done"
    assert result.get_status("skipme") is TaskStatus.SKIPPED
    assert "1-DAG_NODE_T_skipme" not in client.operations


def test_large_payload_reexecutes_to_equal_result():
    """A >256KB DagResult forces ReplayChildren; the DAG re-executes to an equal
    result and a custom summary_generator neither changes it nor hangs replay."""
    big = "x" * (300 * 1024)
    side = {"big": 0, "small": 0}

    def register(d):
        d.step(
            lambda deps, sc: side.__setitem__("big", side["big"] + 1) or big,
            name="big",
        )
        d.step(
            lambda deps, sc: side.__setitem__("small", side["small"] + 1) or "s",
            name="small",
        )

    config = DagConfig(summary_generator=lambda r: "custom-summary-string")
    state, client = make_state()

    r1 = make_context(state).dag(register, name="p", config=config)
    assert side["big"] == 1
    # container stored with replay_children due to large payload
    container = client.operations["1"]
    assert container.context_details.replay_children is True

    # replay: container re-executes body (ReplayChildren), tasks fast-path
    r2 = make_context(state).dag(register, name="p", config=config)
    assert side["big"] == 1  # big task not re-executed (own checkpoint fast path)
    assert side["small"] == 1
    assert r2.get_result("big") == r1.get_result("big") == big
    assert r2.completion_reason is DagCompletionReason.ALL_COMPLETED
