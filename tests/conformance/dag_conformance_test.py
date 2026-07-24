"""Cross-language DAG conformance suite (Python side).

Implements the applicable scenarios from the canonical catalog
``aws-durable-execution-sdk-js/docs/DAG_CONFORMANCE.md`` against the shipped
``context.dag()`` API, asserts each actual outcome against the catalog's
expected *semantic* outcome, and emits one key-sorted normalized JSON record
file to ``dag-conformance-out/python.json`` (schema per catalog Part B).

Applicability (catalog Part C): DAG-1..17 and DAG-19 apply to Python (18
records). DAG-18 (custom result-based completion) is TS+Go only and is OMITTED
here per the catalog's "emit only applicable scenarios" convention.

All scenarios (including DAG-16/17 early completion) now conform to the
canonical catalog outcomes; ``total_count`` equals the registered task count
per spec §2.8.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from aws_durable_execution_sdk_python import exceptions
from aws_durable_execution_sdk_python.config import CompletionConfig
from aws_durable_execution_sdk_python.dag import (
    DagConfig,
    DagResult,
    TaskStatus,
    TriggerRule,
)
from aws_durable_execution_sdk_python.retries import RetryPresets
from tests.dag_support import InMemoryServiceClient, make_context, make_state

NO_RETRY = RetryPresets.none()

OUT_PATH = Path("/Users/parpooya/workplace/dag-conformance-out/python.json")
_NAME_CHARSET = re.compile(r"[A-Za-z0-9_]+")

# Records accumulate here across scenario helpers, then get written once.
RECORDS: dict[str, dict[str, Any]] = {}


# region helpers
def _run(register: Any, name: str, config: DagConfig | None = None):
    state, client = make_state()
    result = make_context(state).dag(register, name=name, config=config)
    return result, client


def _fail(_deps: Any, _sc: Any) -> Any:
    raise RuntimeError("boom")


def _structural_checks(client: InMemoryServiceClient) -> dict[str, bool]:
    """Compute the four per-language structural entity-ID invariants.

    Task ops are exactly the operations whose id contains the reserved
    ``DAG_NODE_T_`` delimiter; counter/other ops never do. For the empty DAG
    there are no task ops, so all four are vacuously ``true``.
    """
    task_ids = [oid for oid in client.operations if "DAG_NODE_T_" in oid]
    counter_ids = [oid for oid in client.operations if "DAG_NODE_T_" not in oid]
    if not task_ids:
        return {
            "name_based": True,
            "has_delimiter": True,
            "dash_free": True,
            "disjoint_from_counter": True,
        }
    short_names = [oid.split("DAG_NODE_T_")[-1] for oid in task_ids]
    name_based = all(
        client.operations[oid].name == oid.split("DAG_NODE_T_")[-1]
        for oid in task_ids
    )
    has_delimiter = all("DAG_NODE_T_" in oid for oid in task_ids)
    dash_free = all(_NAME_CHARSET.fullmatch(s) is not None for s in short_names)
    disjoint = set(task_ids).isdisjoint(counter_ids) and all(
        "DAG_NODE_T_" not in cid for cid in counter_ids
    )
    return {
        "name_based": name_based,
        "has_delimiter": has_delimiter,
        "dash_free": dash_free,
        "disjoint_from_counter": disjoint,
    }


def _normalize_result(value: Any) -> Any:
    """Serialize a task result to a language-neutral JSON value.

    Nested-``dag`` results collapse to ``{completion_reason, counts}`` (never
    raw sub-task values), per catalog Part B.2.
    """
    if isinstance(value, DagResult):
        return {
            "completion_reason": value.completion_reason.value,
            "counts": {
                "success": value.success_count,
                "failure": value.failure_count,
                "skipped": value.skipped_count,
                "total": value.total_count,
            },
        }
    return value


def _task_record(te: Any) -> dict[str, Any]:
    status = te.status.value
    result_val: Any = None
    error_type: str | None = None
    skip_reason: str | None = te.skip_reason.value if te.skip_reason else None
    if te.status is TaskStatus.SUCCEEDED:
        result_val = _normalize_result(te.result)
    elif te.status is TaskStatus.FAILED:
        # Native step failure (CallableRuntimeError/RuntimeError) -> normalized.
        error_type = "StepError"
    return {
        "status": status,
        "result": result_val,
        "error_type": error_type,
        "skip_reason": skip_reason,
    }


def _record(scenario: str, result: Any, client: InMemoryServiceClient) -> dict[str, Any]:
    tasks = {name: _task_record(te) for name, te in result.results.items()}
    rec = {
        "scenario": scenario,
        "tasks": tasks,
        "completion_reason": result.completion_reason.value,
        "counts": {
            "success": result.success_count,
            "failure": result.failure_count,
            "skipped": result.skipped_count,
            "total": result.total_count,
        },
        "structural_id_checks": _structural_checks(client),
        "validation_error": None,
    }
    RECORDS[scenario] = rec
    return rec


def _validation_record(scenario: str, token: str) -> dict[str, Any]:
    rec = {
        "scenario": scenario,
        "tasks": {},
        "completion_reason": None,
        "counts": {"success": 0, "failure": 0, "skipped": 0, "total": 0},
        "structural_id_checks": {
            "name_based": False,
            "has_delimiter": False,
            "dash_free": False,
            "disjoint_from_counter": False,
        },
        "validation_error": token,
    }
    RECORDS[scenario] = rec
    return rec


def _status(rec: dict[str, Any], name: str) -> str | None:
    t = rec["tasks"].get(name)
    return t["status"] if t else None


# endregion helpers


# region scenarios
def test_dag_1_diamond() -> None:
    def reg(d: Any) -> None:
        fetch = d.step(lambda deps, sc: 10, name="fetch")
        ta = d.step(lambda deps, sc: deps["fetch"] + 1, deps=[fetch], name="ta")
        tb = d.step(lambda deps, sc: deps["fetch"] * 2, deps=[fetch], name="tb")
        d.step(lambda deps, sc: deps["ta"] + deps["tb"], deps=[ta, tb], name="merge")

    result, client = _run(reg, "DAG-1")
    rec = _record("DAG-1", result, client)
    assert rec["tasks"]["fetch"]["result"] == 10
    assert rec["tasks"]["ta"]["result"] == 11
    assert rec["tasks"]["tb"]["result"] == 20
    assert rec["tasks"]["merge"]["result"] == 31
    assert rec["completion_reason"] == "ALL_COMPLETED"
    assert rec["counts"] == {"success": 4, "failure": 0, "skipped": 0, "total": 4}
    assert all(rec["structural_id_checks"].values())


def _compensation(scenario: str, charge_ok: bool) -> dict[str, Any]:
    def reg(d: Any) -> None:
        if charge_ok:
            c = d.step(lambda deps, sc: "charged", name="charge")
        else:
            c = d.step(_fail, name="charge")
        d.step(lambda deps, sc: "fulfilled", name="fulfill").after(c)
        d.step(lambda deps, sc: "refunded", name="refund").after(c).trigger_rule(
            TriggerRule.ALL_FAILED
        )
        d.step(lambda deps, sc: "audited", name="audit").after(c).trigger_rule(
            TriggerRule.ALL_DONE
        )

    result, client = _run(reg, scenario, DagConfig(default_retry_strategy=NO_RETRY))
    return _record(scenario, result, client)


def test_dag_2_compensation_charge_fails() -> None:
    rec = _compensation("DAG-2", charge_ok=False)
    assert _status(rec, "charge") == "FAILED"
    assert rec["tasks"]["charge"]["error_type"] == "StepError"
    assert _status(rec, "fulfill") == "SKIPPED"
    assert rec["tasks"]["fulfill"]["skip_reason"] == "TRIGGER_RULE"
    assert rec["tasks"]["refund"]["result"] == "refunded"
    assert rec["tasks"]["audit"]["result"] == "audited"
    assert rec["completion_reason"] == "COMPLETED_WITH_FAILURES"
    assert rec["counts"] == {"success": 2, "failure": 1, "skipped": 1, "total": 4}


def test_dag_3_compensation_charge_succeeds() -> None:
    rec = _compensation("DAG-3", charge_ok=True)
    assert rec["tasks"]["charge"]["result"] == "charged"
    assert rec["tasks"]["fulfill"]["result"] == "fulfilled"
    assert _status(rec, "refund") == "SKIPPED"
    assert rec["tasks"]["refund"]["skip_reason"] == "TRIGGER_RULE"
    assert rec["tasks"]["audit"]["result"] == "audited"
    assert rec["completion_reason"] == "ALL_COMPLETED"
    assert rec["counts"] == {"success": 3, "failure": 0, "skipped": 1, "total": 4}


def test_dag_4_run_if_branching() -> None:
    def reg(d: Any) -> None:
        classify = d.step(lambda deps, sc: "review", name="classify")
        d.step(
            lambda deps, sc: "published",
            deps=[classify],
            name="publish",
            run_if=lambda deps: deps["classify"] == "publish",
        )
        d.step(
            lambda deps, sc: "reviewed",
            deps=[classify],
            name="review",
            run_if=lambda deps: deps["classify"] == "review",
        )
        d.step(
            lambda deps, sc: "blocked",
            deps=[classify],
            name="block",
            run_if=lambda deps: deps["classify"] == "block",
        )

    result, client = _run(reg, "DAG-4")
    rec = _record("DAG-4", result, client)
    assert rec["tasks"]["classify"]["result"] == "review"
    assert rec["tasks"]["review"]["result"] == "reviewed"
    assert _status(rec, "publish") == "SKIPPED"
    assert rec["tasks"]["publish"]["skip_reason"] == "RUN_IF_PREDICATE"
    assert _status(rec, "block") == "SKIPPED"
    assert rec["tasks"]["block"]["skip_reason"] == "RUN_IF_PREDICATE"
    assert rec["counts"] == {"success": 2, "failure": 0, "skipped": 2, "total": 4}
    assert rec["completion_reason"] == "ALL_COMPLETED"


def test_dag_5_trigger_matrix_empty_upstream() -> None:
    rules = [
        ("r_all_success", TriggerRule.ALL_SUCCESS, "SUCCEEDED"),
        ("r_all_failed", TriggerRule.ALL_FAILED, "SKIPPED"),
        ("r_all_done", TriggerRule.ALL_DONE, "SUCCEEDED"),
        ("r_one_success", TriggerRule.ONE_SUCCESS, "SKIPPED"),
        ("r_one_failed", TriggerRule.ONE_FAILED, "SKIPPED"),
        ("r_none_failed", TriggerRule.NONE_FAILED, "SUCCEEDED"),
    ]

    def reg(d: Any) -> None:
        for name, rule, _ in rules:
            d.step(lambda deps, sc: "ok", name=name).trigger_rule(rule)

    result, client = _run(reg, "DAG-5")
    rec = _record("DAG-5", result, client)
    for name, _, expected in rules:
        assert _status(rec, name) == expected, name
        if expected == "SUCCEEDED":
            assert rec["tasks"][name]["result"] == "ok"
        else:
            assert rec["tasks"][name]["skip_reason"] == "TRIGGER_RULE"
    assert rec["counts"] == {"success": 3, "failure": 0, "skipped": 3, "total": 6}
    assert rec["completion_reason"] == "ALL_COMPLETED"


def test_dag_6_trigger_matrix_mixed() -> None:
    consumers = [
        ("c_all_success", TriggerRule.ALL_SUCCESS, "SKIPPED"),
        ("c_all_failed", TriggerRule.ALL_FAILED, "SKIPPED"),
        ("c_all_done", TriggerRule.ALL_DONE, "SUCCEEDED"),
        ("c_one_success", TriggerRule.ONE_SUCCESS, "SUCCEEDED"),
        ("c_one_failed", TriggerRule.ONE_FAILED, "SUCCEEDED"),
        ("c_none_failed", TriggerRule.NONE_FAILED, "SKIPPED"),
    ]

    def reg(d: Any) -> None:
        up_ok = d.step(lambda deps, sc: "ok", name="up_ok")
        up_fail = d.step(_fail, name="up_fail")
        for name, rule, _ in consumers:
            d.step(lambda deps, sc: "c", name=name).after(up_ok, up_fail).trigger_rule(
                rule
            )

    result, client = _run(reg, "DAG-6", DagConfig(default_retry_strategy=NO_RETRY))
    rec = _record("DAG-6", result, client)
    assert rec["tasks"]["up_ok"]["result"] == "ok"
    assert _status(rec, "up_fail") == "FAILED"
    for name, _, expected in consumers:
        assert _status(rec, name) == expected, name
    assert rec["counts"] == {"success": 4, "failure": 1, "skipped": 3, "total": 8}
    assert rec["completion_reason"] == "COMPLETED_WITH_FAILURES"


def test_dag_7_trigger_matrix_all_failed() -> None:
    consumers = [
        ("k_all_success", TriggerRule.ALL_SUCCESS, "SKIPPED"),
        ("k_all_failed", TriggerRule.ALL_FAILED, "SUCCEEDED"),
        ("k_all_done", TriggerRule.ALL_DONE, "SUCCEEDED"),
        ("k_one_success", TriggerRule.ONE_SUCCESS, "SKIPPED"),
        ("k_one_failed", TriggerRule.ONE_FAILED, "SUCCEEDED"),
        ("k_none_failed", TriggerRule.NONE_FAILED, "SKIPPED"),
    ]

    def reg(d: Any) -> None:
        u1 = d.step(_fail, name="u1")
        u2 = d.step(_fail, name="u2")
        for name, rule, _ in consumers:
            d.step(lambda deps, sc: "k", name=name).after(u1, u2).trigger_rule(rule)

    result, client = _run(reg, "DAG-7", DagConfig(default_retry_strategy=NO_RETRY))
    rec = _record("DAG-7", result, client)
    assert _status(rec, "u1") == "FAILED"
    assert _status(rec, "u2") == "FAILED"
    for name, _, expected in consumers:
        assert _status(rec, name) == expected, name
    assert rec["counts"] == {"success": 3, "failure": 2, "skipped": 3, "total": 8}
    assert rec["completion_reason"] == "COMPLETED_WITH_FAILURES"


def test_dag_8_skip_cascade() -> None:
    def reg(d: Any) -> None:
        seed = d.step(lambda deps, sc: 1, name="seed")
        gate = d.step(
            lambda deps, sc: "gate",
            deps=[seed],
            name="gate",
            run_if=lambda deps: deps["seed"] > 100,
        )
        d1 = d.step(lambda deps, sc: "d1", deps=[gate], name="d1")
        d.step(lambda deps, sc: "d2", deps=[d1], name="d2")
        d.step(lambda deps, sc: "sink", name="sink").after(gate).trigger_rule(
            TriggerRule.ALL_DONE
        )

    result, client = _run(reg, "DAG-8")
    rec = _record("DAG-8", result, client)
    assert rec["tasks"]["seed"]["result"] == 1
    assert _status(rec, "gate") == "SKIPPED"
    assert rec["tasks"]["gate"]["skip_reason"] == "RUN_IF_PREDICATE"
    assert rec["tasks"]["d1"]["skip_reason"] == "TRIGGER_RULE"
    assert rec["tasks"]["d2"]["skip_reason"] == "TRIGGER_RULE"
    assert rec["tasks"]["sink"]["result"] == "sink"
    assert rec["counts"] == {"success": 2, "failure": 0, "skipped": 3, "total": 5}
    assert rec["completion_reason"] == "ALL_COMPLETED"


def test_dag_9_nested_dag() -> None:
    def inner(d: Any) -> None:
        x = d.step(lambda deps, sc: 3, name="x")
        d.step(lambda deps, sc: deps["x"] * 10, deps=[x], name="y")

    def outer(d: Any) -> None:
        a = d.step(lambda deps, sc: 2, name="a")
        inn = d.dag(inner, deps=[a], name="inner")
        d.step(
            lambda deps, sc: deps["inner"].get_result("y") + 5,
            deps=[inn],
            name="consume",
        )

    result, client = _run(outer, "DAG-9")
    # scope isolation: inner task names invisible in outer scope
    assert result.get_status("x") is None
    assert result.get_status("y") is None
    rec = _record("DAG-9", result, client)
    assert rec["tasks"]["a"]["result"] == 2
    assert rec["tasks"]["consume"]["result"] == 35
    assert rec["tasks"]["inner"]["result"] == {
        "completion_reason": "ALL_COMPLETED",
        "counts": {"success": 2, "failure": 0, "skipped": 0, "total": 2},
    }
    assert rec["counts"] == {"success": 3, "failure": 0, "skipped": 0, "total": 3}
    assert rec["completion_reason"] == "ALL_COMPLETED"


def test_dag_10_empty() -> None:
    result, client = _run(lambda d: None, "DAG-10")
    rec = _record("DAG-10", result, client)
    assert rec["tasks"] == {}
    assert rec["counts"] == {"success": 0, "failure": 0, "skipped": 0, "total": 0}
    assert rec["completion_reason"] == "ALL_COMPLETED"
    assert all(rec["structural_id_checks"].values())


def test_dag_11_cycle() -> None:
    def reg(d: Any) -> None:
        p = d.step(lambda deps, sc: 1, name="p")
        q = d.step(lambda deps, sc: 1, deps=[p], name="q")
        p.after(q)

    with pytest.raises(exceptions.DagCyclicDependencyError):
        _run(reg, "DAG-11")
    _validation_record("DAG-11", "DagCyclicDependencyError")


def test_dag_12_duplicate() -> None:
    def reg(d: Any) -> None:
        d.step(lambda deps, sc: 1, name="dup")
        d.step(lambda deps, sc: 2, name="dup")

    with pytest.raises(exceptions.DagDuplicateTaskError):
        _run(reg, "DAG-12")
    _validation_record("DAG-12", "DagDuplicateTaskError")


def test_dag_13_invalid_name_dash() -> None:
    def reg(d: Any) -> None:
        d.step(lambda deps, sc: 1, name="fetch-data")

    with pytest.raises(exceptions.DagInvalidTaskNameError):
        _run(reg, "DAG-13")
    _validation_record("DAG-13", "DagInvalidTaskNameError")


def test_dag_14_invalid_name_reserved() -> None:
    def reg(d: Any) -> None:
        d.step(lambda deps, sc: 1, name="DAG_NODE_T_root")

    with pytest.raises(exceptions.DagInvalidTaskNameError):
        _run(reg, "DAG-14")
    _validation_record("DAG-14", "DagInvalidTaskNameError")


def test_dag_15_foreign_dep() -> None:
    captured: dict[str, Any] = {}

    def sibling(d: Any) -> None:
        captured["h"] = d.step(lambda deps, sc: 1, name="foreign")

    _run(sibling, "DAG-15-sibling")

    def reg(d: Any) -> None:
        d.step(lambda deps, sc: 1, deps=[captured["h"]], name="t")

    with pytest.raises(exceptions.DagInvalidDependencyError):
        _run(reg, "DAG-15")
    _validation_record("DAG-15", "DagInvalidDependencyError")


def test_dag_16_min_successful() -> None:
    def reg(d: Any) -> None:
        prev = None
        for i in range(1, 6):
            deps = [prev] if prev is not None else None
            prev = d.step(
                (lambda i: lambda deps, sc: i)(i), deps=deps, name=f"s{i}"
            )

    result, client = _run(
        reg,
        "DAG-16",
        DagConfig(max_concurrency=1, completion_config=CompletionConfig(min_successful=3)),
    )
    rec = _record("DAG-16", result, client)
    assert rec["tasks"]["s1"]["result"] == 1
    assert rec["tasks"]["s2"]["result"] == 2
    assert rec["tasks"]["s3"]["result"] == 3
    assert "s4" not in rec["tasks"] and "s5" not in rec["tasks"]  # absent
    assert rec["completion_reason"] == "MIN_SUCCESSFUL_REACHED"
    assert rec["counts"]["success"] == 3
    assert rec["counts"]["failure"] == 0
    assert rec["counts"]["skipped"] == 0
    # total_count = registered task count (spec §2.8): 5 tasks registered even
    # though s4/s5 never started (absent from results) under min_successful.
    assert rec["counts"]["total"] == 5


def test_dag_17_tolerated_failures() -> None:
    def reg(d: Any) -> None:
        prev = None
        for i in range(1, 5):
            deps = [prev] if prev is not None else None
            h = d.step(_fail, deps=deps, name=f"t{i}")
            if i > 1:
                h.trigger_rule(TriggerRule.ALL_DONE)
            prev = h

    result, client = _run(
        reg,
        "DAG-17",
        DagConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=1),
            default_retry_strategy=NO_RETRY,
        ),
    )
    rec = _record("DAG-17", result, client)
    assert _status(rec, "t1") == "FAILED"
    assert _status(rec, "t2") == "FAILED"
    assert "t3" not in rec["tasks"] and "t4" not in rec["tasks"]  # absent
    assert rec["completion_reason"] == "FAILURE_TOLERANCE_EXCEEDED"
    assert rec["counts"]["success"] == 0
    assert rec["counts"]["failure"] == 2
    assert rec["counts"]["skipped"] == 0
    # total_count = registered task count (spec §2.8): 4 tasks registered even
    # though t3/t4 never started (absent) under tolerated_failure_count.
    assert rec["counts"]["total"] == 4


def test_dag_19_order_independence() -> None:
    def make_reg(swap: bool) -> Any:
        def reg(d: Any) -> None:
            root = d.step(lambda deps, sc: 100, name="root")
            if swap:
                c = d.step(lambda deps, sc: deps["root"] + 2, deps=[root], name="c")
                b = d.step(lambda deps, sc: deps["root"] + 1, deps=[root], name="b")
            else:
                b = d.step(lambda deps, sc: deps["root"] + 1, deps=[root], name="b")
                c = d.step(lambda deps, sc: deps["root"] + 2, deps=[root], name="c")
            d.step(lambda deps, sc: deps["b"] + deps["c"], deps=[b, c], name="merge")

        return reg

    r1, c1 = _run(make_reg(False), "DAG-19")
    rec1 = _record("DAG-19", r1, c1)
    r2, c2 = _run(make_reg(True), "DAG-19-swapped")
    rec2 = dict(rec1)  # keep DAG-19 as the emitted record
    rec2_actual = {
        "scenario": "DAG-19",
        "tasks": {name: _task_record(te) for name, te in r2.results.items()},
        "completion_reason": r2.completion_reason.value,
        "counts": {
            "success": r2.success_count,
            "failure": r2.failure_count,
            "skipped": r2.skipped_count,
            "total": r2.total_count,
        },
        "structural_id_checks": _structural_checks(c2),
        "validation_error": None,
    }
    RECORDS.pop("DAG-19-swapped", None)
    # order-independence: both completion orders yield identical records
    assert rec1 == rec2_actual
    assert rec2["tasks"]["merge"]["result"] == 203
    assert rec1["tasks"]["root"]["result"] == 100
    assert rec1["tasks"]["b"]["result"] == 101
    assert rec1["tasks"]["c"]["result"] == 102
    assert rec1["counts"] == {"success": 4, "failure": 0, "skipped": 0, "total": 4}
    assert rec1["completion_reason"] == "ALL_COMPLETED"


def test_zz_emit_python_json() -> None:
    """Runs last (name-sorted): assemble, validate, and write python.json.

    DAG-18 is intentionally OMITTED (TS+Go only, catalog Part C). Emits exactly
    18 records as key-sorted UTF-8 JSON with 2-space indent + trailing newline.
    """
    expected_scenarios = {f"DAG-{i}" for i in range(1, 18)} | {"DAG-19"}
    assert set(RECORDS) == expected_scenarios, (
        f"missing/extra: {expected_scenarios ^ set(RECORDS)}"
    )
    assert "DAG-18" not in RECORDS  # NOT-APPLICABLE for Python (custom completion)
    assert len(RECORDS) == 18

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(RECORDS, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    OUT_PATH.write_text(text, encoding="utf-8")

    # round-trip + byte-shape sanity
    reloaded = json.loads(OUT_PATH.read_text(encoding="utf-8"))
    assert reloaded == RECORDS
    assert text.endswith("\n")
    assert "DAG-18" not in reloaded


# endregion scenarios
