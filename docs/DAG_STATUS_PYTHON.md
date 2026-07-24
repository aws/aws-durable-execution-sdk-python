# DAG Implementation Status — Python (`context.dag()`)

**Repo:** `aws-durable-execution-sdk-python` · **Branch:** `feature/dag` · **Stability:** ⚠️ EXPERIMENTAL

All 10 ordered tasks (T1–T10) from `DAG_TASKS_PYTHON.md` are **implemented, building, and
fully passing**. Each task was committed individually (`feat(dag): T<n> …`) after its tests
passed, plus two follow-up commits for coverage/type hardening.

## Status: DONE (T1–T10)

| Task | Title | Status | Tests |
| ---- | ----- | ------ | ----- |
| T1 | Types, enums & error taxonomy | ✅ | `dag.py`, `exceptions.py`, `OperationSubType.DAG` |
| T2 | Name-based entity-ID seam (`_create_task_id`) | ✅ | `tests/operation/dag_seam_test.py` |
| T3 | `DagContext` + `TaskHandle` + `DepsMap` + `TaskDef` | ✅ | `tests/operation/dag_context_test.py` |
| T4 | DAG validator (names/dups/foreign-deps/cycles) | ✅ | `tests/operation/dag_validator_test.py` |
| T5 | Dedicated `DagExecutor` scheduler | ✅ | `tests/operation/dag_executor_test.py` |
| T6 | `DagResult` + serialization (`result_kind`) | ✅ | `tests/operation/dag_result_test.py` |
| T7 | Wire `context.dag()` + `dag_handler` + `FutureWarning` | ✅ | `tests/operation/dag_handler_test.py` |
| T8 | Unit tests (trigger truth table + coverage) | ✅ | `tests/operation/dag_trigger_rules_test.py` + all above |
| T9 | Runner integration + replay / large-payload | ✅ | `tests/e2e/dag_int_test.py`, `tests/operation/dag_test.py` |
| T10 | Docs & experimental annotations | ✅ | `docs/dag.md`; `.. warning:: **Experimental.**` on every public symbol |

## Verification (all green)

- **Tests:** `pytest` → **656 passed, 1 skipped** (baseline was 566; +90 new DAG tests).
- **Coverage:** `--cov-fail-under=98` → **98.53%** total (meets CI gate). DAG modules:
  validator 100%, dag.py 96%, result 99%, executor 96%, context 97%, handler 89%.
- **Types:** `mypy src tests` → **Success: no issues found in 67 source files** (matches CI `types:check`).
- **Lint:** new source is at style parity with existing modules (e.g. `concurrency.py`) under
  the repo's ruff config; the only diagnostic is `UP046` (`Generic[T]` vs PEP 695), which
  existing shipping code also carries and which **cannot** be "fixed" because CI runs Python
  3.11 (PEP 695 `class C[T]` syntax is 3.12+).

## New files

```
src/aws_durable_execution_sdk_python/
  dag.py                      # public: enums, TaskExecution, DagConfig, TaskHandle, DepsMap, DagContext, DagResult
  operation/dag.py            # dag_handler, run_nested_dag, first-use FutureWarning
  operation/dag_context.py    # DagContextImpl, TaskDef, per-kind explicit-ID runners
  operation/dag_executor.py   # topological scheduler (trigger rules, run_if, drain, threshold completion)
  operation/dag_validator.py  # names / duplicates / foreign-dep / Kahn cycle detection
  operation/dag_result.py     # DagResultImpl + to_dict/from_dict + create_dag_result_serdes + dag_reason_from_core
docs/dag.md                   # usage guide
tests/dag_support.py          # in-memory DurableServiceClient harness
tests/operation/dag_*_test.py, tests/operation/dag_test.py, tests/e2e/dag_int_test.py
```

Changed existing files (additively only): `context.py` (`_create_task_id`,
`_run_step_with_task_id`, `dag()`), `exceptions.py` (`Dag*Error`), `lambda_service.py`
(`OperationSubType.DAG`), `__init__.py` (public exports). No unrelated code modified;
`concurrency.py`, `child.py`, `step.py`, etc. untouched.

## Design adaptations vs. the spec (grounded in the actual checkout)

The local checkout is an **earlier snapshot** than the spec was grounded against. The
following spec assumptions do not hold here and were adapted to the **real code idioms**
(per instruction rule 5):

1. **No blake2b hashing / `_step_id_prefix` / `_create_step_id_for_logical_step`.** The real
   `_create_step_id()` returns the raw `f"{parent_id}-{counter}"`. Accordingly `_create_task_id`
   composes `f"{parent_id}-DAG_NODE_T_{name}"` (unhashed), matching the SDK's actual nested-id
   scheme. Injectivity rests on the name charset rules (no `-`, no `DAG_NODE_T_` token — enforced
   by the validator) + disjointness from counter ids (`{p}-{int}` vs `{p}-DAG_NODE_T_{name}`).
2. **No `_replay_aware` wrapper exists**, so the "bypass `_replay_aware`" step is a no-op here —
   the DAG simply constructs an explicit-ID `OperationIdentifier` and calls the handler directly
   (the established `concurrency` pattern).
3. **`concurrency` is a single module** (not a package); `BatchResult`/`CompletionReason`/
   `ConcurrentExecutor` imported from `concurrency.py`.
4. **`wait(seconds: int)`**, not a `Duration` object — `DagContext.wait` follows the real signature.
5. **No `ErrorObject` reconstruction registry** exists; `DagExecutionError` is raised directly by
   `throw_if_error()`. `child_handler` has no `error_mapper`; validation `Dag*Error`s are unwrapped
   from the `CallableRuntimeError.__cause__` at the `dag()` boundary (the `wait_for_callback`
   precedent).

## Known limitation (pre-existing base-SDK, not introduced by the DAG)

**Small-payload child-context replay returns `None`.** `state.CheckpointedResult.create_from_operation`
only extracts results for `STEP`/`CALLBACK`/`INVOKE` operations — **not** `CONTEXT`. So when a
successful DAG container's serialized `DagResult` is *small* (< 256 KB), on replay `child_handler`
short-circuits and returns `None` (it cannot read back the CONTEXT payload). This affects
`map`/`parallel`/`run_in_child_context`/`dag` equally and is a limitation of this SDK snapshot,
independent of the DAG feature.

The DAG's replay-correctness therefore relies on the **re-execute** paths, both of which are
tested and pass:
- **Interrupt/resume** (container never reached SUCCEEDED because a task suspended) →
  body re-runs, completed tasks hit their name-based checkpoint fast paths
  (`test_interrupt_and_resume`).
- **Large payload** (≥ 256 KB) → `ReplayChildren` re-executes the body to an equal `DagResult`
  (`test_large_payload_reexecutes_to_equal_result`).

This matches the spec's Python "re-execute" model (§8.2). A fix for small-payload CONTEXT replay
would require changing `state.py`/`child.py` (shared base-SDK code, explicitly out of scope).

## v2-deferred (per spec §11)

- **Custom result-based completion** (`shouldComplete` predicate + `DagCompletionStatus` +
  `CompletionDecision`) — no Python counterpart exists; reuse threshold `CompletionConfig`.
  Should land as a cross-SDK feature.
- **Faithful `STARTED`-set replay under large-payload early completion** — Python's re-execute
  model restarts in-flight tasks; v1 matches existing `map`/`parallel` behavior.
