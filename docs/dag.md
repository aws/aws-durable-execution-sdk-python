# DAG Support (`context.dag()`)

> ## ⚠️ EXPERIMENTAL
>
> `context.dag()` is an **experimental** feature and may change or be removed in a
> future release **without a major-version bump**. Do not depend on it in
> production until it is promoted to stable. Every public DAG symbol carries a
> Sphinx `.. warning:: **Experimental.**` docstring admonition, and the **first
> use** of `context.dag()` in a process emits a one-time `FutureWarning`.

`context.dag()` adds a declarative directed-acyclic-graph primitive: you declare
a graph of typed tasks once in a synchronous *registration phase*; the runtime
validates the graph, schedules tasks topologically, runs independent chains
concurrently, evaluates per-task trigger rules and `run_if` predicates, and
aggregates results into a `DagResult` (returned **synchronously**).

A DAG is implemented as a **child context** (one `run_in_child_context` node in
the parent's operation tree) whose body runs a name-based scheduler. Each task
delegates to the **same operation executor** the equivalent `DurableContext`
method uses — the only difference is that the task's entity id is derived from
its **name** (`{container}-DAG_NODE_T_{name}`) instead of the per-context
counter, so ids are stable regardless of run-time completion ordering.

## Quick start

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event, context: DurableContext):
    def build(d):
        fetch = d.step(lambda deps, sc: fetch_source(event), name="fetch")
        clean = d.step(lambda deps, sc: clean_data(deps["fetch"]), deps=[fetch], name="clean")
        enrich = d.step(lambda deps, sc: enrich(deps["fetch"]), deps=[fetch], name="enrich")
        d.step(
            lambda deps, sc: merge(deps["clean"], deps["enrich"]),
            deps=[clean, enrich],
            name="merge",
        )

    result = context.dag(build, name="pipeline")
    result.throw_if_error()
    return result.get_result("merge")
```

## Declaring dependencies — two styles

1. **Inline `deps=[...]`** — the upstream results are available in the task body's
   `DepsMap` (first positional argument):

   ```python
   c = d.step(process, deps=[a, b], name="c")   # body: deps["a"], deps["b"]
   ```

2. **Ordering-only `.after(...)`** — gate scheduling/trigger-rule/cycle checks but
   do **not** appear in the `DepsMap`:

   ```python
   d.step(notify, name="notify").after(a)       # waits for a; a not in deps map
   e = d.step(process, deps=[a], name="e").after(b)   # deps["a"] present; b ordering-only
   ```

Task bodies always receive `deps` as their first positional argument (an empty
`DepsMap` for root tasks), followed by the operation's native context/args:

| task kind             | body signature                 |
| --------------------- | ------------------------------ |
| `step`                | `(deps, step_ctx)`             |
| `invoke`              | `payload_fn(deps) -> payload`  |
| `wait_for_callback`   | `(deps, callback_id, ctx)`     |
| `wait_for_condition`  | `(deps, state, ctx)`           |
| `run_in_child_context`| `(deps, child_ctx)`            |

## Accessing dependency results

`DepsMap` supports **two** access styles:

```python
data = deps["fetch"]      # string-keyed: always available, typed Any
data = deps[fetch]        # handle-keyed: recovers the static type TaskHandle[T] -> T
```

Handle-keyed access dispatches at runtime on the handle type (it does not rely on
hashing), and is the recommended typed path.

## Trigger rules and `run_if`

`TriggerRule` controls when a task runs based on its upstream **terminal**
statuses:

| rule          | runs when                                             |
| ------------- | ----------------------------------------------------- |
| `ALL_SUCCESS` | every dep SUCCEEDED (default); empty upstream ⇒ runs  |
| `ALL_FAILED`  | every dep FAILED and there is at least one            |
| `ALL_DONE`    | every dep is terminal, regardless of outcome          |
| `ANY_SUCCESS` | at least one dep SUCCEEDED                             |
| `ANY_FAILED`  | at least one dep FAILED                               |
| `NONE_FAILED` | no dep FAILED (SUCCEEDED/SKIPPED allowed)             |

```python
d.step(refund, deps=[charge], name="refund").trigger_rule(TriggerRule.ALL_FAILED)
d.step(audit, deps=[charge], name="audit").trigger_rule(TriggerRule.ALL_DONE)
```

`run_if` is a synchronous, deterministic predicate over the resolved `DepsMap`,
evaluated after the trigger rule passes. Returning `False` marks the task
`SKIPPED` with reason `RUN_IF_PREDICATE`:

```python
d.step(notify, deps=[score], name="notify", run_if=lambda deps: deps["score"] > 0.9)
```

A `SKIPPED` task **mints no entity id and writes no checkpoint**; it is recomputed
deterministically on every replay.

## Failure semantics (drain by default)

A failed task is a **terminal state, not an abort**. By default the DAG **drains**
the reachable graph so compensation/fallback trigger rules can run. `context.dag()`
does **not** raise on task failure; it returns a `DagResult` with
`failure_count > 0` and `completion_reason == COMPLETED_WITH_FAILURES`. Opt into
raising via `result.throw_if_error()` (raises `DagExecutionError`).

This deliberately differs from `map`/`parallel`'s default fail-fast. To get
batch-style early completion, use `completion_config`.

## Threshold completion

`DagConfig.completion_config` reuses the existing threshold-only `CompletionConfig`
(`min_successful`, `tolerated_failure_count`, `tolerated_failure_percentage`).
SKIPPED tasks count toward neither success nor failure; only success/failure feed
the threshold logic.

```python
context.dag(build, name="p", config=DagConfig(
    max_concurrency=5,
    completion_config=CompletionConfig(min_successful=8, tolerated_failure_count=2),
))
```

`max_concurrency <= 0` raises `ValidationError`.

## Large-payload behavior (re-execute)

When a DAG's serialized result exceeds the checkpoint size limit, the container is
checkpointed in **ReplayChildren** mode and the DAG **re-executes** on replay:
`register` rebuilds the graph, the scheduler re-runs, and each completed task hits
its own name-based checkpoint fast path (so side effects are not repeated). This
matches the Python SDK's existing `map`/`parallel` re-execute model.

`DagConfig.summary_generator` is **observability-only** in Python — the summary
string is checkpointed for console/log readability but is **never read back on
replay**, so it cannot change (or hang) the replayed result.

## Validation

Runs once after registration (deterministic; identical on replay):

- **`DagInvalidTaskNameError`** — name must be non-empty, ≤100 chars, match
  `^[a-zA-Z0-9_]+$` (no `-`), and not contain the reserved `DAG_NODE_T_` token.
  Bare `lambda` tasks must pass an explicit `name=`.
- **`DagDuplicateTaskError`** — duplicate task name in the scope.
- **`DagInvalidDependencyError`** — a dep handle from another/parent DAG scope.
- **`DagCyclicDependencyError`** — dependency cycle (Kahn's algorithm).

## v2-deferred items

- **Custom result-based completion** (a `shouldComplete` predicate over per-task
  results). Python's `CompletionConfig` is threshold-only and has no
  custom-completion primitive; v1 reuses the threshold config verbatim. This
  should land as a cross-SDK feature (map/parallel + dag), not DAG-only.
- **Faithful `STARTED`-set replay under large-payload early completion.** Python's
  re-execute model restarts in-flight tasks rather than reporting them as
  `STARTED`; v1 matches the existing `map`/`parallel` behavior.
