# Migrating from 1.x to 2.x

`2.x` is a breaking major release. Every change is a bug fix or brings Python to
parity with the JavaScript and Java SDKs. The changes most likely to touch your
code are the typed, per-operation **error hierarchy**, the
**serialize/deserialize round trip on the first run**, and the new fail-fast
**default `map` / `parallel` completion** (which completes after the first
observed failure and stops scheduling pending items; already-started items are
not cancelled, so with unlimited concurrency all items may already be running -
set `max_concurrency` if you need to bound that). Opt back into process-all with
`CompletionConfig.all_completed()`.

There is no compatibility shim: removed names (for example `CallableRuntimeError`)
are gone with no alias. If you are not ready to migrate, stay on `1.x`.

> Instrumentation plugins are out of scope here. The experimental `plugins=` hook
> already existed in `1.x`; `2.x` adds opt-in auto-discovery and `PluginLoadError`.
> The plugin interface itself also changed incompatibly (hook signatures changed,
> enums moved, and `InvocationEndInfo.status` is now required), so plugin authors
> should expect to update. Because plugins are opt-in and still evolving, they are
> documented with the plugin/OpenTelemetry feature rather than in this guide.

## Porting to 2.0

Each change below lists what changed and what you must do. The ones most likely
to touch your code are the [typed error hierarchy](#callableruntimeerror-and-friends-removed),
the [first-run serialize/deserialize round trip](#first-run-serializedeserialize-round-trip),
and the [fail-fast `map` / `parallel` default](#completionconfigall_completed-tolerates-all-failures).
See [Finding affected code](#finding-affected-code) for a grep checklist.

### `CallableRuntimeError` and friends removed

`CallableRuntimeError`, `UserlandError`, and
`CallableRuntimeErrorSerializableDetails` are gone; typed per-operation errors
replace them.

Catch `StepError`, `InvokeError`, `ChildContextError`, or `WaitForConditionError`
(or the base `DurableOperationError`) instead of `CallableRuntimeError`. See
[Error handling](#error-handling-the-biggest-change) for the full hierarchy.

### `CallbackError` moved out of the termination tree

`CallbackError` is no longer a termination reason, and graded subtypes were
added.

Remove any `termination_reason == TerminationReason.CALLBACK_ERROR` check (the
enum member is gone). Optionally catch `CallbackTimeoutError`,
`CallbackExternalError`, or `CallbackSubmitterError`. See
[Callbacks](#callbacks).

### `BatchResult.throw_if_error()` raises typed errors

It no longer raises `CallableRuntimeError`.

Replace `except CallableRuntimeError` with `ChildContextError` (ordinary item
failure), `SerDesError` (item serialize/deserialize failure), and
`BatchCompletionError` (custom `should_complete` failed the batch with no item
error). `ChildContextError` and `BatchCompletionError` share the base
`DurableOperationError`, but `SerDesError` does not, so catch
`(DurableOperationError, SerDesError)` or list all three:

```python
# 1.x
except CallableRuntimeError:
    ...

# 2.x
except (DurableOperationError, SerDesError):
    ...
```

See [map / parallel](#map--parallel) for a full example.

### Serdes failures surface as `SerDesError`, not `ExecutionError`

`SerDesError` is a direct child of `DurableExecutionsError`, not `ExecutionError`.

If you caught serdes failures with `except ExecutionError`, catch `SerDesError`
(or `DurableExecutionsError`) instead:

```python
# 1.x
except ExecutionError:
    ...

# 2.x
except SerDesError:
    ...
```

### First-run serialize/deserialize round trip

`step`, child contexts, `map`/`parallel`, and `wait_for_condition` now round-trip
their result through the serdes on the first run, returning
`deserialize(serialize(x))` - the same canonical value replay returns.

If you relied on the raw pre-serialization object, use the deserialized shape
instead (or make your `SerDes` round-trip identity). Ensure `wait_for_condition`
`initial_state` is serializable by the configured serdes. For a transient serdes
failure, raise the new `RetryableSerDesError` (retries) instead of `SerDesError`
(permanent). See [Serialize/Deserialize round trip](#serializedeserialize-round-trip).

### Empty-string payloads are preserved

`1.x` dropped empty-string (`""`) payloads when serializing; `2.x` keeps them.

A step, invoke, or child result of `""` that surfaced as `None` (dropped payload)
in `1.x` now surfaces as `""`. If you treated an absent payload as `None`, handle
`""` explicitly.

### `InvokeConfig.timeout` / `timeout_seconds` removed

Both fields are gone.

Remove them. Enforce any timeout inside the invoked function or as a separate
timer.

### `map` / child batching names removed

Removed: `ItemBatcher`, `ItemsPerBatchUnit`, `BatchedInput`, `TerminationMode`,
`StepFuture`, `MapConfig.item_batcher`, `ChildConfig.item_serdes`; also
`ChainedInvokeFailedToStartType`, `ChainedInvokeTimeoutType`, and
`ChainedInvokeStopType` (from `lambda_service`).

Remove all uses. Replace `ChildConfig.item_serdes` with `ChildConfig.serdes`.

### Config validation moved to construction and call time

`MapConfig`, `ParallelConfig`, and `CompletionConfig` now validate arguments at
construction (for example `max_concurrency=0` or `min_successful=0` raise
`ValidationError`). `min_successful > total` is validated at the
`map()`/`parallel()` call, not at construction.

Wrap config construction, and the `map()`/`parallel()` call, in
`try/except ValidationError` when inputs are external.

### `CompletionConfig.all_completed()` tolerates all failures

It now actually tolerates every failure, and the default `map` / `parallel`
completion config is now fail-fast.

If you hand-built the old all-`None` config, use the factory instead. To preserve
1.x process-all behavior, pass it explicitly:

```python
# 2.x - keep processing every item even when some fail
MapConfig(completion_config=CompletionConfig.all_completed())
ParallelConfig(completion_config=CompletionConfig.all_completed())
```

### `BatchResult.all` omits never-started branches

`total_count` and positional iteration differ for early-completed batches,
because never-started branches are no longer included.

If you index `.all` by original position or expect `total_count` to include
unstarted branches, update that logic.

### `summary_generator` output moved into an envelope

For `map`/`parallel`, a custom `summary_generator` output is now stored under a
`"summary"` key in an SDK-owned envelope; it no longer replaces the checkpoint
payload. `ChildConfig.summary_generator` is unchanged: its output is still
checkpointed verbatim.

If you parse `map`/`parallel` summary payloads from execution history, read the
`"summary"` key from the envelope. Child-context summary consumers need no change.

### `WaitDecision` and wait timeouts removed

`WaitDecision` is gone, along with `WaitStrategyConfig.timeout` and
`WaitStrategyConfig.timeout_seconds`.

Use `WaitForConditionDecision` (`stop_polling()` / `continue_waiting(delay)`).

### `wait_for_condition` raises on exhaustion

It raises `WaitForConditionError` when it exhausts `max_attempts`.

Catch `WaitForConditionError` instead of inspecting the returned state.

### Finding affected code

Grep for the removed and changed names before upgrading:

```bash
rg -n "CallableRuntimeError|UserlandError|CallableRuntimeErrorSerializableDetails" .
rg -n "CallbackError|CALLBACK_ERROR" .
rg -n "InvokeConfig\(|\.timeout_seconds" .
rg -n "WaitDecision|WaitStrategyConfig\(|item_batcher|ItemBatcher|ItemsPerBatchUnit" .
rg -n "TerminationMode|BatchedInput|StepFuture|ChildConfig\(" .
rg -n "ChainedInvoke|except ExecutionError" .
```

## Error Handling (the biggest change)

In `1.x` nearly every user-land failure surfaced as one `CallableRuntimeError`,
so a failed step was indistinguishable from a failed invoke or child branch. `2.x`
raises a specific type per operation, all under a new base `DurableOperationError`.
Inspect the failure through its fields, not `__cause__`: `error_type`, `message`,
`data`, and `stack_trace`. Do not rely on `__cause__` being the original
exception - the SDK reconstructs a `DurableOperationError` stand-in carrying
those same fields (on both the first run and replay, for determinism), so the
original type is not preserved (a `ValueError` does not stay a `ValueError`) and
custom attributes are lost.

For `StepError`, `InvokeError`, `ChildContextError`, and `WaitForConditionError`,
`error_type` is the name of the error that escaped your code (e.g. `"ValueError"`).
The graded callback errors are different: `CallbackTimeoutError`,
`CallbackExternalError`, and `CallbackSubmitterError` are constructed without the
originating `error_type`, so `error_type` is the callback class name, not the
underlying cause. Use the specific callback exception type (and `message` /
`data` / `stack_trace`) to distinguish those.

```python
# 1.x
from aws_durable_execution_sdk_python.exceptions import CallableRuntimeError
try:
    result = context.step(charge_card, name="charge")
except CallableRuntimeError as e:
    context.logger.error("something failed: %s", e.message)

# 2.x
from aws_durable_execution_sdk_python import StepError, DurableOperationError
try:
    result = context.step(charge_card, name="charge")
except StepError as e:               # or `except DurableOperationError` to catch any operation
    context.logger.error("charge step failed: %s", e.message)
```

New types, all exported from the package root: `DurableOperationError` (base),
`StepError`, `InvokeError`, `ChildContextError`, `WaitForConditionError`,
`CallbackError` (+ `CallbackExternalError`, `CallbackTimeoutError`,
`CallbackSubmitterError`), plus `SerDesError` (now exported) and
`RetryableSerDesError`. `SerDesError` stays a direct child of
`DurableExecutionsError`; `RetryableSerDesError` is a retryable `InvocationError`.

### Callbacks

`context.wait_for_callback(...)` returns the payload directly and raises the
callback error from the call itself (there is no `callback.result()`):

```python
from aws_durable_execution_sdk_python import (
    CallbackError, CallbackTimeoutError, CallbackSubmitterError,
)
try:
    payload = context.wait_for_callback(submit_approval, name="approval")
except CallbackTimeoutError:
    ...                               # timeout / heartbeat expiry
except CallbackSubmitterError:
    ...                               # the submitter step failed
except CallbackError as e:            # external + internal
    context.logger.error("callback failed: %s", e.message)
```

### map / parallel

`throw_if_error()` can raise three types: `ChildContextError` for the ordinary
item/branch failure, `SerDesError` if an item result failed to serialize or
deserialize, and `BatchCompletionError` when a custom `should_complete` predicate
marked the batch failed with no failed item (see the completion-predicate section
below). `ChildContextError` and `BatchCompletionError` share the base
`DurableOperationError`, but `SerDesError` does not, so catch
`(DurableOperationError, SerDesError)` or list all three explicitly.

```python
from aws_durable_execution_sdk_python import (
    ChildContextError, SerDesError, BatchCompletionError,
)

result = context.map(items, process_item)
try:
    result.throw_if_error()
except (ChildContextError, SerDesError, BatchCompletionError):
    for err in result.get_errors():   # every failed item's ErrorObject
        context.logger.error("%s: %s", err.type, err.message)
```

## Serialize/Deserialize Round Trip

`1.x` returned the raw in-memory result on the first run but the deserialized
result on replay, so a non-identity custom `SerDes` produced different values.
`2.x` round-trips (`serialize` then `deserialize`) on the first run for `step`,
child contexts, `map`/`parallel`, and `wait_for_condition` (which also feeds the
deserialized state to the wait strategy). This makes first-run behavior match
replay for deterministic, reversible custom serdes. If your code depended on
the raw pre-serialization object on the first run, switch to the deserialized
shape (or make the serdes round-trip identity). This also surfaces genuine
serialization bugs on the first run instead of later on replay.

`invoke` and `wait` are unaffected. `wait_for_callback` is implemented via a
child context, so its result is serialized and deserialized before it returns:
do not rely on callback-result object identity. The enclosing child context uses
the default (extended-type) serdes, not `WaitForCallbackConfig.serdes`, so the
value your callback deserializer returns must itself be serializable by the
default serdes; otherwise the child raises `SerDesError`.

`wait_for_condition` also round-trips `initial_state` through the serdes before
the first check, so `initial_state` must now be serializable by the configured
serdes. Custom serdes should serialize polling state to a non-empty string; an
empty string checkpoint payload is currently treated as no stored polling state
on resume, so the operation may restart from `initial_state`.

## New in 2.x: Custom Completion Predicate (Optional)

`2.x` adds a `should_complete` predicate to `CompletionConfig`, giving `map` and
`parallel` full control over when a batch completes early. This is a new feature,
not a breaking change - no action is required unless you adopt it.

```python
from aws_durable_execution_sdk_python import complete_batch, continue_batch
from aws_durable_execution_sdk_python.config import CompletionConfig

config = CompletionConfig(
    should_complete=lambda status: (
        complete_batch() if status.success_count >= 2 else continue_batch()
    )
)
```

The predicate receives a `CompletionStatus` snapshot (counts plus per-item
statuses) and returns a `CompletionDecision` - `continue_batch()`, or
`complete_batch(CompletionOutcome.SUCCEEDED)` / `complete_batch(CompletionOutcome.FAILED)`
(the outcome defaults to `SUCCEEDED`). A `FAILED` outcome marks the whole batch
failed; `throw_if_error()` then raises `BatchCompletionError` (a
`DurableOperationError` subtype) even when no individual item failed. Individual
item/branch failures still surface as `ChildContextError`. Notes:

- It cannot be combined with `min_successful` or the `tolerated_failure_*`
  fields; doing so raises `ValidationError` at construction.
- The predicate runs before any branch is scheduled (`completed_count == 0`)
  and on suspension state changes. At those points, unscheduled item statuses
  are `None`; handle missing statuses explicitly so the predicate does not
  fail the batch or complete it before useful work starts.
- The predicate must be deterministic, side-effect-free, and monotonic: once a
  progress snapshot returns `complete_batch(outcome)`, every later snapshot
  containing that progress must return `complete_batch(outcome)` with the same
  `CompletionOutcome`. Replaying an already-completed batch uses the
  checkpointed decision, but a mid-run resume re-runs the batch live and
  re-evaluates the predicate as completed branches replay, possibly in a
  different order.
- New exports: `complete_batch`, `continue_batch`, `CompletionStatus`,
  `CompletionDecision`, `CompletionOutcome`, `CompletionItemStatus`,
  `BatchItemStatus`, `BatchCompletionError`.

## New in 2.x: Attempt Number in Contexts (Optional)

`StepContext` and `WaitForConditionCheckContext` now expose an `attempt` field
(the current attempt number, starting at 1). Read it inside a step or a
`wait_for_condition` check to branch on the retry count. The SDK injects these
contexts, so normal usage needs no change. But `attempt` is a required
dataclass field with no default: if you construct these contexts directly (in
tests, fixtures, or wrappers), you must now pass `attempt` or construction fails
with `TypeError`.

## Recommended Validation After Upgrading

1. Build and run your test suite against `2.x`, and grep for the removed names above.
2. Trigger a failure in a `step`, an `invoke`, and a `map`/`parallel` branch;
   confirm you catch `StepError`, `InvokeError`, and `ChildContextError`.
3. Exercise a `wait_for_callback` timeout and a submitter-step failure
   (`CallbackTimeoutError`, `CallbackSubmitterError`).
4. Exercise a `wait_for_condition` that exhausts its attempts (`WaitForConditionError`).
5. If you use a custom `SerDes`, run a workflow that checkpoints a result, an
   error payload, and `wait_for_condition` polling state; confirm first-run
   output equals replay output.
