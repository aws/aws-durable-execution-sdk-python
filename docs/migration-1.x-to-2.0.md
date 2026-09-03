# Migrating from 1.x to 2.0

`2.0` is a major release. It contains breaking changes. The changes
most likely to touch your code are the typed, per-operation
[error hierarchy](#error-handling), the
[first-run serialize and deserialize round trip](#serialize-and-deserialize-round-trip),
and the new fail-fast [default `map` and `parallel` completion](#completionconfigall_completed-tolerates-all-failures).

## Why upgrade to 2.0

- **Typed errors tell you which operation failed.** `1.x` collapsed almost
  every failure into one `CallableRuntimeError`. `2.0` raises `StepError`,
  `InvokeError`, `ChildContextError`, `WaitForConditionError`, or a graded
  `CallbackError` subtype, so you branch on the operation that failed instead of
  parsing a message. See [Error handling](#error-handling).
- **`should_complete` gives `map` and `parallel` custom completion.** A
  predicate decides when a batch finishes early, so you express quorum and
  dependency rules directly. See
  [Custom completion predicate](#custom-completion-predicate).
- **First-run output matches replay.** A step returns the same value the first
  time it runs and on every replay, so a non-identity custom serdes no longer
  produces two different values. See
  [Serialize and deserialize round trip](#serialize-and-deserialize-round-trip).
- **`attempt` is available inside steps and condition checks.** Read the current
  attempt number to branch on retry count. See
  [Attempt number in contexts](#attempt-number-in-contexts).
- **Replay catches non-determinism early.** The SDK validates each operation's
  identity against its checkpoint on replay and fails fast on drift instead of
  consuming the wrong checkpoint. See
  [Replay validates operation identity](#replay-validates-operation-identity).

## Porting to 2.0

The changes
most likely to touch your code are the
[typed error hierarchy](#callableruntimeerror-and-friends-removed), the
[first-run round trip](#first-run-serialize-and-deserialize-round-trip), and the
[fail-fast `map` and `parallel` default](#completionconfigall_completed-tolerates-all-failures).
[Finding affected code](#finding-affected-code) lists a grep checklist.

### `CallableRuntimeError` and friends removed

`2.0` removes `CallableRuntimeError`, `UserlandError`, and
`CallableRuntimeErrorSerializableDetails`, and replaces them with typed
per-operation errors.

Catch `StepError`, `InvokeError`, `ChildContextError`, or `WaitForConditionError`
(or the base `DurableOperationError`) instead of `CallableRuntimeError`.
[Error handling](#error-handling) describes the full hierarchy.

### `CallbackError` moved out of the termination tree

`CallbackError` no longer names a termination reason, and `2.0` adds graded
subtypes.

Remove any `termination_reason == TerminationReason.CALLBACK_ERROR` check,
because the enum member no longer exists. Catch `CallbackTimeoutError`,
`CallbackExternalError`, or `CallbackSubmitterError` when you need the specific
mode. See [Callbacks](#callbacks).

### `BatchResult.throw_if_error()` raises typed errors

`throw_if_error()` no longer raises `CallableRuntimeError`.

Replace `except CallableRuntimeError` with three types: `ChildContextError` for
an ordinary item failure, `SerDesError` for an item that failed to serialize or
deserialize, and `BatchCompletionError` when a custom `should_complete` predicate
fails the batch with no item error. `ChildContextError` and `BatchCompletionError`
share the base `DurableOperationError`, but `SerDesError` does not, so catch
`(DurableOperationError, SerDesError)` or list all three.

```python
# 1.x
except CallableRuntimeError:
    ...

# 2.0
except (DurableOperationError, SerDesError):
    ...
```

[map and parallel](#map-and-parallel) shows a full example.

### Serdes failures surface as `SerDesError`, not `ExecutionError`

`SerDesError` descends directly from `DurableExecutionsError`, not from
`ExecutionError`.

Catch `SerDesError` (or `DurableExecutionsError`) where you caught
`ExecutionError` for a serdes failure.

```python
# 1.x
except ExecutionError:
    ...

# 2.0
except SerDesError:
    ...
```

### First-run serialize and deserialize round trip

`step`, child contexts, `map`, `parallel`, and `wait_for_condition` now
round-trip their result through the serdes on the first run and return
`deserialize(serialize(x))`, the same canonical value replay returns.

Switch to the deserialized shape if your code depended on the raw
pre-serialization object, or make your `SerDes` round-trip to an identical value.
`wait_for_condition` also round-trips `initial_state`, so the configured serdes
must serialize it. Raise the new `RetryableSerDesError` for a transient serdes
failure, which replays the invocation. Read
[Serialize and deserialize round trip](#serialize-and-deserialize-round-trip)
before you adopt `RetryableSerDesError`, because an invocation replay re-runs a
non-idempotent step body.

### 2.0 preserves empty-string payloads

`1.x` dropped an empty-string (`""`) payload when it serialized. `2.0` keeps it.

A step, invoke, or child result of `""` that surfaced as `None` in `1.x` now
surfaces as `""`. Handle `""` explicitly if you treated an absent payload as
`None`.

### `InvokeConfig.timeout` and `timeout_seconds` removed

`2.0` removes both fields.

Remove them. Enforce a timeout inside the invoked function or with a separate
timer.

### Removed 1.x-only names

`2.0` removes `ItemBatcher`, `ItemsPerBatchUnit`, `BatchedInput`,
`TerminationMode`, `StepFuture`, `MapConfig.item_batcher`, and
`ChildConfig.item_serdes`. It also removes `ChainedInvokeFailedToStartType`,
`ChainedInvokeTimeoutType`, and `ChainedInvokeStopType` from `lambda_service`.

Remove all uses. Replace `ChildConfig.item_serdes` with `ChildConfig.serdes`.

### Config validation moved to construction and call time

`MapConfig`, `ParallelConfig`, and `CompletionConfig` now validate their
arguments at construction. `max_concurrency=0` and `min_successful=0` raise
`ValidationError`. The `map()` and `parallel()` call validates
`min_successful > total`, because that check needs the item count.

Wrap config construction, and the `map()` or `parallel()` call, in
`try/except ValidationError` when the inputs come from outside your code.

### `max_concurrency` bounds in-flight branches

`1.x` used `max_concurrency` to cap worker threads. `2.0` caps in-flight
branches. A branch that suspends while it awaits an invoke result or a callback
holds its slot until it reaches a terminal state, and a new branch starts only
when a slot frees up. When every in-flight branch suspends and no slot is free,
the parent suspends too.

Revisit the value if you sized `max_concurrency` around thread count rather than
concurrent in-flight work.

### `CompletionConfig.all_completed()` tolerates all failures

`all_completed()` now tolerates every failure, and the default `map` and
`parallel` completion config runs fail-fast: the batch completes after the first
observed failure and stops scheduling pending items. The SDK does not cancel
already-started items, so with unlimited concurrency every item may already be
running. Set `max_concurrency` to bound that.

Call the factory instead of hand-building the old all-`None` config. Pass
`all_completed()` explicitly to keep the `1.x` process-all behavior.

```python
# 2.0: keep processing every item even when some fail
MapConfig(completion_config=CompletionConfig.all_completed())
ParallelConfig(completion_config=CompletionConfig.all_completed())
```

### `BatchResult.all` omits never-started branches

`BatchResult.all` no longer includes never-started branches, so `total_count`
and positional iteration differ for an early-completed batch.

Update any logic that indexes `.all` by original position or expects
`total_count` to include unstarted branches.

### `summary_generator` output moved into an envelope

For `map` and `parallel`, the SDK now stores a custom `summary_generator` output
under a `"summary"` key inside an SDK-owned envelope instead of using it as the
checkpoint payload. `ChildConfig.summary_generator` behaves as before and
checkpoints its output verbatim.

Read the `"summary"` key from the envelope if you parse `map` or `parallel`
summary payloads from the execution history. A child-context summary consumer
needs no change.

### `WaitDecision` and wait timeouts removed

`2.0` removes `WaitDecision`, `WaitStrategyConfig.timeout`, and
`WaitStrategyConfig.timeout_seconds`.

Use `WaitForConditionDecision` (`stop_polling()` or `continue_waiting(delay)`).

### `create_wait_strategy` raises on exhaustion

A strategy from `create_wait_strategy(WaitStrategyConfig(...))` raises
`WaitForConditionError` when it exhausts `max_attempts`. `WaitForConditionConfig`
has no `max_attempts` of its own, so a custom wait strategy must enforce its own
limit or it polls indefinitely.

Catch `WaitForConditionError` instead of inspecting the returned state.

### Replay validates operation identity

On replay, the SDK validates each operation's checkpoint against the current
code by `type`, `sub_type`, `name`, and `parent_id`. Any mismatch raises
`NonDeterministicExecutionError` and fails the execution. `1.x` let a renamed or
reordered operation consume a neighboring checkpoint silently. `2.0` fails fast.
A `map` or `parallel` batch also validates FLAT against NESTED nesting drift.

`NonDeterministicExecutionError` descends from `ExecutionError`, so it is
unrecoverable and you should not catch it. Treat this change as a deployment
constraint rather than a code change. Do not rename an operation (the `name=`
argument, or a step function's name when you omit `name`), change a batch's
`nesting_type`, or reparent an operation while executions are in flight. Drain
in-flight executions before you deploy such a change.

### `wait_for_condition` fails on unreadable polling state

When a custom serdes fails to deserialize checkpointed polling state on a
resumed invocation, `2.0` fails the operation with a typed error. `1.x` caught
the failure and silently restarted from `initial_state`, so the loop could
succeed with a result computed from the wrong state.

If your custom serdes can fail while restoring state, expect the operation to
fail instead of restarting.

### Finding affected code

Grep for the removed and changed names before you upgrade. This list covers the
renamed and removed identifiers, not the behavioral changes above, so treat it as
a starting point rather than a complete check:

```bash
rg -n "CallableRuntimeError|UserlandError|CallableRuntimeErrorSerializableDetails" .
rg -n "CallbackError|CALLBACK_ERROR" .
rg -n "InvokeConfig\(|\.timeout_seconds" .
rg -n "WaitDecision|WaitStrategyConfig\(|item_batcher|ItemBatcher|ItemsPerBatchUnit" .
rg -n "TerminationMode|BatchedInput|StepFuture|ChildConfig\(" .
rg -n "ChainedInvoke|except ExecutionError" .
```

## Error Handling

In `1.x` nearly every user-land failure surfaced as one `CallableRuntimeError`,
so a failed step looked identical to a failed invoke or child branch. `2.0`
raises a specific type per operation under a new base `DurableOperationError`.
Inspect the failure through its fields, `error_type`, `message`, `data`, and
`stack_trace`, not `__cause__`. The SDK reconstructs a `DurableOperationError`
stand-in that carries those fields on both the first run and replay, so
`__cause__` does not hold the original exception. A `ValueError` does not stay a
`ValueError`, and custom attributes do not survive.

For `StepError`, `InvokeError`, `ChildContextError`, and `WaitForConditionError`,
`error_type` holds the name of the error that escaped your code, for example
`"ValueError"`. The graded callback errors work differently. The SDK constructs
`CallbackTimeoutError`, `CallbackExternalError`, and `CallbackSubmitterError`
without the originating `error_type`, so `error_type` holds the callback class
name rather than the underlying cause. Branch on the specific callback exception
type, and read `message`, `data`, and `stack_trace`, to tell those apart.

```python
# 1.x
from aws_durable_execution_sdk_python.exceptions import CallableRuntimeError
try:
    result = context.step(charge_card, name="charge")
except CallableRuntimeError as e:
    context.logger.error("something failed: %s", e.message)

# 2.0
from aws_durable_execution_sdk_python import StepError, DurableOperationError
try:
    result = context.step(charge_card, name="charge")
except StepError as e:               # or `except DurableOperationError` for any operation
    context.logger.error("charge step failed: %s", e.message)
```

The package root exports every new type: `DurableOperationError` (base),
`StepError`, `InvokeError`, `ChildContextError`, `WaitForConditionError`,
`CallbackError` (with `CallbackExternalError`, `CallbackTimeoutError`, and
`CallbackSubmitterError`), and now `SerDesError` and `RetryableSerDesError`.
`SerDesError` descends directly from `DurableExecutionsError`.
`RetryableSerDesError` is a retryable `InvocationError`.

### Callbacks

`context.wait_for_callback(...)` returns the payload directly, so do not call
`.result()` on its return value. It raises the callback error from the call
itself. The separate `context.create_callback()` API still returns a `Callback`
whose `.result()` waits for completion, so keep that wait in a manual callback
flow.

```python
from aws_durable_execution_sdk_python import (
    CallbackError, CallbackTimeoutError, CallbackSubmitterError,
)
try:
    payload = context.wait_for_callback(submit_approval, name="approval")
except CallbackTimeoutError:
    ...                               # timeout or heartbeat expiry
except CallbackSubmitterError:
    ...                               # the submitter step failed
except CallbackError as e:            # external and internal
    context.logger.error("callback failed: %s", e.message)
```

### map and parallel

`throw_if_error()` raises one of three types: `ChildContextError` for an ordinary
item or branch failure, `SerDesError` for an item that failed to serialize or
deserialize, and `BatchCompletionError` when a custom `should_complete` predicate
fails the batch with no failed item (see
[Custom completion predicate](#custom-completion-predicate)). `ChildContextError`
and `BatchCompletionError` share the base `DurableOperationError`, but
`SerDesError` does not, so catch `(DurableOperationError, SerDesError)` or list
all three.

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

## Serialize and Deserialize Round Trip

`1.x` returned the raw in-memory result on the first run but the deserialized
result on replay, so a non-identity custom `SerDes` returned two different
values. `2.0` round-trips the result, running `serialize` then `deserialize`, on
the first run for `step`, child contexts, `map`, `parallel`, and
`wait_for_condition`, which also feeds the deserialized state to the wait
strategy. First-run output now matches replay for a deterministic, reversible
custom serdes. Switch to the deserialized shape if your code depended on the raw
pre-serialization object, or make the serdes round-trip to an identical value.
The round trip also surfaces a genuine serialization bug on the first run rather
than later on replay.

`invoke` and `wait` do not round-trip. `wait_for_callback` runs in a child
context, so the child context serializes and deserializes its result before it
returns. Do not rely on callback-result object identity. That child context uses
the default extended-type serdes, not `WaitForCallbackConfig.serdes`, so the
default serdes must serialize whatever your callback deserializer returns, or the
child raises `SerDesError`.

`wait_for_condition` round-trips `initial_state` through the serdes before the
first check, so the configured serdes must serialize `initial_state`. On a
resumed invocation, a serdes that fails to deserialize the stored polling state
fails the operation. `1.x` silently restarted from `initial_state` in that case.
A custom serdes should serialize polling state to a non-empty string. The SDK
currently treats an empty-string checkpoint payload as no stored polling state
on resume, so the operation can restart from `initial_state`.

`RetryableSerDesError` does more than retry serialization. An executor re-raises
it before it writes a success checkpoint, so the backend re-invokes the whole
execution. An `AT_LEAST_ONCE_PER_RETRY` step re-runs its body and repeats its
side effects on that re-invocation, so raise `RetryableSerDesError` only from an
idempotent step or accept the duplicate work. An `AT_MOST_ONCE_PER_RETRY` step
already wrote its START checkpoint, so the retried invocation treats the step as
interrupted and applies the step retry strategy instead of re-running the body.
Raise `SerDesError` for a permanent failure.

## Custom Completion Predicate

`2.0` adds a `should_complete` predicate to `CompletionConfig` that gives `map`
and `parallel` control over when a batch completes early. This is a new feature,
not a breaking change, and requires no action unless you adopt it.

```python
from aws_durable_execution_sdk_python import complete_batch, continue_batch
from aws_durable_execution_sdk_python.config import CompletionConfig

config = CompletionConfig(
    should_complete=lambda status: (
        complete_batch() if status.success_count >= 2 else continue_batch()
    )
)
```

The predicate receives a `CompletionStatus` snapshot, which holds the counts and
the per-item statuses, and returns a `CompletionDecision`. Return
`continue_batch()` to keep going, `complete_batch(CompletionOutcome.SUCCEEDED)`
to finish successfully, or `complete_batch(CompletionOutcome.FAILED)` to finish
with a failure. The outcome defaults to `SUCCEEDED`. A `FAILED` outcome marks the
whole batch failed, and `throw_if_error()` then raises `BatchCompletionError`, a
`DurableOperationError` subtype, even when no individual item failed. An
individual item or branch failure still surfaces as `ChildContextError`.

Three constraints govern the predicate:

- It cannot combine with `min_successful` or the `tolerated_failure_*` fields.
  Combining them raises `ValidationError` at construction.
- It runs before the SDK schedules any branch (`completed_count == 0`) and on
  each suspension state change. At those points an unscheduled item has status
  `None`, so handle a missing status explicitly, or the predicate can fail the
  batch or complete it before useful work starts.
- It must stay deterministic, side-effect-free, and monotonic. Once a progress
  snapshot returns `complete_batch(outcome)`, every later snapshot that contains
  that progress must return `complete_batch(outcome)` with the same
  `CompletionOutcome`. Replaying an already-completed batch uses the checkpointed
  decision, but a mid-run resume re-runs the batch live and re-evaluates the
  predicate as completed branches replay, possibly in a different order.

The package root exports `complete_batch`, `continue_batch`, `CompletionStatus`,
`CompletionDecision`, `CompletionOutcome`, `CompletionItemStatus`,
`BatchItemStatus`, and `BatchCompletionError`.

## Attempt Number in Contexts

`StepContext` and `WaitForConditionCheckContext` now expose an `attempt` field,
the current attempt number starting at 1. Read it inside a step or a
`wait_for_condition` check to branch on the retry count. The SDK injects these
contexts, so normal usage needs no change. `attempt` is a required dataclass
field with no default, so a context you construct directly, in a test, fixture,
or wrapper, must now pass `attempt` or construction raises `TypeError`.

## Instrumentation plugins

Instrumentation plugins remain experimental in this release, and the plugin
interface changed incompatibly. See the
[observability and plugin documentation](https://docs.aws.amazon.com/durable-execution/sdk-reference/observability/logging/)
for details.

## Recommended Validation After Upgrading

1. Build your project against `2.0`, run your test suite, and grep for the
   removed names above.
2. Fail a `step`, an `invoke`, and a `map` or `parallel` branch, and confirm you
   catch `StepError`, `InvokeError`, and `ChildContextError`.
3. Time out a `wait_for_callback` and fail its submitter step, and confirm you
   catch `CallbackTimeoutError` and `CallbackSubmitterError`.
4. Exhaust a `wait_for_condition` and confirm you catch `WaitForConditionError`.
5. Run a workflow that checkpoints a result, an error payload, and
   `wait_for_condition` polling state through your custom `SerDes`, and confirm
   the first-run output equals the replay output.
