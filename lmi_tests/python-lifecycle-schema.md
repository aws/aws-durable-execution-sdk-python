# Python lifecycle regression observations

This SDK-owned version 1 contract reuses the common evidence envelope and
retirement helpers with local `python-lmi-*` requirement IDs. It is validated by
`lmi_tests.python_catalog`, not registered in the shared CLI or its packaged schema.
It applies when `coverageProfile` is `python-lifecycle` and `runtime` is `python`,
and preserves the 14 SDK-owned base fixture variants on each native-concurrency profile
1 and 2. It is not a portable promise to terminate arbitrary non-cooperative I/O.
The standard LMI profile remains unchanged.

The runner validates the common envelope, original launch ownership, manifest
identity, declared budgets, and final retirement independently of these scenario
assertions. `validate_lifecycle_regressions(evidence, phase=...)` validates each
base cell. `validate_lifecycle_events` also supplies the lifetime checks for OTel
cases, and `validate_replay_evidence(evidence, execution_arn)` validates one replay
execution within a warm sequence. OTel cells additionally use `lmi_tests.python_otel` and the shared canonical
OTel model; passing lifecycle checks alone cannot accept telemetry.

## Common observations

Each retained attempt adds its immutable fixture input `marker`; warm sequences
also record `fixture` (`success`, `failure`, or `replay`). Native-overlap evidence
records both the OS `pid` and the stable `processId`, plus the common environment,
worker, request and execution identities. Source-specific diagnostics belong in
the retained raw evidence rather than durable operation names.

Every event uses the common `requestId`, `ownerRequestId`, `executionArn`, `kind`
and `atMs`, plus a nonnegative, unique per-request `sequence`. The canonical
sequence follows actual semantic boundary times and resolves equal timestamps
using observed causal order. It is **not necessarily the source's emission
index**. The Python fixture records actual SDK completion time and later emits a
retrospective wrapper diagnostic after OTel exit. An adapter must normalize that
record to the actual SDK boundary, retaining the original `sourceSequence`,
`observedAtMs` and source timestamps in raw evidence. Do not invent ordering from
missing observations or use a later resource snapshot to claim an earlier
boundary was quiescent. OTel envelopes use UTC Unix epoch milliseconds.

Each request needs `wrapper_enter`, `user_exit`, and exactly one `wrapper_return`
or `wrapper_raise`, with the actual observed `outcome`. User exit must precede
wrapper exit. The native platform outcome and SDK wrapper outcome remain
independent after a proven `platform_timeout`: a process can later return or
raise (normalized wrapper outcome `FAILED`) after its native request timed out.
Retain that actual wrapper outcome and its error; do not relabel it `TIMED_OUT`.
Non-timeout requests still require matching outcomes. Task/request settlement,
clock, collection, and stale-activity assertions apply to both paths. Wrapper entry and exit carry `resources` snapshots:

```json
{"threads": [{"id": "thread-id", "name": "MainThread"}], "fds": 12, "rssBytes": 1048576}
```

Thread identities must be unique. New `dex-`/`ThreadPoolExecutor` workers or
threads explicitly tagged with that invocation's `ownerRequestId` cannot survive
wrapper exit. This is a Python-profile diagnostic, not a universal thread naming
contract. Finite FD and RSS counters support the warm baseline checks.

Tracked `task_start`/`task_exit` and `io_start`/`io_exit` pairs use stable `taskId`.
`checkpoint_call`/`checkpoint_exit` and `poll_call`/`poll_exit` pairs use `callId`.
Every observed start must settle before wrapper exit. `body`, `effect`,
`checkpoint_call`, `checkpoint_ack`, `checkpoint_exit`, `poll_call`, and `poll_exit`
are forbidden after that boundary, including acknowledgment/settlement of an
earlier request. `body` carries `operation`, `operationId`, `attempt` and `value`;
its operation ID/name must match the collected service history. Names may repeat
under distinct parent contexts; IDs remain authoritative.

An actual timed-out invocation may lack a wrapper exit. It must then have an
observed `platform_timeout` with `source: platform`, plus actual root
`task_exit` (`taskId: root`) or `worker_retired` with `source: platform` under the
explicit `replace-worker` recovery policy. Deadline/recovery cases require an
explicit `reuse-worker` or `replace-worker` policy, bound to the manifest. Absence of a final log is not positive
retirement evidence. Recovery and final owned-resource retirement still apply.
`escape` and `control_error` records cannot establish successful SDK cleanup.

All gates belong to `faultsCreated`. `blocked`, `effect`, `alive`, and `io_exit`
identify the `gateId`; `fault_released` records the same gate, actual time and
`mode: normal`. Emergency or unclassified releases fail the assertion. The
validator also examines final `retirement.faults`, so later release evidence
cannot erase an earlier failure.

A timeout fixture also records the invocation's actual `deadlineAtMs`. An owned
`runtime_deadline` event captures `source: runtime`, `atMs`, and the runtime
context's positive `remainingTimeMs`. The remaining time cannot exceed the
deployed `profile.timeoutMs`; `deadlineAtMs` must equal that observation's time
plus remaining time, allowing at most one millisecond for timestamp rounding.
The platform timeout must cross this actual deadline. Cleanup/recovery/retry
budgets start there. Do not derive it from wrapper entry plus the configured full
timeout: native dispatch and setup may already have consumed invocation time.
An input-injected deadline or missing runtime observation cannot establish this
precondition. The configured timeout remains deployment identity.

## Manifest-declared budgets

`regression.budgets` must exactly match the case's manifest `behavior_budgets`.
The adapter cannot increase these limits after seeing observations. All base
cases require positive `observationGraceMs`, covered by collection after wrapper
exit. Additional fields are:

| Cases | Additional declared budgets |
| --- | --- |
| `early-completion-parallel/map/nested` | `cleanupMs` |
| `deadline-recovery` | `cleanupMs`, `recoveryMs` |
| `timeout-retry` | `cleanupMs`, `retryMs` |
| `nested-progress` | `progressMs` |
| `warm-lifecycle` | Nonnegative `maxThreadGrowth`, `maxFdGrowth`, `maxRssGrowthBytes` |

Budgets are fixture acceptance settings, not product SLAs. Runner provisioning,
placement, execution, collection and retirement budgets remain independent.

## Required scenario evidence

| ID and selector | Observations and assertions |
| --- | --- |
| 1, `native-concurrency` | c1 selects `victim`; c2 selects `victim` and `peer`. All selected requests block in one environment, with distinct execution/request/process/PID identities. `alive` or `effect` positively covers the overlap pivot; missing exits alone do not prove liveness. |
| 4, `deadline-recovery` | Original `victim` has controlled work blocked before its actual platform deadline. No original BODY/EFFECT/checkpoint activity survives deadline plus `cleanupMs`. c2 first establishes a healthy `peer`. A full recovered cohort (`recovery`, plus `recovery-peer` for c2) overlaps in the original environment within `recoveryMs`, before fault release. `reuse-worker` includes the original process/worker; `replace-worker` identifies it through `replacesWorkerId`. Recovery requests succeed. |
| 5, `replay-callback-retry` | At least one real PENDING request precedes a successful resume of the same execution. `success` and `failure` bodies each occur exactly once with the original marker and stable service operation ID. `stored_failure` is observed on at least two requests with normalized `{type: ValueError, message: expected:<marker>}`. `retry` body attempts are exactly integer `[1, 2]` (booleans are invalid), each with the original marker/ID. Actual history contains successful WAIT, CALLBACK, and CONTEXT operations. |
| 6, `warm-lifecycle` | `regression.warmSequence` lists nine distinct executions in `[success, failure, replay] * 3` order, each `{executionArn, fixture}`. Executions run sequentially; fixture routing and terminal outcomes agree. Replay executions satisfy the full replay validator. At least three wrapper samples reuse one environment/process. Compare samples after the second against that post-cache baseline using the declared thread/FD/RSS allowances. |
| 9, `early-completion-parallel/map/nested` | `regression.gateId` selects the held loser. An effect precedes `winner_ready`, with no prior loser exit; `winner_selected` follows. Wrapper return and cessation of activity occur within `cleanupMs`, before any driver release. Actual service history contains `race-parallel`, `race-map`, or nested `outer-map`/`nested-child`/`race-parallel` scopes respectively. |
| 10, `checkpoint-settlement` | A real `checkpoint_call` receives `checkpoint_ack` (`source: service`, `operationIds`) for the actual `checkpoint-loser` operation. `ack_held` carries the matching `callId`, `operationId`, and gate. After positive `blocked`, normal release precedes `checkpoint_exit`, then `branch_join` of the actual `checkpoint-pool` context, then wrapper exit. An unrelated acknowledgment cannot establish this precondition. |
| 11, `timeout-retry` | Actual selected timeout is followed within `retryMs` by selected `retry` on the same execution ARN. `service_retry` is observed from the platform and binds `previousRequestId` to the original request. The original successful step body occurs exactly once across all retained requests. Interrupted I/O effects are not given an exactly-once requirement. |
| 12, `late-operation` | `scope_closed` and `late_attempt` identify the same actual service child-context ID. Both observed closure and `winner_selected` precede the late attempt; their relative order is not prescribed. In particular, Python's public operation-end hook runs before the SDK call returns and the fixture emits `winner_selected`. The losing I/O settles before the late attempt. `late_rejected` binds the same scope with normalized `OrphanedChildException`. No `late_accepted`, late body, late checkpoint dispatch or `late-work` service operation is allowed. The invocation may still be open: child closure is a separate boundary. |
| 13, `return-inflight` / `failure-inflight` | Root `user_exit` occurs after winner selection while losing I/O is still held. Normal release/IO settlement precedes the actual wrapper exit. c2 positively observes peer progress during this interval. Preserve success or the original normalized `ValueError`/marker failure after settlement. |
| 14, `pending-cleanup` | Root `cleanup_enter` precedes the held gate; normal release and IO exit precede `cleanup_exit`, `user_exit`, then PENDING wrapper return. A later request resumes the same execution, completes a real WAIT, and does not repeat the completed success body. c2 peer progress is observed while cleanup is held. |
| 15, `nested-progress` | All native workers establish live overlap, then `progress_begin`/`progress` bound the nested operation budget. Each request returns its own `[[marker:0, marker:1], marker]` result. Actual `outer-parallel`, `inner-map`, and `progress-child` operations succeed. `operation_config` records the public fixture's integer `maxConcurrency: 1` for the two pool operations, bound by operation ID and `source: public-fixture`; this is not invented service-history metadata. |

The six OTel-only selectors (`runtime-carrier-sampled`,
`runtime-carrier-unsampled`, `otel-warm-lifecycle`, `otel-concurrent`,
`export-error-recovery`, `held-export`) retain the common settled-lifecycle checks
and delegate their remaining assertions to the OTel extension. The SDK adapter
must implement these fixtures through real public APIs and preserve raw source
evidence. Synthetic positive controls validate the harness; they do not count as
real LMI SDK acceptance or authorize replacing Python-specific regression tests.

For in-flight return/failure and root-cleanup peer measurements,
`regression.gateId` selects the victim's actual held-work gate. A separate
winner-admission handshake may produce additional `blocked` events, but cannot
supply that gate's liveness or prevent its early exit from being detected. Peer
and other native cohort roles still require one unambiguous blocked gate.

A deadline fixture that normally releases its held work after measurement can
retain `regression.deadlineMeasurement`: `selectedRequests`, `deadlineAtMs`,
`cleanupCutoffAtMs`, `observationCutoffAtMs`, `observedAtMs`, and
`sourceEvidenceUri`. The selected requests and cutoffs must match the actual
runtime deadline and immutable case budgets. Its source URI must be retained in
`collection.evidenceUris`, and capture must cover the full cleanup-plus-grace
window before the fault's `releaseRequestedAtMs`.

Only a valid snapshot permits the stale-activity window to end when the
controller begins normal release. Activity between capture and release is still
checked. New user bodies and SDK dispatch after the cutoff remain failures even
after release; only paired completion of already dispatched I/O is cleanup.
All events remain in the main ledger, including later normal cleanup, and task,
request, wrapper and retirement checks still inspect that complete ledger.
Missing snapshots retain the legacy full-window check; inconsistent snapshots
fail rather than shorten the observation window. This separates deliberately
released cleanup from the measured deadline behavior without removing actual
pre-release late effects.
