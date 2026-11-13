# Python-owned OTel lifecycle checks

The SDK-owned `python-lifecycle` suite defines 48 observability cells: twelve scenario selectors, both
OTel views, and deployed native concurrency 1 and 2. It supplements the 28 base
lifecycle cells. Each cell declares one `otelView` and collects exactly that view
under `telemetry.views`. The original generic LMI profile still collects both
views in a cohort. Python requirements use `python-lmi-*` IDs and are registered by
`lmi_tests.python_catalog.suite_definition()`. Local positive controls do not
establish real-LMI acceptance.

`validate_lifecycle_telemetry` checks all plugin-enabled lifecycle scenarios,
including deadline, timeout retry, pending cleanup, late operation and root
return/failure with work in flight. Call it together with the SDK lifecycle
validator. The matrix fixes the required scenarios, views, configurations and
`behavior_budgets`; the adapter must not select weaker assertions after observing
results. `regression.budgets.exportMs` is the configured exporter-call bound,
alongside `observationGraceMs` and applicable lifecycle budgets.

Use the existing owned-event fields (`requestId`, `ownerRequestId`,
`executionArn`, `atMs`, `sequence`) for these observations:

| Event | Additional observed fields |
| --- | --- |
| `otel_enter` | `providerId`, `exporterId`, `contextBefore: {traceId, spanId}`; both context IDs may be null. |
| `otel_factory` | The actual `pluginInstanceId` and factory `pluginExecutionArn`. Python creates a distinct plugin instance per native request. |
| `otel_export_started` | Actual exporter entry with `traceId` and `spanId`. |
| `otel_export` | Matching exporter completion, with `outcome: success | failure | held`. |
| `otel_exit` | Post-call `contextAfter: {traceId, spanId}` and observed integer `exporterShutdowns`, which must be zero. |
| `export_held` / `export_released` | The same case-owned `gateId`; held export also binds `traceId` and `spanId`, and normal release records `mode: normal`. |

The real boundary order is `otel_enter`, factory creation inside the SDK call,
export completion, actual SDK `wrapper_return`/`wrapper_raise`, then the
`otel_exit` diagnostic. The latter observes restored context after the SDK call;
it is not the SDK completion timestamp. An adapter may record the saved SDK
boundary after emitting a diagnostic, but must retain that original boundary
time. Normalize semantic event order separately from raw emission order and
retain raw emission sequence/timestamps in the referenced evidence. Held export
must prevent actual SDK completion until normal release, independently of when
post-call diagnostics arrive. Emergency release cannot establish this result.

Every `telemetry.views[otelView].spans` record retains its independent export
request, a boolean `sampled` from the actual SpanContext, and the canonical
`span_to_dict` payload. Failed exporter attempts retain their actual canonical
spans and matching failure events; they do not claim backend delivery. Carrier
records use the genuine Python `process-environment` source. The sampled and
unsampled runtime cases additionally retain `callerHeader` from the actual Invoke
request, allowing the runtime Root and sampling decision to be compared with the
caller. Missing, empty, malformed and injected carriers are local negative
controls, never substitutes for genuine runtime coverage.

The validators require one logical Invocation span per sampled request and one
terminal Workflow with its actual success/failure status in terminal fixtures.
Logical identities are counted across replay; identical transport re-exports are
retained without promising exactly-once export. A Workflow may start at the
service execution boundary before native admission and export from a terminal
invocation, retaining its original runtime parent. Its recorded service history
must establish that earlier boundary. Export failure must be followed by a
successful invocation in the same worker/process with the same provider. Held
peers must remain active and make progress after another request ends.

`regression.warmSequence` identifies six distinct executions and their actual
`fixture` values in the order `[success, failure, replay]` twice. Their invocation
records retain `fixture` and the checkpointed `marker`. Replays must satisfy the
same stored-result, stored-failure, callback/wait and retry proof used by the Python
lifecycle validator. Warm process/provider identity is observed rather than
assumed from consecutive requests. These object-allocation and fixture details
belong to this explicit Python profile, not the language-neutral generic suite.
