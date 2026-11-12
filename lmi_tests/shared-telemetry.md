# Shared telemetry observations

The LMI fixture uses the real public invocation/execution OTel factories and
providers. `ObservedPlugin` forwards all seven lifecycle hooks and propagation
metadata, binding the invocation owner captured when that actual plugin was
created. An export snapshots that owner and the actual completed span data when
the configured processor accepts the work. Background export never falls back
to whichever invocation happens to be current later.

Process-owned runtimes are keyed by `(otelView, exportMode)`. Selecting a variant
does not mutate environment variables, a shared plugin configuration or another
invocation's client. Providers remain alive across invocation completion and warm
reuse. Completed owners, export records and flush records are pruned after their
observations are published; the process registry does not retain past traces or
their S3 clients.

## Export modes

`sync` uses the standard `SimpleSpanProcessor`. Its exporter runs on the caller
of `span.end()`, so a held export prevents that call and the SDK wrapper from
returning until the gate is released.

`async-bounded-flush` queues the same exporter work with its captured owner. The
processor's configured wait is `DEFAULT_FLUSH_TIMEOUT_MS = 5000`, capped by the
timeout supplied through the real `TracerProvider.force_flush()` API. The shared
manifest separately declares a 10-second acceptance budget, allowing observation
and scheduling overhead. Neither value is a product SLA. Tests can inject a
shorter processor wait through the private runtime constructor; invocation input
cannot mutate the process-owned variant's timeout.

A bounded wait returning does not settle its exports. The runtime records
`flush_started`, `flush_wait_completed` and actual `flush_completed` independently,
including the actual configured/effective timeout and pending export IDs. The
original invocation continues to own asynchronous work after wrapper return.
Success and failure are observed from actual exporter results, not inferred from
an elapsed timeout.

`Trace.gate()` still returns `None`, preserving existing checkpoint serialization.
Its private `gate_release()` observer reports the first actual held poll and the
normal or emergency release receipt. Only calls that actually waited emit
canonical `export_held`/`export_released` pairs. Later exports after an open gate
remain in the ledger without being falsely labeled as held. Shared case aborts
take precedence over individual gate releases; legacy control order is unchanged.

## Capture, publication and retirement

`Trace.capture_observation()` reserves a per-request `captureSequence` and actual
boundary timestamp using a small lock independent of S3 publication. The SDK
return boundary is captured before resource sampling and post-call diagnostics.
`publish_observation()` retains that capture while the original `sourceSequence`
records publication order. The shared assembler sorts by actual time and capture
sequence; it does not turn delayed log arrival into delayed SDK completion.

Canonical telemetry observations are published on a process-owned worker so
diagnostic S3 latency cannot extend the SDK's bounded flush wait. Publication is
still owned work: failures remain outstanding and cannot produce quiescence.
Legacy uppercase OTel records remain available to the existing SDK-local tests.

`telemetry.active_work(trace)` and `runtime.pending_for_case(case_id)` report actual
pending export/flush IDs, publication count and publication errors. A canonical
`export_quiescence` signal is emitted only after invocation closure, actual work
completion and publication of all earlier observations. The controller must also
observe the actual wrapper and lifecycle retirement; the telemetry signal alone
does not settle a durable execution or authorize a deployment update.

## Driver conversion

`shared_telemetry.collect_telemetry(raw_events, attempts, *, view,
expected_sampled, caller_headers, observed_until_ms, export_mode=None)` converts
actual ledger spans with the pinned shared canonical model. Caller headers and
sampling expectations come from the admitted driver's Invoke configuration;
runtime carriers come from the observed process environment. Source sequence
gaps, missing wrapper/quiescence observations and unknown export owners remain
incomplete. The converter never fills in a missing export or completion.

Only `telemetry.py`, `shared_exporter.py` and the lifecycle observation helpers run
inside Lambda. `shared_telemetry.py`, the shared conformance packages and the
scenario coordinator run on the driver. Local tests exercise public SDK handlers,
the real local durable service, both OTel views and both export modes. They do not
establish real-LMI placement or claim to fix the runtime defects tracked by #741.
