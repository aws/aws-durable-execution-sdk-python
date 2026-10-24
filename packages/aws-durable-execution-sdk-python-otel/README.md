# AWS Durable Execution SDK - OpenTelemetry Plugin

OpenTelemetry instrumentation plugin for the [AWS Durable Execution SDK for Python](https://github.com/aws/aws-durable-execution-sdk-python). Emits durable execution spans on one execution trace, with deterministic workflow, synthetic-root, and operation span IDs.

## Features

- **Shared Execution Trace**: Workflow and Invocation spans share one trace, anchored to a propagated backend parent when available or a deterministic synthetic execution root otherwise
- **Same-Trace Ambient Parenting**: Invocation spans use the active ambient span only when it already belongs to the execution trace
- **Span-per-Operation**: Each durable operation (step, wait, invoke) gets its own span with accurate timing
- **Continuation Spans**: Operations completing in another invocation produce a new correlated span without fabricating an unobserved prior span context
- **Log Correlation**: Enrich application logs with trace ID and span ID for end-to-end observability
- **Provider Integration**: Use the global ADOT provider or supply an explicit SDK `TracerProvider`
- **Execution Sampling**: Resolve sampling once per invocation and apply it consistently to Workflow, Invocation, operation, and attempt spans

## Installation

For an existing Core 2.x / OTel 1.x application, start with the
[Core 3.0 / OTel 2.0 migration guide](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/docs/migration-2.x-to-3.0.md#migrate-opentelemetry-registrations).

When using an ADOT or community OpenTelemetry Lambda layer:

```bash
pip install aws-durable-execution-sdk-python-otel
```

The base package intentionally does not install OpenTelemetry libraries. The
Lambda layer supplies a version-aligned API, SDK, exporter, and propagators,
preventing packages in the function artifact from shadowing parts of the layer.

The generated `aws-durable-execution-sdk-python-otel-plugin` Lambda layer includes
Core's runtime dependencies, including its required boto3/botocore service model.
It still leaves OpenTelemetry provider packages to the ADOT/community layer.
Dependencies bundled in the function artifact take precedence over layer packages;
those application-owned boto3/botocore versions must also meet Core's minimum.

For an application that configures its own OpenTelemetry provider instead of
using a Lambda layer:

```bash
pip install "aws-durable-execution-sdk-python-otel[standalone]"
```

The `standalone` extra installs the OpenTelemetry API, SDK, and AWS X-Ray
propagator. The application remains responsible for configuring its provider,
processors, and exporter.

## Quick Start using X-Ray/CloudWatch Tracing

1. Add the [ADOT Lambda Layer](#1-adot-lambda-layer) to your function and set `AWS_LAMBDA_EXEC_WRAPPER=/opt/otel-instrument`
2. Enable [X-Ray Active Tracing](#2-aws-x-ray-active-tracing) on the function
3. Pass `InvocationOtelPluginFactory()` to your handler's `plugins` list
4. Add X-Ray write permissions

The SDK's `plugins` list takes plugin *factories*, not plugin instances: it calls
each factory's `create_plugin` once per invocation and the plugin it returns
serves that one invocation. `InvocationOtelPluginFactory` and
`ExecutionOtelPluginFactory` are the factories for the two bundled plugins; each
takes the optional `OtelPluginConfig` that every plugin it builds will use.

Alternatively, install this package in the function artifact or a Lambda layer
and select either OTel plugin by entry-point name:

```text
DURABLE_EXECUTION_PLUGINS=otel-invocation
DURABLE_EXECUTION_PLUGINS=otel-execution
```

`otel-invocation` names a default-configured `InvocationOtelPluginFactory`;
`otel-execution` names a default-configured `ExecutionOtelPluginFactory`. The SDK
discovers the selected package entry point at cold start and calls its
`create_plugin` once per invocation, so the handler does not need to import or
explicitly register the plugin.

OTel 2.0 requires core SDK 3.x and the per-invocation factory contract. Publish
the coordinated core release before the plugin. Core 2.x plugin instances and
OTel 1.x entry points are rejected by core 3 at handler initialization.

Configure one OTel view: `InvocationOtelPluginFactory` shows work within each
Lambda invocation; `ExecutionOtelPluginFactory` shows logical operations across
the execution. Their factories declare the same exclusive group, so registering
both raises `PluginLoadError` at cold start, before any plugin is constructed.
Keep one view across `plugins=[...]` and `DURABLE_EXECUTION_PLUGINS`. Unrelated
plugin factories may accompany it. Repeating the same factory object through an
entry point is deduplicated by identity.

Factory classes can opt into registration metadata with
`__durable_registration_api__ = 1`. Each opted-in class in the MRO contributes its
`exclusive_group`; inherited groups remain in force when a subclass adds another
group. The closest explicit unsupported marker disables the contract. Unmarked
legacy fields and callbacks are ignored. An optional `on_registration_result`
callback receives acceptance or rejection without constructing invocation plugins.

### 1. ADOT Lambda Layer

This plugin requires the [AWS Distro for OpenTelemetry (ADOT) Lambda layer](https://aws-otel.github.io/docs/getting-started/lambda) to export traces from your Lambda function.

The layer ARN follows the format:

```
arn:aws:lambda:<region>:<awsAccountId>:layer:aws-otel-python-<arch>-ver-<version>
```

Refer to the [ADOT Lambda Layer ARNs](https://aws-otel.github.io/docs/getting-started/lambda/lambda-python) page for the latest version number, architecture, and supported regions.

**AWS CLI:**

```bash
aws lambda update-function-configuration \
  --function-name your-function-name \
  --layers "arn:aws:lambda:<region>:<awsAccountId>:layer:aws-otel-python-amd64-ver-<version>"
```

You must also set the `AWS_LAMBDA_EXEC_WRAPPER` environment variable:

```bash
aws lambda update-function-configuration \
  --function-name your-function-name \
  --environment "Variables={AWS_LAMBDA_EXEC_WRAPPER=/opt/otel-instrument}"
```

> **Note:** Replace `<region>` with your function's region and `<version>`/`<arch>` with the latest layer version and architecture from the ADOT docs.

**CloudFormation / SAM:**

```yaml
MyFunction:
  Type: AWS::Serverless::Function
  Properties:
    Layers:
      - !Sub arn:aws:lambda:${AWS::Region}:<awsAccountId>:layer:aws-otel-python-amd64-ver-<version>
    Environment:
      Variables:
        AWS_LAMBDA_EXEC_WRAPPER: /opt/otel-instrument
```

**CDK:**

```python
from aws_cdk import aws_lambda as lambda_

adot_layer = lambda_.LayerVersion.from_layer_version_arn(
    self,
    "AdotLayer",
    f"arn:aws:lambda:<region>:<awsAccountId>:layer:aws-otel-python-amd64-ver-<version>",
)

fn = lambda_.Function(
    self,
    "MyFunction",
    runtime=lambda_.Runtime.PYTHON_3_12,
    handler="index.handler",
    code=lambda_.Code.from_asset("lambda"),
    layers=[adot_layer],
    environment={"AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument"},
)
```

> **Tip:** Pin the layer version to a specific number in production deployments to avoid unexpected behavior from automatic version changes.

### 2. AWS X-Ray Active Tracing

Enable active tracing on your Lambda function so the `_X_AMZN_TRACE_ID` environment variable is populated at invocation time. The plugin uses this header to anchor the execution trace on the propagated X-Ray `Root`/`Parent` when both are valid, and preserves `Sampled=1` or `Sampled=0` as the backend sampling decision.

**AWS Console:** Lambda → Configuration → Monitoring and operations tools → Active tracing → Enable

**AWS CLI:**

```bash
aws lambda update-function-configuration \
  --function-name your-function-name \
  --tracing-config Mode=Active
```

**CloudFormation / SAM:**

```yaml
MyFunction:
  Type: AWS::Lambda::Function
  Properties:
    TracingConfig:
      Mode: Active
```

**CDK:**

```python
lambda_.Function(
    self,
    "MyFunction",
    tracing=lambda_.Tracing.ACTIVE,
)
```

### Handler context propagation

The core runs the existing Start hooks, handler, output preparation, resource
cleanup and End hooks on one invocation worker. Start finishes before checkpoint
processing begins. End follows registered-branch joins and checkpoint shutdown,
and output serialization or checkpoint errors retain their normal classification.
There is no separate handler-context plugin API.

During Start, Invocation view preserves a valid active span on the canonical
execution trace; when it is absent or unrelated, it attaches the Invocation span.
Execution view attaches the Workflow span, including when an incoming span is on
the same trace. The Invocation span's own ambient parenting is separate from the
active context supplied to handler instrumentation. Both views preserve baggage.

Start hooks run in registration order. A later successful plugin that deliberately
sets or clears the active span wins; OTel does not apply a second correction pass.
Place OTel after a span-replacing plugin when OTel's view-specific context is
desired. Baggage-only plugins that extend the current context can appear on either
side. The SDK also copies the coordinator's current bindings for each `map` or
`parallel` branch admission/resume when plugins are registered. Baggage and
other successful bindings reach these SDK-managed callbacks, while a branch's
changes cannot leak into siblings, the coordinator or a reused worker.
End hooks also retain registration order and reset tokens in their owning
Context; they do not promise a reverse-stack observation of other plugins' spans.
The factory list is handler-scoped; each invocation owns fresh plugin instances.
Checkpoint formats are unchanged.

Use the coordinated core 3.x / OTel 2.x pair for this factory API. CI tests both
packages as installed wheels and verifies that legacy OTel 1.x registration is
rejected clearly by core 3.

### 3. In your Lambda handler (index.py)

```python
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_otel import InvocationOtelPluginFactory


@durable_execution(plugins=[InvocationOtelPluginFactory()])
def handler(event: dict, context: DurableContext) -> dict:
    result = context.step(lambda _: fetch_data(event["id"]), name="fetch-data")

    context.wait(duration=Duration.from_seconds(5))

    context.step(lambda _: process(result), name="process")

    return result
```

The ADOT layer supplies the global `TracerProvider`; the plugin handles
deterministic ID generation and span lifecycle.

### 4. Grant Permissions

The function's execution role needs the `AWSXRayDaemonWriteAccess` managed policy (or equivalent permissions) if using X-Ray as the tracing backend.

### Environment Variables for ADOT layer

| Variable                      | Description                                                                                   | Default           |
| ----------------------------- | --------------------------------------------------------------------------------------------- | ----------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint for the OTLP exporter (e.g., `http://localhost:4318` for the ADOT collector sidecar) | Set by ADOT layer |
| `AWS_LAMBDA_EXEC_WRAPPER`     | Set to `/opt/otel-instrument` for the ADOT layer to instrument your function                  | —                 |
| `OTEL_TRACES_SAMPLER`         | Sampler to use (e.g., `traceidratio` for ratio-based sampling)                                | `always_on`       |
| `OTEL_TRACES_SAMPLER_ARG`     | Argument for the sampler (e.g., `0.3` to sample 30% of traces)                                | —                 |

See the [ADOT sampling configuration](https://aws-otel.github.io/docs/getting-started/lambda#sampling-configuration) for more details. When the backend header contains an explicit `Sampled` value, that backend decision takes precedence over local sampler configuration for durable spans.

## Configuration

### Plugin Options

```python
from aws_durable_execution_sdk_python_otel import (
    InvocationOtelPluginFactory,
    OtelPluginConfig,
    xray_context_extractor,
)

plugin_factory = InvocationOtelPluginFactory(
    OtelPluginConfig(
        # Use a custom context extractor (default: xray_context_extractor).
        context_extractor=xray_context_extractor,
        # Custom instrumentation scope name
        # (default: "aws-durable-execution-sdk-python").
        instrument_name="my-service",
        # Install a root-logger filter that stamps trace context onto every
        # log record (default: True).
        enrich_logger=True,
    )
)
```

The config is resolved once and shared by every plugin the factory builds, so
configuration is per handler while plugin state is per invocation.

### Context Extractors

Context extractors return an `ExtractedContext` object, or `None` when no
durable execution trace context is available. The object carries:

- `trace_id`: 128-bit OpenTelemetry trace ID
- `parent_span_id`: 64-bit OpenTelemetry parent span ID
- `sampling`: `Sampling.SAMPLED`, `Sampling.NOT_SAMPLED`, or `Sampling.UNDECIDED`

The plugin supports multiple strategies for extracting durable execution trace
context:

```python
from aws_durable_execution_sdk_python_otel import (
    InvocationOtelPluginFactory,
    OtelPluginConfig,
    w3c_client_context_extractor,
    xray_context_extractor,
)

# Default: X-Ray trace header (recommended for most Lambda deployments).
InvocationOtelPluginFactory(OtelPluginConfig(context_extractor=xray_context_extractor))

# W3C Trace Context via clientContext (placeholder for backend propagation support).
InvocationOtelPluginFactory(
    OtelPluginConfig(context_extractor=w3c_client_context_extractor)
)
```

Custom extractors should return `ExtractedContext`, not an OpenTelemetry
`Context`.

### Trace Structure

Both bundled plugins use the same execution ancestor:

- a propagated backend parent when `_X_AMZN_TRACE_ID` contains a valid `Root`
  and `Parent`
- otherwise a deterministic `DurableExecutionRoot` span derived from the
  durable execution ARN, with `durable.execution.synthetic_root=true`

`InvocationOtelPlugin` keeps durable operation spans under the Invocation span
and links operations to Workflow:

```text
Execution ancestor
├── Workflow
└── Invocation
    └── operation
        └── operation attempt 1
```

`ExecutionOtelPlugin` keeps operation spans under Workflow and links operations
to the current Invocation span:

```text
Execution ancestor
├── Workflow
│   └── operation
│       └── operation attempt 1
└── Invocation
```

If an ambient Lambda span is active and already has the execution trace ID, the
Invocation span uses that ambient span as its parent. Ambient spans on a
different trace are ignored for durable parenting so Invocation remains on the
execution trace.

### Synthetic fallback anchor

Each sampled fallback invocation creates and ends `DurableExecutionRoot` at
invocation start and attempts to flush it through the normal invocation-end
lifecycle, including when it returns `PENDING` or `RETRY`. A successfully
delivered copy supplies ancestry if the execution is later stopped or times out
without resuming. Export and flush failures, or termination before flushing,
do not provide a delivery guarantee.

The anchor uses the existing execution-scoped span ID and the stable execution
start timestamp for both its start and end. It carries no terminal outcome;
`Workflow` remains the span for execution duration and success/failure. Retries
of the first invocation and later invocations may re-export the same anchor to
recover lost telemetry; export is not exactly once. Two executions sharing a
propagated trace ID retain distinct ARN-scoped anchor IDs.
The anchor is intentionally zero-duration, so its descendants end after it;
trace viewers may flag that timing relationship even though the ancestry is valid.

The anchor uses the same configured tracer, provider resource and processors as
other durable spans, but carries only the resolved sampling decision. Its SDK
attributes are `durable.execution.arn` and `durable.execution.synthetic_root`;
sampler-added attributes and tracestate are omitted from this repeated identity.
Ordinary durable spans retain the complete resolved sampler metadata. SDK-added
IDs, timestamps and anchor attributes stay stable across recovery exports;
provider resources and custom processor enrichment may vary. The SDK does not
replace configured service identity or freeze detector fields.

Complete remote parents are externally owned and are never materialized by the
SDK. Unsampled invocations emit no anchor. Sampling remains resolved per
invocation: a later sampled fallback invocation also emits the anchor even if
an earlier invocation was unsampled. Missing execution start time continues to
disable telemetry; no wall-clock timestamp is invented for the anchor.

### Sampling

Sampling is resolved once per invocation and carried to every durable span in
that invocation. Each span consumes its private sampling intent before processors
run. Processor-created spans use their own configured sampling policy and normal
parent-context semantics, including when a processor forwards the supplied parent
context. Precedence is:

1. `Sampled=1` or `Sampled=0` from `_X_AMZN_TRACE_ID`
2. a same-trace ambient span's recording/sampled state
3. the configured OpenTelemetry sampler

The resolved decision is applied to Workflow, Invocation, operation, and attempt
spans. This avoids independently querying stateful or ratio-based samplers for
each durable span in the same invocation.

### Status attributes

`durable.invocation.status` uses `RETRYING` when the core plugin hook reports
`InvocationStatus.RETRY`. The core enum remains unchanged. Operation spans use
OTel `OK` only for `SUCCEEDED`, `ERROR` when error details are delivered, and
`UNSET` for other outcomes without error details, including `FAILED`,
`CANCELLED`, `TIMED_OUT`, and `STOPPED`. The original durable operation status
remains in `durable.operation.status`.

### Invocation context isolation

With core 3.x, invocation hooks and the handler run on one worker in an
invocation-local Context initialized from the host's bindings. Start bindings
remain visible to later hooks, the handler and resource cleanup even when Start
raises. The SDK retains the plugin and contains ordinary hook exceptions without
rolling back context changes. Plugins own cleanup of partial bindings; End runs
in the same Context as Start and can reset its tokens. The host's bindings remain
unchanged after return, including after Start or End failures. This isolates the
invocation's context-variable bindings from its caller; it does not undo mutations
to shared objects or external side effects. The isolation applies when plugins
are registered.

### Log Correlation

When `enrich_logger=True` (the default), the plugin installs a logging filter on
the root logger at invocation start. The filter stamps the trace context that is
active for the emitting invocation onto log records, using these attributes:

- `traceId`: 32-char hex trace identifier
- `spanId`: 16-char hex span identifier
- `otelTraceSampled`: boolean indicating if the trace is sampled

Which span a record carries follows the emitting thread: the operation attempt
span inside a step, the context span inside a child context, and the Invocation
span for top-level handler code. A span you start yourself on the execution
trace is used for records emitted inside it. A span the runtime already had
active when the invocation started — the Lambda invocation span the ADOT layer
creates under X-Ray active tracing — is the parent of the Invocation span rather
than a substitute for it, so a top-level record still names the Invocation span.
Correlation holds from the first statement of the handler, before any durable
operation, and holds when several invocations run concurrently in one
environment (Lambda Managed Instances): the plugin claims the invocation thread
at invocation start and the SDK runs the handler body in a copy of that thread's
context, so each record resolves to the invocation that emitted it rather than
to whichever invocation started last. A branch of a `map` or `parallel` runs on
a pool thread the invocation's context was not copied into, and the plugin
claims that thread from the hooks that run on it before your branch body does.

Two cases are left unstamped, so any log formatter or schema must treat the
fields as optional:

- No invocation is open — for example during environment initialization or
  teardown.
- The record is emitted on a thread that carries no invocation claim. A thread
  your code starts itself is such a thread, since Python does not copy context
  into a new thread, as is the SDK's background checkpointing thread. The number
  of invocations open in the environment does not change this: attributing an
  unclaimed record to the single open invocation would be wrong whenever the
  emitting thread belongs to a different invocation, and an uncorrelated record
  is preferred over one carrying another execution's trace.

## Chained-invoke propagation

Both views supply the synchronous `provide_propagation_metadata` hook with
SDK-owned types. It encodes canonical X-Ray `Root`, the calling operation's
`Parent`, and resolved `Sampled=1` or `Sampled=0`, without creating an extra span.
An existing operation's actual span ID is used; before span creation, its stable
ID is derived from the execution ARN and operation ID. Unrelated ambient spans
cannot replace execution ownership. An operation that has already started uses
its actual span's sampled flag. A deferred operation contributes metadata only
while the instrumentation tracer still uses the installed `DurableSampler`:
replacing that sampler after invocation Start may give the operation a different
sampling decision, so the producer abstains instead of guessing or querying the
sampler early. Inactive/mismatched executions contribute nothing. Tracer,
provider and resource ownership remain unchanged.

The core now calls the collector only for a new invoke START and persists the
contribution in flat `ChainedInvokeOptions.XAmznTraceId`. Pending and terminal
replay do not recollect metadata; an uncommitted START can be retried. Separate
invokes carry separate operation parents. Public invoke tests cover both views,
sampled/unsampled context, parallel branches, preserved tenant/payload and replay.

For [#751](https://github.com/aws/aws-durable-execution-sdk-python/issues/751),
deployed backend delivery and topology still require validation. The core requires
boto3 1.43.114 or newer, which depends on a botocore model containing the optional
field. The normal request tests use the published serializer and parameter
validation. Python has no distributed-map
model/START path; existing map/parallel APIs are CONTEXT operations.

OTel 2 requires core 3.x and per-invocation plugin factories. Chained-invoke
propagation uses the coordinated core release and its boto3 minimum. The
published model alone does not establish deployed downstream topology.

## Verification

After deploying your function with the plugin configured:

1. **Invoke your durable function** — trigger at least one execution that includes multiple steps or a wait/resume cycle.

2. **Check the CloudWatch console** — Navigate to CloudWatch → Traces in the AWS Console. You should see an execution trace with:
   - A "Workflow" span exported on the terminal invocation
   - An "Invocation" span per invocation
   - Child spans for each durable operation (named after your step names)
   - All invocations of the same execution grouped under one trace ID

3. **Check log correlation** — verify that your logs include `traceId` and `spanId` fields matching the spans in X-Ray.

4. **Confirm sampling** — If you set `OTEL_TRACES_SAMPLER=traceidratio` and `OTEL_TRACES_SAMPLER_ARG` to a value less than 1.0, verify that only the expected proportion of traces appear.

5. **Span links** — For operations that span multiple invocations (e.g., after a wait resumes), though span links are set, they are not visualized within the CloudWatch console.

### Troubleshooting

| Symptom                           | Likely Cause                                                    |
| --------------------------------- | --------------------------------------------------------------- |
| No traces appear                  | ADOT layer not configured, or `AWS_LAMBDA_EXEC_WRAPPER` not set |
| Traces appear but are fragmented  | Backend trace context is not propagated to every invocation     |
| Missing spans for some operations | `OTEL_TRACES_SAMPLER_ARG` set below 1.0                         |
| `_X_AMZN_TRACE_ID` not populated  | X-Ray active tracing not enabled                                |

## API Reference

### `InvocationOtelPluginFactory`

Factory for the invocation-rooted plugin, and what belongs in the SDK's `plugins`
list. Satisfies `DurableInstrumentationPluginFactory` from
`aws_durable_execution_sdk_python`: its `create_plugin(info)` takes an
`InvocationStartInfo` and returns the `InvocationOtelPlugin` for that invocation.

```python
InvocationOtelPluginFactory(
    OtelPluginConfig(
        tracer_provider=None,
        context_extractor=None,
        instrument_name="aws-durable-execution-sdk-python",
        enrich_logger=True,
        workflow_span_name="Workflow",
    )
)
```

Pass `tracer_provider=...` when the application owns the OpenTelemetry SDK
provider. When omitted, the globally configured provider is used, resolved per
invocation so a provider installed after the handler module is imported is still
picked up.

`INVOCATION_OTEL_PLUGIN_FACTORY` is the default-configured instance the
`otel-invocation` entry point names.

### `ExecutionOtelPluginFactory`

Factory for the execution-rooted plugin, with the same construction and config as
`InvocationOtelPluginFactory`. `EXECUTION_OTEL_PLUGIN_FACTORY` is the
default-configured instance the `otel-execution` entry point names.

### `InvocationOtelPlugin`

Invocation-rooted view. Implements `DurableInstrumentationPlugin` from
`aws_durable_execution_sdk_python`. One instance serves exactly one invocation:
the SDK builds it from the factory before the first hook fires and drops it when
the invocation ends, so it holds its span registry and context tokens in ordinary
instance state. Construct it directly only when driving the hooks yourself; in a
handler, register the factory instead.

### `ExecutionOtelPlugin`

Execution-rooted view. Uses the same execution ancestor and sampling behavior as
`InvocationOtelPlugin`, but parents operation spans under Workflow and links
them to Invocation. Also one instance per invocation; the identities that must
agree across invocations (the trace ID, the Workflow span ID, and each
operation's span ID) are derived deterministically from the execution ARN rather
than carried in memory.

### `DeterministicIdGenerator`

A custom OpenTelemetry `IdGenerator` that produces reproducible trace and span IDs from execution metadata. Exported for advanced use cases.

`use_ids` retains its existing scope-wide trace override by default. The
synthetic-root implementation uses a separate internal one-span override,
preventing processor-created roots from inheriting that internal ID. Existing
`use_ids` signatures and default behavior remain unchanged.

### `xray_context_extractor`

Default context extractor. Reads the `_X_AMZN_TRACE_ID` environment variable and
returns `ExtractedContext` containing parsed `Root`, `Parent`, and `Sampled`
fields when present.

### `w3c_client_context_extractor`

Alternative context extractor placeholder. Returns `None` until backend W3C
`traceparent` propagation is supported.

### `ContextExtractor`

Type alias for custom context extractor functions:
`Callable[[InvocationStartInfo], ExtractedContext | None]`.

### `ExtractedContext` / `Sampling`

Structured trace context and sampling decision returned by context extractors.

### `OtelContextLogFilter` / `install_log_filter`

The logging filter (and its installer) used to stamp trace context onto log
records. Installed automatically when `enrich_logger=True`; exported for manual
setups, where `install_log_filter(target_logger)` attaches it to a logger of your
choice. The filter carries no invocation identity of its own: it resolves the
invocation a record belongs to at emit time, so one filter serves every
invocation the environment runs, including concurrent ones.

## Requirements

- Python >= 3.11
- `aws-durable-execution-sdk-python` >= 3.0.0
- An ADOT/community OpenTelemetry Lambda layer, or the `standalone` extra

## License

Apache-2.0
