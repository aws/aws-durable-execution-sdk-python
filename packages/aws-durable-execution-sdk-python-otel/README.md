# AWS Durable Execution SDK - OpenTelemetry Plugin

> **⚠️ Experimental Beta:** This plugin is currently in experimental beta. Functionality may change without notice between releases. It is not recommended for production workloads at this time.

OpenTelemetry instrumentation plugin for the AWS Durable Execution SDK for Python. Emits distributed traces that correlate across multiple Lambda invocations of a single durable execution, producing deterministic span and trace IDs so that spans from different invocations are stitched into a single coherent trace.

This package provides two plugin implementations:

| Plugin                 | Trace Structure                                                                         |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `ExecutionOtelPlugin`  | Workflow span as synthetic root; operations parent under it and link to the invocation span |
| `InvocationOtelPlugin` | Invocation span as the trace root; operations parent under the invocation span          |

The two plugins differ in how they obtain a `TracerProvider`:

- **`ExecutionOtelPlugin`** accepts an `OtelPluginConfig` and supports three provider modes:
  1. **Auto-created** (default) — creates its own `TracerProvider` with OTLP export to `localhost:4318`.
  2. **Custom** — you pass your own `tracer_provider`.
  3. **Global default** — set `use_default_tracer_provider=True` to use the globally registered provider (e.g. from the ADOT layer).
- **`InvocationOtelPlugin`** takes plain constructor arguments (`trace_provider`, `context_extractor`, `instrument_name`, `enrich_logger`). It uses the `trace_provider` you pass, or the globally registered provider (`opentelemetry.trace.get_tracer_provider()`) when omitted — it does **not** auto-create an OTLP provider. For a community-collector deployment you construct an OTLP-exporting `TracerProvider` yourself and pass it in; for an ADOT deployment you omit it and the layer's global provider is used.

Both plugins can be deployed with either the **ADOT Lambda layer** or the **OpenTelemetry community collector-only layer**.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Choosing a Plugin](#choosing-a-plugin)
- [Lambda Layer Options](#lambda-layer-options)
- [Deployment Matrix](#deployment-matrix)
- [Configuration](#configuration)
- [Export Strategies](#export-strategies)
- [Collector Configuration](#collector-configuration)
- [IAM Permissions](#iam-permissions)
- [Environment Variables](#environment-variables)
- [SAM/CloudFormation Templates](#samcloudformation-templates)
- [Trace Structure Comparison](#trace-structure-comparison)
- [Log Correlation](#log-correlation)
- [Additional Python Dependencies](#additional-python-dependencies)
- [API Reference](#api-reference)
- [Verification](#verification)
- [License](#license)

## Installation

```bash
pip install aws-durable-execution-sdk-python-otel
```

When the plugin auto-creates its `TracerProvider` (default mode) and you want AWS SDK / HTTP auto-instrumentation, install the optional `instrumentation` extra:

```bash
pip install "aws-durable-execution-sdk-python-otel[instrumentation]"
```

---

## Quick Start

Both plugins are used the same way — only the import and class name differ:

```python
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_otel import ExecutionOtelPlugin
# OR: from aws_durable_execution_sdk_python_otel import InvocationOtelPlugin


@durable_execution(plugins=[ExecutionOtelPlugin()])
# OR: @durable_execution(plugins=[InvocationOtelPlugin()])
def handler(event: dict, context: DurableContext) -> dict:
    result = context.step(lambda _: fetch_data(event["id"]), name="fetch-data")

    context.wait(duration=Duration.from_seconds(5), name="cooldown")

    processed = context.step(lambda _: process(result), name="process")

    return processed
```

With no configuration, `ExecutionOtelPlugin` auto-creates a `TracerProvider` with:

- OTLP export to `http://localhost:4318/v1/traces`
- AWS SDK (botocore) and HTTP (urllib3) instrumentations (when the `instrumentation` extra is installed)
- AWS X-Ray + W3C TraceContext propagators
- Deterministic trace and span ID generation

`InvocationOtelPlugin()` with no arguments uses the globally registered `TracerProvider` (e.g. the one the ADOT layer configures). Outside an ADOT deployment, pass your own `trace_provider` so spans are exported.

---

## Choosing a Plugin

| Aspect                   | `ExecutionOtelPlugin`                        | `InvocationOtelPlugin`                                    |
| ------------------------ | -------------------------------------------- | --------------------------------------------------------- |
| Trace root               | Workflow span (synthetic, deterministic)     | Invocation span                                           |
| Operation parent         | Workflow span                                | Invocation span                                           |
| Invocation span role     | Child of Workflow span; operations link to it | Trace root / parent of operations                        |
| Export timing            | Operations deferred until complete           | All spans exported immediately                            |
| Non-terminal invocations | Workflow span discarded (clean traces)       | Invocation span emitted per invocation                    |
| Trace continuity         | Single trace across all invocations          | Per-invocation traces, correlated via span links          |

**Use `ExecutionOtelPlugin` when** you want a single unified trace view across all invocations of a durable execution, with the workflow as the logical root.

**Use `InvocationOtelPlugin` when** you want a lighter-weight, per-invocation view, or want to delegate to the ADOT layer's ambient invocation span (deploy on the ADOT layer and omit `trace_provider`).

---

## Lambda Layer Options

Both plugins can use either Lambda layer. The layer provides span transport (a collector that listens on `localhost:4318` and forwards to X-Ray/CloudWatch).

| Layer                              | What It Provides                                       | ARN Format                                                                      |
| ---------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------- |
| **ADOT Lambda Layer**              | OTel SDK auto-instrumentation + collector extension    | `arn:aws:lambda:<region>:<account>:layer:aws-otel-python-<arch>-ver-<version>`  |
| **Community Collector-Only Layer** | Collector extension only (no SDK auto-instrumentation) | `arn:aws:lambda:<region>:<account>:layer:opentelemetry-collector-<arch>-<version>` |

Consult the [ADOT Lambda (Python) docs](https://aws-otel.github.io/docs/getting-started/lambda/lambda-python) for the current layer ARN, architecture, and supported regions. Pin the layer version in production.

**ADOT Layer:** Registers a global `TracerProvider` with auto-instrumentation. Use `use_default_tracer_provider=True` so the plugin delegates to that provider. Set `AWS_LAMBDA_EXEC_WRAPPER=/opt/otel-instrument` to activate it.

**Community Collector Layer:** Only runs a collector process at `localhost:4318`. The plugin creates its own `TracerProvider` (default mode) and exports spans to the collector. Requires a `collector.yaml` in your function bundle and `OPENTELEMETRY_COLLECTOR_CONFIG_URI=/var/task/collector.yaml`.

> **Tip:** The community collector layer is smaller and purpose-built for span transport. The ADOT layer is convenient if you want zero-config auto-instrumentation from the layer itself.

---

## Deployment Matrix

| #   | Plugin                 | Layer                     | Provider selection                          | `AWS_LAMBDA_EXEC_WRAPPER` | `collector.yaml` needed? |
| --- | ---------------------- | ------------------------- | ------------------------------------------- | ------------------------- | ------------------------ |
| 1   | `ExecutionOtelPlugin`  | ADOT Layer                | `use_default_tracer_provider=True`          | `/opt/otel-instrument`    | No                       |
| 2   | `ExecutionOtelPlugin`  | Community Collector Layer | auto-created (default)                      | Do NOT set                | Yes                      |
| 3   | `InvocationOtelPlugin` | ADOT Layer                | omit `trace_provider` (uses global)         | `/opt/otel-instrument`    | No                       |
| 4   | `InvocationOtelPlugin` | Community Collector Layer | pass your own OTLP `trace_provider`         | Do NOT set                | Yes                      |

### 1. ExecutionOtelPlugin + ADOT Layer

The ADOT layer provides both the collector and a global `TracerProvider`. The plugin uses the global provider and produces a Workflow span as the trace root.

**Handler code:**

```python
from aws_durable_execution_sdk_python_otel import ExecutionOtelPlugin

plugin = ExecutionOtelPlugin(OtelPluginConfig(use_default_tracer_provider=True))
```

**SAM template:**

```yaml
MyFunction:
  Type: AWS::Serverless::Function
  Properties:
    Runtime: python3.12
    Handler: index.handler
    CodeUri: ./src
    Layers:
      - !Sub arn:aws:lambda:${AWS::Region}:<account>:layer:aws-otel-python-amd64-ver-<version>
    Environment:
      Variables:
        AWS_LAMBDA_EXEC_WRAPPER: /opt/otel-instrument
    Tracing: Active
    DurableConfig:
      ExecutionTimeout: 3600
      RetentionPeriodInDays: 7
    Policies:
      - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy
      - arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess
    AutoPublishAlias: live
```

### 2. ExecutionOtelPlugin + Community Collector Layer

The plugin creates its own `TracerProvider` and exports spans to the collector on `localhost:4318`. Produces a Workflow span as the trace root.

**Handler code:**

```python
from aws_durable_execution_sdk_python_otel import ExecutionOtelPlugin

plugin = ExecutionOtelPlugin()
```

**SAM template:**

```yaml
MyFunction:
  Type: AWS::Serverless::Function
  Properties:
    Runtime: python3.12
    Handler: index.handler
    CodeUri: ./src
    Layers:
      - !Sub arn:aws:lambda:${AWS::Region}:<account>:layer:opentelemetry-collector-amd64-<version>
    Environment:
      Variables:
        OPENTELEMETRY_COLLECTOR_CONFIG_URI: /var/task/collector.yaml
    Tracing: Active
    DurableConfig:
      ExecutionTimeout: 3600
      RetentionPeriodInDays: 7
    Policies:
      - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy
      - arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess
    AutoPublishAlias: live
```

### 3. InvocationOtelPlugin + ADOT Layer

The ADOT layer provides both the collector and a global `TracerProvider`. The plugin uses that global provider (omit `trace_provider`); the invocation span parents to the ADOT layer's ambient invocation span, and operations link to it.

**Handler code:**

```python
from aws_durable_execution_sdk_python_otel import InvocationOtelPlugin

# No trace_provider passed -> uses the ADOT layer's globally registered provider.
plugin = InvocationOtelPlugin()
```

Use the same SAM template as option 1 (ADOT layer + `AWS_LAMBDA_EXEC_WRAPPER`).

### 4. InvocationOtelPlugin + Community Collector Layer

The plugin does not auto-create a provider, so construct an OTLP-exporting `TracerProvider` (targeting the collector on `localhost:4318`) and pass it in. Produces an invocation span as the trace root with operations parented beneath it.

**Handler code:**

```python
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from aws_durable_execution_sdk_python_otel import InvocationOtelPlugin

provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
)
plugin = InvocationOtelPlugin(trace_provider=provider)
```

> If you want zero-config auto-creation of the OTLP provider, use `ExecutionOtelPlugin` (option 2) instead.

Use the same SAM template as option 2 (community collector layer + `collector.yaml`).

### Which Combination Should I Use?

| Scenario                                               | Recommendation                                        |
| ------------------------------------------------------ | ----------------------------------------------------- |
| New deployment, want unified trace per execution       | ExecutionOtelPlugin + Community Collector (option 2)  |
| New deployment, want per-invocation traces             | InvocationOtelPlugin + Community Collector (option 4) |
| Already have ADOT layer, want unified execution traces | ExecutionOtelPlugin + ADOT Layer (option 1)           |
| Already have ADOT layer, want per-invocation traces    | InvocationOtelPlugin + ADOT Layer (option 3)          |
| Want smallest layer size                               | Community Collector (collector-only, no bundled SDK)  |
| Want zero-config auto-instrumentation from ADOT        | ADOT Layer with `use_default_tracer_provider=True`    |

---

## Configuration

`ExecutionOtelPlugin` accepts an `OtelPluginConfig`:

```python
from dataclasses import dataclass

@dataclass
class OtelPluginConfig:
    # Explicit provider to use as-is. Highest priority; skips auto-setup.
    tracer_provider: SdkTracerProvider | None = None

    # Use the globally registered TracerProvider (e.g. from ADOT). Defaults to False.
    use_default_tracer_provider: bool | None = None

    # Upstream trace-context extractor. Defaults to xray_context_extractor.
    context_extractor: ContextExtractor | None = None

    # Instrumentation scope name. Defaults to "aws-durable-execution-sdk-python".
    instrument_name: str = DEFAULT_INSTRUMENT_NAME

    # Whether to register HTTP (urllib3) instrumentation. Defaults to True.
    enable_http_instrumentation: bool = True

    # OTLP exporter settings for the auto-configured provider (endpoint, headers).
    exporter_config: ExporterConfig = ExporterConfig()

    # Custom propagators. Replaces the default [AWS X-Ray, W3C TraceContext].
    propagators: Sequence[TextMapPropagator] | None = None

    # Custom Workflow span name (ExecutionOtelPlugin). Defaults to "Workflow".
    workflow_span_name: str = DEFAULT_WORKFLOW_SPAN_NAME

    # Install the root-logger OTel context filter for log correlation. Defaults to True.
    enrich_logger: bool = True


@dataclass
class ExporterConfig:
    endpoint: str | None = None
    headers: dict[str, str] | None = None
```

**TracerProvider precedence:** explicit `tracer_provider` > `use_default_tracer_provider=True` > auto-created.

**Usage examples:**

```python
from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
    ExporterConfig,
)

# Zero-config (auto-creates TracerProvider with OTLP export)
plugin = ExecutionOtelPlugin()

# Use the ADOT layer's globally registered TracerProvider (ExecutionOtelPlugin)
plugin = ExecutionOtelPlugin(OtelPluginConfig(use_default_tracer_provider=True))

# Custom endpoint and headers (third-party vendor)
plugin = ExecutionOtelPlugin(
    OtelPluginConfig(
        exporter_config=ExporterConfig(
            endpoint="https://api.honeycomb.io/v1/traces",
            headers={"x-honeycomb-team": os.environ["HONEYCOMB_API_KEY"]},
        )
    )
)

# Bring your own TracerProvider (either plugin)
from opentelemetry.sdk.trace import TracerProvider

provider = TracerProvider()  # your config
plugin = ExecutionOtelPlugin(OtelPluginConfig(tracer_provider=provider))
# InvocationOtelPlugin takes the provider directly (no OtelPluginConfig):
plugin = InvocationOtelPlugin(trace_provider=provider)
```

`InvocationOtelPlugin` is constructed with plain keyword arguments rather than an `OtelPluginConfig`:

```python
InvocationOtelPlugin(
    trace_provider=None,        # provider to use; falls back to the global provider
    context_extractor=None,     # defaults to xray_context_extractor
    instrument_name="aws-durable-execution-sdk-python",
    enrich_logger=True,
)
```

---

## Export Strategies

When the plugin auto-creates its `TracerProvider` (default mode), you can configure where spans go:

### Via a Collector Layer (Recommended)

```
Lambda → OTLP (localhost:4318) → Collector Extension → X-Ray/CloudWatch
```

No code changes needed — auto-created providers target `localhost:4318` by default.

### Direct to CloudWatch OTLP Endpoint

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://xray.us-east-1.amazonaws.com/v1/traces
```

> **Note:** Direct export requires SigV4-signed requests.

### Via Third-Party OTLP Endpoint

```python
plugin = ExecutionOtelPlugin(
    OtelPluginConfig(
        exporter_config=ExporterConfig(
            endpoint="https://api.honeycomb.io/v1/traces",
            headers={"x-honeycomb-team": os.environ["HONEYCOMB_API_KEY"]},
        )
    )
)
```

Or via environment variables:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://api.honeycomb.io/v1/traces
OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=YOUR_API_KEY
```

---

## Collector Configuration

When using the community collector-only layer, include a `collector.yaml` in your function bundle:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: "localhost:4318"

exporters:
  awsxray:
    region: "${AWS_REGION}"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [awsxray]
```

Set the environment variable:

```bash
OPENTELEMETRY_COLLECTOR_CONFIG_URI=/var/task/collector.yaml
```

### Why Use a Collector?

Using the community collector-only layer lets you export traces directly to third-party observability platforms (such as Datadog, Honeycomb, or Grafana) without first sending them to AWS and then re-exporting from CloudWatch or X-Ray.

---

## IAM Permissions

### Via Collector Layer (ADOT or Community)

The function's execution role needs X-Ray write permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["xray:PutTraceSegments", "xray:PutTelemetryRecords"],
      "Resource": "*"
    }
  ]
}
```

Or attach: `arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess`

### Via Third-Party Endpoint

No AWS IAM permissions required. Authentication is handled via headers in `OTEL_EXPORTER_OTLP_HEADERS` or `exporter_config.headers`.

---

## Environment Variables

| Variable                             | Description                                                                                                                       | Default                           |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`        | OTLP exporter endpoint URL                                                                                                        | `http://localhost:4318/v1/traces` |
| `OTEL_EXPORTER_OTLP_HEADERS`         | Comma-separated key=value headers for the exporter                                                                                | —                                 |
| `OTEL_DURABLE_SAMPLING_RATIO`        | Trace-ID-based probabilistic sampling ratio (0.0 to 1.0). All invocations of the same execution are sampled/dropped consistently. | `1.0` (all traces sampled)        |
| `AWS_LAMBDA_EXEC_WRAPPER`            | Set to `/opt/otel-instrument` to activate the ADOT layer's auto-instrumentation                                                   | —                                 |
| `OPENTELEMETRY_COLLECTOR_CONFIG_URI` | Path to `collector.yaml` for the community collector layer                                                                        | —                                 |
| `AWS_LAMBDA_FUNCTION_NAME`           | Set by the Lambda runtime. Used to detect the Lambda environment and populate resource attributes.                                | —                                 |
| `AWS_REGION`                         | Set by the Lambda runtime. Used for resource attributes and collector configuration.                                              | —                                 |
| `AWS_LAMBDA_FUNCTION_MEMORY_SIZE`    | Set by the Lambda runtime. Populates the `faas.max_memory` span attribute (in MB).                                                | —                                 |

---

## SAM/CloudFormation Templates

See the [Deployment Matrix](#deployment-matrix) section for plugin-specific templates with both layer options. Below are additional templates for alternative export targets.

### Direct to CloudWatch (No Layer)

```yaml
MyFunction:
  Type: AWS::Serverless::Function
  Properties:
    Runtime: python3.12
    Handler: index.handler
    CodeUri: ./src
    DurableConfig:
      ExecutionTimeout: 3600
      RetentionPeriodInDays: 7
    Environment:
      Variables:
        OTEL_EXPORTER_OTLP_ENDPOINT: !Sub "https://xray.${AWS::Region}.amazonaws.com/v1/traces"
    Policies:
      - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy
      - arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess
    AutoPublishAlias: live
```

### Third-Party OTLP Endpoint

```yaml
MyFunction:
  Type: AWS::Serverless::Function
  Properties:
    Runtime: python3.12
    Handler: index.handler
    CodeUri: ./src
    DurableConfig:
      ExecutionTimeout: 3600
      RetentionPeriodInDays: 7
    Layers:
      # Optional: collector layer for reliability (retry/buffering)
      - !Sub arn:aws:lambda:${AWS::Region}:<account>:layer:opentelemetry-collector-amd64-<version>
    Environment:
      Variables:
        OTEL_EXPORTER_OTLP_ENDPOINT: "https://api.honeycomb.io/v1/traces"
        OTEL_EXPORTER_OTLP_HEADERS: "x-honeycomb-team=YOUR_API_KEY"
    Policies:
      - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy
    AutoPublishAlias: live
```

---

## Trace Structure Comparison

The Python SDK exposes these durable operations, each of which produces an Operation span: `step` (STEP), `wait` (WAIT), `wait_for_condition` (WAIT_FOR_CONDITION), `wait_for_callback` (CALLBACK), `invoke` (chained invoke), `map`, and `parallel`. `map` and `parallel` run their branches in child contexts, which appear as CONTEXT operation spans.

### ExecutionOtelPlugin

Produces a hierarchical trace with the Workflow span as the synthetic root:

```
Workflow span (deterministic ID from execution ARN, exported on terminal status only)
├── Invocation span (one per Lambda invocation, always exported)
├── Operation span: "fetch-data" (STEP)
│   ├── Attempt span: "fetch-data attempt 1"
│   │   └── HTTP span: GET https://api.example.com/data
│   └── [link → Invocation span]
├── Operation span: "cooldown" (WAIT)
│   └── [link → Invocation span]
└── Operation span: "process" (STEP)
    └── [link → Invocation span]
```

> When `use_default_tracer_provider=True`, the plugin's Invocation span is parented to the ambient invocation span from the ADOT layer's context.

### InvocationOtelPlugin

Produces a per-invocation trace with the invocation span as root:

```
Invocation span (one per Lambda invocation)
├── Operation span: "fetch-data" (STEP)
│   ├── Attempt span: "fetch-data attempt 1"
│   │   └── HTTP span: GET https://api.example.com/data
│   └── [link → deterministic operation span ID]
├── Operation span: "cooldown" (WAIT)
└── Operation span: "process" (STEP)
```

Cross-invocation operations are correlated via span links to deterministic span IDs.

### Span Attributes

- **Workflow span** (ExecutionOtelPlugin): `durable.execution.arn`, `durable.execution.status`
- **Invocation span**: `durable.execution.arn`, `durable.invocation.first`, `durable.invocation.status`, and (when the plugin owns the provider) `faas.invocation_id`, `faas.coldstart`, `cloud.provider`, `cloud.platform`
- **Operation span**: `durable.execution.arn`, `durable.operation.id`, `durable.operation.type`, `durable.operation.name`, `durable.operation.subtype`, `durable.operation.status`, and `durable.attempt.number` (STEP / WAIT_FOR_CONDITION, on completion)
- **Attempt span**: all operation attributes plus `durable.attempt.number` and `durable.attempt.outcome`

**Span status mapping:**

- Invocation span: `SUCCEEDED`/`PENDING` → `OK`, `RETRY`/`FAILED` → `ERROR`
- Workflow span: `SUCCEEDED` → `OK`, `FAILED` → `ERROR` (non-terminal statuses are never exported, so they stay `UNSET`)

---

## Log Correlation

When `enrich_logger=True` (the default), the plugin installs an `OtelContextLogFilter` on the root logger's handlers. The filter stamps the active OTel trace context onto every log record:

- `traceId` — 32-char hex trace ID
- `spanId` — 16-char hex span ID
- `otelTraceSampled` — whether the trace is sampled

These fields are only added when a valid span is active, so any log formatter must treat them as optional. You can also install the filter manually on a specific logger:

```python
from aws_durable_execution_sdk_python_otel import install_log_filter

install_log_filter(plugin, target_logger=my_logger)
```

---

## Additional Python Dependencies

The core package depends on `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp`, and `opentelemetry-propagator-aws-xray`.

When the plugin auto-creates its `TracerProvider` (default mode) and you want AWS SDK / HTTP auto-instrumentation, install the optional `instrumentation` extra, which adds:

```bash
pip install "aws-durable-execution-sdk-python-otel[instrumentation]"
# adds: opentelemetry-instrumentation-botocore, opentelemetry-instrumentation-urllib3
```

The instrumentation module degrades gracefully: if these packages are not installed, AWS SDK / HTTP calls simply are not auto-traced. When using `use_default_tracer_provider=True` (ADOT layer mode), the ADOT layer provides its own instrumentation.

---

## API Reference

### `ExecutionOtelPlugin`

Plugin that produces a Workflow span as the synthetic trace root. Implements `DurableInstrumentationPlugin`.

```python
ExecutionOtelPlugin(config: OtelPluginConfig | None = None)
```

### `InvocationOtelPlugin`

Plugin that produces an invocation span as the trace root. Implements `DurableInstrumentationPlugin`.

```python
InvocationOtelPlugin(trace_provider=None, context_extractor=None, enrich_logger=True, ...)
```

### `OtelPluginConfig` / `ExporterConfig`

OtelPluginConfig is used by ExecutionOtelPlugin (see [Configuration](#configuration)).

### `DeterministicIdGenerator`

Custom OpenTelemetry `IdGenerator` that produces reproducible trace and span IDs from execution metadata.

### `derive_workflow_span_id(execution_arn: str) -> int`

Derives a deterministic 64-bit span ID from an execution ARN.

### `operation_id_to_span_id(execution_arn: str, operation_id: str) -> int`

Derives a deterministic span ID for a durable operation.

### `xray_context_extractor`

Default context extractor. Reads the X-Ray trace header to derive trace context.

### `w3c_client_context_extractor`

Alternative context extractor. Reads `traceparent` from the invocation's client context.

### `ContextExtractor`

Type alias for custom context-extractor callables.

### `OtelContextLogFilter` / `install_log_filter`

Logging filter and installer for trace/span log correlation (see [Log Correlation](#log-correlation)).

### `create_tracer_provider` / `ProviderResult`

Lower-level factory used internally to resolve/auto-create the `TracerProvider`.

---

## Verification

> **Important:** When using the community collector layer, you must enable **CloudWatch Transaction Search** in your AWS account for traces to be visible in X-Ray. Navigate to CloudWatch → Settings → Traces and Logs and turn on Transaction Search.

After deploying with either plugin and either layer:

1. **Invoke your durable function** — trigger an execution with multiple steps or a wait/resume cycle.
2. **Check the CloudWatch console** — Navigate to CloudWatch → Traces. You should see spans grouped under one trace ID.
3. **Check log correlation** — With `enrich_logger=True`, verify logs include `traceId` and `spanId`.
4. **Confirm sampling** — Set `OTEL_DURABLE_SAMPLING_RATIO` below 1.0 and verify only the expected proportion of traces appear.

### Troubleshooting

| Symptom                         | Likely Cause                                                                                                                                       |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| No traces appear                | Collector layer not attached, or config env var not set                                                                                            |
| No traces with ADOT layer       | `AWS_LAMBDA_EXEC_WRAPPER` not set (when using `use_default_tracer_provider=True`)                                                                   |
| Traces fragmented across IDs    | X-Ray active tracing not enabled on the function                                                                                                   |
| Missing operation spans         | Sampling ratio set below 1.0                                                                                                                        |
| AWS SDK / HTTP spans missing    | The `instrumentation` extra is not installed                                                                                                       |
| Collector layer errors          | Check `collector.yaml` is in the function bundle at the path specified                                                                             |
| Duplicate spans with ADOT layer | `AWS_LAMBDA_EXEC_WRAPPER` is set but `use_default_tracer_provider` is `False` — either remove the env var or set `use_default_tracer_provider=True` |

## License

Apache-2.0
