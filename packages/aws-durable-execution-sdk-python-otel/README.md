# AWS Durable Execution SDK - OpenTelemetry Plugin

OpenTelemetry instrumentation plugin for the [AWS Durable Execution SDK for Python](https://github.com/aws/aws-durable-execution-sdk-python). Emits distributed traces that correlate across multiple Lambda invocations of a single durable execution, producing deterministic span and trace IDs so that spans from different invocations are stitched into a single coherent trace.

## Features

- **Deterministic Trace IDs**: All invocations of the same durable execution share a single trace, derived from the X-Ray trace header or execution ARN
- **Span-per-Operation**: Each durable operation (step, wait, invoke) gets its own span with accurate timing
- **Continuation Spans**: Operations completing in a different invocation are linked back to the original span
- **Log Correlation**: Enrich application logs with trace ID and span ID for end-to-end observability
- **Configurable Sampling**: Control trace volume via plugin options
- **Self-Contained Setup**: No manual TracerProvider configuration required

## Installation

```bash
pip install aws-durable-execution-sdk-python-otel
```

## Quick Start using X-Ray/CloudWatch Tracing

1. Add the [ADOT Lambda Layer](#1-adot-lambda-layer) to your function and set `AWS_LAMBDA_EXEC_WRAPPER=/opt/otel-instrument`
2. Enable [X-Ray Active Tracing](#2-aws-x-ray-active-tracing) on the function
3. Pass `OtelPlugin` to your handler's `plugins` list
4. Add X-Ray write permissions

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

Enable active tracing on your Lambda function so the `_X_AMZN_TRACE_ID` environment variable is populated at invocation time. The plugin uses this header to derive deterministic trace IDs that remain consistent across all invocations of the same durable execution.

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

### 3. In your Lambda handler (index.py)

```python
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_otel import OtelPlugin


@durable_execution(plugins=[OtelPlugin()])
def handler(event: dict, context: DurableContext) -> dict:
    result = context.step(lambda _: fetch_data(event["id"]), name="fetch-data")

    context.wait(duration=Duration.from_seconds(5))

    context.step(lambda _: process(result), name="process")

    return result
```

That's it. The plugin handles TracerProvider setup, deterministic ID generation, and span lifecycle internally.

### 4. Grant Permissions

The function's execution role needs the `AWSXRayDaemonWriteAccess` managed policy (or equivalent permissions) if using X-Ray as the tracing backend.

### Environment Variables for ADOT layer

| Variable                      | Description                                                                                   | Default           |
| ----------------------------- | --------------------------------------------------------------------------------------------- | ----------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint for the OTLP exporter (e.g., `http://localhost:4318` for the ADOT collector sidecar) | Set by ADOT layer |
| `AWS_LAMBDA_EXEC_WRAPPER`     | Set to `/opt/otel-instrument` for the ADOT layer to instrument your function                  | —                 |
| `OTEL_TRACES_SAMPLER`         | Sampler to use (e.g., `traceidratio` for ratio-based sampling)                                | `always_on`       |
| `OTEL_TRACES_SAMPLER_ARG`     | Argument for the sampler (e.g., `0.3` to sample 30% of traces)                                | —                 |

See the [ADOT sampling configuration](https://aws-otel.github.io/docs/getting-started/lambda#sampling-configuration) for more details.

## Configuration

### Plugin Options

```python
from aws_durable_execution_sdk_python_otel import (
    OtelPlugin,
    xray_context_extractor,
)

plugin = OtelPlugin(
    # Provide your own TracerProvider if you already have one configured.
    # Defaults to the globally configured tracer provider.
    trace_provider=None,
    # Use a custom context extractor (default: xray_context_extractor).
    context_extractor=xray_context_extractor,
    # Custom instrumentation scope name
    # (default: "aws-durable-execution-sdk-python").
    instrument_name="my-service",
    # Install a root-logger filter that stamps trace context onto every
    # log record (default: True).
    enrich_logger=True,
)
```

### Context Extractors

The plugin supports multiple strategies for extracting upstream trace context:

```python
from aws_durable_execution_sdk_python_otel import (
    OtelPlugin,
    w3c_client_context_extractor,
    xray_context_extractor,
)

# Default: X-Ray trace header (recommended for most Lambda deployments)
OtelPlugin(context_extractor=xray_context_extractor)

# W3C Trace Context via clientContext (requires backend propagation support)
OtelPlugin(context_extractor=w3c_client_context_extractor)
```

### Log Correlation

When `enrich_logger=True` (the default), the plugin installs a logging filter on
the root logger at invocation start. The filter stamps the active OTel trace
context onto every emitted log record using these attributes:

- `traceId`: 32-char hex trace identifier
- `spanId`: 16-char hex span identifier
- `otelTraceSampled`: boolean indicating if the trace is sampled

These attributes are only set when a valid span context is active, so any log
formatter or schema must treat the fields as optional.

## Verification

After deploying your function with the plugin configured:

1. **Invoke your durable function** — trigger at least one execution that includes multiple steps or a wait/resume cycle.

2. **Check the CloudWatch console** — Navigate to CloudWatch → Traces in the AWS Console. You should see a trace with:
   - An "invocation" span per invocation
   - Child spans for each durable operation (named after your step names)
   - All invocations of the same execution grouped under one trace ID

3. **Check log correlation** — verify that your logs include `traceId` and `spanId` fields matching the spans in X-Ray.

4. **Confirm sampling** — If you set `OTEL_TRACES_SAMPLER=traceidratio` and `OTEL_TRACES_SAMPLER_ARG` to a value less than 1.0, verify that only the expected proportion of traces appear.

5. **Span links** — For operations that span multiple invocations (e.g., after a wait resumes), though span links are set, they are not visualized within the CloudWatch console.

### Troubleshooting

| Symptom                           | Likely Cause                                                    |
| --------------------------------- | --------------------------------------------------------------- |
| No traces appear                  | ADOT layer not configured, or `AWS_LAMBDA_EXEC_WRAPPER` not set |
| Traces appear but are fragmented  | X-Ray active tracing not enabled on the Lambda function         |
| Missing spans for some operations | `OTEL_TRACES_SAMPLER_ARG` set below 1.0                         |
| `_X_AMZN_TRACE_ID` not populated  | X-Ray active tracing not enabled                                |

## API Reference

### `OtelPlugin`

The main plugin class. Implements `DurableInstrumentationPlugin` from `aws_durable_execution_sdk_python`.

```python
OtelPlugin(
    trace_provider=None,
    context_extractor=None,
    instrument_name="aws-durable-execution-sdk-python",
    enrich_logger=True,
)
```

### `DeterministicIdGenerator`

A custom OpenTelemetry `IdGenerator` that produces reproducible trace and span IDs from execution metadata. Exported for advanced use cases.

### `xray_context_extractor`

Default context extractor. Reads the `_X_AMZN_TRACE_ID` environment variable to derive trace context.

### `w3c_client_context_extractor`

Alternative context extractor. Reads W3C `traceparent` from `context.clientContext.custom.traceparent`. Requires backend `clientContext` propagation to be enabled.

### `ContextExtractor`

Type alias for custom context extractor functions.

### `OtelContextLogFilter` / `install_log_filter`

The logging filter (and its installer) used to stamp trace context onto log
records. Installed automatically when `enrich_logger=True`; exported for manual
setups.

## Requirements

- Python >= 3.11
- `aws-durable-execution-sdk-python` >= 1.5.0
- `opentelemetry-api` >= 1.20.0
- `opentelemetry-sdk` >= 1.20.0

## License

Apache-2.0
