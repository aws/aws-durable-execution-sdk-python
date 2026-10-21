# AWS Durable Execution SDK for Python — Durable Execution Telemetry Exporter

Durable Execution Telemetry instrumentation plugin for the AWS Durable
Execution SDK for Python. A port of the JavaScript SDK's
`durableExecutionTelemetry()` plugin: it listens to the SDK's instrumentation
hooks and emits one curated `DurableExecutionTelemetry` record per execution
to the configured exporters. The wire record keeps the JS camelCase field
names so records read identically across SDKs.

> **Experimental.** Like its JS counterpart, this plugin is experimental and may
> change or be removed in future releases.

## Install

```bash
pip install aws-durable-execution-sdk-python-telemetry-exporter
# with the S3 exporter's local-dev dependency:
pip install "aws-durable-execution-sdk-python-telemetry-exporter[s3]"
```

## Usage

```python
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python_telemetry_exporter import (
    DurableExecutionTelemetryConfig,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_telemetry_exporter.exporters import S3Exporter

@durable_execution(
    plugins=[
        durable_execution_telemetry(
            DurableExecutionTelemetryConfig(
                exporters=[
                    S3Exporter(bucket="my-bucket", prefix="durable-execution-telemetry/")
                ],
            )
        )
    ]
)
def handler(event, context):
    ...
```

With no exporter configured, records are written to the function's own
CloudWatch log group (the `LambdaLogExporter` default), one Lambda JSON log
line per record whose `message` is the compact record JSON carrying the
name-keyed `operationsByName` summary (see
[LambdaLogExporter](#lambdalogexporter)). The `S3Exporter` writes the
lossless per-occurrence `operations` array, one object per execution
(upsert-by-execution-name, so re-emission overwrites rather than appends).

## Record fields shared with the JS exporter

Records follow the JS exporter's shape, which the record readers rely on for
every SDK:

- `error.name` (the execution `error`, `operations[].error`, and
  `operationsByName.<name>.error`) is the simple type name of the error the
  user's code raised, for example `PaymentError`: no module prefix, and not the
  SDK wrapper (`StepError`, `ChildContextError`, ...). Hooks deliver
  checkpointed `ErrorObject`s, so the type string is cut to its last segment
  after any `.` and any `$`, and an SDK wrapper is resolved to the user error
  of the failed operation it was raised from (same message, data and stack
  trace). With no user error (for example a callback timeout or an SDK
  failure), the SDK error's simple class name is used. A failure on its first
  invocation and the same failure replayed later get the same name. `message`
  is unchanged.
- An opted-in operation `result` (`operations[].result`,
  `operationsByName.<name>.result`, and the value passed to an override's
  `result` transform) is the plain JSON value, for example `{"amount": 99.99}`.
  Results checkpointed by the SDK's default serializer as a `{"t": ..., "v":
  ...}` envelope are decoded with that serializer and converted to JSON values
  the way the Java SDK's default serializer writes them: `Decimal` becomes a
  number (`null` when not finite), `datetime`/`date` an ISO-8601 string from
  `isoformat()`, `UUID` a string, `bytes` a base64 string, `tuple` a list, and
  a map/parallel `BatchResult` its `to_dict()` form. A payload that is not an
  envelope (for example from a custom `SerDes`, which is never run) is passed
  through as parsed JSON, or as the raw string when it is not JSON; an
  envelope that cannot be decoded is passed through as parsed JSON.
- Every record built at invocation end carries `endTime` (the emission time)
  and `durationMs` (`endTime - startTime`), including the `RUNNING` record of
  an invocation that suspends or retries (`on-change` mode). Records built at
  invocation start or on an operation change carry neither. Do not use the
  presence of `endTime` as a completion signal; use `status`.

## Exporters

All exporters live in `aws_durable_execution_sdk_python_telemetry_exporter.exporters` and
are re-exported from the package root. Each serializes the record as compact
JSON. Exporters that call AWS accept an injected `client=` for tests and use
the Lambda runtime's boto3 otherwise; none adds a required dependency.

| Exporter | Destination | Upsert | Operations shape | Default size limit |
| --- | --- | --- | --- | --- |
| `LambdaLogExporter` | Function's own log group | No | `operationsByName` | 256 KB |
| `CloudWatchLogsExporter` | Any log group, one stream per day | No | `operationsByName` | 256 KB |
| `S3Exporter` | S3 object per execution | Yes (key) | `operations` | 5 MB |
| `DynamoDBExporter` | DynamoDB item | Configurable | `operationsByName` | 400 KB |
| `AuroraExporter` | Aurora MySQL/PostgreSQL row (Data API) | Yes (upsert) | full record as JSON column | 1 MB |
| `RedshiftExporter` | Redshift row (Data API) | Yes (MERGE) | full record as SUPER column | 1 MB |
| `OpenSearchExporter` | OpenSearch document | Yes (`_id`) | `operations` | 10 MB |
| `FirehoseExporter` | Firehose delivery stream | N/A | `operations_format` | 1 MB |
| `EventBridgeExporter` | EventBridge event | N/A | `operations_format` | 256 KB |
| `SQSExporter` | SQS message | N/A | `operations_format` | 256 KB |
| `OTelExporter` | OTLP/HTTP logs endpoint | N/A | `operations_format` | 1 MB |
| `HttpExporter` | Any HTTP endpoint | N/A | `operations_format` | none |
| `FileExporter` | Directory (EFS, mount, `/tmp`) | Configurable | `operations_format` | none |

`operations_format` is `"array"` (default), `"by-name"`, or `"both"`
(`OperationsFormat`). `max_record_size_bytes` raises or lowers an exporter's
size limit; omitting it keeps the default. `HttpExporter` and `FileExporter`
have no default and do not truncate unless a limit is set.

### LambdaLogExporter

Prints one line per record to stdout, so it lands in the function's own log
group with no extra IAM. The line is the Lambda JSON log envelope, with keys
in this order:

```json
{"timestamp":"2026-01-01T00:00:00.000Z","level":"INFO","message":"{\"recordType\":\"DurableExecutionTelemetry\",...}"}
```

`timestamp` is the UTC write time with millisecond precision, `level` is
always `INFO`, and `message` is the compact record JSON as a string. This is
the shape the Node.js runtime gives the JS exporter's `console.log` line in the
JSON log format, and record readers for this destination filter on
`level = "INFO" and message like /DurableExecutionTelemetry/` and parse
`message`. Lambda captures Python `print` output as written in both the Text
and JSON log formats, and does not re-encode lines that are already JSON, so
the envelope is never wrapped twice. With the JSON log format and an
application log level above `INFO` (`WARN`, `ERROR`, `FATAL`), Lambda filters
these lines out, as it does for the JS exporter's lines. `requestId` is not
included, because exporters do not receive the request id.
`max_record_size_bytes` (default 256 KB) is measured on the rendered record,
that is the `message` string; the envelope keys and the JSON escaping of
`message` add bytes on top of it.

### CloudWatchLogsExporter

Writes one `PutLogEvents` event per record to `log_group_name`, in a stream
named `{log_stream_prefix}{YYYY}/{MM}/{DD}` (default prefix `durable-execution-telemetry/`).
IAM: `logs:CreateLogStream`, `logs:PutLogEvents` on the log group.

```python
CloudWatchLogsExporter(log_group_name="/custom/durable-execution-telemetry")
```

### DynamoDBExporter

`PutItem` keyed by `partition_key` (default `pk`) = `executionArn`. With the
default `sort_key="sk"` (= `emittedAt`) every export adds an item; pass
`sort_key=None` for a key-only table that upserts. IAM: `dynamodb:PutItem`.

```python
DynamoDBExporter(table_name="durable-execution-telemetry")
```

### AuroraExporter

Upserts a row by `execution_arn` through the RDS Data API; `engine` is
`"postgresql"` or `"mysql"` and selects the dialect. Columns: `execution_arn,
execution_name, function_name, status, start_time, end_time, duration_ms,
record_json, emitted_at`. IAM: `rds-data:ExecuteStatement`,
`secretsmanager:GetSecretValue`.

```python
AuroraExporter(
    resource_arn="arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
    secret_arn="arn:aws:secretsmanager:us-east-1:123456789012:secret:my-db-creds",
    database="telemetry",
    engine="postgresql",
)
```

### RedshiftExporter

`MERGE` by `execution_arn` through the Redshift Data API into
`{schema}.{table}` (default `public.durable_execution_telemetry`, same columns as Aurora,
`record_json` as `SUPER`). Provide `workgroup_name` (Serverless) or
`cluster_identifier` (provisioned, with `db_user` or `secret_arn`). IAM:
`redshift-data:ExecuteStatement` plus `redshift-serverless:GetCredentials` or
`secretsmanager:GetSecretValue`. The statement is submitted and not awaited, so
statement failures are not reported and `on-change` records may land out of
order; prefer the default `on-complete` emit mode with this exporter.

```python
RedshiftExporter(database="telemetry", workgroup_name="telemetry-wg")
```

### OpenSearchExporter

`PUT {endpoint}/{index_name}/_doc/{executionArn}` (default index
`durable-execution-telemetry`). `auth="sigv4"` (default) signs with the runtime's
credentials via botocore; `auth="basic"` uses `username`/`password`. IAM:
`es:ESHttpPut` on the domain.

```python
OpenSearchExporter(endpoint="https://my-domain.us-east-1.es.amazonaws.com", region="us-east-1")
```

### FirehoseExporter

`PutRecord` of one JSON line (trailing newline) per record. IAM:
`firehose:PutRecord`.

```python
FirehoseExporter(delivery_stream_name="durable-execution-telemetry-stream")
```

### EventBridgeExporter

`PutEvents` with `Source` (default `software.amazon.lambda.durable.telemetry`),
`DetailType` = record status, `Detail` = record. All arguments optional. IAM:
`events:PutEvents`.

```python
EventBridgeExporter(event_bus_name="default")
```

### SQSExporter

`SendMessage` with the record as body and `status`/`functionName` message
attributes. A `.fifo` queue URL enables `MessageGroupId` (default
`executionArn`, or `message_group_id`) and a deduplication id of
`executionArn:emittedAt`. IAM: `sqs:SendMessage`.

```python
SQSExporter(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/telemetry")
```

### OTelExporter

POSTs one OTLP `ExportLogsServiceRequest` (`http/json` only) per record to
`endpoint`; identity fields become attributes and the record is the log body.
Pass vendor auth in `headers`. No IAM.

```python
OTelExporter(endpoint="https://otlp.vendor.com/v1/logs", headers={"x-api-key": "..."})
```

### HttpExporter

`POST` (or `method="PUT"`) the record as JSON to `url` with
`Content-Type: application/json` plus `headers`; a non-2xx status raises.
`timeout_ms` defaults to 10000 and currently applies to each socket operation
(the connect, each write of the request, and each read of the status line,
headers, and a non-2xx error body; a 2xx body is never read) rather than to the
whole request, so an endpoint that consumes or responds slowly can hold the
export open longer than `timeout_ms`; a future release may enforce it as a
whole-request deadline. No IAM.

```python
HttpExporter(url="https://hooks.example.com/telemetry", headers={"Authorization": "Bearer ..."})
```

### FileExporter

`mode="ndjson"` (default) appends to `{directory}/{YYYY-MM-DD}.ndjson`;
`mode="json"` writes `{directory}/{executionName}.json`, overwriting on update.
For Lambda use an EFS mount (IAM: `elasticfilesystem:ClientMount`,
`elasticfilesystem:ClientWrite`) or `/tmp` for testing. Appends from many
concurrent environments to one NDJSON file on a network file system can
interleave; use `mode="json"` when several environments share a directory.

```python
FileExporter(directory="/mnt/efs/durable-execution-telemetry")
```

Emission behavior, record schema (`recordType: DurableExecutionTelemetry`,
`schemaVersion: "1.0"`), sampling, content configuration (input/output
omission, `include_errors`, per-operation result opt-in), truncation phases,
and `top-level` vs `full-tree` operation detail all mirror the JS plugin.
Behavior is validated cross-SDK by the `telemetry` conformance suite
(`aws-durable-execution-conformance-tests-telemetry`).

> **Note (asynchronous export).** Export rendering, truncation, `export()`, and
> `flush()` run on one lazy background worker per plugin. Checkpoint hooks only
> replace that execution's latest pending snapshot and wake the worker.
> Consecutive `on-change` snapshots for one execution coalesce whenever the
> worker has not yet claimed the previous one, including when no export is
> running, so under fast in-memory checkpoints (the local runner) a burst may
> deliver only its first and latest snapshots. Snapshots from different
> executions never coalesce with each other. An invocation that emits a record
> drains every pending snapshot and flushes exporters before it returns;
> invocations that emit nothing do not start or flush the worker.

## Requirements

- `aws-durable-execution-sdk-python` with the plugin invocation hooks that
  surface `execution_input` / `execution_result` (included since the version
  this package declares as its minimum).
