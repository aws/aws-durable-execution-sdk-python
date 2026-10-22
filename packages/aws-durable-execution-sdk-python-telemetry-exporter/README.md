# AWS Durable Execution SDK for Python — Durable Execution Telemetry Exporter

Durable Execution Telemetry instrumentation plugin for the AWS Durable
Execution SDK for Python. A port of the JavaScript SDK's
`durableExecutionTelemetry()` plugin: it listens to the SDK's instrumentation
hooks and emits one curated `DurableExecutionTelemetry` record per execution
to the configured exporters. The wire record keeps the JS camelCase field
names so records read identically across SDKs.

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
CloudWatch log group as single JSON lines (the `LambdaLogExporter` default, see
below), carrying the name-keyed `operationsByName` summary. The `S3Exporter` writes the
lossless per-occurrence `operations` array, one object per execution
(upsert-by-execution-name, so re-emission overwrites rather than appends).

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

Prints one line per record to stdout, which Lambda sends to the function's own
log group. The line is the Lambda JSON log envelope, with keys in this order:

```json
{"timestamp":"2026-01-01T00:00:01.234Z","level":"INFO","message":"<compact record JSON>"}
```

`timestamp` is UTC with millisecond precision and a `Z` suffix. `message` is
the compact record JSON as a string; parse it to get the record. The exporter
writes the envelope itself because Lambda captures Python `print` output as
plain text, in both the plain text and the JSON log format, and Lambda does not
double-encode output that is already JSON (see
[Log and monitor Python Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/python-logging.html)
and [Lambda log formats](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs-logformat.html)).

`max_record_size_bytes` (default 256 KB) limits the record in `message`. The
JSON escaping of `message` can make the line up to about 2x the record; a
CloudWatch Logs event can be up to 1 MB
([PutLogEvents](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html)).
The lines have `level` `INFO`, so Lambda drops them when the function's
application log level is `WARN` or above.

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

## Error names and causes

Every error in a record (the execution `error`, each `operations[].error`, and
each `operationsByName.<name>.error`) has the shape
`{"name": ..., "message": ..., "cause": ...}`, with keys in that order. `name`
is the SDK's error type, unchanged, and `message` is the error message. `cause`
has the same shape, recursively, and is present only when the SDK provides a
cause.

In Python, the plugin hooks carry only an `ErrorObject` (type, message, data,
stack trace) with no cause, so Python records have `name` and `message` and
never have `cause`. The exporter does not build a cause from other operations
or from `ErrorData`. `include_errors=False` removes operation errors.

Names are each language's own and differ across SDKs. For example, a step that
throws `PaymentError` can be reported as `StepError` in JS,
`aws_durable_execution_sdk_python.exceptions.StepError` or `PaymentError` in
Python, and `StepFailedException` or `PaymentError` in Java. Query error names
per SDK, or query on `message` and the `cause` chain.

## Operation results

An opted-in operation `result` (`OperationOverride(result=...)`) is passed to
the transform as a plain JSON value. When the checkpointed string is the
default serializer's (`ExtendedTypeSerDes`) envelope, a JSON object whose keys
are exactly `t` and `v` with a string `t`, it is decoded with that serializer
and converted to plain JSON:

- `Decimal`: an integer when it is integral and `adjusted() < 300`; otherwise a
  number when the float round-trips exactly; otherwise `str(value)` (for
  example `"12345678901234567.89"` or `"1E+5000"`). NaN and Infinity become
  `null`.
- `datetime` and `date`: `isoformat()`. `UUID`: string. `bytes`: base64.
  `tuple`: list. `BatchResult`: `to_dict()`.

Any other result is parsed as JSON, or kept as the raw string when it is not
JSON. A result that fails to decode keeps its parsed or raw value, and the rest
of the record is still emitted.

The hooks do not say which serializer wrote a result, so the envelope is
recognized by shape alone. A result written by `JsonSerDes` or a custom
`SerDes` that has this exact shape is decoded too: `{"t": "s", "v": "x"}` is
reported as `"x"`.

## Requirements

- `aws-durable-execution-sdk-python` with the plugin invocation hooks that
  surface `execution_input` / `execution_result` (included since the version
  this package declares as its minimum).
