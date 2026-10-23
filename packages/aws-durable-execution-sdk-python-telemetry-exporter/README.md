# AWS Durable Execution SDK for Python — Durable Execution Telemetry plugin

Durable Execution Telemetry instrumentation plugin for the AWS Durable Execution SDK for
Python. A port of the JavaScript SDK's `durableExecutionTelemetry()` plugin: it listens to
the SDK's instrumentation hooks and emits one curated `DurableExecutionTelemetry` record
per execution to the configured exporters. The wire record keeps the JS
camelCase field names so records read identically across SDKs.

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

`durable_execution_telemetry()` returns a plugin *factory*, which is what the SDK's
`plugins` argument takes: the SDK calls its `create_plugin` once per invocation to
build that invocation's plugin instance. The factory holds the resolved
configuration and the exporters, so configuration is per handler while record
state is per invocation. Its type is `DurableExecutionTelemetryPluginFactory`, exported from
the package root for annotating a value you hold.

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

`max_record_size_bytes` (default 256,000 bytes) limits the printed line: the
envelope, including the JSON escaping of `message`, in UTF-8 bytes without the
trailing newline. Truncation measures that line in one pass. A CloudWatch Logs
event can be up to 1 MB
([PutLogEvents](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html)).
U+2028 and U+2029 are written as `\u2028` and `\u2029`, so a tool that treats
them as line breaks still sees one line per record. A lone surrogate (a string
character with no UTF-8 encoding) is written as its `\uXXXX` escape, so
printing it never fails. The limit counts these escapes; other exporters'
limits count the characters as they write them.

The lines have `level` `INFO`. With the JSON log format and an application log
level of `WARN` or above, Lambda drops them. With the Text log format, Lambda
does not filter them.

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
A number DynamoDB cannot store (more than 38 significant digits, a magnitude
outside 1E-130 to 9.99...E+125, NaN or Infinity) is written as a string
attribute holding its JSON text, so it never drops the item. The same field can
therefore be a number (`N`) in one item and a string (`S`) in another; readers
that filter or compare on the attribute type should allow for both. An integer
with more than 38 digits but at most 38 significant digits, such as `10**38`,
is stored as a number.

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
> `flush()` run on one lazy background worker per registered factory. Checkpoint
> hooks only replace the latest pending snapshot and wake the worker. Consecutive
> `on-change` snapshots may coalesce while an export is in flight. Every
> sampled-in invocation end waits for a covering flush to complete after its
> drain starts, including an end that emitted no record. Overlapping ends may
> share a flush when it covers both entry snapshots. Only a sampled-out execution
> neither exports nor flushes. Exporter failures are logged and isolated; this is
> best-effort telemetry, not a guarantee of delivery to the destination.

> **Note (invocation-end latency under concurrency).** The drain an invocation
> end performs captures a finite snapshot of work already scheduled across the
> factory's executions. It waits until those records have been offered to the
> exporters and a covering flush completes. If a pending record is coalesced, its
> replacement satisfies the earlier obligation. Once the worker takes that
> replacement, later records for the same execution belong to a new queue slot.
> Later drains cannot extend an earlier flush target: they share a covering
> target or queue a subsequent one. Thus continuing arrivals do not move an
> earlier drain's finish line. One worker serializes exports and flushes, so
> invocation-end latency still includes other executions' work present at entry
> and any earlier flushes. Budget for that finite backlog, especially when an
> exporter makes a network call per record.

## Error names and causes

Every error in a record (the execution `error`, each `operations[].error`, and
each `operationsByName.<name>.error`) has the shape
`{"name": ..., "message": ..., "cause": ...}`, with keys in that order. `name`
is the SDK's error type, unchanged, and `message` is the error message. `cause`
has the same shape, recursively, and is present only when the SDK provides a
cause.

In Python, the plugin hooks carry only an `ErrorObject` (type, message, data,
stack trace) with no cause, so no error in a Python record has a `cause`: not
the execution `error`, not an operation error. The exporter does not build a
cause from other operations or from `ErrorData`. `include_errors=False`
removes operation errors.

Every `name` and `message` is capped at 8,192 characters, counted as Unicode
code points (a Python `str` is a sequence of code points, so a cut never splits
a character). A longer value keeps its first 8,192 code points followed by `…`.

When a record is over its exporter's size limit, truncation first removes every
`cause` and sets `droppedCauses: true`, then runs its usual steps (operation
results, then whole operations, then input, then output). Python records have
no cause, so the first step never changes them. With the name and message
caps, a record whose operations, input and output are dropped fits any limit of
64 KB or more, assuming ARN fields of normal length (a few hundred bytes, as in
real Lambda ARNs) and an error `name` of normal length (a type name such as
`module.QualName`). Without the `name` assumption, the worst case (a name and
message of 8,192 control characters, each escaped as `\u00XX`) fits any limit
of 128 KB (131,072 bytes) or more.

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

- `int`: unchanged when its absolute value is at most `2**53 - 1`
  (9007199254740991), the largest integer a JavaScript reader parses exactly.
  Otherwise `str(value)`, for example `"12345678901234567"`.
- `Decimal`: an integer when it is integral and its absolute value is at most
  `2**53 - 1`. Otherwise a float when the float round-trips exactly and is
  within DynamoDB's number range (magnitude 1E-130 to 9.99...E+125). Otherwise `str(value)`, for example `"9007199254740993"`,
  `"12345678901234567.89"` or `"1E+299"`. NaN and Infinity become `null`.
  Every zero, of any sign or exponent (`Decimal("-0")`, `Decimal("-0E+20")`),
  becomes `0` and loses its sign.
- `datetime` and `date`: `isoformat()`. `UUID`: string. `bytes`: base64.
  `tuple`: list. `BatchResult`: `to_dict()`.

Any other result is parsed as JSON, or kept as the raw string when it is not
JSON. In parsed JSON, NaN and Infinity become `null`, and an integer whose
absolute value is greater than `2**53 - 1` becomes its decimal string, as
above; nothing else changes. A
result that fails to decode keeps its parsed or raw value, and the rest of the
record is still emitted.

The hooks do not say which serializer wrote a result, so the envelope is
recognized by shape alone. A result written by `JsonSerDes` or a custom
`SerDes` that has this exact shape is decoded too: `{"t": "s", "v": "x"}` is
reported as `"x"`. Only the core serializer's known tags are decoded, and the
core may coerce `v` while decoding: `{"t": "b", "v": "yes"}` is reported as
`true`. An unknown tag, such as `{"t": "temperature", "v": 21.5}`, fails to
decode and passes through unchanged.

## Requirements

- `aws-durable-execution-sdk-python` with the plugin invocation hooks that
  surface `execution_input` / `execution_result` (included since the version
  this package declares as its minimum).
