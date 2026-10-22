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

- `error.name`: see [Error names](#error-names) below.
- An opted-in operation `result` (`operations[].result`,
  `operationsByName.<name>.result`, and the value passed to an override's
  `result` transform) is the plain JSON value, for example `{"amount": 99.99}`.
  Results checkpointed by the SDK's default serializer as a `{"t": ..., "v":
  ...}` envelope are decoded with that serializer and converted to JSON values:
  `datetime`/`date` become an ISO-8601 string from `isoformat()`, `UUID` a
  string, `bytes` a base64 string, `tuple` a list, and a map/parallel
  `BatchResult` its `to_dict()` form. A `Decimal` keeps its numeric value, but
  not its scale or trailing zeros (`Decimal("1.10")` is `1.1`, `Decimal("1E+2")`
  is `100`): an integral one of at most 300 digits is a JSON integer, any other
  is a JSON number only when that float converts back to exactly the same
  `Decimal`, and otherwise the string `str(value)` (for example
  `"12345678901234567.89"`, or `"1E+5000"`). A non-finite `Decimal`
  or `float` (NaN, Infinity) becomes `null`, so the record is strict JSON. A
  payload that is not an envelope is passed through as parsed JSON (non-finite
  numbers again become `null`), or as the raw string when it is not JSON; an
  envelope that cannot be decoded is passed through as parsed JSON.
- The plugin cannot tell which serializer wrote a result, because the hooks do
  not carry it, so an envelope is recognised by its shape alone: a JSON object
  with exactly the keys `t` and `v`, where `t` is a string. A result written by
  `JsonSerDes`, a custom `SerDes` (which the plugin never runs) or a chained
  invoke that has this exact shape, and whose `t` is one of the default
  serializer's type tags, is decoded as if the default serializer wrote it. For
  example `{"t": "s", "v": "x"}` is reported as `"x"`, `{"t": "b", "v": 0}` as
  `false`, and `{"t": "l", "v": [...]}` as the decoded list. A shape-matching
  object whose `t` is not a default tag, or whose `v` does not decode, is
  passed through unchanged. Avoid a root object of exactly `{"t", "v"}` in
  results that you opt in when they do not use the default serializer.
- Every record built at invocation end carries `endTime` (the emission time)
  and `durationMs` (`endTime - startTime`), including the `RUNNING` record of
  an invocation that suspends or retries (`on-change` mode). Records built at
  invocation start or on an operation change carry neither. Do not use the
  presence of `endTime` as a completion signal; use `status`.

## Error names

The rule for the `error.name` field is the same in the JS, Python and Java
exporters. `error.message` is never changed by this rule. The rule applies to
the execution-level `error`, every `operations[].error`, and every
`operationsByName.<name>.error`.

1. A record names the error that caused the failure. A wrapper error that the
   SDK added is never the name, except in the bounded cases of rules 3 and 9.
2. The SDK unwraps an error only when the error's type is one of the SDK's own
   wrapper types. A user error is never unwrapped, even if it has a cause.
3. Unwrapping follows the chain of SDK wrappers. The depth counts unwrap steps,
   one per wrapper passed, not levels in the operation tree. At most 16 unwraps
   happen. If the error reached after 16 unwraps is still a wrapper, its name
   is reported. If the chain reaches an error it has already visited, the walk
   stops and the last error visited before the repeat is reported.
4. If the chain ends at a user error, the name is that error's type name.
5. If the chain ends at an SDK error that is not a wrapper, the name is that SDK
   error's simple class name. A callback timeout is an example.
6. A type name that is a qualified string is reduced to its last segment after
   any `.` or `$`. If that segment is empty, the string is kept as it is. A
   missing type, or a type made only of blank characters, is `Error`. The blank
   characters are the six ASCII whitespace characters: space, `\t`, `\n`,
   `\r`, `\f` and `\v`. No other character counts as blank, and names are
   never trimmed.
7. A failure gets the same name whether the record is built live or from a
   replayed checkpoint.
8. When the execution fails because an operation's error propagated, the
   execution `error.name` equals that operation's `error.name`.
9. If a checkpoint holds only a wrapper type, which an older SDK can write, the
   name is that wrapper's name. The exporter never guesses a cause from other
   operations.

A callback that is failed externally with an explicit `ErrorType` is named
after that type, after the reduction of rule 6. This includes the type
`"Error"`, which reports `Error`. Only a missing or blank type reports the SDK
callback-failure class.

User error types have the same name in every SDK, because the user names them.
SDK error types keep each language's own class name. For example, a callback
timeout is `CallbackTimeoutError` in JS and Python and
`CallbackTimeoutException` in Java. Every SDK name for a callback timeout
contains `Timeout`. An invoke timeout does not always: Java names it
`InvokeTimedOutException`.

How Python applies the rule:

- The core SDK does the unwrapping (rules 2 to 5) when it checkpoints an
  error: a failed step, child context, map item, parallel branch,
  `wait_for_callback` context and the execution itself record the type of the
  error that a wrapper wraps, not the wrapper. The SDK's wrapper types are
  `StepError`, `ChildContextError` (also used for map items and parallel
  branches), `InvokeError`, `WaitForConditionError`, `CallbackSubmitterError`,
  `CallbackExternalError` and the base `DurableOperationError`. Every other
  type is never unwrapped: user errors (including a user subclass of a
  wrapper), and SDK errors such as `CallbackTimeoutError`, `SerDesError`,
  `ValidationError` and `BatchCompletionError`.
- The exporter names each failed operation by its own `ErrorType`, reduced by
  rule 6, and the execution by the execution's own `ErrorType`, reduced. It
  does not match errors by message, stack trace or position in the tree. The
  depth of the operation tree does not matter: a step in 18 nested child
  contexts is `PaymentError` at every level.
- When two map items or parallel branches fail with different types and
  `throw_if_error()` raises, each item or branch keeps its own name, and the
  execution is named after the error that `throw_if_error()` raised.
- A checkpoint written by an older SDK can hold a wrapper type such as
  `aws_durable_execution_sdk_python.exceptions.StepError`. Its name is then the
  wrapper's simple class name, for example `StepError` (rule 9).
- A callback that times out is `CallbackTimeoutError` on the callback
  operation (the service type is `Callback.Timeout`), on the `wait_for_callback`
  context, and on the execution. A callback that an external system fails with
  an error type is named by that type's simple name, for example
  `PaymentError` for `com.acme.PaymentError`, and `Error` for `Error`. With
  no error type, or a blank one, it is `CallbackExternalError`, the class the
  SDK raises. A `wait_for_callback` submitter that fails with an empty message
  is reported with the SDK's message `Callback submitter failed` on the
  execution and is still named after the submitter's error.
- A user error class with no explicit name is named by its class name.

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
that is the `message` string. The envelope keys and the JSON escaping of
`message` add bytes on top of it, so the printed line can be up to about twice
the record size: escaping turns each `"` and `\` in the record into two bytes.
A typical record grows by about 20%. CloudWatch Logs accepts log events of up
to 1 MB (see the `PutLogEvents` API reference), so a line written for a record
at the default limit fits. A larger `max_record_size_bytes` should stay at or
below about 500 KB for this exporter to keep that margin.

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
