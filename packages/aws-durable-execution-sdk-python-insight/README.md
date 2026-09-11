# AWS Durable Execution SDK for Python — Workflow Insight plugin

Workflow Insight instrumentation plugin for the AWS Durable Execution SDK for
Python. A port of the JavaScript SDK's `workflowInsight()` plugin: it listens to
the SDK's instrumentation hooks and emits one curated `WorkflowInsight` record
per execution to the configured exporters. The wire record keeps the JS
camelCase field names so records read identically across SDKs.

> **Experimental.** Like its JS counterpart, this plugin is experimental and may
> change or be removed in future releases.

## Install

```bash
pip install aws-durable-execution-sdk-python-insight
# with the S3 exporter's local-dev dependency:
pip install "aws-durable-execution-sdk-python-insight[s3]"
```

## Usage

```python
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python_insight import (
    WorkflowInsightConfig,
    workflow_insight,
)
from aws_durable_execution_sdk_python_insight.exporters import S3Exporter

@durable_execution(
    plugins=[
        workflow_insight(
            WorkflowInsightConfig(
                exporters=[
                    S3Exporter(bucket="my-bucket", prefix="workflow-insight/")
                ],
            )
        )
    ]
)
def handler(event, context):
    ...
```

With no exporter configured, records are written to the function's own
CloudWatch log group as single JSON lines (the `LambdaLogExporter` default),
carrying the name-keyed `operationsByName` summary. The `S3Exporter` writes the
lossless per-occurrence `operations` array, one object per execution
(upsert-by-execution-name, so re-emission overwrites rather than appends).

## Exporters

All exporters live in `aws_durable_execution_sdk_python_insight.exporters` and
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

### CloudWatchLogsExporter

Writes one `PutLogEvents` event per record to `log_group_name`, in a stream
named `{log_stream_prefix}{YYYY}/{MM}/{DD}` (default prefix `workflow-insight/`).
IAM: `logs:CreateLogStream`, `logs:PutLogEvents` on the log group.

```python
CloudWatchLogsExporter(log_group_name="/custom/workflow-insight")
```

### DynamoDBExporter

`PutItem` keyed by `partition_key` (default `pk`) = `executionArn`. With the
default `sort_key="sk"` (= `emittedAt`) every export adds an item; pass
`sort_key=None` for a key-only table that upserts. IAM: `dynamodb:PutItem`.

```python
DynamoDBExporter(table_name="workflow-insight")
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
    database="insight",
    engine="postgresql",
)
```

### RedshiftExporter

`MERGE` by `execution_arn` through the Redshift Data API into
`{schema}.{table}` (default `public.workflow_insight`, same columns as Aurora,
`record_json` as `SUPER`). Provide `workgroup_name` (Serverless) or
`cluster_identifier` (provisioned, with `db_user` or `secret_arn`). IAM:
`redshift-data:ExecuteStatement` plus `redshift-serverless:GetCredentials` or
`secretsmanager:GetSecretValue`. The statement is submitted and not awaited, so
statement failures are not reported and `on-change` records may land out of
order; prefer the default `on-complete` emit mode with this exporter.

```python
RedshiftExporter(database="insight", workgroup_name="insight-wg")
```

### OpenSearchExporter

`PUT {endpoint}/{index_name}/_doc/{executionArn}` (default index
`workflow-insight`). `auth="sigv4"` (default) signs with the runtime's
credentials via botocore; `auth="basic"` uses `username`/`password`. IAM:
`es:ESHttpPut` on the domain.

```python
OpenSearchExporter(endpoint="https://my-domain.us-east-1.es.amazonaws.com", region="us-east-1")
```

### FirehoseExporter

`PutRecord` of one JSON line (trailing newline) per record. IAM:
`firehose:PutRecord`.

```python
FirehoseExporter(delivery_stream_name="workflow-insight-stream")
```

### EventBridgeExporter

`PutEvents` with `Source` (default `aws.durable-execution.insight`),
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
SQSExporter(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/insight")
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
`timeout_ms` defaults to 10000. No IAM.

```python
HttpExporter(url="https://hooks.example.com/insight", headers={"Authorization": "Bearer ..."})
```

### FileExporter

`mode="ndjson"` (default) appends to `{directory}/{YYYY-MM-DD}.ndjson`;
`mode="json"` writes `{directory}/{executionName}.json`, overwriting on update.
For Lambda use an EFS mount (IAM: `elasticfilesystem:ClientMount`,
`elasticfilesystem:ClientWrite`) or `/tmp` for testing. Appends from many
concurrent environments to one NDJSON file on a network file system can
interleave; use `mode="json"` when several environments share a directory.

```python
FileExporter(directory="/mnt/efs/workflow-insight")
```

Emission behavior, record schema (`recordType: WorkflowInsight`,
`schemaVersion: "1.0"`), sampling, content configuration (input/output
omission, `include_errors`, per-operation result opt-in), truncation phases,
and `top-level` vs `full-tree` operation detail all mirror the JS plugin.
Behavior is validated cross-SDK by the `insight` conformance suite
(`aws-durable-execution-conformance-tests-insight`).

> **Note (asynchronous export).** Export rendering, truncation, `export()`, and
> `flush()` run on one lazy background worker per plugin. Checkpoint hooks only
> replace the latest pending snapshot and wake the worker. Consecutive
> `on-change` snapshots may coalesce while an export is in flight. An invocation
> that emits a record drains the latest snapshot and flushes exporters before it
> returns; invocations that emit nothing do not start or flush the worker.

## Requirements

- `aws-durable-execution-sdk-python` with the plugin invocation hooks that
  surface `execution_input` / `execution_result` (included since the version
  this package declares as its minimum).
