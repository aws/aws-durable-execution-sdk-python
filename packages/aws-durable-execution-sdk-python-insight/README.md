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

Emission behavior, record schema (`recordType: WorkflowInsight`,
`schemaVersion: "1.0"`), sampling, content configuration (input/output
omission, `include_errors`, per-operation result opt-in), truncation phases,
and `top-level` vs `full-tree` operation detail all mirror the JS plugin.
Behavior is validated cross-SDK by the `insight` conformance suite
(`aws-durable-execution-conformance-tests-insight`).

## Requirements

- `aws-durable-execution-sdk-python` with the plugin invocation hooks that
  surface `execution_input` / `execution_result` (included since the version
  this package declares as its minimum).
