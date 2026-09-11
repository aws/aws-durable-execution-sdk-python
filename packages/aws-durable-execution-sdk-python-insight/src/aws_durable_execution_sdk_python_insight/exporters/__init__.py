# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""First-party Workflow Insight exporters.

One module per destination, so no single file accretes every backend's imports
and optional dependencies. Concrete exporters are re-exported here so the
public import path is stable:
``from aws_durable_execution_sdk_python_insight.exporters import S3Exporter``.
Shared serialization and transport helpers live in the private ``_common``
module.

Every exporter serializes the curated record as compact JSON (no whitespace,
non-ASCII preserved). Records are written verbatim -- no synthetic emission.
"""

from __future__ import annotations

from aws_durable_execution_sdk_python_insight.exporters.aurora_exporter import (
    AuroraEngine,
    AuroraExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.cloudwatch_logs_exporter import (
    CloudWatchLogsExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.dynamodb_exporter import (
    DynamoDBExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.eventbridge_exporter import (
    EventBridgeExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.file_exporter import (
    FileExporter,
    FileMode,
)
from aws_durable_execution_sdk_python_insight.exporters.firehose_exporter import (
    FirehoseExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.http_exporter import (
    HttpExporter,
    HttpMethod,
)
from aws_durable_execution_sdk_python_insight.exporters.lambda_log_exporter import (
    LambdaLogExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.opensearch_exporter import (
    OpenSearchAuth,
    OpenSearchExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.otel_exporter import (
    OTelExporter,
    OTelProtocol,
)
from aws_durable_execution_sdk_python_insight.exporters.redshift_exporter import (
    RedshiftExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.s3_exporter import (
    S3Exporter,
    S3Partitioning,
)
from aws_durable_execution_sdk_python_insight.exporters.sqs_exporter import (
    SQSExporter,
)


__all__ = [
    "AuroraEngine",
    "AuroraExporter",
    "CloudWatchLogsExporter",
    "DynamoDBExporter",
    "EventBridgeExporter",
    "FileExporter",
    "FileMode",
    "FirehoseExporter",
    "HttpExporter",
    "HttpMethod",
    "LambdaLogExporter",
    "OTelExporter",
    "OTelProtocol",
    "OpenSearchAuth",
    "OpenSearchExporter",
    "RedshiftExporter",
    "S3Exporter",
    "S3Partitioning",
    "SQSExporter",
]
