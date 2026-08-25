# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""First-party Workflow Insight exporters.

One module per exporter, mirroring the JS package's ``src/exporters/`` layout
(``aws-durable-execution-sdk-js-insight``). Each destination lives in its own
module so the set can grow to the full JS parity surface (S3, CloudWatch Logs,
DynamoDB, Firehose, EventBridge, SQS, OpenSearch, Redshift, Aurora, HTTP, OTel,
file, ...) without any single file accreting every backend's imports and
optional dependencies.

Concrete exporters are re-exported here so the public import path is stable:
``from aws_durable_execution_sdk_python_insight.exporters import S3Exporter``
keeps working exactly as before this package was split out of a single module.
Shared serialization helpers live in the private ``_common`` module.

Both shipped exporters serialize the curated record with JS-compatible compact
JSON (no whitespace) so the wire bytes match across SDKs. Records are written
verbatim -- no synthetic emission.
"""

from __future__ import annotations

from aws_durable_execution_sdk_python_insight.exporters.lambda_log_exporter import (
    LambdaLogExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.s3_exporter import (
    S3Exporter,
)


__all__ = [
    "LambdaLogExporter",
    "S3Exporter",
]
