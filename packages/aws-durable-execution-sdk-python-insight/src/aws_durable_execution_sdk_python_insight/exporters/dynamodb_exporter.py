# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""DynamoDB Workflow Insight exporter."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import compact_dumps
from aws_durable_execution_sdk_python_insight.operations_index import (
    with_operations_by_name,
)


class DynamoDBExporter:
    """Writes ``operationsByName`` records to a DynamoDB table with PutItem.

    The partition key holds ``executionArn``. With the default sort key
    (``sk`` = ``emittedAt``) every export adds a new item, keeping the full
    history. With ``sort_key=None`` later exports overwrite the item.
    """

    def __init__(
        self,
        table_name: str,
        partition_key: str = "pk",
        sort_key: str | None = "sk",
        region: str | None = None,
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.table_name = table_name
        self.partition_key = partition_key
        # ``None`` and ``""`` both disable the sort key.
        self.sort_key = sort_key or None
        self.max_record_size_bytes: int | None = (
            400_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("dynamodb", region_name=region)
                if region
                else boto3.client("dynamodb")
            )
        from boto3.dynamodb.types import TypeSerializer  # deferred, same reason

        self._serializer = TypeSerializer()

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return with_operations_by_name(record)

    def export(self, record: dict[str, Any]) -> None:
        item = self.render(record)
        item[self.partition_key] = record["executionArn"]
        if self.sort_key:
            item[self.sort_key] = record["emittedAt"]
        self._client.put_item(TableName=self.table_name, Item=self._marshal(item))

    def flush(self) -> None:
        return None

    def _marshal(self, item: dict[str, Any]) -> dict[str, Any]:
        # DynamoDB numbers must be ``Decimal``; a JSON round trip converts every
        # float and leaves the rest of the record untouched.
        plain = json.loads(compact_dumps(item), parse_float=Decimal)
        return {key: self._serializer.serialize(value) for key, value in plain.items()}
