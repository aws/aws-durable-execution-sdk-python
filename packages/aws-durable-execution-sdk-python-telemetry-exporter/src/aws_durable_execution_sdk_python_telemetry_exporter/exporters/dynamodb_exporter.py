# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""DynamoDB Durable Execution Telemetry exporter."""

from __future__ import annotations

import decimal
import json
from typing import Any

from aws_durable_execution_sdk_python_telemetry_exporter.exporters._common import (
    compact_dumps,
)
from aws_durable_execution_sdk_python_telemetry_exporter.operations_index import (
    with_operations_by_name,
)


class DynamoDBExporter:
    """Writes ``operationsByName`` records to a DynamoDB table with PutItem.

    Numbers DynamoDB cannot store (more than 38 significant digits, a
    magnitude outside 1E-130 to 9.99...E+125, NaN or Infinity) are written as
    their JSON text in a string attribute, so such a value never drops the
    record. A field can therefore be a number in one item and a string in
    another. An integer with more than 38 digits but at most 38 significant
    ones (for example ``10**38``) is stored as a number.

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
        # deferred, same reason
        from boto3.dynamodb.types import DYNAMODB_CONTEXT, TypeSerializer

        self._serializer = TypeSerializer()
        self._context = DYNAMODB_CONTEXT

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

    def _number(self, text: str) -> decimal.Decimal | str:
        # TypeSerializer checks numbers with DYNAMODB_CONTEXT, which traps any
        # inexact, rounded, overflowing or underflowing value. Check the same
        # way, plus DynamoDB's own magnitude range (1E-130 to 9.99...E+125),
        # and keep a number either would reject as its JSON text.
        try:
            number = self._context.create_decimal(text)
        except decimal.DecimalException:
            # An integer with trailing zeros, such as 10**38, has more than 38
            # digits but few significant ones; its normalized form fits.
            digits = text.removeprefix("-")
            significant = digits.rstrip("0")
            if not digits.isdigit() or len(significant) > 38:
                return text
            sign = text[: len(text) - len(digits)]
            exponent = len(digits) - len(significant)
            number = decimal.Decimal(f"{sign}{significant}E{exponent}")
            return number if number.adjusted() <= 125 else text
        if number and not -130 <= number.adjusted() <= 125:
            return text
        return decimal.Decimal(text)

    def _marshal(self, item: dict[str, Any]) -> dict[str, Any]:
        # DynamoDB numbers must be ``Decimal``; a JSON round trip converts every
        # number and leaves the rest of the record untouched.
        plain = json.loads(
            compact_dumps(item),
            parse_float=self._number,
            parse_int=self._number,
            parse_constant=str,
        )
        return {key: self._serializer.serialize(value) for key, value in plain.items()}
