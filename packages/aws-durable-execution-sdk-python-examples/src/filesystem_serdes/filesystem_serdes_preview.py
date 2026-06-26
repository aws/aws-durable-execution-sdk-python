"""Example demonstrating filesystem serdes with preview configuration.

When a preview generator is configured, the checkpoint envelope stores a compact
preview alongside the file pointer. This makes key fields visible in the console
and API without reading the full file from storage.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.filesystem_serdes import (
    FileSystemSerDesConfig,
    FileSystemSerDes,
)
from aws_durable_execution_sdk_python.preview import (
    PreviewConfig,
    PreviewField,
    PreviewMode,
    build_preview,
)


MOUNT_PATH = os.environ.get("FILESYSTEM_MOUNT_PATH", "/mnt/s3")


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Demonstrate filesystem serdes with preview for observability.

    The preview config controls which fields appear inline in the checkpoint:
    - include: fields to show
    - mask: fields to show with masked values (e.g., "***")
    - exclude: fields to never show (takes priority over mask)
    """
    # Configure filesystem serdes with preview
    fs_serdes = FileSystemSerDes(
        MOUNT_PATH,
        FileSystemSerDesConfig(
            generate_preview=lambda value: build_preview(
                value,
                PreviewConfig(
                    mode=PreviewMode.EXCLUDE_ALL,
                    include=[
                        PreviewField(name="order_id"),
                        PreviewField(name="status"),
                        PreviewField(name="total"),
                    ],
                    mask=[PreviewField(name="email")],
                ),
            ),
        ),
    )

    # Process an order — full data stored on filesystem,
    # but order_id, status, total, and masked email are visible in checkpoint
    order = context.step(
        lambda _: {
            "order_id": "ORD-12345",
            "status": "completed",
            "total": 99.99,
            "email": "customer@example.com",
            "items": [
                {"sku": "ITEM-001", "quantity": 2, "price": 29.99},
                {"sku": "ITEM-002", "quantity": 1, "price": 40.01},
            ],
            "shipping_address": {
                "street": "123 Main St",
                "city": "Seattle",
                "state": "WA",
            },
        },
        name="process_order",
        config=StepConfig(serdes=fs_serdes),
    )

    return {
        "order_id": order["order_id"],
        "status": order["status"],
        "item_count": len(order["items"]),
    }
