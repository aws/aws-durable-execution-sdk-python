"""Example demonstrating basic filesystem serdes usage.

This example shows how to use create_filesystem_serdes to store step results
on a durable filesystem (e.g., Amazon S3 Files or EFS mounted to Lambda).

In ALWAYS mode (the default), every step result is written to a file on the
mounted filesystem, and only a file pointer is stored in the checkpoint. This
keeps checkpoint sizes small regardless of payload size.

WARNING: This requires a durable filesystem mount (S3 Files or EFS).
Do NOT use /tmp — it is ephemeral and not shared across invocations.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.filesystem_serdes import (
    FileSystemSerdesConfig,
    FileSystemSerdesMode,
    create_filesystem_serdes,
)


# Mount path for the durable filesystem (S3 Files or EFS)
# In production, this would be /mnt/s3 or /mnt/efs
MOUNT_PATH = os.environ.get("FILESYSTEM_MOUNT_PATH", "/mnt/s3")


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Process data using filesystem serdes for large payloads.

    This example demonstrates:
    1. Creating a filesystem serdes instance
    2. Using it with context.step() via StepConfig
    3. The serdes transparently writes results to mounted storage
    """
    # Create filesystem serdes - ALWAYS mode writes every result to file
    fs_serdes = create_filesystem_serdes(MOUNT_PATH)

    # Step 1: Generate some data (result stored on filesystem)
    data = context.step(
        lambda _: {
            "users": [
                {"id": i, "name": f"user_{i}", "email": f"user{i}@example.com"}
                for i in range(10)
            ],
            "metadata": {"total": 10, "page": 1},
        },
        name="generate_data",
        config=StepConfig(serdes=fs_serdes),
    )

    # Step 2: Process the data (result also stored on filesystem)
    processed = context.step(
        lambda _: {
            "processed_count": len(data["users"]),
            "user_ids": [u["id"] for u in data["users"]],
        },
        name="process_data",
        config=StepConfig(serdes=fs_serdes),
    )

    return {
        "success": True,
        "processed_count": processed["processed_count"],
        "user_ids": processed["user_ids"],
    }
