"""Example demonstrating filesystem serdes in OVERFLOW mode.

In OVERFLOW mode, small values are stored inline in the checkpoint (as JSON),
and only values exceeding the ~256KB checkpoint size limit overflow to a file.
This is ideal for mixed workloads where most payloads are small but some may
be large.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.filesystem_serdes import (
    FileSystemSerDesConfig,
    FileSystemSerDesMode,
    FileSystemSerDes,
)


MOUNT_PATH = os.environ.get("FILESYSTEM_MOUNT_PATH", "/mnt/s3")


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Demonstrate OVERFLOW mode — inline for small, file for large.

    This example shows that:
    - Small step results stay inline in the checkpoint (fast, no file I/O)
    - Large step results automatically overflow to the filesystem
    """
    # OVERFLOW mode: inline if small, file if > ~256KB
    fs_serdes = FileSystemSerDes(
        MOUNT_PATH,
        FileSystemSerDesConfig(storage_mode=FileSystemSerDesMode.OVERFLOW),
    )

    # Step 1: Small payload — stays inline in checkpoint
    small_result = context.step(
        lambda _: {"status": "ok", "count": 42},
        name="small_step",
        config=StepConfig(serdes=fs_serdes),
    )

    # Step 2: Large payload — overflows to filesystem
    large_result = context.step(
        lambda _: {
            "records": [
                {"id": i, "data": "x" * 1000}
                for i in range(300)  # ~300KB payload
            ]
        },
        name="large_step",
        config=StepConfig(serdes=fs_serdes),
    )

    return {
        "small_status": small_result["status"],
        "large_record_count": len(large_result["records"]),
    }
