"""Demonstrates waitForCallback with custom serialization/deserialization."""

import json
from datetime import datetime
from typing import Any, Optional, TypedDict

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes


class CustomDataMetadata(TypedDict):
    """Metadata for CustomData."""

    version: str
    processed: bool


class CustomData(TypedDict):
    """Custom data structure with datetime."""

    id: int
    message: str
    timestamp: datetime
    metadata: CustomDataMetadata


class CustomSerdes(SerDes[CustomData]):
    """Custom serialization/deserialization for CustomData."""

    @staticmethod
    def serialize(data: CustomData, _=None) -> str:
        """Serialize CustomData to JSON string."""
        if data is None:
            return None

        serialized_data = {
            "id": data["id"],
            "message": data["message"],
            "timestamp": data["timestamp"].isoformat(),
            "metadata": data["metadata"],
            "_serializedBy": "custom-serdes-v1",
        }
        return json.dumps(serialized_data)

    @staticmethod
    def deserialize(data_str: str, _=None) -> CustomData:
        """Deserialize JSON string to CustomData."""
        if data_str is None:
            return None

        parsed = json.loads(data_str)
        return CustomData(
            id=parsed["id"],
            message=parsed["message"],
            timestamp=datetime.fromisoformat(
                parsed["timestamp"].replace("Z", "+00:00")
            ),
            metadata=CustomDataMetadata(
                version=parsed["metadata"]["version"],
                processed=parsed["metadata"]["processed"],
            ),
        )


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with custom serdes."""

    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(10),
        heartbeat_timeout=Duration.from_seconds(20),
        serdes=CustomSerdes(),
    )

    result: CustomData = context.wait_for_callback(
        lambda _callback_id, _context: None,
        name="custom-serdes-callback",
        config=config,
    )

    isDateObject = isinstance(result["timestamp"], datetime)
    # convert timestamp to isoformat because lambda only accepts default json type as result
    result["timestamp"] = result["timestamp"].isoformat()

    return {
        "receivedData": result,
        "isDateObject": isDateObject,
    }
