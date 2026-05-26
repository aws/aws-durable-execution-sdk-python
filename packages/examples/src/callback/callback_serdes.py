"""Demonstrates createCallback with custom serialization/deserialization for Date objects."""

import json
from datetime import datetime, timezone
from typing import Any, Optional

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class CustomData:
    """Data structure with datetime."""

    def __init__(self, id: int, message: str, timestamp: datetime):
        self.id = id
        self.message = message
        self.timestamp = timestamp

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "CustomData":
        """Create from dictionary."""
        return CustomData(
            id=data["id"],
            message=data["message"],
            timestamp=datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")),
        )


class CustomDataSerDes(SerDes[CustomData]):
    """Custom serializer for CustomData that handles datetime conversion."""

    def serialize(self, value: Optional[CustomData], _: SerDesContext) -> Optional[str]:
        """Serialize CustomData to JSON string."""
        if value is None:
            return None
        return json.dumps(value.to_dict())

    def deserialize(
        self, payload: Optional[str], _: SerDesContext
    ) -> Optional[CustomData]:
        """Deserialize JSON string to CustomData."""
        if payload is None:
            return None
        data = json.loads(payload)
        return CustomData.from_dict(data)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating createCallback with custom serdes."""
    callback_config = CallbackConfig(
        timeout=Duration.from_seconds(30),
        serdes=CustomDataSerDes(),
    )

    callback = context.create_callback(
        name="custom-serdes-callback",
        config=callback_config,
    )

    result: CustomData = callback.result()

    return {
        "receivedData": result.to_dict(),
        "isDateObject": isinstance(result.timestamp, datetime),
    }
