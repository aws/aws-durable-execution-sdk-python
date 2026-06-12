"""Token models."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class CheckpointToken:
    """Model a checkpoint token. This isn't exactly the same format as the actual svc, but it will do for testing purposes."""

    execution_arn: str
    token_sequence: int

    def to_str(self) -> str:
        data = {"arn": self.execution_arn, "seq": self.token_sequence}
        json_str = json.dumps(data, separators=(",", ":"))
        # str -> bytes -> base64 bytes -> str
        return base64.b64encode(json_str.encode()).decode()

    @classmethod
    def from_str(cls, token: str) -> CheckpointToken:
        # str -> base64 bytes -> str
        decoded = base64.b64decode(token).decode()
        data = json.loads(decoded)
        return cls(execution_arn=data["arn"], token_sequence=data["seq"])


@dataclass(frozen=True)
class CallbackToken:
    """Model a callback token."""

    execution_arn: str
    operation_id: str

    def to_str(self) -> str:
        data = {"arn": self.execution_arn, "op": self.operation_id}
        json_str = json.dumps(data, separators=(",", ":"))
        # str -> bytes -> base64 bytes -> str
        return base64.b64encode(json_str.encode()).decode()

    @classmethod
    def from_str(cls, token: str) -> CallbackToken:
        # str -> base64 bytes -> str
        decoded = base64.b64decode(token).decode()
        data = json.loads(decoded)
        return cls(execution_arn=data["arn"], operation_id=data["op"])
