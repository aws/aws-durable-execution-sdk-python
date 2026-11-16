# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The job document that the operation sends to the agent runtime.

The document has the fields of the MicroVM job request, without
``microvmId``. So the worker parses both with the same code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aws_durable_execution_sdk_python_extras.compute.microvm.job import (
    PAYLOAD_VERSION,
    dumps,
)


@dataclass(frozen=True, kw_only=True)
class AgentcoreJobRequest:
    """The ``payload`` of the ``InvokeAgentRuntime`` request.

    Wire names: ``version``, ``region``, ``callbackId``,
    ``heartbeatTimeoutSeconds``, and ``input``.
    """

    region: str
    """The Region of the durable function, for the callback API calls.

    The agent runtime can run in another Region than the durable function.
    So the worker takes the Region of the callback calls from the document.
    """
    callback_id: str
    """The callback that the worker completes with the job's outcome."""
    job_input: Any
    """The caller's input. It must be JSON-serializable."""
    heartbeat_timeout_seconds: int | None = None
    """The heartbeat timeout, when the caller set one."""

    def to_json(self) -> str:
        """Serialize the request as compact JSON, as the MicroVM job request.

        Raises:
            TypeError: When the input is not JSON-serializable.
            ValueError: When the input holds ``nan`` or an infinite float.
        """
        document: dict[str, Any] = {
            "version": PAYLOAD_VERSION,
            "region": self.region,
            "callbackId": self.callback_id,
        }
        if self.heartbeat_timeout_seconds is not None:
            document["heartbeatTimeoutSeconds"] = self.heartbeat_timeout_seconds
        document["input"] = self.job_input
        return dumps(document)
