# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The subtypes that :func:`agentcore` and :func:`agentcore_session`
record on their durable operations."""

from __future__ import annotations

from enum import StrEnum


class AgentcoreOperationSubType(StrEnum):
    """The subtypes that ``agentcore`` and ``agentcore_session`` record on
    their durable operations.

    Execution history and plugins can filter on them without parsing
    operation names. The values equal ``AgentcoreOperationSubType`` in the
    JavaScript SDK.

    Replay compares each subtype with the checkpoint. So a changed value
    breaks the executions in flight, and these values must stay fixed.
    """

    AGENTCORE = "Agentcore"
    """The child context of an ``agentcore`` call."""
    SESSION = "AgentcoreSession"
    """The child context of an ``agentcore_session`` call."""
    SESSION_JOB = "AgentcoreSessionJob"
    """The child context of one :meth:`AgentcoreSession.invoke` job."""
    SESSION_ID = "AgentcoreSessionId"
    """The step that generates and checkpoints the runtime session ID."""
    CALLBACK = "AgentcoreCallback"
    """The callback that the agent completes with the job result."""
    REQUEST = "AgentcoreRequest"
    """The step that calls ``InvokeAgentRuntime`` with the job."""
    STOP = "AgentcoreStop"
    """The step that calls ``StopRuntimeSession``."""
