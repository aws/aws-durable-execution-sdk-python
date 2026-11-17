# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The types of ``agentcore_session``: run several jobs in one AgentCore
Runtime session."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import DurableContext

    from aws_durable_execution_sdk_python_extras.compute.agentcore.config import (
        AgentcoreInvokeOptions,
    )


class AgentcoreSession(Protocol):
    """The handle that an :func:`agentcore_session` handler receives.

    Each :meth:`invoke` creates its durable operations in the context that
    the caller passes. Parallel :meth:`invoke` calls on one session run in
    the same microVM, so the job handlers must allow it.
    """

    @property
    def runtime_session_id(self) -> str:
        """The ``runtimeSessionId`` that every job of the session uses."""
        ...

    def invoke(
        self,
        context: DurableContext,
        name: str,
        *,
        job_input: Any,
        options: AgentcoreInvokeOptions,
    ) -> Any:
        """Send one job to the session, and return the job's result.

        Args:
            context: The context to create the job's durable operations in:
                the session's child context, or a context nested in it.
            name: The job's child context name.
            job_input: The job's input. It must be JSON-serializable, and the
                same on every replay.
            options: The timeouts of this job.

        Returns:
            The value that the agent passed to
            ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

        Raises:
            AgentcoreJobFailedError: When the agent reports a failure.
            AgentcoreTimeoutError: When the job or its heartbeat times out.
            AgentcoreDeliveryError: When the request fails after all
                retries, or the agent does not accept the job.
        """
        ...
