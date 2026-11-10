# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The errors that the ``decider`` and ``llm`` operations raise.

This module is experimental. Its API can change in any release.

Every error is an :class:`AiError`. The operation raises the same class on the
first run and on every replay:

1. Inside the step, an attempt raises a subclass, such as
   :class:`DeciderError`. The step retry strategy sees that error.
2. When the step fails, the SDK checkpoints the error with the class's
   fully-qualified name as the error type, and its message and data.
3. The SDK raises ``StepError``. It rebuilds the error as ``__cause__`` from
   its registry of error types. This module registers every class with that
   registry.
4. The operation raises a copy of ``__cause__``, from the ``StepError``.

A field such as :attr:`DeciderError.status` is stored in ``data`` as JSON. So
it is the same on replay.

The fully-qualified name is part of the checkpoint format. So the classes must
stay in this module, with these names.
"""

from __future__ import annotations

import json
from typing import Any

from aws_durable_execution_sdk_python.exceptions import (
    DurableOperationError,
    register_operation_error,
)


def _data_field(data: str | None, key: str) -> Any:
    if data is None:
        return None
    try:
        fields = json.loads(data)
    except ValueError:
        return None
    return fields.get(key) if isinstance(fields, dict) else None


class AiError(DurableOperationError):
    """The base class of every error that ``decider`` and ``llm`` raise.

    Experimental. It can change or be removed in any release.
    """


class DeciderError(AiError):
    """The decision model could not be reached, or answered with an error.

    Experimental. It can change or be removed in any release.

    :attr:`status` is the HTTP status when the model answered. It is ``None``
    when the model could not be reached. The default retry strategy retries
    no status, 408, 429, and 5xx. It does not retry another status, such as
    422 for an input that does not fit the model's window.
    """

    def __init__(
        self,
        message: str | None = None,
        error_type: str | None = None,
        data: str | None = None,
        stack_trace: list[str] | None = None,
        *,
        status: int | None = None,
    ) -> None:
        if status is not None and data is None:
            data = json.dumps({"status": status})
        super().__init__(
            message=message, error_type=error_type, data=data, stack_trace=stack_trace
        )

    @property
    def status(self) -> int | None:
        """The HTTP status, when the decision model answered."""
        value = _data_field(self.data, "status")
        return value if isinstance(value, int) else None


class DeciderInvalidResponseError(AiError):
    """The decision model answered, but the answer is not usable.

    Experimental. It can change or be removed in any release.

    The reply is not JSON, has the wrong shape, names an option that the
    caller did not offer, or has a score outside the scale. This usually
    means that the server speaks another version of the ``/v1/systemone``
    API. It is not retried.
    """


class AiTimeoutError(AiError):
    """A request was still running 10 seconds before the invocation ends.

    Experimental. It can change or be removed in any release.

    The step stops waiting, so it can record the failure. A later attempt runs
    in a new invocation. So the default retry strategy retries it.
    """


class LlmStoppedError(AiError):
    """The text model stopped for a reason other than the end of its reply.

    Experimental. It can change or be removed in any release.

    Examples are a guardrail that intervened and a filtered reply. The reply is
    not returned. :attr:`stop_reason` is the Converse ``stopReason``. The
    default retry strategy retries ``malformed_model_output`` and
    ``malformed_tool_use``, because a new reply can succeed. It does not retry
    the other reasons, because the same input stops the same way.
    """

    def __init__(
        self,
        message: str | None = None,
        error_type: str | None = None,
        data: str | None = None,
        stack_trace: list[str] | None = None,
        *,
        stop_reason: str | None = None,
    ) -> None:
        if stop_reason is not None:
            data = data or json.dumps({"stop_reason": stop_reason})
            message = message or (
                f'The model stopped with the reason "{stop_reason}". '
                "The reply is not returned."
            )
        super().__init__(
            message=message, error_type=error_type, data=data, stack_trace=stack_trace
        )

    @property
    def stop_reason(self) -> str | None:
        """The Converse ``stopReason``, such as ``guardrail_intervened``."""
        value = _data_field(self.data, "stop_reason")
        return value if isinstance(value, str) else None


class LlmMaxTokensError(AiError):
    """The text model's reply stopped at the ``max_tokens`` limit.

    Experimental. It can change or be removed in any release.

    A retry with the same limit stops at the same place. So it is not retried.
    Raise ``max_tokens`` instead.
    """


for _error_class in (
    AiError,
    DeciderError,
    DeciderInvalidResponseError,
    AiTimeoutError,
    LlmStoppedError,
    LlmMaxTokensError,
):
    # Each class takes the four keywords that the registry passes: message,
    # error_type, data, and stack_trace.
    register_operation_error(_error_class)
