# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Runs jobs from AWS Lambda durable functions inside an AWS Lambda MicroVM.

This package is experimental. Its API can change in any release.

The exports match the JavaScript worker's ``index.ts``. ``CancelScope`` and
``CallCancelledError`` are extra, because the public ``heartbeat()``
signature needs them, and Python has no ``AbortSignal``.
"""

from aws_durable_execution_sdk_python_microvm_worker.__about__ import __version__
from aws_durable_execution_sdk_python_microvm_worker.callback_reporter import (
    MAX_CALLBACK_RESULT_BYTES,
    CallbackReporter,
    CallCancelledError,
    CancelScope,
    ResultSerializationError,
    ResultTooLargeError,
    is_terminal_callback_error,
)
from aws_durable_execution_sdk_python_microvm_worker.logger import (
    MicrovmWorkerLogger,
)
from aws_durable_execution_sdk_python_microvm_worker.payload import (
    SUPPORTED_PAYLOAD_VERSION,
    InvalidRunHookPayloadError,
    MicrovmJobDocument,
    MicrovmJobRequest,
    MicrovmRunHookPayload,
    RunHookRequest,
)


__all__ = [
    "MAX_CALLBACK_RESULT_BYTES",
    "SUPPORTED_PAYLOAD_VERSION",
    "CallCancelledError",
    "CallbackReporter",
    "CancelScope",
    "InvalidRunHookPayloadError",
    "MicrovmJobDocument",
    "MicrovmJobRequest",
    "MicrovmRunHookPayload",
    "MicrovmWorkerLogger",
    "ResultSerializationError",
    "ResultTooLargeError",
    "RunHookRequest",
    "__version__",
    "is_terminal_callback_error",
]
