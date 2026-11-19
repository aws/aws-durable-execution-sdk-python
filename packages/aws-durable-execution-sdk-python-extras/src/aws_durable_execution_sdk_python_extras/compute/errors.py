# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The base class of the errors that the compute operations raise."""

from __future__ import annotations

from aws_durable_execution_sdk_python.exceptions import DurableOperationError


class ComputeError(DurableOperationError):
    """The base class of every error that a compute operation raises.

    Catch it to handle a failure of any compute type the same way. Each compute
    type has its own subclass, such as ``MicrovmError`` for the MicroVM
    operations. That subclass has one subclass per kind of failure.

    No operation raises this class itself. Each subclass records its own
    fully-qualified name as the error type in the checkpoint. So this class is
    not registered with the SDK's registry of error types.
    """
