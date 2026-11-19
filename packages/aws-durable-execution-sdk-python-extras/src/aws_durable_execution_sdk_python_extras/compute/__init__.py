# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Durable operations that run jobs on compute outside the durable function.

This package is experimental. Its API can change in any release.

Each compute type has its own module, such as
``aws_durable_execution_sdk_python_extras.compute.microvm``. Every error that
a compute operation raises is a :class:`ComputeError`.
"""

from aws_durable_execution_sdk_python_extras.compute.errors import ComputeError


__all__ = ["ComputeError"]
