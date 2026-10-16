# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Custom durable operations for the AWS Durable Execution SDK for Python.

This package is experimental. Its API can change in any release.

Each operation composes the SDK's built-in operations and calls AWS APIs. The
operations live in this package, so the core SDK does not depend on those
APIs' clients. Import each operation from its module, for example
``aws_durable_execution_sdk_python_extras.microvm``.
"""

from aws_durable_execution_sdk_python_extras.__about__ import __version__


__all__ = ["__version__"]
