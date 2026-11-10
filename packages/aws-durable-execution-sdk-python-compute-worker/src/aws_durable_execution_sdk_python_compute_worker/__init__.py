# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Runs jobs from AWS Lambda durable functions on compute that is defined
outside the durable function.

This package is experimental. Its API can change in any release.

Each compute type has its own subpackage. The worker for AWS Lambda MicroVMs
is in :mod:`aws_durable_execution_sdk_python_compute_worker.microvm`.
"""

from aws_durable_execution_sdk_python_compute_worker.__about__ import __version__


__all__ = ["__version__"]
