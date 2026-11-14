# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Python fixture configuration for the shared LMI harness."""

from aws_durable_execution_conformance_tests.lmi.runtime import RuntimeDefinition

RUNTIME = RuntimeDefinition("python", "lmi_tests.fixture.handler")
