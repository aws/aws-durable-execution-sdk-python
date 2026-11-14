# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Compose the shared LMI adapter with Python fixture policies."""

from aws_durable_execution_conformance_tests.lmi.adapter import SharedAdapter
from lmi_tests.cloud import Cloud
from lmi_tests.shared_contract import configuration_identity
from lmi_tests.shared_evidence import build_evidence
from lmi_tests.shared_scenarios import ScenarioDriver


def create_adapter(manifest):
    return SharedAdapter(
        manifest,
        cloud_factory=Cloud,
        driver_factory=ScenarioDriver,
        evidence_builder=build_evidence,
        configuration_identity=configuration_identity,
    )
