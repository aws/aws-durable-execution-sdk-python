# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Bind shared lifecycle choreography to Python fixture entry points."""

from aws_durable_execution_conformance_tests.lmi.scenarios import (
    ScenarioDriver as SharedScenarioDriver,
)

FIXTURES = {
    "native-overlap": "barrier",
    "normal-return": "success",
    "deadline-during-step": "deadline",
    "peer-recovery": "deadline",
    "suspension-retry": "replay",
    "warm-reuse": "warm-owner",
    "normal-failure": "failure",
}


class ScenarioDriver(SharedScenarioDriver):
    def __init__(self, context, cloud, **options):
        super().__init__(
            context,
            cloud,
            runtime="python",
            fixtures={
                **FIXTURES,
                "peer": "barrier",
                "recovery": "barrier",
                "warm": "warm-probe",
            },
            **options,
        )
