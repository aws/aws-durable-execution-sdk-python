# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Python expected outcomes and worker identity for shared evidence normalization."""

from aws_durable_execution_conformance_tests.lmi.source_evidence import (
    build_evidence as shared_build_evidence,
)


def expected_outcome(item, scenario):
    fixture = item.get("payload", {}).get("scenario")
    if fixture == "failure":
        return {
            "status": "FAILED",
            "error": {
                "type": "ValueError",
                "message": "expected:" + item["marker"],
            },
        }
    elif fixture == "deadline" and scenario in {
        "deadline-during-step",
        "peer-recovery",
    }:
        # This fixture asserts native deadline/retry behavior. Its logical
        # execution may still be active or may finish after normal release.
        return {
            "allowedStatuses": [
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "TIMED_OUT",
                "STOPPED",
            ]
        }
    else:
        return {"status": "SUCCEEDED", "result": item["marker"]}


def build_evidence(context, cloud, *, phase="observations"):
    return shared_build_evidence(
        context,
        cloud,
        runtime="python",
        expected_outcome=expected_outcome,
        worker_identity=lambda raw: raw["process"],
        phase=phase,
    )
