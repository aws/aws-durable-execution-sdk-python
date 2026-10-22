# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Prevent emulator builds from racing their release's PyPI dependencies."""

from pathlib import Path

import yaml


WORKFLOWS = Path(__file__).parents[2] / "workflows"


def test_emulator_release_requires_successful_pypi_publication() -> None:
    pypi = yaml.safe_load((WORKFLOWS / "pypi-publish.yml").read_text())
    ecr = yaml.safe_load((WORKFLOWS / "ecr-release.yml").read_text())
    # PyYAML's YAML 1.1 loader treats the unquoted "on" key as True.
    triggers = ecr[True]
    assert set(triggers) == {"workflow_call"}

    caller = pypi["jobs"]["publish-emulator-image"]
    assert caller["uses"] == "./.github/workflows/ecr-release.yml"
    assert caller["needs"] == "pypi-publish"
    assert "if" not in caller  # Preserve the default success() gate.

    publisher = pypi["jobs"]["pypi-publish"]
    packages = {entry["name"] for entry in publisher["strategy"]["matrix"]["package"]}
    assert {
        "aws-durable-execution-sdk-python",
        "aws-durable-execution-sdk-python-testing",
    } <= packages
    assert not publisher.get("continue-on-error", False)


def test_emulator_checks_out_and_parses_the_published_release_tag() -> None:
    pypi = yaml.safe_load((WORKFLOWS / "pypi-publish.yml").read_text())
    ecr = yaml.safe_load((WORKFLOWS / "ecr-release.yml").read_text())
    assert pypi["jobs"]["publish-emulator-image"]["with"]["release_tag"] == (
        "${{ github.event.release.tag_name }}"
    )
    release_tag = ecr[True]["workflow_call"]["inputs"]["release_tag"]
    assert release_tag["type"] == "string"
    assert release_tag["required"] is True

    checkouts = [
        step
        for job in ecr["jobs"].values()
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/checkout@")
    ]
    assert checkouts
    assert all(step["with"]["ref"] == "${{ inputs.release_tag }}" for step in checkouts)
    parser = next(
        step for step in ecr["jobs"]["preflight"]["steps"] if step.get("id") == "tag"
    )
    assert parser["env"]["RELEASE_TAG"] == "${{ inputs.release_tag }}"


def test_emulator_call_passes_only_its_required_release_permissions() -> None:
    pypi = yaml.safe_load((WORKFLOWS / "pypi-publish.yml").read_text())
    ecr = yaml.safe_load((WORKFLOWS / "ecr-release.yml").read_text())
    caller = pypi["jobs"]["publish-emulator-image"]
    assert caller["permissions"] == {"contents": "read", "id-token": "write"}
    assert caller["secrets"] == {
        "ECR_UPLOAD_IAM_ROLE_ARN": "${{ secrets.ECR_UPLOAD_IAM_ROLE_ARN }}"
    }
    assert set(ecr[True]["workflow_call"]["secrets"]) == {"ECR_UPLOAD_IAM_ROLE_ARN"}
