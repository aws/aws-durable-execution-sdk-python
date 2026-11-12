# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Cloud acceptance must use the same pinned runner as local adapter tests."""

from pathlib import Path
import tomllib

import yaml

from lmi_tests.shared_contract import CONFORMANCE_REVISION

ROOT = Path(__file__).resolve().parents[3]


def test_cloud_workflow_uses_pinned_shared_acceptance_and_final_cleanup():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/lmi-e2e-tests.yml").read_text()
    )
    assert workflow["env"]["CONFORMANCE_REVISION"] == CONFORMANCE_REVISION
    assert "workflow_dispatch" in workflow["jobs"]["cloud"]["if"]
    for job in workflow["jobs"].values():
        checkout = next(
            step
            for step in job["steps"]
            if step.get("with", {}).get("repository")
            == "aws/aws-durable-execution-conformance-tests"
        )
        assert checkout["with"]["ref"] == "${{ env.CONFORMANCE_REVISION }}"
        assert any(
            step.get("run") == "hatch run lmi:shared-setup" for step in job["steps"]
        )
    steps = workflow["jobs"]["cloud"]["steps"]
    acceptance = next(
        step for step in steps if step.get("run") == "hatch run lmi:shared"
    )
    assert acceptance["timeout-minutes"] == 60
    cleanup = next(
        step for step in steps if "lmi:shared-cleanup" in step.get("run", "")
    )
    assert cleanup["if"] == "always()"
    assert not any("lmi:cloud" in step.get("run", "") for step in steps)
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["hatch"][
        "envs"
    ]["lmi"]["scripts"]
    assert scripts["shared"].startswith("python -m lmi_tests.shared run")
    assert scripts["shared-cleanup"].startswith("python -m lmi_tests.shared cleanup")


def test_codebuild_runner_has_isolated_python_and_keeps_untrusted_prs_hosted():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/lmi-e2e-tests.yml").read_text()
    )
    assert workflow["jobs"]["harness"]["runs-on"] == (
        "${{ github.event_name == 'pull_request' && "
        "(github.event.pull_request.head.repo.full_name != github.repository || "
        "github.actor == 'dependabot[bot]') && 'ubuntu-latest' || "
        "format('codebuild-github-actions-runner-{0}-{1}', "
        "github.run_id, github.run_attempt) }}"
    )
    assert workflow["jobs"]["cloud"]["runs-on"] == (
        "codebuild-github-actions-runner-${{ github.run_id }}-${{ github.run_attempt }}"
    )
    assert workflow["env"]["PIP_CONFIG_FILE"] == "/dev/null"
    assert workflow["env"]["PYTHONPATH"] == ""
    for job in workflow["jobs"].values():
        setup = next(
            step
            for step in job["steps"]
            if step.get("uses", "").startswith("astral-sh/setup-uv@")
        )
        assert setup["with"]["python-version"] == "3.14"
        assert setup["with"]["activate-environment"] is True
        assert any(
            step.get("run") == "uv pip install hatch==1.16.5" for step in job["steps"]
        )
        assert not any(
            step.get("uses", "").startswith("actions/setup-python@")
            for step in job["steps"]
        )


def test_cloud_runs_automatically_for_trusted_prs_and_supports_manual_dispatch():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/lmi-e2e-tests.yml").read_text()
    )
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers["pull_request"]["types"]) == {
        "opened",
        "synchronize",
        "reopened",
    }
    assert "workflow_dispatch" in triggers
    cloud = workflow["jobs"]["cloud"]
    assert cloud["if"] == (
        "github.actor != 'dependabot[bot]' && "
        "(github.event_name == 'workflow_dispatch' || "
        "(github.event_name == 'pull_request' && "
        "github.event.pull_request.head.repo.full_name == github.repository))"
    )
    assert cloud["needs"] == "harness"
    assert cloud["concurrency"]["group"] == "lmi-e2e-shared-capacity-provider"
    assert cloud["concurrency"]["cancel-in-progress"] is False
