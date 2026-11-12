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
