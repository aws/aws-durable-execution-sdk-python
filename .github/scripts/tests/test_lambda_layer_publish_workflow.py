from pathlib import Path

import yaml


WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "lambda-layer-publish.yml"


def _step_by_name(steps: list[dict[str, object]], name: str) -> dict[str, object]:
    return next(step for step in steps if step.get("name") == name)


def test_publish_workflow_uses_resolved_credential_matrix() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    build_job = workflow["jobs"]["build-distributions"]
    publish_job = workflow["jobs"]["publish-layer"]

    assert build_job["outputs"]["publish_targets"] == (
        "${{ steps.publish-targets.outputs.publish_targets }}"
    )
    resolve_step = _step_by_name(build_job["steps"], "Resolve publish targets")
    assert resolve_step["env"]["LAYER_REGIONS"] == (
        "${{ inputs.regions || vars.LAYER_PUBLISH_REGIONS }}"
    )
    assert ".github/scripts/resolve_layer_publish_targets.py" in resolve_step["run"]

    matrix = publish_job["strategy"]["matrix"]
    assert matrix["publish_target"] == (
        "${{ fromJSON(needs.build-distributions.outputs.publish_targets) }}"
    )
    assert set(matrix) == {"publish_target"}


def test_publish_workflow_resolves_role_and_partition_settings_from_matrix() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    publish_steps = workflow["jobs"]["publish-layer"]["steps"]
    credentials_step = _step_by_name(publish_steps, "Configure AWS credentials")
    publish_step = _step_by_name(publish_steps, "Publish layer versions")

    assert credentials_step["with"]["role-to-assume"] == (
        "${{ secrets[matrix.publish_target.role_secret] }}"
    )
    assert credentials_step["with"]["aws-region"] == (
        "${{ matrix.publish_target.aws_region }}"
    )
    assert credentials_step["with"]["audience"] == (
        "${{ matrix.publish_target.audience }}"
    )
    assert publish_step["env"]["LAYER_REGIONS"] == (
        "${{ matrix.publish_target.regions }}"
    )
