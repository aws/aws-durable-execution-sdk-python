from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize(
    ("filename", "job_id"),
    [
        ("ai-pr-review.yml", "ai-pr-review"),
        ("issue-triage.yml", "triage"),
        ("notify.yml", "notify"),
        ("stale-issue-closer.yml", "close-stale"),
    ],
)
def test_shared_workflow_caller_passes_codebuild_runner(
    filename: str, job_id: str
) -> None:
    path = Path(__file__).parents[2] / "workflows" / filename
    workflow = yaml.safe_load(path.read_text())

    assert workflow["jobs"][job_id]["with"]["runs-on"] == (
        "codebuild-github-actions-runner-${{ github.run_id }}-${{ github.run_attempt }}"
    )
