from pathlib import Path

import yaml


WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "notify.yml"


def _step_by_name(steps: list[dict[str, object]], name: str) -> dict[str, object]:
    return next(step for step in steps if step.get("name") == name)


def test_notify_workflow_grants_only_required_job_permissions() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    jobs = workflow["jobs"]

    assert workflow["permissions"] == {}
    assert jobs["summarize"]["permissions"] == {
        "contents": "read",
        "models": "read",
    }
    assert jobs["summarize"]["outputs"]["summary"] == (
        "${{ steps.summary.outputs.summary }}"
    )
    assert jobs["notify-pr"]["permissions"] == {}
    assert jobs["notify-issues"]["permissions"] == {}
    assert jobs["notify-release"]["permissions"] == {}


def test_notify_workflow_generates_summary_from_immutable_toolkit() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    summarize = workflow["jobs"]["summarize"]
    steps = summarize["steps"]
    checkout = _step_by_name(steps, "Check out notification toolkit")
    summarize = _step_by_name(steps, "Generate concise notification summary")

    assert checkout["with"]["repository"] == "${{ job.workflow_repository }}"
    assert checkout["with"]["ref"] == "${{ job.workflow_sha }}"
    assert checkout["with"]["path"] == ".notification-toolkit"
    assert checkout["with"]["sparse-checkout"] == (
        ".github/scripts/summarize_notification.py"
    )
    assert checkout["with"]["sparse-checkout-cone-mode"] is False
    assert checkout["with"]["fetch-depth"] == 1
    assert checkout["with"]["persist-credentials"] is False
    assert summarize["id"] == "summary"
    assert summarize["env"]["GITHUB_TOKEN"] == "${{ github.token }}"
    assert (
        ".notification-toolkit/.github/scripts/summarize_notification.py"
        in (summarize["run"])
    )


def test_model_and_slack_secrets_are_isolated_between_jobs() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    jobs = workflow["jobs"]
    summarize = jobs["summarize"]
    notify_jobs = [
        jobs["notify-pr"],
        jobs["notify-issues"],
        jobs["notify-release"],
    ]

    assert "SLACK_WEBHOOK" not in str(summarize)
    for job in notify_jobs:
        assert job["needs"] == "summarize"
        assert len(job["steps"]) == 1
        assert "GITHUB_TOKEN" not in str(job)
        assert "models" not in str(job["permissions"])

        step = job["steps"][0]
        assert "slackapi/slack-github-action@" in step["uses"]
        payload = step["with"]["payload"]
        assert '"summary": ${{ toJSON(needs.summarize.outputs.summary) }}' in payload
