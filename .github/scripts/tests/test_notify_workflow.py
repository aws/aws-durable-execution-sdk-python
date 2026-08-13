import re
from pathlib import Path


WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "notify.yml"
SHARED_WORKFLOW_SHA = "6cade565b59817027aa0fe4949d5923286b6675c"


def test_notify_workflow_reuses_pinned_shared_implementation() -> None:
    workflow = WORKFLOW_PATH.read_text()
    match = re.search(r"(?ms)^  notify:\n(.*)\Z", workflow)

    assert match is not None
    notify_job = match.group(1)
    assert (
        "uses: aws/aws-durable-execution-ci/.github/workflows/notify.yml@"
        f"{SHARED_WORKFLOW_SHA}"
    ) in notify_job
    assert "contents: read" in notify_job
    assert "models: read" in notify_job
    assert "runs-on:" not in notify_job
    assert "summarize_notification.py" not in notify_job
    assert "SLACK_WEBHOOK_URL_PR" in notify_job
    assert "SLACK_WEBHOOK_URL_ISSUE" in notify_job
    assert "SLACK_WEBHOOK_URL_RELEASE" in notify_job
