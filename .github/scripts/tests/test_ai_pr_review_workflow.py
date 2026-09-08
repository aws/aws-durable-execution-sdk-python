from pathlib import Path


WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "ai-pr-review.yml"


def test_ai_pr_review_caller_subscribes_to_supported_events() -> None:
    workflow = WORKFLOW_PATH.read_text()

    supported_events = (
        "on:\n"
        "  pull_request_target:\n"
        "    types: [opened, synchronize, reopened, ready_for_review]\n"
        "  pull_request_review:\n"
        "    types: [submitted, edited, dismissed]\n"
        "  pull_request_review_comment:\n"
        "    types: [created, edited, deleted]\n"
        "  issue_comment:\n"
        "    types: [created]\n"
    )
    assert supported_events in workflow


def test_ai_pr_review_caller_grants_reusable_workflow_permissions() -> None:
    workflow = WORKFLOW_PATH.read_text()

    required_permissions = (
        "  ai-pr-review:\n"
        "    permissions:\n"
        "      contents: write\n"
        "      id-token: write\n"
        "      pull-requests: write\n"
    )
    assert required_permissions in workflow
