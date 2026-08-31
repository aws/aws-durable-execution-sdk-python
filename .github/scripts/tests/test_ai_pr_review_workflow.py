from pathlib import Path


WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "ai-pr-review.yml"


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
