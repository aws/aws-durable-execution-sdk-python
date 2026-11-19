import re
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "closed-issue-comment.yml"
TEST_PARSER_PATH = Path(__file__).parents[2] / "workflows" / "test-parser.yml"


@pytest.fixture(name="workflow")
def workflow_fixture() -> dict[Any, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text())


def test_runs_on_new_issue_comments_only(workflow: dict[Any, Any]) -> None:
    # PyYAML reads the bare key `on` as the boolean True.
    assert workflow[True] == {"issue_comment": {"types": ["created"]}}


def test_calls_shared_workflow_pinned_to_commit(workflow: dict[Any, Any]) -> None:
    uses = workflow["jobs"]["closed-issue-comment"]["uses"]

    assert re.fullmatch(
        r"aws/aws-durable-execution-ci/\.github/workflows/closed-issue-comment\.yml"
        r"@[0-9a-f]{40}",
        uses,
    )


def test_passes_issue_webhook_secret(workflow: dict[Any, Any]) -> None:
    secrets = workflow["jobs"]["closed-issue-comment"]["secrets"]

    assert secrets == {"SLACK_WEBHOOK_URL_ISSUE": "${{ secrets.SLACK_WEBHOOK_URL_ISSUE }}"}


def test_grants_no_token_permissions(workflow: dict[Any, Any]) -> None:
    assert workflow["permissions"] == {}
    assert "permissions" not in workflow["jobs"]["closed-issue-comment"]


def test_workflow_triggers_script_tests() -> None:
    parser = yaml.safe_load(TEST_PARSER_PATH.read_text())
    path = ".github/workflows/closed-issue-comment.yml"

    assert path in parser[True]["pull_request"]["paths"]
    assert path in parser[True]["push"]["paths"]
