import re
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "pr-status-labels.yml"
TEST_PARSER_PATH = Path(__file__).parents[2] / "workflows" / "test-parser.yml"


@pytest.fixture(name="workflow")
def workflow_fixture() -> dict[Any, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text())


def test_runs_on_events_that_change_status(workflow: dict[Any, Any]) -> None:
    # PyYAML reads the bare key `on` as the boolean True.
    assert workflow[True] == {
        "pull_request_target": {
            "types": ["opened", "reopened", "ready_for_review", "synchronize"]
        }
    }


def test_calls_shared_workflow_pinned_to_commit(workflow: dict[Any, Any]) -> None:
    uses = workflow["jobs"]["pr-status-labels"]["uses"]

    assert re.fullmatch(
        r"aws/aws-durable-execution-ci/\.github/workflows/pr-status-labels\.yml"
        r"@[0-9a-f]{40}",
        uses,
    )


def test_grants_only_label_permissions(workflow: dict[Any, Any]) -> None:
    assert workflow["permissions"] == {}
    assert workflow["jobs"]["pr-status-labels"]["permissions"] == {
        "contents": "read",
        "pull-requests": "write",
    }


def test_passes_no_secrets(workflow: dict[Any, Any]) -> None:
    # pull_request_target runs for fork pull requests with the base
    # repository's token, so the job gets no secrets.
    assert "secrets" not in workflow["jobs"]["pr-status-labels"]


def test_workflow_triggers_script_tests() -> None:
    parser = yaml.safe_load(TEST_PARSER_PATH.read_text())
    path = ".github/workflows/pr-status-labels.yml"

    assert path in parser[True]["pull_request"]["paths"]
    assert path in parser[True]["push"]["paths"]
