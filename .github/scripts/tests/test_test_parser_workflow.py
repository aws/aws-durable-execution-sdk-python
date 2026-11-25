from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / "workflows" / "test-parser.yml"


@pytest.fixture(name="workflow")
def workflow_fixture() -> dict[Any, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text())


def test_test_scripts_runs_all_tests_in_directory(workflow: dict[Any, Any]) -> None:
    steps = workflow["jobs"]["test-scripts"]["steps"]
    run_step = next(step for step in steps if step.get("name") == "Run script tests")
    assert run_step["run"].strip() == "python -m pytest .github/scripts/tests"
