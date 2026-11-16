"""Local tests for the parts of the AgentCore example.

The cloud test needs a deployed agent runtime. These tests check the
example's settings, its catalog entry, and the agent's handler without one.
"""

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


# The example imports the extras. The examples package does not depend on
# the extras. So an environment without them skips this module.
pytest.importorskip("aws_durable_execution_sdk_python_extras")

from src.agentcore import agentcore_run_job  # noqa: E402


PACKAGE_DIR = Path(__file__).resolve().parents[2]
RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/agent-abc"


def test_returns_the_agent_runtime_arn(monkeypatch):
    monkeypatch.setenv("AGENT_RUNTIME_ARN", RUNTIME_ARN)

    assert agentcore_run_job._require_agent_runtime_arn() == RUNTIME_ARN


@pytest.mark.parametrize("unset", [True, False], ids=["unset", "empty"])
def test_names_the_variable_when_it_is_missing(unset, monkeypatch):
    if unset:
        monkeypatch.delenv("AGENT_RUNTIME_ARN", raising=False)
    else:
        monkeypatch.setenv("AGENT_RUNTIME_ARN", "")

    with pytest.raises(RuntimeError, match="AGENT_RUNTIME_ARN must be set"):
        agentcore_run_job._require_agent_runtime_arn()


def test_the_catalog_entry_needs_an_agent_runtime():
    examples = json.loads(
        (PACKAGE_DIR / "examples-catalog.json").read_text(encoding="utf-8")
    )["examples"]
    entry = next(e for e in examples if e["name"] == "AgentCore Run Job")

    assert entry["usesAgentcore"] is True
    assert entry["handler"] == "agentcore_run_job.handler"
    assert (PACKAGE_DIR / entry["path"]).is_file()


def test_the_agent_counts_the_words_and_returns_the_session_id():
    # The agent imports the worker, which the dev-examples environment does
    # not install. So that environment skips this test.
    pytest.importorskip("aws_durable_execution_sdk_python_compute_worker.agentcore")
    path = PACKAGE_DIR / "agentcore-agent" / "app.py"
    spec = importlib.util.spec_from_file_location("agentcore_agent_app", path)
    assert spec is not None
    assert spec.loader is not None
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    text = "the quick  brown fox\njumps"

    output = app.count_words({"text": text}, SimpleNamespace(session_id="s-1"))

    assert output == {
        "wordCount": 5,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "sessionId": "s-1",
    }
