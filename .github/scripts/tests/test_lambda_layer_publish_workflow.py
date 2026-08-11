from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = REPOSITORY_ROOT / ".github" / "workflows" / "lambda-layer-publish.yml"


def test_workflow_builds_and_publishes_one_agnostic_layer() -> None:
    workflow = WORKFLOW.read_text()

    assert "strategy:" not in workflow
    assert "matrix." not in workflow
    assert "--target-python" not in workflow
    assert "--architecture" not in workflow
    assert "--compatible-runtime" not in workflow
    assert "--compatible-architecture" not in workflow
    assert "otel-plugin-layer" in workflow
    assert "${{ env.LAYER_NAME }}.zip" in workflow
