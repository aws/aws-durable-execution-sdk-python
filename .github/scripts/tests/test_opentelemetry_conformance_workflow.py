from pathlib import Path


WORKFLOW_PATH = (
    Path(__file__).parents[2] / "workflows" / "opentelemetry-conformance-tests.yml"
)


def test_opentelemetry_conformance_caller_uses_current_workflow_contract() -> None:
    workflow = WORKFLOW_PATH.read_text()

    assert "otlp_endpoint:" not in workflow

    for secret_name in (
        "DATADOG_ACCESS_TOKEN",
        "DATADOG_API_KEY",
        "DATADOG_APPLICATION_KEY",
    ):
        mapping = f"{secret_name}: ${{{{ secrets.{secret_name} }}}}"
        assert mapping in workflow

    for obsolete_secret_name in (
        "DD_API_KEY",
        "DD_APPLICATION_KEY",
        "DATADOG_OTLP_HEADERS",
    ):
        assert f"{obsolete_secret_name}:" not in workflow
