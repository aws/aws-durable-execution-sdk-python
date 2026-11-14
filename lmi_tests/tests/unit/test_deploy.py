from pathlib import Path
from unittest.mock import Mock
import pytest
from lmi_tests import deploy


def test_failed_build_clears_prior_run_before_subprocess(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    artifacts = tmp_path / "lmi_tests/artifacts"
    deploy.save(artifacts / "manifest.json", {"run": "previous", "owned": True})
    deploy.save(artifacts / "cleanup.json", {"run": "previous"})
    monkeypatch.setattr(deploy, "ROOT", tmp_path)
    monkeypatch.setattr(deploy, "ARTIFACTS", artifacts)
    monkeypatch.setattr(
        deploy.subprocess, "run", Mock(side_effect=RuntimeError("build failed"))
    )
    with pytest.raises(RuntimeError, match="build failed"):
        deploy.build()
    assert not (artifacts / "manifest.json").exists()
    assert not (artifacts / "cleanup.json").exists()
