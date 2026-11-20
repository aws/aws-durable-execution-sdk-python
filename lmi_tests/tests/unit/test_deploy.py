from pathlib import Path
import json
import stat
import sys
import zipfile
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


def test_manual_cleanup_cannot_use_legacy_run_controls(monkeypatch):
    manager = Mock()
    monkeypatch.setattr(deploy, "manager", manager)
    monkeypatch.setattr(sys, "argv", ["lmi_tests.deploy", "cleanup"])
    with pytest.raises(SystemExit) as error:
        deploy.main()
    assert error.value.code == 2
    manager.assert_not_called()


@pytest.mark.parametrize("command", ["build", "deploy"])
def test_build_and_deploy_preserve_unresolved_journal(monkeypatch, tmp_path, command):
    from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError

    journal = tmp_path / "deployment-update.json"
    journal.write_text('{"run":"current"}')
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"run":"previous"}')
    monkeypatch.setattr(deploy, "ARTIFACTS", tmp_path)
    build_command = Mock()
    monkeypatch.setattr(deploy.subprocess, "run", build_command)

    deploy_manager = Mock()
    monkeypatch.setattr(deploy, "manager", deploy_manager)
    monkeypatch.setattr(sys, "argv", ["lmi_tests.deploy", command])
    with pytest.raises(ProvisioningError, match="before rebuilding or deploying"):
        deploy.main()
    deploy_manager.assert_not_called()

    assert json.loads(journal.read_text()) == {"run": "current"}
    assert json.loads(manifest.read_text()) == {"run": "previous"}
    build_command.assert_not_called()


@pytest.mark.parametrize(
    ("manifest_run", "functions", "pending_update", "map_status", "diagnostic"),
    [
        (
            "previous",
            {"c1": "test:c1", "c2": "test:c2"},
            True,
            "exact",
            "different collection run",
        ),
        ("current", {}, False, "exact", "no verified collection targets"),
        (
            "current",
            {"c1": "test:c1"},
            False,
            "exact",
            "no verified collection targets",
        ),
        (
            "current",
            {"c1": "test:c1", "c2": "test:c2"},
            True,
            "exact",
            "no verified collection targets",
        ),
        (
            "current",
            {"c1": "test:c1", "c2": "test:c2"},
            False,
            "absent",
            "no verified collection targets",
        ),
        (
            "current",
            {"c1": "test:c1", "c2": "test:c2"},
            False,
            "mismatch",
            "no verified collection targets",
        ),
        ("current", {"c1": "test:c1", "c2": "test:c2"}, False, "exact", None),
    ],
)
def test_collection_checks_active_run_before_constructing_cloud(
    monkeypatch,
    tmp_path,
    manifest_run,
    functions,
    pending_update,
    map_status,
    diagnostic,
):
    from aws_durable_execution_conformance_tests.lmi.errors import CollectionError
    from lmi_tests import cloud

    manifest = {"run": manifest_run, "functions": functions}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    if map_status != "absent":
        name_map = {
            key: {deploy.RUNTIME.handler: arn} for key, arn in functions.items()
        }
        if map_status == "mismatch":
            name_map["c2"] = {deploy.RUNTIME.handler: "test:stale"}
        (tmp_path / "function-name-map.json").write_text(json.dumps(name_map))
    if pending_update:
        (tmp_path / "deployment-update.json").write_text('{"run":"current"}')
    monkeypatch.setattr(deploy, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(sys, "argv", ["lmi_tests.deploy", "collect"])
    monkeypatch.setenv("LMI_SHARED_RUN_ID", "current")
    transport = Mock()
    monkeypatch.setattr(cloud, "Cloud", transport)

    if diagnostic:
        with pytest.raises(CollectionError, match=diagnostic):
            deploy.main()
        transport.assert_not_called()
    else:
        deploy.main()
        transport.assert_called_once_with(manifest)
        transport.return_value.collect.assert_called_once_with()


@pytest.mark.parametrize(
    "arguments",
    [[], ["--run-id", "UPPER"], ["--run-id", "valid", "--stack-name", ".."]],
)
def test_invalid_cli_deploy_does_not_touch_previous_manifest(
    monkeypatch, tmp_path, arguments
):
    from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError

    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"run":"previous"}')
    monkeypatch.setattr(deploy, "ARTIFACTS", tmp_path)
    manager = Mock()
    monkeypatch.setattr(deploy, "manager", manager)
    monkeypatch.setattr(sys, "argv", ["lmi_tests.deploy", "deploy", *arguments])
    with pytest.raises(ProvisioningError):
        deploy.main()
    assert manifest.read_text() == '{"run":"previous"}'
    manager.assert_not_called()


def test_finished_bundle_has_lambda_file_modes(monkeypatch, tmp_path):
    fixture_files = tmp_path / "lmi_tests"
    fixture_files.mkdir()
    for name in ("__init__.py", "fixture.py", "shared_observations.py"):
        (fixture_files / name).write_text("# fixture")
    artifacts = fixture_files / "artifacts"
    build = fixture_files / "build"

    def build_or_install(command, **options):
        if command[0] == "hatch":
            (build / "aws_durable_execution_sdk_python-example.whl").write_bytes(
                b"wheel"
            )
        else:
            target = build / "package"
            target.mkdir()
            program = target / "helper"
            program.write_text("#!/bin/sh")
            program.chmod(0o711)
            (build / "dependencies.json").write_text('{"install":[]}')

    monkeypatch.setattr(deploy, "ROOT", tmp_path)
    monkeypatch.setattr(deploy, "ARTIFACTS", artifacts)
    monkeypatch.setattr(deploy.subprocess, "run", build_or_install)
    monkeypatch.setattr(deploy.subprocess, "check_output", Mock(return_value="sha"))

    deploy.build()

    with zipfile.ZipFile(build / "function.zip") as archive:
        assert (
            stat.S_IMODE(archive.getinfo("lmi_tests/fixture.py").external_attr >> 16)
            == 0o644
        )
        assert stat.S_IMODE(archive.getinfo("helper").external_attr >> 16) == 0o755


def test_direct_deploy_requires_pinned_install_before_manager(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, "ARTIFACTS", tmp_path)
    checkout = tmp_path / "conformance"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lmi_tests.deploy",
            "deploy",
            "--run-id",
            "test-1",
            "--checkout",
            str(checkout),
        ],
    )
    verify = Mock(side_effect=ValueError("pin mismatch"))
    manager = Mock()
    monkeypatch.setattr(deploy, "verify_install", verify)
    monkeypatch.setattr(deploy, "manager", manager)

    with pytest.raises(ValueError, match="pin mismatch"):
        deploy.main()

    verify.assert_called_once_with(checkout)
    manager.assert_not_called()
