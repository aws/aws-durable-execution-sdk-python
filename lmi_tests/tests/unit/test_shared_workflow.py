from pathlib import Path
import json
import sys
from unittest.mock import Mock

import pytest
import yaml
from lmi_tests.shared_contract import CONFORMANCE_REVISION

ROOT = Path(__file__).resolve().parents[3]


def test_shared_setup_bootstraps_before_conformance_is_installed():
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import importlib.abc
        import sys
        from pathlib import Path
        from unittest.mock import patch

        class NoConformance(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith('aws_durable_execution_conformance_tests'):
                    raise AssertionError('bootstrap tried to import uninstalled conformance')
                return None

        sys.meta_path.insert(0, NoConformance())
        from lmi_tests import shared
        with patch.object(shared, 'verify_checkout') as verify, patch.object(shared.subprocess, 'run') as install:
            shared.setup(Path('.conformance'))
            verify.assert_called_once_with(Path('.conformance'))
            assert install.call_args.args[0][1:4] == ['-m', 'pip', 'install']
    """)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_workflow_calls_the_pinned_shared_definition():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/lmi-e2e-tests.yml").read_text()
    )
    caller = workflow["jobs"]["lifecycle"]
    assert list(workflow["jobs"]) == ["lifecycle"]
    assert (
        caller["uses"]
        == f"aws/aws-durable-execution-conformance-tests/.github/workflows/lmi-lifecycle.yml@{CONFORMANCE_REVISION}"
    )
    assert caller["with"]["conformance-ref"] == CONFORMANCE_REVISION
    assert (ROOT / caller["with"]["sdk-script"]).is_file()
    assert set(caller["secrets"]) == {
        "TEST_ROLE_ARN",
        "TEST_ACCOUNT_ID",
        "TEST_LAMBDA_EXECUTION_ROLE_ARN",
        "CAPACITY_PROVIDER_ARN",
    }
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers["pull_request"]["types"]) == {
        "opened",
        "synchronize",
        "reopened",
    }
    assert triggers["pull_request"]["branches"] == ["main"]
    assert set(triggers["pull_request"]["paths"]) == {
        ".github/workflows/lmi-e2e-tests.yml",
        ".github/scripts/lmi-ci.sh",
        "lmi_tests/**",
        "!lmi_tests/**/*.md",
        "packages/aws-durable-execution-sdk-python/src/**",
        "packages/aws-durable-execution-sdk-python/pyproject.toml",
        "packages/aws-durable-execution-sdk-python-testing/src/**",
        "packages/aws-durable-execution-sdk-python-testing/pyproject.toml",
        "pyproject.toml",
    }
    assert "workflow_dispatch" in triggers


def test_stage_dispatch_propagates_failure_and_preserves_deployment_identity(tmp_path):
    import os
    import subprocess

    fake = tmp_path / "hatch"
    fake.write_text(
        '#!/bin/bash\nprintf "%s\\n" "$@" > "$CALL_LOG"\nexit "${HATCH_RESULT:-0}"\n'
    )
    fake.chmod(0o755)
    calls = tmp_path / "calls.txt"
    env = {
        **os.environ,
        "PATH": str(tmp_path) + ":" + os.environ["PATH"],
        "CALL_LOG": str(calls),
        "LMI_SHARED_RUN_ID": "123-2",
        "LMI_RUNTIME": "python3.14",
        "LMI_STACK_NAME": "python-lmi-e2e",
    }
    script = ROOT / ".github/scripts/lmi-ci.sh"
    subprocess.run(["bash", str(script), "deploy"], env=env, check=True)
    assert calls.read_text().splitlines() == [
        "run",
        "lmi:python",
        "-m",
        "lmi_tests.deploy",
        "deploy",
        "--run-id",
        "123-2",
        "--runtime",
        "python3.14",
        "--stack-name",
        "python-lmi-e2e",
    ]
    failed = subprocess.run(
        ["bash", str(script), "run"], env={**env, "HATCH_RESULT": "17"}, check=False
    )
    assert failed.returncode == 17
    assert calls.read_text().splitlines() == ["run", "lmi:shared"]


def test_plan_preview_leaves_actual_run_directory_unwritten(monkeypatch, tmp_path):
    from lmi_tests import shared

    monkeypatch.setattr(shared, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(shared, "verify_install", Mock())
    monkeypatch.setattr(
        shared, "make_manifest", lambda deployment, coverage, output: deployment
    )
    monkeypatch.setattr(shared, "run", Mock(return_value=0))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"run": "local"}))
    args = ["lmi_tests.shared", "plan", "--manifest", str(manifest)]
    monkeypatch.setattr(sys, "argv", args)
    shared.main()
    preview = tmp_path / "preview"
    assert sorted(item.name for item in preview.iterdir()) == sorted(
        shared.COVERAGE_PROFILES
    )
    assert not (tmp_path / "shared").exists()

    args[1] = "run"
    with pytest.raises(SystemExit) as exit_status:
        shared.main()
    assert exit_status.value.code == 0
    shared.run.assert_called_once_with({"run": "local"}, tmp_path / "shared")

    actual_run = tmp_path / "shared"
    actual_run.mkdir()
    retained = actual_run / "manifest.json"
    retained.write_text("retained evidence")
    monkeypatch.setattr(
        sys, "argv", [*args[:1], "plan", *args[2:], "--output", str(actual_run)]
    )
    with pytest.raises(FileExistsError):
        shared.main()
    assert retained.read_text() == "retained evidence"


@pytest.mark.parametrize("explicit_output", [False, True])
def test_cleanup_loads_requested_recovery_manifest_before_checking_run(
    monkeypatch, tmp_path, explicit_output
):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from lmi_tests import shared

    manifest = tmp_path / "custom-manifest.json"
    manifest.write_text(json.dumps({"run": "previous"}))
    loader = Mock(return_value={"run": "current"})
    monkeypatch.setattr(
        shared_deployment, "load_cleanup_manifest", loader, raising=False
    )
    monkeypatch.setattr(shared, "verify_install", Mock())
    cleanup = Mock(return_value=0)
    monkeypatch.setattr(shared, "cleanup", cleanup)
    monkeypatch.setenv("LMI_SHARED_RUN_ID", "current")
    argv = ["lmi_tests.shared", "cleanup", "--manifest", str(manifest)]
    output = tmp_path / "explicit" if explicit_output else None
    if output is not None:
        argv.extend(["--output", str(output)])
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as exit_status:
        shared.main()
    assert exit_status.value.code == 0
    loader.assert_called_once_with(tmp_path, "current", active_manifest=manifest)
    cleanup.assert_called_once_with(
        {"run": "current", "deploymentArtifactDirectory": str(tmp_path)}, output
    )


def test_pinned_cleanup_still_checks_manifest_run_and_uses_default_output(
    monkeypatch, tmp_path
):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from aws_durable_execution_conformance_tests.lmi.errors import CollectionError
    from lmi_tests import shared

    monkeypatch.delattr(shared_deployment, "load_cleanup_manifest", raising=False)
    monkeypatch.setattr(shared, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(shared, "verify_install", Mock())
    cleanup = Mock(return_value=0)
    monkeypatch.setattr(shared, "cleanup", cleanup)
    monkeypatch.setenv("LMI_SHARED_RUN_ID", "current")
    monkeypatch.setattr(sys, "argv", ["lmi_tests.shared", "cleanup"])
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"run": "previous"}))
    with pytest.raises(CollectionError, match="different CI run"):
        shared.main()
    cleanup.assert_not_called()
    manifest.write_text(json.dumps({"run": "current"}))
    with pytest.raises(SystemExit) as exit_status:
        shared.main()
    assert exit_status.value.code == 0
    cleanup.assert_called_once_with({"run": "current"}, tmp_path / "shared")


@pytest.mark.parametrize("pre_archive", [True, False], ids=["pre-archive", "active"])
def test_cleanup_routes_receipt_to_its_run(monkeypatch, tmp_path, pre_archive):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from aws_durable_execution_conformance_tests.lmi import runner
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key
    from lmi_tests import shared

    if not hasattr(shared_deployment, "load_cleanup_manifest"):
        pytest.skip("Published runner predates recovery-manifest lookup")

    manifest = tmp_path / "manifest.json"
    deployment = {
        "run": "current",
        "stack": "stack",
        "bucket": "bucket",
        "commit": "revision",
        "region": "us-west-2",
        "deploymentArtifactDirectory": str(tmp_path),
    }
    output = tmp_path / "custom"
    if pre_archive:
        manifest.write_text(json.dumps({"run": "previous"}))
        (tmp_path / "deployment-update.json").write_text(
            json.dumps({**deployment, "recoveryManifest": deployment})
        )
        output.mkdir()
        (output / "cleanup.json").write_text(json.dumps({"runId": "previous"}))
        receipt = tmp_path / "cleanup" / case_key("current") / "cleanup.json"
    else:
        manifest.write_text(json.dumps(deployment))
        receipt = output / "cleanup.json"
    (tmp_path / "case-output.json").write_text(
        json.dumps(
            {
                "runId": "previous" if pre_archive else "current",
                "directory": str(output),
            }
        )
    )
    reconcile = Mock(return_value="current")
    monkeypatch.setattr(runner, "recover_deployment_update", reconcile)
    monkeypatch.setattr(shared, "verify_install", Mock())
    monkeypatch.setenv("LMI_SHARED_RUN_ID", "current")
    monkeypatch.setattr(
        sys, "argv", ["lmi_tests.shared", "cleanup", "--manifest", str(manifest)]
    )

    with pytest.raises(SystemExit) as exit_status:
        shared.main()

    assert exit_status.value.code == 0
    assert reconcile.call_args.args[:2] == (deployment, tmp_path)
    report = json.loads(receipt.read_text())
    assert report["runId"] == "current"
    if pre_archive:
        assert json.loads((output / "cleanup.json").read_text()) == {
            "runId": "previous"
        }
    assert not (tmp_path / "shared").exists()


def test_legacy_pin_cannot_reconcile_an_existing_journal(monkeypatch, tmp_path):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from aws_durable_execution_conformance_tests.lmi.errors import CollectionError
    from lmi_tests import shared

    monkeypatch.delattr(shared_deployment, "load_cleanup_manifest", raising=False)
    (tmp_path / "deployment-update.json").write_text(json.dumps({"run": "current"}))
    with pytest.raises(CollectionError, match="recover its journal"):
        shared.cleanup_inputs(tmp_path / "manifest.json", None, "current")


def test_pinned_run_and_cleanup_share_manifest_relative_default(monkeypatch, tmp_path):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from lmi_tests import shared

    monkeypatch.delattr(shared_deployment, "load_cleanup_manifest", raising=False)
    monkeypatch.setattr(shared, "verify_install", Mock())
    downloaded = tmp_path / "downloaded"
    downloaded.mkdir()
    manifest = downloaded / "manifest.json"
    manifest.write_text(json.dumps({"run": "current"}))
    link = tmp_path / "manifest.json"
    link.symlink_to(manifest)
    expected_output = downloaded / "shared"
    run = Mock(return_value=0)
    cleanup = Mock(return_value=0)
    monkeypatch.setattr(shared, "run", run)
    monkeypatch.setattr(shared, "cleanup", cleanup)
    monkeypatch.setenv("LMI_SHARED_RUN_ID", "current")
    for command in ("run", "cleanup"):
        monkeypatch.setattr(
            sys, "argv", ["lmi_tests.shared", command, "--manifest", str(link)]
        )
        with pytest.raises(SystemExit) as exit_status:
            shared.main()
        assert exit_status.value.code == 0
    run.assert_called_once_with({"run": "current"}, expected_output)
    cleanup.assert_called_once_with({"run": "current"}, expected_output)


def test_recovery_prefers_journal_beside_symlink_to_previous_manifest(
    monkeypatch, tmp_path
):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from lmi_tests import shared

    previous = tmp_path / "previous"
    previous.mkdir()
    manifest = previous / "manifest.json"
    manifest.write_text(json.dumps({"run": "previous"}))
    link = tmp_path / "manifest.json"
    link.symlink_to(manifest)
    (tmp_path / "deployment-update.json").write_text(json.dumps({"run": "current"}))
    current = {"run": "current", "deploymentArtifactDirectory": str(tmp_path)}
    loader = Mock(return_value=current)
    monkeypatch.setattr(
        shared_deployment, "load_cleanup_manifest", loader, raising=False
    )
    assert shared.cleanup_inputs(link, None, "current") == (current, None)
    loader.assert_called_once_with(tmp_path, "current", active_manifest=manifest)


@pytest.mark.parametrize("nested", [False, True])
def test_verify_install_rejects_core_outside_pinned_source(
    monkeypatch, tmp_path, nested
):
    import aws_durable_execution_conformance_tests as core
    from lmi_tests import shared

    checkout = tmp_path / "pinned"
    verify = Mock()
    monkeypatch.setattr(shared, "verify_checkout", verify)
    installed = checkout / ".venv/lib/site-packages" if nested else tmp_path / "other"
    monkeypatch.setattr(
        core,
        "__file__",
        str(installed / "aws_durable_execution_conformance_tests/__init__.py"),
    )
    with pytest.raises(ValueError, match="not from the pinned checkout"):
        shared.verify_install(checkout)
    verify.assert_called_once_with(checkout)


@pytest.mark.parametrize("absolute", [False, True])
def test_manual_cleanup_relies_on_local_downloaded_artifact_paths(
    monkeypatch, tmp_path, absolute
):
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from aws_durable_execution_conformance_tests.lmi.errors import CollectionError
    from lmi_tests import shared

    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {"run": "current", "deploymentArtifactDirectory": "/previous/artifacts"}
        )
    )
    (tmp_path / "case-output.json").write_text(
        json.dumps(
            {
                "runId": "current",
                "directory": "/previous/output" if absolute else "custom",
            }
        )
    )
    loader = Mock()
    monkeypatch.setattr(
        shared_deployment, "load_cleanup_manifest", loader, raising=False
    )
    monkeypatch.setattr(shared, "verify_install", Mock())
    cleanup = Mock(return_value=0)
    monkeypatch.setattr(shared, "cleanup", cleanup)
    monkeypatch.delenv("LMI_SHARED_RUN_ID", raising=False)
    argv = ["lmi_tests.shared", "cleanup", "--manifest", str(manifest)]
    monkeypatch.setattr(sys, "argv", argv)

    if absolute:
        with pytest.raises(CollectionError, match="Specify --output"):
            shared.main()
        cleanup.assert_not_called()
        argv.extend(["--output", str(tmp_path / "custom")])
    with pytest.raises(SystemExit) as exit_status:
        shared.main()

    assert exit_status.value.code == 0
    loader.assert_not_called()
    cleanup.assert_called_once_with(
        {"run": "current", "deploymentArtifactDirectory": str(tmp_path)},
        tmp_path / "custom" if absolute else None,
    )


@pytest.mark.parametrize("modern", [False, True])
def test_summary_keeps_expected_run_when_using_either_runner(
    monkeypatch, tmp_path, modern
):
    from lmi_tests import summary

    def current(directory, *, expected_run=None):
        return f"{directory.name}:{expected_run}"

    old = Mock(return_value="legacy acceptance")

    def legacy(directory):
        return old(directory)

    monkeypatch.setattr(summary, "summarize", current if modern else legacy)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"run": "older"}))
    if modern:
        assert summary.render(tmp_path, "current") == f"{tmp_path.name}:current"
    else:
        assert "acceptance is unavailable" in summary.render(tmp_path, "current")
        old.assert_not_called()
        manifest.write_text(json.dumps({"run": "current"}))
        assert summary.render(tmp_path, "current") == "legacy acceptance"
        old.assert_called_once_with(tmp_path)


def test_pinned_summary_reports_build_failure_without_manifest(monkeypatch, tmp_path):
    from lmi_tests import summary

    from aws_durable_execution_conformance_tests.lmi.summary import summarize

    def legacy(directory):
        return summarize(directory)

    monkeypatch.setattr(summary, "summarize", legacy)
    (tmp_path / "build-error.json").write_text(json.dumps({"type": "BuildError"}))
    assert "build-error: BuildError" in summary.render(tmp_path, "current")


def test_healthy_peer_and_recovery_require_success():
    from lmi_tests.shared_evidence import expected_outcome

    assert expected_outcome(
        {"payload": {"scenario": "barrier"}, "marker": "worker"}, "peer-recovery"
    ) == {
        "status": "SUCCEEDED",
        "result": "worker",
    }


def test_replay_policy_counts_logical_items_and_keeps_first_allowance():
    from lmi_tests.shared_evidence import replay_effects

    bodies = [
        {"operation": "item", "operationId": "bool", "value": True},
        {"operation": "item", "operationId": "first", "value": 1},
        {"operation": "item", "operationId": "first", "value": 1},
        {"operation": "item", "operationId": "extra", "value": 1},
        {"operation": "item", "operationId": "invalid", "value": 4},
        {"operation": "item", "operationId": "second", "value": 2},
    ]
    counts, missing = replay_effects(bodies)
    assert counts == {"first": 1, "extra": 0, "invalid": 0, "bool": 0, "second": 1}
    assert set(missing) == {"item:3", "success", "failure", "retry", "left", "right"}


@pytest.mark.parametrize("capability", ["pinned", "per-name", "policy"])
def test_python_evidence_binding_passes_only_supported_policy_options(
    monkeypatch, capability
):
    from lmi_tests import shared_evidence

    evidence = {"collectionErrors": ["preserved example"]}
    observations = Mock(return_value=evidence)

    def pinned(*args, **kwargs):
        return observations()

    def per_name(*args, expected_replay_effects, **kwargs):
        return observations(expected_replay_effects)

    def policy(*args, expected_replay_effects, replay_effect_policy, **kwargs):
        return observations(expected_replay_effects, replay_effect_policy)

    monkeypatch.setattr(
        shared_evidence,
        "shared_build_evidence",
        {
            "pinned": pinned,
            "per-name": per_name,
            "policy": policy,
        }[capability],
    )
    cloud = Mock()
    assert shared_evidence.build_evidence(Mock(), cloud) is evidence
    if capability == "pinned":
        observations.assert_called_once_with()
    elif capability == "per-name":
        observations.assert_called_once_with(shared_evidence._EXPECTED_REPLAY_EFFECTS)
    else:
        observations.assert_called_once_with(
            shared_evidence._EXPECTED_REPLAY_EFFECTS, shared_evidence.replay_effects
        )
    cloud._save.assert_not_called()


@pytest.mark.parametrize("local_source", [True, False])
def test_harness_checks_sdk_provenance_in_lmi_environment(tmp_path, local_source):
    import os
    import subprocess

    fake = tmp_path / "hatch"
    fake.write_text(
        '#!/bin/bash\nprintf "%s\n" "$*" >> "$CALL_LOG"\n'
        'if [[ "$*" == "run lmi:python -" ]]; then exec "$TEST_PYTHON" -; fi\n'
    )
    fake.chmod(0o755)
    calls = tmp_path / "calls.txt"
    root = tmp_path / "checkout"
    root.mkdir()
    source = root / "packages/aws-durable-execution-sdk-python/src"
    module = (
        source if local_source else tmp_path / "installed"
    ) / "aws_durable_execution_sdk_python"
    module.mkdir(parents=True)
    (module / "__init__.py").touch()

    completed = subprocess.run(
        ["bash", str(ROOT / ".github/scripts/lmi-ci.sh"), "harness"],
        cwd=root,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CALL_LOG": str(calls),
            "TEST_PYTHON": sys.executable,
            "PYTHONPATH": str(module.parent),
        },
        capture_output=True,
        text=True,
    )
    expected = ["run lmi:lint", "run lmi:python -"]
    if local_source:
        assert completed.returncode == 0
        expected.append("run lmi:shared-tests --junitxml=lmi_tests/artifacts/unit.xml")
    else:
        assert completed.returncode != 0
        assert "AssertionError" in completed.stderr
        assert str(module) in completed.stderr
    assert calls.read_text().splitlines() == expected


def test_python_cleanup_stops_execution_from_relocated_case_journal(
    monkeypatch, tmp_path
):
    from aws_durable_execution_conformance_tests.lmi import artifacts, runner
    from lmi_tests import shared

    configuration = "c1"
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    arn = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {configuration: target},
        "deploymentArtifactDirectory": "/old/workspace",
    }
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(deployment))
    coverage = tmp_path / "shared/standard"
    journal = (
        coverage
        / "source/cases"
        / artifacts.case_key("case")
        / "invocations/marker.json"
    )
    journal.parent.mkdir(parents=True)
    journal.write_text(
        json.dumps(
            {
                "payload": {"run": "run", "caseId": "case", "marker": "marker"},
                "function": target,
                "arn": arn,
                "durableExecutionName": "name",
            }
        )
    )
    (coverage / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "sdk": {
                    "artifacts_dir": "/old/workspace/shared/standard/source",
                    "artifacts_dir_relative": "source",
                },
                "profiles": [{"profile_id": configuration, "resource_id": target}],
                "scenarios": [{"case_id": "case", "profile_id": configuration}],
            }
        )
    )
    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = []
    service.get_durable_execution.return_value = {"Status": "RUNNING"}
    monkeypatch.setattr(
        runner, "client", lambda name, _: storage if name == "s3" else service
    )
    monkeypatch.setattr(shared, "verify_install", Mock())
    monkeypatch.setenv("LMI_SHARED_RUN_ID", "run")
    monkeypatch.setattr(
        sys, "argv", ["lmi_tests.shared", "cleanup", "--manifest", str(manifest)]
    )
    with pytest.raises(SystemExit) as exit_status:
        shared.main()
    assert exit_status.value.code == 0
    service.stop_durable_execution.assert_called_once_with(DurableExecutionArn=arn)
    assert (
        json.loads((tmp_path / "shared/cleanup.json").read_text())["status"]
        == "CLEANUP_REQUESTED"
    )
