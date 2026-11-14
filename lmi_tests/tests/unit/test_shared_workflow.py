from pathlib import Path
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
