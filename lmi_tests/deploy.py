# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Build the Python fixture and delegate deployment to the shared LMI harness."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import stat
import sys
import zipfile

from aws_durable_execution_conformance_tests.lmi.artifacts import (
    clear_run_artifacts,
    save,
)
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import (
    CollectionError,
    ProvisioningError,
)
from aws_durable_execution_conformance_tests.lmi.runtime import CONCURRENCIES
from lmi_tests.runtime import RUNTIME
from lmi_tests.shared import verify_install

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "lmi_tests/artifacts"
DEFAULT_STACK = "python-lmi-e2e"


def manager():
    return DeploymentManager(
        runtime=RUNTIME,
        owner="python-sdk-lmi-e2e",
        artifacts=ARTIFACTS,
        bundle=ROOT / "lmi_tests/build/function.zip",
        bucket_prefix="py-lmi-e2e",
    )


def require_settled_deployment() -> None:
    if (ARTIFACTS / "deployment-update.json").exists():
        raise ProvisioningError(
            "Clean up the interrupted deployment before rebuilding or deploying"
        )


def build():
    require_settled_deployment()
    clear_run_artifacts(ARTIFACTS)
    directory = ROOT / "lmi_tests/build"
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir()
    # The tested core SDK comes from this checkout; pip resolves its dependencies.
    for package in ("aws-durable-execution-sdk-python",):
        subprocess.run(
            ["hatch", "build", "-t", "wheel", str(directory)],
            cwd=ROOT / "packages" / package,
            env={k: v for k, v in os.environ.items() if k != "HATCH_ENV_ACTIVE"},
            check=True,
        )
    wheels = sorted(directory.glob("*.whl"))
    if len(wheels) != 1:
        raise ProvisioningError("Expected exactly the checked-out core SDK wheel")
    wheel = next(directory.glob("aws_durable_execution_sdk_python-*.whl"))
    target = directory / "package"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--only-binary=:all:",
            "--platform",
            "manylinux2014_aarch64",
            "--platform",
            "manylinux_2_28_aarch64",
            "--python-version",
            "3.14",
            "--implementation",
            "cp",
            "--abi",
            "cp314",
            "--no-compile",
            "--target",
            str(target),
            "--report",
            str(directory / "dependencies.json"),
            *map(str, wheels),
        ],
        check=True,
    )
    (target / "lmi_tests").mkdir()
    for name in (
        "__init__.py",
        "fixture.py",
        "shared_observations.py",
    ):
        shutil.copy(ROOT / "lmi_tests" / name, target / "lmi_tests" / name)
    artifact = directory / "function.zip"
    resolved = json.loads((directory / "dependencies.json").read_text())
    with zipfile.ZipFile(artifact, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(target.rglob("*")):
            if path.is_file():
                info = zipfile.ZipInfo(path.relative_to(target).as_posix())
                info.compress_type = zipfile.ZIP_DEFLATED
                mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                info.external_attr = (stat.S_IFREG | mode) << 16
                archive.writestr(info, path.read_bytes())
    save(
        ARTIFACTS / "build.json",
        {
            "commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "wheel": wheel.name,
            "wheels": {
                item.name: hashlib.sha256(item.read_bytes()).hexdigest()
                for item in wheels
            },
            "dependencies": [
                {
                    "name": item["metadata"]["name"],
                    "version": item["metadata"]["version"],
                    "hashes": item.get("download_info", {})
                    .get("archive_info", {})
                    .get("hashes", {}),
                }
                for item in resolved["install"]
            ],
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["build", "deploy", "collect"])
    parser.add_argument("--run-id")
    parser.add_argument(
        "--checkout",
        type=Path,
        default=Path(
            os.environ.get("LMI_CONFORMANCE_CHECKOUT", str(ROOT / ".conformance"))
        ),
    )
    parser.add_argument("--stack-name", default=DEFAULT_STACK)
    parser.add_argument("--runtime", choices=["python3.14"], default="python3.14")
    args = parser.parse_args()
    try:
        if args.command == "build":
            build()
        elif args.command == "deploy":
            require_settled_deployment()
            if not args.run_id or not re.fullmatch(r"[a-z0-9-]{1,24}", args.run_id):
                raise ProvisioningError(
                    "run-id must be 1-24 lowercase letters, digits or hyphens"
                )
            if not re.fullmatch(r"[a-z][a-z0-9-]{0,49}", args.stack_name):
                raise ProvisioningError(
                    "stack-name must be 1-50 lowercase letters, digits or hyphens, starting with a letter"
                )
            verify_install(args.checkout)
            manager().deploy(args)
        else:
            manifest = json.loads((ARTIFACTS / "manifest.json").read_text())
            expected_run = args.run_id or os.environ.get("LMI_SHARED_RUN_ID")
            if expected_run and manifest.get("run") != expected_run:
                raise CollectionError(
                    "Deployment manifest belongs to a different collection run"
                )
            functions = manifest["functions"]
            name_map = ARTIFACTS / "function-name-map.json"
            if (
                (ARTIFACTS / "deployment-update.json").exists()
                or set(functions) != set(CONCURRENCIES)
                or not name_map.is_file()
                or json.loads(name_map.read_text())
                != {key: {RUNTIME.handler: arn} for key, arn in functions.items()}
            ):
                raise CollectionError(
                    "Deployment manifest has no verified collection targets"
                )
            from lmi_tests.cloud import Cloud

            Cloud(manifest).collect()
    except Exception as error:
        save(
            ARTIFACTS / f"{args.command}-error.json",
            {"type": type(error).__name__, "message": str(error)},
        )
        raise


if __name__ == "__main__":
    main()
