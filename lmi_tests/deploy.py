# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Build the Python fixture and delegate deployment to the shared LMI harness."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

from aws_durable_execution_conformance_tests.lmi.artifacts import (
    clear_run_artifacts,
    save,
)
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError
from lmi_tests.runtime import RUNTIME

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


def build():
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
    parser.add_argument("command", choices=["build", "deploy", "collect", "cleanup"])
    parser.add_argument("--run-id")
    parser.add_argument("--stack-name", default=DEFAULT_STACK)
    parser.add_argument("--runtime", choices=["python3.14"], default="python3.14")
    args = parser.parse_args()
    try:
        if args.command == "build":
            build()
        elif args.command == "deploy":
            manager().deploy(args)
        else:
            manifest = json.loads((ARTIFACTS / "manifest.json").read_text())
            if args.command == "cleanup":
                manager().cleanup(manifest)
            else:
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
