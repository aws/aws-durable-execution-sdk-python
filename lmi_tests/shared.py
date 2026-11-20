# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Bootstrap the pinned shared harness and run it with the Python adapter."""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping
from lmi_tests.shared_contract import CONFORMANCE_REVISION

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "lmi_tests/artifacts"
COVERAGE_PROFILES = ("standard",)


def verify_checkout(checkout: Path) -> None:
    revision = subprocess.check_output(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True
    ).strip()
    if revision != CONFORMANCE_REVISION:
        raise ValueError(
            f"Conformance checkout must be pinned to {CONFORMANCE_REVISION}"
        )
    changed = subprocess.check_output(
        ["git", "-C", str(checkout), "status", "--porcelain", "--", "packages"],
        text=True,
    )
    if changed:
        raise ValueError("Pinned conformance package sources have local changes")


def setup(checkout: Path) -> None:
    verify_checkout(checkout)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--editable",
            str(checkout / "packages/aws-durable-execution-conformance-tests"),
        ],
        check=True,
    )


def verify_install(checkout: Path) -> None:
    verify_checkout(checkout)
    import aws_durable_execution_conformance_tests as core

    source = (
        checkout.resolve()
        / "packages/aws-durable-execution-conformance-tests/src/aws_durable_execution_conformance_tests/__init__.py"
    )
    if Path(core.__file__).resolve() != source:
        raise ValueError(
            "Installed conformance package is not from the pinned checkout source; run shared setup"
        )


def make_manifest(
    deployment: Mapping[str, Any], coverage: str, directory: Path
) -> dict[str, Any]:
    from aws_durable_execution_conformance_tests.lmi.runner import (
        make_manifest as shared_manifest,
    )
    from lmi_tests.shared_contract import configuration_identity

    core_wheels = {
        name: digest
        for name, digest in deployment["wheels"].items()
        if name.startswith("aws_durable_execution_sdk_python-")
    }
    if len(core_wheels) != 1:
        raise ValueError("Deployment does not retain an exact SDK wheel identity")
    return shared_manifest(
        deployment,
        coverage,
        directory,
        runtime="python",
        artifact_id="sha256:" + next(iter(core_wheels.values())),
        conformance_revision=CONFORMANCE_REVISION,
        configuration_identity=configuration_identity,
    )


def run(deployment: Mapping[str, Any], directory: Path) -> int:
    from aws_durable_execution_conformance_tests.lmi.runner import run as shared_run

    return shared_run(
        deployment,
        directory,
        manifest_factory=make_manifest,
        adapter_name="lmi_tests.shared_adapter:create_adapter",
    )


def cleanup(deployment: Mapping[str, Any], directory: Path | None) -> int:
    from aws_durable_execution_conformance_tests.lmi.runner import (
        cleanup as shared_cleanup,
    )

    return shared_cleanup(deployment, directory)


def cleanup_inputs(
    manifest: Path, output: Path | None, expected_run: str | None
) -> tuple[dict[str, Any], Path | None]:
    from aws_durable_execution_conformance_tests.lmi import (
        deployment as shared_deployment,
    )
    from aws_durable_execution_conformance_tests.lmi.errors import CollectionError

    supplied_artifacts = manifest.parent.resolve()
    manifest = manifest.resolve()
    artifacts = manifest.parent
    # Before archival, the active manifest can still link to older artifacts.
    # The journal beside the supplied link owns the requested recovery.
    if (supplied_artifacts / "deployment-update.json").is_file():
        artifacts = supplied_artifacts
    journal = artifacts / "deployment-update.json"
    pending_update = journal.is_file()
    loader = getattr(shared_deployment, "load_cleanup_manifest", None)
    if loader is None and pending_update:
        raise CollectionError(
            "Use the deployment's conformance revision to recover its journal"
        )
    if loader is not None and expected_run:
        deployment = loader(artifacts, expected_run, active_manifest=manifest)
    else:
        deployment = json.loads(manifest.read_text())

    run = deployment.get("run")
    if expected_run and run != expected_run:
        raise CollectionError("Deployment manifest belongs to a different CI run")
    if loader is None:
        return deployment, output or artifacts / "shared"

    if output is None and deployment.get("deploymentArtifactDirectory") != str(
        artifacts
    ):
        pointer = artifacts / "case-output.json"
        if pointer.is_file():
            saved = json.loads(pointer.read_text())
            directory = saved.get("directory")
            if saved.get("runId") == run and isinstance(directory, str) and directory:
                if Path(directory).is_absolute():
                    raise CollectionError(
                        "Specify --output for relocated artifacts with an absolute case output path"
                    )
    # The shared reconciler verifies journal identity before changing AWS.
    # Only completed artifacts may be relocated to their local manifest parent.
    if not pending_update:
        deployment["deploymentArtifactDirectory"] = str(artifacts)
    return deployment, output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "plan", "run", "cleanup"))
    parser.add_argument(
        "--checkout",
        type=Path,
        default=Path(
            os.environ.get("LMI_CONFORMANCE_CHECKOUT", str(ROOT / ".conformance"))
        ),
    )
    parser.add_argument("--manifest", type=Path, default=ARTIFACTS / "manifest.json")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "setup":
        setup(args.checkout)
        return
    from aws_durable_execution_conformance_tests.lmi.artifacts import save
    from aws_durable_execution_conformance_tests.lmi.errors import CollectionError

    verify_install(args.checkout)
    expected_run = os.environ.get("LMI_SHARED_RUN_ID")
    if args.command == "cleanup":
        deployment, directory = cleanup_inputs(args.manifest, args.output, expected_run)
        raise SystemExit(cleanup(deployment, directory))

    deployment = json.loads(args.manifest.read_text())
    if expected_run and deployment.get("run") != expected_run:
        raise CollectionError("Deployment manifest belongs to a different CI run")

    destination = args.output or args.manifest.resolve().parent / (
        "preview" if args.command == "plan" else "shared"
    )
    if args.command == "plan":
        destination.mkdir(parents=True)
        for coverage in COVERAGE_PROFILES:
            output = destination / coverage
            save(output / "manifest.json", make_manifest(deployment, coverage, output))
        return
    raise SystemExit(run(deployment, destination))


if __name__ == "__main__":
    main()
