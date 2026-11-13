# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Run the seven shared LMI lifecycle requirements."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping

from lmi_tests.deploy import ARTIFACTS, ROOT, client, save
from lmi_tests.shared_contract import CONFORMANCE_REVISION, configuration_identity
from lmi_tests.evidence import CollectionError


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

    for module in (core,):
        if not Path(module.__file__).resolve().is_relative_to(checkout.resolve()):
            raise ValueError(
                "Installed conformance packages are not from the pinned checkout; run shared setup"
            )


def make_manifest(
    deployment: Mapping[str, Any], coverage: str, directory: Path
) -> dict[str, Any]:
    from aws_durable_execution_conformance_tests.lmi.matrix import required_cases

    if coverage not in COVERAGE_PROFILES:
        raise ValueError("Unknown Python adapter coverage profile")
    actual = deepcopy(dict(deployment))
    actual["conformanceRevision"] = CONFORMANCE_REVISION
    core_wheels = {
        name: digest
        for name, digest in actual["wheels"].items()
        if name.startswith("aws_durable_execution_sdk_python-")
    }
    if len(core_wheels) != 1:
        raise ValueError("Deployment does not retain an exact SDK wheel identity")
    artifact = "sha256:" + next(iter(core_wheels.values()))
    profiles = [
        {
            "profile_id": key,
            "resource_id": actual["functions"][key],
            "native_concurrency": actual["concurrencies"][key],
            "capacity": actual["concurrencies"][key],
            "artifact_id": artifact,
            "configuration_id": configuration_identity(actual, key),
            "recovery_policy": "reuse-worker",
        }
        for key in ("c1", "c2")
    ]
    scenarios = []
    for required in required_cases("python", "lifecycle", coverage):
        concurrency = required.minimum_native_concurrency
        case_id = f"{required.requirement_id}-{required.scenario}"
        scenarios.append(
            {
                "case_id": case_id,
                "requirement_id": required.requirement_id,
                "profile_id": f"c{concurrency}",
                "scenario": required.scenario,
                "roles": list(required.roles),
                "slots": concurrency,
                "exclusive": required.exclusive,
                "behavior_budgets": {
                    "observationGraceMs": 1000,
                    "admissionMs": 5000,
                    "cleanupMs": 5000,
                    "recoveryMs": 15000,
                },
            }
        )
    return {
        "schemaVersion": 1,
        "runId": actual["run"],
        "runtime": "python",
        "region": actual["region"],
        "coverageProfile": coverage,
        "capabilityProfile": "lifecycle",
        "scenarioConcurrency": 8,
        "profiles": profiles,
        "scenarios": scenarios,
        "budgets": {
            "provisioning": 120,
            "lease": 1800,
            "placement": 120,
            "execution": 240,
            "collection": 180,
            "release": 60,
            "retirement": 180,
        },
        "sdk": {
            "deployment": actual,
            "artifacts_dir": str(directory.resolve() / "source"),
        },
    }


def run(deployment: Mapping[str, Any], directory: Path) -> int:

    from aws_durable_execution_conformance_tests.lmi.app import (
        build_report,
        coverage_inventory,
        live_results,
        load_manifest,
    )
    from aws_durable_execution_conformance_tests.reporters import (
        render_console,
        write_report,
    )

    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError(
            "Shared run output must be empty; preserve prior attempts and failure evidence"
        )
    exit_code = 0
    for coverage in COVERAGE_PROFILES:
        output = directory / coverage
        output.mkdir()
        plan = make_manifest(deployment, coverage, output)
        path = output / "manifest.json"
        save(path, plan)
        validated = load_manifest(path)
        results = asyncio.run(
            live_results(
                validated,
                "lmi_tests.shared_adapter:create_adapter",
                audit_dir=output,
            )
        )
        report = build_report(validated, results)
        save(output / "evidence.json", results)
        save(output / "coverage.json", coverage_inventory(validated))
        print(render_console(report))
        for format_name in ("json", "junit"):
            write_report(report, format_name, str(output / "report"))
        exit_code = max(exit_code, report.exit_code())
    return exit_code


def cleanup(deployment: Mapping[str, Any], directory: Path) -> int:
    """Release this run's controls and request stops without altering its reports.

    GitHub Actions holds the cloud job's concurrency slot through this step.
    Successful requests do not imply that every runtime task has retired.
    """
    plans = []
    for coverage in COVERAGE_PROFILES:
        path = directory / coverage / "manifest.json"
        if not path.exists():
            continue
        plan = json.loads(path.read_text())
        if plan.get("runId") != deployment["run"]:
            raise CollectionError("Cleanup manifest belongs to another run")
        profiles = {profile["profile_id"]: profile for profile in plan["profiles"]}
        for case in plan["scenarios"]:
            target = profiles[case["profile_id"]]["resource_id"]
            if target not in deployment["functions"].values():
                raise CollectionError("Cleanup target is outside this deployment")
        plans.append((coverage, plan))
    if not plans:
        save(
            directory / "cleanup.json",
            {
                "runId": deployment["run"],
                "status": "NO_CASES",
                "errors": [],
                "reason": "No shared case manifests were recorded; no cleanup requests were sent",
            },
        )
        return 0
    storage, service = (
        client("s3", deployment["region"]),
        client("lambda", deployment["region"]),
    )
    errors: list[str] = []
    for coverage, plan in plans:
        profiles = {profile["profile_id"]: profile for profile in plan["profiles"]}
        for case in plan["scenarios"]:
            target = profiles[case["profile_id"]]["resource_id"]
            if target not in deployment["functions"].values():
                raise CollectionError("Cleanup target is outside the owned deployment")
            owner = hashlib.sha256(case["case_id"].encode()).hexdigest()[:20]
            prefix = f"runs/{deployment['run']}/cases/{owner}/control/"
            case_dir = directory / coverage / "source" / "cases" / owner
            try:
                objects = [
                    item["Key"]
                    for page in storage.get_paginator("list_objects_v2").paginate(
                        Bucket=deployment["bucket"], Prefix=prefix
                    )
                    for item in page.get("Contents", [])
                ]
                if any(not key.startswith(prefix) for key in objects):
                    raise CollectionError(
                        "Control listing escaped the case-owned prefix"
                    )
                executions: set[str] = set()
                for path in (case_dir / "invocations").glob("*.json"):
                    invocation = json.loads(path.read_text())
                    payload = invocation.get("payload", {})
                    if (
                        payload.get("run") != deployment["run"]
                        or payload.get("caseId") != case["case_id"]
                    ):
                        raise CollectionError(
                            "Invocation intent belongs to another case"
                        )
                    if invocation.get("arn"):
                        executions.add(invocation["arn"])
                    else:
                        recovered_executions: set[str] = set()
                        marker = payload["marker"]
                        event_prefix = f"runs/{deployment['run']}/events/{marker}/"
                        for page in storage.get_paginator("list_objects_v2").paginate(
                            Bucket=deployment["bucket"], Prefix=event_prefix
                        ):
                            for obj in page.get("Contents", []):
                                with storage.get_object(
                                    Bucket=deployment["bucket"], Key=obj["Key"]
                                )["Body"] as stream:
                                    event = json.loads(stream.read())
                                if (
                                    event.get("run") != deployment["run"]
                                    or event.get("caseId") != case["case_id"]
                                ):
                                    raise CollectionError(
                                        "Recovered invocation evidence belongs to another case"
                                    )
                                recovered_executions.add(event["execution"])
                        executions.update(recovered_executions)
                        if not recovered_executions:
                            errors.append(
                                f"Unreconciled Invoke: {case['case_id']}/{marker}"
                            )
                if any(
                    not arn.startswith(target + "/durable-execution/")
                    for arn in executions
                ):
                    raise CollectionError("Refusing to stop a foreign execution")
                for key in sorted(
                    objects, key=lambda value: value != prefix + "release-all"
                ):
                    with storage.get_object(Bucket=deployment["bucket"], Key=key)[
                        "Body"
                    ] as stream:
                        state = stream.read()
                    if state not in {b"hold", b"release"}:
                        raise CollectionError(
                            "Unknown control content; refusing to overwrite it"
                        )
                    if state == b"hold":
                        storage.put_object(
                            Bucket=deployment["bucket"], Key=key, Body=b"release"
                        )
                for arn in executions:
                    if (
                        service.get_durable_execution(DurableExecutionArn=arn)["Status"]
                        == "RUNNING"
                    ):
                        try:
                            service.stop_durable_execution(DurableExecutionArn=arn)
                        except Exception:
                            if (
                                service.get_durable_execution(DurableExecutionArn=arn)[
                                    "Status"
                                ]
                                == "RUNNING"
                            ):
                                raise
            except Exception as error:
                errors.append(f"{case['case_id']}: {type(error).__name__}: {error}")
    save(
        directory / "cleanup.json",
        {
            "runId": deployment["run"],
            "errors": errors,
            "status": "FAILED" if errors else "CLEANUP_REQUESTED",
            "reason": "Run-scoped control release and execution-stop requests; case reports retain task retirement findings",
        },
    )
    return int(bool(errors))


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
    parser.add_argument("--output", type=Path, default=ARTIFACTS / "shared")
    args = parser.parse_args()
    if args.command == "setup":
        setup(args.checkout)
        return
    verify_install(args.checkout)
    deployment = json.loads(args.manifest.read_text())
    expected_run = os.environ.get("LMI_SHARED_RUN_ID")
    if expected_run and deployment.get("run") != expected_run:
        raise CollectionError("Deployment manifest belongs to a different CI run")
    if args.command == "cleanup":
        raise SystemExit(cleanup(deployment, args.output))
    if args.command == "plan":
        for coverage in COVERAGE_PROFILES:
            output = args.output / coverage
            save(output / "manifest.json", make_manifest(deployment, coverage, output))
        return
    raise SystemExit(run(deployment, args.output))


if __name__ == "__main__":
    main()
