# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Run the SDK-owned fixtures through the pinned shared conformance package."""

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
from lmi_tests.shared_lock import OwnershipError, RunLease


COVERAGE_PROFILES = ("python-lifecycle", "export-lifecycle")
BEHAVIOR_BUDGETS = {
    "observationGraceMs": 1000,
    "cleanupMs": 5000,
    "recoveryMs": 5000,
    "retryMs": 40000,
    "progressMs": 15000,
    "maxThreadGrowth": 1,
    "maxFdGrowth": 8,
    "maxRssGrowthBytes": 32 * 1024 * 1024,
    "exportMs": 30000,
    "exportCompletionMs": 30000,
    "flushWaitMs": 10000,
}


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
            "--editable",
            str(checkout / "packages/aws-durable-execution-conformance-tests-otel"),
        ],
        check=True,
    )


def verify_install(checkout: Path) -> None:
    verify_checkout(checkout)
    import aws_durable_execution_conformance_tests as core
    import aws_durable_execution_conformance_tests_otel as otel

    for module in (core, otel):
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
    if "sharedLease" not in deployment:
        raise ValueError(
            "Deployment was not prepared with persistent shared-run ownership"
        )
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
    for required in required_cases("python", "lifecycle-otel", coverage):
        case_id = f"{coverage}-{required.scenario}-c{required.native_concurrency}-{required.otel_view or 'baseline'}-{required.export_mode or 'sdk-default'}"
        scenarios.append(
            {
                "case_id": case_id,
                "cohort_id": "cohort-1",
                "requirement_id": required.requirement_id,
                "profile_id": f"c{required.native_concurrency}",
                "scenario": required.scenario,
                "otel_view": required.otel_view,
                "export_mode": required.export_mode,
                "roles": list(required.roles),
                "slots": required.minimum_native_concurrency,
                "exclusive": required.exclusive,
                "behavior_budgets": {
                    name: BEHAVIOR_BUDGETS[name] for name in required.budget_names
                },
            }
        )
    return {
        "schemaVersion": 1,
        "runId": actual["run"],
        "runtime": "python",
        "region": actual["region"],
        "coverageProfile": coverage,
        "capabilityProfile": "lifecycle-otel",
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
            "lease": actual["sharedLease"],
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
    lease = RunLease.from_manifest(
        {"deployment": deployment, "lease": deployment["sharedLease"]}
    )
    exit_code = 0
    completed = False
    try:
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
        completed = True
    finally:
        if completed:
            try:
                lease.release()
                save(
                    directory / "ownership-released.json",
                    {"runId": deployment["run"], "lease": lease.to_manifest()},
                )
            except OwnershipError as error:
                save(
                    directory / "ownership-retained.json",
                    {"error": str(error), "lease": lease.to_manifest()},
                )
                exit_code = 1
        else:
            save(
                directory / "ownership-retained.json",
                {
                    "reason": "Interrupted or incomplete shared run",
                    "lease": lease.to_manifest(),
                },
            )
    return exit_code


def cleanup(deployment: Mapping[str, Any], directory: Path) -> int:
    """Release only persisted case controls; unknown retirement retains ownership.

    This is the final CI recovery path after the runner process exits. It never
    rewrites assertion results or clears quarantine based on service status alone.
    """
    receipt = directory / "ownership-released.json"
    lease = RunLease.from_manifest(
        {"deployment": deployment, "lease": deployment["sharedLease"]}
    )
    if receipt.exists() and json.loads(receipt.read_text()) == {
        "runId": deployment["run"],
        "lease": lease.to_manifest(),
    }:
        return 0
    record, _ = lease._read()
    record = lease._owned(record)
    if all(value == "quiescent" for value in record["profiles"].values()):
        lease.release()
        save(receipt, {"runId": deployment["run"], "lease": lease.to_manifest()})
        return 0
    storage, service = (
        client("s3", deployment["region"]),
        client("lambda", deployment["region"]),
    )
    errors: list[str] = []
    for coverage in COVERAGE_PROFILES:
        plan_path = directory / coverage / "manifest.json"
        if not plan_path.exists():
            continue
        plan = json.loads(plan_path.read_text())
        if plan.get("runId") != deployment["run"]:
            raise OwnershipError("Cleanup manifest belongs to another run")
        profiles = {profile["profile_id"]: profile for profile in plan["profiles"]}
        for case in plan["scenarios"]:
            target = profiles[case["profile_id"]]["resource_id"]
            if target not in deployment["functions"].values():
                raise OwnershipError("Cleanup target is outside the owned deployment")
            owner = hashlib.sha256(
                f"{case['case_id']}:{case.get('cohort_id', 'cohort-1')}".encode()
            ).hexdigest()[:20]
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
                    raise OwnershipError(
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
                        raise OwnershipError(
                            "Invocation intent belongs to another case"
                        )
                    if invocation.get("arn"):
                        executions.add(invocation["arn"])
                    else:
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
                                    raise OwnershipError(
                                        "Recovered invocation evidence belongs to another case"
                                    )
                                executions.add(event["execution"])
                        if not executions:
                            errors.append(
                                f"Unreconciled Invoke: {case['case_id']}/{marker}"
                            )
                if any(
                    not arn.startswith(target + "/durable-execution/")
                    for arn in executions
                ):
                    raise OwnershipError("Refusing to stop a foreign execution")
                for key in sorted(
                    objects, key=lambda value: value != prefix + "release-all"
                ):
                    with storage.get_object(Bucket=deployment["bucket"], Key=key)[
                        "Body"
                    ] as stream:
                        state = stream.read()
                    if state not in {b"hold", b"release"}:
                        raise OwnershipError(
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
    for target, status in record["profiles"].items():
        if status != "quiescent":
            lease.finish_profile(target, False)
    save(
        directory / "cleanup.json",
        {
            "runId": deployment["run"],
            "errors": errors,
            "status": "OWNERSHIP_RETAINED",
            "reason": "Fault release and Stop do not prove task/export quiescence; retained run requires reconciliation",
        },
    )
    return 1


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
        raise OwnershipError("Deployment manifest belongs to a different CI run")
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
