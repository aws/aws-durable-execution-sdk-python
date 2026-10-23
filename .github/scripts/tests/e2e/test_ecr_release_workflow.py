# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exercise release workflow shell steps against a local registry model.

The workflow remains the source of the shell logic. Only external CLIs are
replaced, so publishing tests do not need credentials or access to a registry.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, TypedDict

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[4]
WORKFLOW = ROOT / ".github/workflows/ecr-release.yml"
VERSION = "1.2.3"
REGISTRY = "public.ecr.aws"
REPOSITORY = "durable-functions/aws-durable-execution-emulator"


def image_manifest(label: str) -> str:
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [],
            "annotations": {"org.example.release": label},
        },
        indent=2,
    )


RELEASE_MANIFEST = image_manifest(VERSION)
OLD_MANIFEST = image_manifest("1.2.2")
OTHER_MANIFEST = image_manifest("2.0.0")
MANIFESTS = {
    "sha256:" + hashlib.sha256(raw.encode()).hexdigest(): raw
    for raw in (RELEASE_MANIFEST, OLD_MANIFEST, OTHER_MANIFEST)
}
RELEASE_DIGEST, OLD_DIGEST, OTHER_DIGEST = MANIFESTS


class RegistryState(TypedDict):
    tags: dict[str, str]
    manifests: dict[str, str]
    responses: dict[str, list[str | None]]
    tag_overrides: dict[str, list[str]]


# This models registry data and CLI I/O, not the workflow's release decisions.
CLI = r"""
import hashlib
import json
import os
import sys
from pathlib import Path

name = sys.argv[1]
args = sys.argv[2:]
if name == "python":
    if args[:3] == ["-m", "pip", "install"]:
        raise SystemExit(0)
    os.execv(sys.executable, [sys.executable, *args])
if name == "sleep":
    raise SystemExit(0)

state_path = Path(os.environ["REGISTRY_STATE"])
state = json.loads(state_path.read_text())
with Path(os.environ["CLI_LOG"]).open("a") as log:
    log.write(json.dumps([name, *args]) + "\n")

def save() -> None:
    state_path.write_text(json.dumps(state))

def error(code: str) -> None:
    print(f"An error occurred ({code}) when calling the operation", file=sys.stderr)
    raise SystemExit(254)

def option(flag: str, default: str | None = None) -> str | None:
    return args[args.index(flag) + 1] if flag in args else default

def injected_error(key: str) -> None:
    responses = state.get("responses", {}).get(key, [])
    if responses:
        response = responses.pop(0)
        save()
        if response:
            error(response)

if name == "aws" and args[:2] == ["ecr-public", "describe-images"]:
    image_id = option("--image-ids")
    injected_error(image_id)
    kind, value = image_id.split("=", 1)
    digest = state["tags"].get(value) if kind == "imageTag" else value
    if not digest or digest not in state["manifests"]:
        error("ImageNotFoundException")
    tags = state.get("tag_overrides", {}).get(digest)
    if tags is None:
        tags = [tag for tag, target in state["tags"].items() if target == digest]
    query = option("--query", "")
    if query == "imageDetails[0].imageDigest":
        print(digest)
    elif query == "imageDetails[0].imageTags[]":
        print("\t".join(tags))
    else:
        print(json.dumps({"imageDetails": [{"imageDigest": digest, "imageTags": tags}]}))
elif name == "docker" and args[:3] == ["buildx", "imagetools", "inspect"]:
    assert "--raw" in args, args
    source = args[-1]
    assert "@sha256:" in source, source
    digest = source.split("@", 1)[1]
    sys.stdout.write(state["manifests"][digest])
elif name == "docker" and args[:2] == ["manifest", "create"]:
    manifests = []
    for source in args[3:]:
        digest = state["tags"][source.rsplit(":", 1)[1]]
        manifests.append({
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "digest": digest,
            "size": len(state["manifests"][digest].encode()),
            "platform": {},
        })
    state.setdefault("local_manifests", {})[args[2]] = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "manifests": manifests,
    }
    save()
elif name == "docker" and args[:2] == ["manifest", "annotate"]:
    digest = state["tags"][args[3].rsplit(":", 1)[1]]
    descriptor = next(
        item for item in state["local_manifests"][args[2]]["manifests"]
        if item["digest"] == digest
    )
    descriptor["platform"] = {"architecture": option("--arch"), "os": option("--os")}
    save()
elif name == "docker" and args[:2] == ["manifest", "push"]:
    raw = json.dumps(state["local_manifests"][args[2]], indent=2)
    digest = "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
    state["manifests"][digest] = raw
    state["tags"][args[2].rsplit(":", 1)[1]] = digest
    save()
    print(digest)
elif name == "aws" and args[:2] == ["ecr-public", "put-image"]:
    injected_error("put-image")
    raw = Path(option("--image-manifest").removeprefix("file://")).read_text()
    digest = "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
    assert option("--image-digest") == digest, (args, digest)
    state["manifests"][digest] = raw
    state["tags"][option("--image-tag")] = digest
    save()
    injected_error("put-image-after-write")
    print(json.dumps({"image": {"imageId": {"imageDigest": digest}}}))
else:
    raise AssertionError(f"Unexpected external command: {name} {args}")
"""


class ReleaseWorkflow:
    def __init__(self, directory: Path, interpreter: Path | None = None) -> None:
        self.directory = directory
        self.workflow: dict[str, Any] = yaml.safe_load(WORKFLOW.read_text())
        self.state_path = directory / "registry.json"
        self.log_path = directory / "commands.jsonl"
        self.output_path = directory / "output"
        self.summary_path = directory / "summary"
        binaries = directory / "bin"
        binaries.mkdir()
        stub = directory / "stub.py"
        stub.write_text(CLI)
        for name in ("aws", "docker", "python", "sleep"):
            binary = binaries / name
            # CodeBuild interpreter paths can exceed the kernel's shebang limit.
            command = shlex.join([str(interpreter or sys.executable), str(stub), name])
            binary.write_text(f'#!/bin/sh\nexec {command} "$@"\n')
            binary.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "REGISTRY_STATE": str(self.state_path),
            "CLI_LOG": str(self.log_path),
            "GITHUB_OUTPUT": str(self.output_path),
            "GITHUB_STEP_SUMMARY": str(self.summary_path),
            "VERSION": VERSION,
            "ECR_REGISTRY": REGISTRY,
            "ECR_REPOSITORY": REPOSITORY,
        }
        self.registry(
            {f"v{VERSION}": RELEASE_DIGEST, "v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST}
        )

    def registry(
        self,
        tags: dict[str, str],
        *,
        responses: dict[str, list[str | None]] | None = None,
        tag_overrides: dict[str, list[str]] | None = None,
    ) -> None:
        self.state_path.write_text(
            json.dumps(
                {
                    "tags": tags,
                    "manifests": MANIFESTS,
                    "responses": responses or {},
                    "tag_overrides": tag_overrides or {},
                }
            )
        )

    @property
    def state(self) -> RegistryState:
        return json.loads(self.state_path.read_text())

    @property
    def commands(self) -> list[list[str]]:
        if not self.log_path.exists():
            return []
        return [json.loads(line) for line in self.log_path.read_text().splitlines()]

    @property
    def outputs(self) -> dict[str, str]:
        return dict(
            line.split("=", 1) for line in self.output_path.read_text().splitlines()
        )

    @property
    def writes(self) -> list[list[str]]:
        return [
            command
            for command in self.commands
            if command[:3] == ["aws", "ecr-public", "put-image"]
            or (command[0] == "docker" and "push" in command)
        ]

    def run(self, job: str, name: str, **env: str) -> subprocess.CompletedProcess[str]:
        step = next(
            step
            for step in self.workflow["jobs"][job]["steps"]
            if step.get("name") == name
        )
        script = step["run"].replace("${{ env.aws_region }}", "us-east-1")
        assert "${{" not in script, "Add the required workflow context to this harness"
        self.output_path.write_text("")
        return subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
            cwd=ROOT,
            env={**self.env, **env},
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )

    def advance_latest(self) -> subprocess.CompletedProcess[str]:
        return self.run(
            "update-latest-manifest", "Advance latest to this version when it is newer"
        )

    def verify(self, *, latest: bool = True) -> subprocess.CompletedProcess[str]:
        return self.run(
            "verify-publish",
            "Verify the version and latest tags on public ECR",
            VERIFY_LATEST=str(latest).lower(),
        )


@pytest.fixture
def release(tmp_path: Path) -> ReleaseWorkflow:
    return ReleaseWorkflow(tmp_path)


def assert_success(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, result.stdout + result.stderr


def test_cli_launchers_support_long_interpreter_paths(tmp_path: Path) -> None:
    interpreter = tmp_path / ("runner" * 20) / ("workspace" * 14) / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.symlink_to(sys.executable)
    assert len(os.fsencode(interpreter)) > 255
    directory = tmp_path / "fixture with spaces and 'quotes'"
    directory.mkdir()
    release = ReleaseWorkflow(directory, interpreter=interpreter)

    assert_success(
        release.run("preflight", "Check whether the image tag already exists")
    )
    assert release.outputs["exists"] == "true"
    assert release.commands[0][:3] == ["aws", "ecr-public", "describe-images"]

    arguments = ["argument with spaces", "'literal quotes'"]
    result = subprocess.run(
        [
            str(directory / "bin/python"),
            "-c",
            "import json, sys; print(json.dumps(sys.argv[1:]))",
            *arguments,
        ],
        env=release.env,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert_success(result)
    assert json.loads(result.stdout) == arguments


def test_non_testing_release_skips_without_contacting_registry(
    release: ReleaseWorkflow,
) -> None:
    assert_success(
        release.run(
            "preflight",
            "Parse testing version from the release tag",
            RELEASE_TAG="sdk-v2.0.0,otel-v1.0.0",
        )
    )
    assert release.outputs["tag_version"] == ""
    assert_success(
        release.run(
            "preflight", "Emit release plan", TAG_VERSION="", VERSION="", EXISTS=""
        )
    )
    assert release.outputs == {
        "should_build": "false",
        "should_update_latest": "false",
        "version": "",
    }
    assert release.commands == []


def test_source_tag_mismatch_stops_before_publishing(release: ReleaseWorkflow) -> None:
    about = release.directory / "__about__.py"
    about.write_text('__version__ = "1.2.2"\n')
    result = release.run(
        "preflight",
        "Verify the tag version matches the source",
        TAG_VERSION=VERSION,
        ABOUT_PATH=str(about),
    )
    assert result.returncode != 0
    assert release.commands == []


@pytest.mark.parametrize("built_this_run", ["true", "false"])
def test_existing_version_is_not_republished_even_with_cached_build_plan(
    release: ReleaseWorkflow, built_this_run: str
) -> None:
    assert_success(
        release.run("preflight", "Check whether the image tag already exists")
    )
    assert release.outputs["exists"] == "true"
    assert_success(
        release.run(
            "preflight", "Emit release plan", TAG_VERSION=VERSION, EXISTS="true"
        )
    )
    assert release.outputs["should_build"] == "false"
    assert release.outputs["should_update_latest"] == "true"
    assert_success(
        release.run(
            "create-ecr-manifest-per-arch",
            "Create the version manifest",
            BUILT_THIS_RUN=built_this_run,
        )
    )
    assert release.state["tags"][f"v{VERSION}"] == RELEASE_DIGEST
    assert release.writes == []


@pytest.mark.parametrize(
    "per_arch_tags",
    [{}, {f"v{VERSION}-arm64": OTHER_DIGEST, f"v{VERSION}-x86_64": OTHER_DIGEST}],
)
def test_recovery_promotes_exact_published_manifest_without_architecture_tags(
    release: ReleaseWorkflow, per_arch_tags: dict[str, str]
) -> None:
    release.registry(
        {
            f"v{VERSION}": RELEASE_DIGEST,
            "v1.2.2": OLD_DIGEST,
            "latest": OLD_DIGEST,
            **per_arch_tags,
        }
    )
    assert_success(release.advance_latest())
    assert release.state["tags"]["latest"] == RELEASE_DIGEST
    assert release.state["manifests"][RELEASE_DIGEST] == RELEASE_MANIFEST
    assert len(release.writes) == 1
    assert all(
        "-arm64" not in argument and "-x86_64" not in argument
        for command in release.commands
        for argument in command
    )
    assert_success(release.verify())


def test_first_latest_tag_can_be_published(release: ReleaseWorkflow) -> None:
    release.registry({f"v{VERSION}": RELEASE_DIGEST})
    assert_success(release.advance_latest())
    assert release.state["tags"]["latest"] == RELEASE_DIGEST


def test_backport_does_not_replace_newer_latest(release: ReleaseWorkflow) -> None:
    release.registry(
        {f"v{VERSION}": RELEASE_DIGEST, "v2.0.0": OTHER_DIGEST, "latest": OTHER_DIGEST}
    )
    assert_success(release.advance_latest())
    assert release.writes == []
    assert release.state["tags"]["latest"] == OTHER_DIGEST
    assert_success(release.verify())


@pytest.mark.parametrize(
    "job,step,image_id",
    [
        (
            "preflight",
            "Check whether the image tag already exists",
            f"imageTag=v{VERSION}",
        ),
        (
            "create-ecr-manifest-per-arch",
            "Create the version manifest",
            f"imageTag=v{VERSION}",
        ),
        (
            "update-latest-manifest",
            "Advance latest to this version when it is newer",
            "imageTag=latest",
        ),
        (
            "update-latest-manifest",
            "Advance latest to this version when it is newer",
            f"imageDigest={OLD_DIGEST}",
        ),
    ],
)
def test_lookup_errors_fail_closed_without_writes(
    release: ReleaseWorkflow, job: str, step: str, image_id: str
) -> None:
    release.registry(
        {f"v{VERSION}": RELEASE_DIGEST, "v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST},
        responses={image_id: ["AccessDeniedException"] * 20},
    )
    result = release.run(job, step, BUILT_THIS_RUN="true")
    assert result.returncode != 0
    assert release.writes == []
    assert release.state["tags"]["latest"] == OLD_DIGEST


@pytest.mark.parametrize(
    "tags,tag_overrides",
    [
        ({f"v{VERSION}": RELEASE_DIGEST}, {}),
        ({f"v{VERSION}": RELEASE_DIGEST, "latest": OTHER_DIGEST}, {}),
        (
            {f"v{VERSION}": RELEASE_DIGEST, "latest": OTHER_DIGEST},
            {OTHER_DIGEST: ["latest", f"v{VERSION}"]},
        ),
        (
            {f"v{VERSION}": RELEASE_DIGEST, "v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST},
            {},
        ),
    ],
)
def test_verification_rejects_missing_unknown_inconsistent_or_older_latest(
    release: ReleaseWorkflow, tags: dict[str, str], tag_overrides: dict[str, list[str]]
) -> None:
    release.registry(tags, tag_overrides=tag_overrides)
    assert release.verify().returncode != 0
    assert release.writes == []


def test_verification_retries_registry_propagation(release: ReleaseWorkflow) -> None:
    release.registry(
        {f"v{VERSION}": RELEASE_DIGEST, "latest": RELEASE_DIGEST},
        responses={
            "imageTag=latest": ["ImageNotFoundException", "ServerException", None]
        },
    )
    assert_success(release.verify())
    latest_reads = [
        command for command in release.commands if "imageTag=latest" in command
    ]
    assert len(latest_reads) >= 3
    assert release.writes == []


def test_fresh_version_is_published_before_latest_and_verifies(
    release: ReleaseWorkflow,
) -> None:
    release.registry(
        {
            "v1.2.2": OLD_DIGEST,
            "latest": OLD_DIGEST,
            f"v{VERSION}-arm64": RELEASE_DIGEST,
            f"v{VERSION}-x86_64": OTHER_DIGEST,
        }
    )
    assert_success(
        release.run("preflight", "Check whether the image tag already exists")
    )
    assert release.outputs["exists"] == "false"
    assert_success(
        release.run(
            "preflight", "Emit release plan", TAG_VERSION=VERSION, EXISTS="false"
        )
    )
    assert release.outputs == {
        "should_build": "true",
        "should_update_latest": "true",
        "version": VERSION,
    }
    assert_success(
        release.run(
            "create-ecr-manifest-per-arch",
            "Create the version manifest",
            BUILT_THIS_RUN="true",
        )
    )
    version_digest = release.state["tags"][f"v{VERSION}"]
    manifest = json.loads(release.state["manifests"][version_digest])
    assert {
        (item["platform"]["architecture"], item["platform"]["os"], item["digest"])
        for item in manifest["manifests"]
    } == {
        ("arm64", "linux", RELEASE_DIGEST),
        ("amd64", "linux", OTHER_DIGEST),
    }
    assert release.writes == [
        ["docker", "manifest", "push", f"{REGISTRY}/{REPOSITORY}:v{VERSION}"]
    ]
    assert release.state["tags"]["latest"] == OLD_DIGEST
    assert_success(release.advance_latest())
    assert release.state["tags"]["latest"] == version_digest
    assert_success(release.verify())


def test_recovery_fails_without_writes_if_published_version_disappears(
    release: ReleaseWorkflow,
) -> None:
    release.registry({"v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST})
    result = release.run(
        "create-ecr-manifest-per-arch",
        "Create the version manifest",
        BUILT_THIS_RUN="false",
    )
    assert result.returncode != 0
    assert release.writes == []


def test_put_image_failure_stops_promotion(release: ReleaseWorkflow) -> None:
    release.registry(
        {f"v{VERSION}": RELEASE_DIGEST, "v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST},
        responses={"put-image": ["ServerException"]},
    )
    assert release.advance_latest().returncode != 0
    assert release.state["tags"]["latest"] == OLD_DIGEST
    assert len(release.writes) == 1


@pytest.mark.parametrize("write_completed", [True, False])
def test_already_exists_requires_verification_of_actual_registry_state(
    release: ReleaseWorkflow, write_completed: bool
) -> None:
    response_key = "put-image-after-write" if write_completed else "put-image"
    release.registry(
        {f"v{VERSION}": RELEASE_DIGEST, "v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST},
        responses={response_key: ["ImageAlreadyExistsException"]},
    )
    assert_success(release.advance_latest())
    assert len(release.writes) == 1
    verification = release.verify()
    assert (verification.returncode == 0) is write_completed
    assert len(release.writes) == 1


def test_post_release_version_is_promoted_and_recognized(
    release: ReleaseWorkflow,
) -> None:
    version = VERSION + ".post1"
    release.env["VERSION"] = version
    release.registry(
        {f"v{version}": RELEASE_DIGEST, f"v{VERSION}": OLD_DIGEST, "latest": OLD_DIGEST}
    )
    assert_success(
        release.run(
            "preflight",
            "Parse testing version from the release tag",
            RELEASE_TAG=f"testing-v{version}",
        )
    )
    assert release.outputs["tag_version"] == version
    assert_success(
        release.run(
            "preflight", "Emit release plan", TAG_VERSION=version, EXISTS="true"
        )
    )
    assert release.outputs["should_update_latest"] == "true"
    assert_success(release.advance_latest())
    assert release.state["tags"]["latest"] == RELEASE_DIGEST
    assert_success(release.verify())


@pytest.mark.parametrize("version", ["2.1.0a1", "2.1.0b1", "2.1.0rc1", "2.1.0.dev1"])
def test_prerelease_plan_preserves_version_publication(
    release: ReleaseWorkflow, version: str
) -> None:
    release.env["VERSION"] = version
    assert_success(
        release.run(
            "preflight", "Emit release plan", TAG_VERSION=version, EXISTS="false"
        )
    )
    assert release.outputs == {
        "should_build": "true",
        "should_update_latest": "false",
        "version": version,
    }


@pytest.mark.parametrize("already_published", [False, True])
@pytest.mark.parametrize("latest_exists", [False, True])
def test_prerelease_publication_and_recovery_leave_latest_untouched(
    release: ReleaseWorkflow, already_published: bool, latest_exists: bool
) -> None:
    version = VERSION + "rc1"
    release.env["VERSION"] = version
    tags = {"v1.2.2": OLD_DIGEST}
    if latest_exists:
        tags["latest"] = OLD_DIGEST
    if already_published:
        tags[f"v{version}"] = RELEASE_DIGEST
    else:
        tags[f"v{version}-arm64"] = RELEASE_DIGEST
        tags[f"v{version}-x86_64"] = OTHER_DIGEST
    release.registry(tags)

    assert_success(
        release.run("preflight", "Check whether the image tag already exists")
    )
    exists = release.outputs["exists"]
    assert exists == str(already_published).lower()
    assert_success(
        release.run(
            "preflight", "Emit release plan", TAG_VERSION=version, EXISTS=exists
        )
    )
    assert release.outputs == {
        "should_build": str(not already_published).lower(),
        "should_update_latest": "false",
        "version": version,
    }
    assert_success(
        release.run(
            "create-ecr-manifest-per-arch",
            "Create the version manifest",
            BUILT_THIS_RUN=str(not already_published).lower(),
        )
    )
    assert_success(release.verify(latest=False))
    assert release.state["tags"].get("latest") == tags.get("latest")
    assert set(release.state["tags"]) == set(tags) | {f"v{version}"}
    assert release.writes == (
        []
        if already_published
        else [["docker", "manifest", "push", f"{REGISTRY}/{REPOSITORY}:v{version}"]]
    )
    assert all("imageTag=latest" not in command for command in release.commands)


def test_prerelease_verification_requires_its_version_image(
    release: ReleaseWorkflow,
) -> None:
    release.env["VERSION"] = VERSION + "rc1"
    release.registry({"v1.2.2": OLD_DIGEST, "latest": OLD_DIGEST})
    assert release.verify(latest=False).returncode != 0
    assert release.writes == []
    assert all("imageTag=latest" not in command for command in release.commands)


def test_verification_rejects_missing_latest_policy(release: ReleaseWorkflow) -> None:
    release.env.pop("VERIFY_LATEST", None)
    result = release.run(
        "verify-publish", "Verify the version and latest tags on public ECR"
    )
    assert result.returncode != 0
    assert release.commands == []


def test_stable_verification_rejects_a_newer_prerelease_latest(
    release: ReleaseWorkflow,
) -> None:
    release.registry(
        {
            f"v{VERSION}": RELEASE_DIGEST,
            "v2.0.0rc1": OTHER_DIGEST,
            "latest": OTHER_DIGEST,
        }
    )
    assert release.verify().returncode != 0
    assert release.writes == []


def test_entire_release_is_serialized_without_dropping_pending_runs(
    release: ReleaseWorkflow,
) -> None:
    assert release.workflow["concurrency"] == {
        "group": "ecr-release",
        "queue": "max",
        "cancel-in-progress": False,
    }
    jobs = release.workflow["jobs"]
    assert all("concurrency" not in job for job in jobs.values())
    assert jobs["build-and-upload-image-to-ecr"]["needs"] == "preflight"
    assert (
        "needs.preflight.outputs.should_build"
        in jobs["build-and-upload-image-to-ecr"]["if"]
    )
    assert set(jobs["create-ecr-manifest-per-arch"]["needs"]) == {
        "preflight",
        "build-and-upload-image-to-ecr",
    }
    assert "!cancelled()" in jobs["create-ecr-manifest-per-arch"]["if"]
    assert (
        "needs.preflight.outputs.version != ''"
        in jobs["create-ecr-manifest-per-arch"]["if"]
    )
    assert "should_update_latest" not in jobs["create-ecr-manifest-per-arch"]["if"]
    assert (
        "needs.create-ecr-manifest-per-arch.result == 'success'"
        in jobs["update-latest-manifest"]["if"]
    )
    assert (
        "needs.preflight.outputs.should_update_latest == 'true'"
        in jobs["update-latest-manifest"]["if"]
    )
    assert set(jobs["verify-publish"]["needs"]) == {
        "preflight",
        "create-ecr-manifest-per-arch",
        "update-latest-manifest",
    }
    # An explicit status check allows verification after the latest job is skipped.
    assert "!cancelled()" in jobs["verify-publish"]["if"]
    assert (
        "needs.create-ecr-manifest-per-arch.result == 'success'"
        in jobs["verify-publish"]["if"]
    )
    assert (
        "needs.update-latest-manifest.result == 'success'"
        in jobs["verify-publish"]["if"]
    )
    assert (
        "needs.preflight.outputs.should_update_latest == 'false'"
        in jobs["verify-publish"]["if"]
    )
    assert (
        "needs.update-latest-manifest.result == 'skipped'"
        in jobs["verify-publish"]["if"]
    )
    verify_step = next(
        step
        for step in jobs["verify-publish"]["steps"]
        if step.get("name") == "Verify the version and latest tags on public ECR"
    )
    assert (
        verify_step["env"]["VERIFY_LATEST"]
        == "${{ needs.preflight.outputs.should_update_latest }}"
    )
