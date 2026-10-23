#!/usr/bin/env python3
"""Build the MicroVM image that the MicroVM examples run, or reuse it.

The script builds the worker wheel from this repo. It zips the wheel with
microvm-image/app.py, the Dockerfile, and constraints.txt. It uploads the zip
and calls CreateMicrovmImage.

The image name ends with a hash of the inputs: the worker's source and
pyproject.toml, app.py, the Dockerfile, constraints.txt, and the
CreateMicrovmImage settings. So a run with unchanged inputs finds the image
and skips the build. A change to any input builds a new image.

It writes the image ARN and the MicroVM execution role ARN as JSON to the file
named by --output. The cloud tests pass both to the SAM template, which sets
them as environment variables of the MicroVM examples.

The resource names come from the language testing stack in the test account.
That stack names them after the account and Region. Environment variables
override them for another account:
  MICROVM_ARTIFACT_BUCKET       the bucket for the image code zip
  MICROVM_IMAGE_BUILD_ROLE_ARN  the role that the image build assumes
  MICROVM_EXECUTION_ROLE_ARN    the role that a running MicroVM assumes

Usage:
  TEST_ACCOUNT_ID=123456789012 AWS_REGION=us-west-2 \\
    hatch run examples:ensure-microvm-image --output image.json

This is a port of ensure-microvm-image.ts in the JavaScript SDK examples.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import boto3
from botocore.exceptions import ClientError


EXAMPLES_DIR = Path(__file__).resolve().parent.parent
IMAGE_DIR = EXAMPLES_DIR / "microvm-image"
OUT_DIR = EXAMPLES_DIR / ".microvm-image-build"
WORKER_DIR = EXAMPLES_DIR.parent / "aws-durable-execution-sdk-python-microvm-worker"
IMAGE_FILES = ("Dockerfile", "app.py", "constraints.txt")
IMAGE_NAME_PREFIX = "sdk-python-examples"
HASH_LENGTH = 12
POLL_INTERVAL_SECONDS = 10.0
BUILD_TIMEOUT_SECONDS = 20 * 60.0

# GetMicrovmImage reports an image as missing for a short time after
# CreateMicrovmImage returns. A JavaScript test run saw this for one 10-second
# poll. A create in that time would fail on the duplicate name. So the script
# waits this long before it creates again.
CREATE_VISIBILITY_SECONDS = 60.0

# The worker answers the ready hook at image build and the run hook at each
# launch. The service requires the ready hook whenever a lifecycle hook is
# enabled. The resume hook lets a session's worker accept jobs again after it
# suspended its own MicroVM. On the suspend hook the worker refuses new jobs,
# and on the terminate hook it fails the callbacks of running jobs. A
# terminate hook gets 10 seconds, because the worker reports before it
# answers.
IMAGE_HOOKS: dict[str, Any] = {
    "port": 8080,
    "microvmHooks": {
        "run": "ENABLED",
        "runTimeoutInSeconds": 10,
        "resume": "ENABLED",
        "resumeTimeoutInSeconds": 10,
        "suspend": "ENABLED",
        "suspendTimeoutInSeconds": 10,
        "terminate": "ENABLED",
        "terminateTimeoutInSeconds": 10,
    },
    "microvmImageHooks": {"ready": "ENABLED", "readyTimeoutInSeconds": 60},
}


class ImageService(Protocol):
    """The image operations that :func:`ensure_image` needs."""

    def get(self) -> dict[str, Any] | None:
        """Return the GetMicrovmImage response, or None for a missing image."""

    def create(self) -> None:
        """Upload the code and call CreateMicrovmImage."""

    def delete(self) -> None:
        """Call DeleteMicrovmImage."""


def ensure_image(
    service: ImageService,
    name: str,
    *,
    timeout_seconds: float = BUILD_TIMEOUT_SECONDS,
    poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
    now: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
) -> str:
    """Wait until the image has an active version, and create it when needed.

    Returns the active version.

    It acts on each state that GetMicrovmImage reports:

    - CREATED or UPDATED with an active version: the image is ready.
    - Missing or DELETED: it creates the image.
    - CREATING, UPDATING, or DELETING: it waits.
    - CREATE_FAILED: it deletes the image, then creates it again. It does this
      once per run, because a second failure most likely has the same cause.
    - DELETE_FAILED, UPDATE_FAILED, or another state: it raises. Only an
      administrator of the account can clear these states.

    The image name carries a hash of the inputs. So a failed build fails every
    later run with the same inputs, until the failed image is deleted. The
    delete and the recreate let a run recover from a failure that does not
    repeat, such as a timeout of the ready hook, without an administrator.
    """
    log_group = f"/aws/lambda-microvms/{name}"
    deadline = now() + timeout_seconds
    deleted_failed_image = False
    created_after_delete = False
    creates = 0
    last_create_at: float | None = None
    first_poll = True

    while True:
        image = service.get()
        state = (image or {}).get("state") or "NOT_FOUND"
        version = (image or {}).get("latestActiveImageVersion")

        if state in {"CREATED", "UPDATED"}:
            if version:
                log(
                    f"Reusing MicroVM image {name}, version {version}"
                    if first_poll
                    else f"MicroVM image {name} is {state}, version {version}"
                )
                return version
        elif state in {"NOT_FOUND", "DELETED"}:
            create_is_recent = (
                last_create_at is not None
                and now() - last_create_at < CREATE_VISIBILITY_SECONDS
            )
            if not create_is_recent:
                # One create at the start and one after a failed build.
                if creates >= 2:
                    msg = (
                        f"MicroVM image {name} is still {state} after "
                        f"{creates} CreateMicrovmImage calls"
                    )
                    raise RuntimeError(msg)
                log(f"Creating MicroVM image {name}")
                service.create()
                creates += 1
                last_create_at = now()
                created_after_delete = deleted_failed_image
        elif state in {"CREATING", "UPDATING", "DELETING"}:
            pass
        elif state == "CREATE_FAILED":
            # GetMicrovmImage may still report the failed image right after
            # DeleteMicrovmImage returns. So the state counts as a second
            # failure only after the rebuild started.
            if deleted_failed_image and created_after_delete:
                msg = (
                    f"MicroVM image {name} failed to build again after a "
                    f"rebuild. Read the build logs in the {log_group} log group."
                )
                raise RuntimeError(msg)
            if not deleted_failed_image:
                log(
                    f"MicroVM image {name} is CREATE_FAILED. Deleting it to "
                    "build it again. The build logs are in the "
                    f"{log_group} log group."
                )
                service.delete()
                deleted_failed_image = True
                last_create_at = None
        else:
            msg = (
                f"MicroVM image {name} is {state}. The script does not clear "
                "this state. An administrator of the test account must delete "
                f"the image. Read the logs in the {log_group} log group."
            )
            raise RuntimeError(msg)

        first_poll = False
        if now() > deadline:
            msg = (
                f"MicroVM image {name} is still {state} after "
                f"{timeout_seconds / 60:g} minutes"
            )
            raise RuntimeError(msg)
        log(f"Waiting for MicroVM image {name}: {state}")
        sleep(poll_interval_seconds)


def _error_code(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Code", "")


def _error_message(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Message", "")


@dataclass
class BotoImageService:
    """The :class:`ImageService` that calls S3 and the MicroVMs API.

    ``microvms`` is a boto3 ``lambda-microvms`` client, and ``s3`` a boto3 S3
    client. Tests pass fakes with the same methods.
    """

    microvms: Any
    s3: Any
    name: str
    image_arn: str
    bucket: str
    key: str
    zip_bytes: bytes
    settings: dict[str, Any]
    log: Callable[[str], None] = print

    def get(self) -> dict[str, Any] | None:
        try:
            return self.microvms.get_microvm_image(imageIdentifier=self.image_arn)
        except ClientError as error:
            if _error_code(error) == "ResourceNotFoundException":
                return None
            raise

    def create(self) -> None:
        try:
            self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=self.zip_bytes)
        except ClientError as error:
            if _error_code(error) == "NoSuchBucket":
                msg = (
                    f"The bucket {self.bucket} does not exist. The MicroVM "
                    "examples need the MicroVM resources of the language "
                    "testing stack. For another account, set "
                    "MICROVM_ARTIFACT_BUCKET, MICROVM_IMAGE_BUILD_ROLE_ARN, "
                    "and MICROVM_EXECUTION_ROLE_ARN."
                )
                raise RuntimeError(msg) from error
            raise
        try:
            self.microvms.create_microvm_image(
                **create_image_request(self.name, self.bucket, self.key, self.settings)
            )
        except Exception as error:
            # Two runs can build the same image at the same time. CI runs the
            # build only in the Python 3.13 job. All runs of that job share
            # one serialized concurrency group, cloud-tests-Py313, with
            # queue: max. So two CI runs never build at the same time. So
            # this case covers a manual run that overlaps a CI run.
            #
            # The service rejects the second create with a ValidationException,
            # because the name exists. The image is then the one this run
            # needs, because its name carries the hash of the inputs. So the
            # run waits for it.
            #
            # GetMicrovmImage can still report the new image as missing. So
            # the error message is the first signal, and the image state the
            # second.
            name_exists = (
                isinstance(error, ClientError)
                and _error_code(error) == "ValidationException"
                and "already exists" in _error_message(error)
            )
            if name_exists or self.get() is not None:
                self.log(f"MicroVM image {self.name} was created by another run")
                return
            raise

    def delete(self) -> None:
        try:
            self.microvms.delete_microvm_image(imageIdentifier=self.image_arn)
        except Exception:
            # Two runs can see the same failed image and delete it at the
            # same time. The later delete then fails, because the image is
            # gone or is being deleted. The poll loop handles both states. So
            # the run continues when the image is missing or DELETING after
            # the error.
            #
            # CI runs the build only in the Python 3.13 job. All runs of that
            # job share one serialized concurrency group, cloud-tests-Py313,
            # with queue: max. So this case covers a manual run that overlaps
            # a CI run.
            image = self.get()
            if image is None or image.get("state") == "DELETING":
                self.log(f"MicroVM image {self.name} was deleted by another run")
                return
            raise


def create_image_request(
    name: str, bucket: str, key: str, settings: dict[str, Any]
) -> dict[str, Any]:
    """Return the CreateMicrovmImage parameters, in boto3 names."""
    return {
        "name": name,
        "description": "The MicroVM image that the durable execution SDK examples run",
        "codeArtifact": {"uri": f"s3://{bucket}/{key}"},
        **settings,
    }


def image_settings(
    region: str, account: str, build_role_arn: str | None = None
) -> dict[str, Any]:
    """Return the CreateMicrovmImage settings other than the name and code."""
    return {
        "baseImageArn": f"arn:aws:lambda:{region}:aws:microvm-image:al2023-1",
        "buildRoleArn": build_role_arn
        or f"arn:aws:iam::{account}:role/microvm-image-build",
        # The build pulls the Python base image from Amazon ECR Public, and
        # boto3 from PyPI.
        "egressNetworkConnectors": [
            f"arn:aws:lambda:{region}:aws:network-connector:"
            "aws-network-connector:INTERNET_EGRESS"
        ],
        "hooks": IMAGE_HOOKS,
    }


def _is_source_file(path: Path) -> bool:
    # Local test runs leave bytecode next to the source. CI runs do not. So
    # the hash skips it, and both compute the same name.
    return (
        path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
        and path.name != ".DS_Store"
    )


def hash_inputs(
    worker_dir: Path, image_dir: Path, settings: dict[str, Any]
) -> list[tuple[str, bytes]]:
    """Return the (name, bytes) pairs that the content hash covers.

    The pairs cover the worker's src/ tree and pyproject.toml, the image
    files, and the settings as JSON with sorted keys. They do not cover the
    wheel. Two builds of the same source can produce wheels that differ byte
    for byte. Two runners would then compute different names, and reuse would
    fail.

    The Dockerfile names a floating python:3.13-alpine tag, and the hash does
    not cover the image behind the tag. Lambda snapshots the image at build
    time. So a tag change takes effect at the next build.
    """
    inputs: list[tuple[str, bytes]] = []
    src_dir = worker_dir / "src"
    for path in sorted(src_dir.rglob("*")):
        if _is_source_file(path):
            relative = path.relative_to(worker_dir).as_posix()
            inputs.append((f"worker/{relative}", path.read_bytes()))
    inputs.append(
        ("worker/pyproject.toml", (worker_dir / "pyproject.toml").read_bytes())
    )
    inputs.extend(
        (f"image/{file_name}", (image_dir / file_name).read_bytes())
        for file_name in IMAGE_FILES
    )
    settings_json = json.dumps(settings, sort_keys=True, separators=(",", ":"))
    inputs.append(("settings.json", settings_json.encode()))
    return inputs


def content_hash(inputs: Iterable[tuple[str, bytes]]) -> str:
    """Return the first 12 hex characters of a SHA-256 over the inputs.

    Each entry adds its name, its length, and its bytes. The length separates
    one entry from the next. So no two different input sets give the same
    byte stream.
    """
    digest = hashlib.sha256()
    for name, data in sorted(inputs):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(str(len(data)).encode())
        digest.update(b"\0")
        digest.update(data)
    return digest.hexdigest()[:HASH_LENGTH]


def build_worker_wheel(worker_dir: Path, wheel_dir: Path) -> Path:
    """Build the worker wheel from the repo, without its dependencies."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--quiet",
            "--no-deps",
            "--wheel-dir",
            str(wheel_dir),
            str(worker_dir),
        ],
        check=True,
    )
    wheels = sorted(wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        msg = f"Expected one worker wheel in {wheel_dir}, found {len(wheels)}"
        raise RuntimeError(msg)
    return wheels[0]


def _add_to_zip(archive: zipfile.ZipFile, name: str, data: bytes) -> None:
    # A fixed timestamp and mode keep the zip free of local file metadata.
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.external_attr = 0o644 << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    archive.writestr(info, data)


@dataclass(frozen=True)
class ImageArtifact:
    """The image code zip and the content hash of its inputs."""

    zip_path: Path
    zip_bytes: bytes
    content_hash: str


def build_artifact(
    settings: dict[str, Any],
    *,
    worker_dir: Path = WORKER_DIR,
    image_dir: Path = IMAGE_DIR,
    out_dir: Path = OUT_DIR,
    build_wheel: Callable[[Path, Path], Path] = build_worker_wheel,
) -> ImageArtifact:
    """Build the image code zip in ``out_dir``, and hash its inputs.

    The zip holds the Dockerfile at its root, app.py, constraints.txt, and
    wheels/<worker wheel>. CreateMicrovmImage reads the Dockerfile from the
    root.
    """
    shutil.rmtree(out_dir, ignore_errors=True)
    wheel_dir = out_dir / "wheels"
    wheel_dir.mkdir(parents=True)
    wheel = build_wheel(worker_dir, wheel_dir)

    zip_path = out_dir / "microvm-image.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for file_name in IMAGE_FILES:
            _add_to_zip(archive, file_name, (image_dir / file_name).read_bytes())
        _add_to_zip(archive, f"wheels/{wheel.name}", wheel.read_bytes())

    # The zip is read once here. A rebuild uploads the same bytes, even if
    # another run has replaced the file since.
    return ImageArtifact(
        zip_path=zip_path,
        zip_bytes=zip_path.read_bytes(),
        content_hash=content_hash(hash_inputs(worker_dir, image_dir, settings)),
    )


def warn_on_custom_lambda_endpoint(
    region: str, environ: dict[str, str] | None = None
) -> bool:
    """Warn when the functions use a Lambda endpoint other than the default.

    The worker in the MicroVM sends its callbacks to the default endpoint of
    the Region. So an execution on another endpoint never receives them, and
    the MicroVM tests time out. Returns True when it warned.
    """
    endpoint = (environ if environ is not None else os.environ).get("LAMBDA_ENDPOINT")
    if not endpoint:
        return False
    default_endpoint = f"https://lambda.{region}.amazonaws.com"
    if re.sub(r"/+$", "", endpoint) == default_endpoint:
        return False
    # The value is not printed, because CI passes it as a secret.
    print(
        f"WARNING: LAMBDA_ENDPOINT is not {default_endpoint}. The worker in "
        "the MicroVM sends its callbacks to the default endpoint. So the "
        "MicroVM examples will time out.",
        file=sys.stderr,
    )
    return True


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        msg = f"{name} is not set"
        raise SystemExit(msg)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the MicroVM image for the examples, or reuse it"
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Write {imageArn, executionRoleArn} as JSON to this file",
    )
    args = parser.parse_args(argv)

    region = _require_env("AWS_REGION")
    account = _require_env("TEST_ACCOUNT_ID")
    # An unset GitHub secret or variable reaches the step as an empty string.
    # So an empty override counts as unset.
    bucket = (
        os.environ.get("MICROVM_ARTIFACT_BUCKET")
        or f"microvm-image-artifacts-{account}-{region}"
    )
    execution_role_arn = (
        os.environ.get("MICROVM_EXECUTION_ROLE_ARN")
        or f"arn:aws:iam::{account}:role/microvm-execution"
    )
    settings = image_settings(
        region, account, os.environ.get("MICROVM_IMAGE_BUILD_ROLE_ARN")
    )

    warn_on_custom_lambda_endpoint(region)

    artifact = build_artifact(settings)
    name = f"{IMAGE_NAME_PREFIX}-{artifact.content_hash}"
    # GetMicrovmImage accepts only the image ARN, not the name.
    image_arn = f"arn:aws:lambda:{region}:{account}:microvm-image:{name}"
    print(f"MicroVM image {name}, zip {len(artifact.zip_bytes)} bytes")

    service = BotoImageService(
        microvms=boto3.client("lambda-microvms", region_name=region),
        s3=boto3.client("s3", region_name=region),
        name=name,
        image_arn=image_arn,
        bucket=bucket,
        key=f"examples/{name}.zip",
        zip_bytes=artifact.zip_bytes,
        settings=settings,
    )
    ensure_image(service, name)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps({"imageArn": image_arn, "executionRoleArn": execution_role_arn})
    )
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
