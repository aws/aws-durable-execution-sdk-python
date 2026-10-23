"""Tests for scripts/ensure_microvm_image.py.

The tests make no AWS calls. A fake image service and a fake clock drive
ensure_image(). Fake clients drive BotoImageService.
"""

import importlib.util
import json
import shutil
import sys
import zipfile
from pathlib import Path

import pytest
from botocore.exceptions import ClientError


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "ensure_microvm_image.py"
)
SPEC = importlib.util.spec_from_file_location("ensure_microvm_image", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
ensure_microvm_image = importlib.util.module_from_spec(SPEC)
# dataclasses reads the module from sys.modules.
sys.modules[SPEC.name] = ensure_microvm_image
SPEC.loader.exec_module(ensure_microvm_image)

NAME = "sdk-python-examples-test"
MISSING = None


class FakeImageService:
    """A scripted image service.

    get() returns the states in ``queue`` one per call, and repeats the last
    one. create() and delete() replace the queue with the states that the
    scenario gives for that call. ``MISSING`` stands for a missing image.
    """

    def __init__(self, queue, after_create=None, after_delete=None):
        self.queue = list(queue)
        self.after_create = after_create or (lambda _creates: ["CREATING", "CREATED"])
        self.after_delete = after_delete or (lambda: ["DELETING", MISSING])
        self.creates = 0
        self.deletes = 0
        self.gets = 0

    def get(self):
        self.gets += 1
        state = self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        if state is MISSING:
            return None
        image = {"state": state}
        if state in {"CREATED", "UPDATED"}:
            image["latestActiveImageVersion"] = "1.0"
        return image

    def create(self):
        self.creates += 1
        self.queue = list(self.after_create(self.creates))

    def delete(self):
        self.deletes += 1
        self.queue = list(self.after_delete())


class FakeClock:
    """A clock whose sleep() advances now() at once."""

    def __init__(self):
        self.time = 0.0

    def now(self):
        return self.time

    def sleep(self, seconds):
        self.time += seconds


def run(service):
    clock = FakeClock()
    messages = []
    result = ensure_microvm_image.ensure_image(
        service,
        NAME,
        now=clock.now,
        sleep=clock.sleep,
        log=messages.append,
    )
    return result, messages, clock


def run_failing(service, match):
    clock = FakeClock()
    with pytest.raises(RuntimeError, match=match):
        ensure_microvm_image.ensure_image(
            service, NAME, now=clock.now, sleep=clock.sleep, log=lambda _m: None
        )
    return clock


# ensure_image


def test_reuses_an_image_that_is_ready():
    service = FakeImageService(["CREATED"])

    result, messages, _ = run(service)

    assert result == "1.0"
    assert service.creates == 0
    assert messages[0].startswith("Reusing")


def test_creates_a_missing_image_and_waits_for_it():
    service = FakeImageService([MISSING])

    result, _, _ = run(service)

    assert result == "1.0"
    assert service.creates == 1


def test_creates_the_image_after_a_delete_in_progress_finishes():
    service = FakeImageService(["DELETING", "DELETING", "DELETING", MISSING])

    result, messages, _ = run(service)

    assert result == "1.0"
    assert service.creates == 1
    assert not any(message.startswith("Reusing") for message in messages)


def test_treats_a_deleted_record_as_a_missing_image():
    service = FakeImageService(["DELETED"])

    result, _, _ = run(service)

    assert result == "1.0"
    assert service.creates == 1


def test_deletes_a_failed_image_from_an_earlier_run_and_builds_it_again():
    service = FakeImageService(["CREATE_FAILED"])

    result, _, _ = run(service)

    assert result == "1.0"
    assert service.deletes == 1
    assert service.creates == 1


def test_rebuilds_a_failed_build_once_then_raises_with_the_log_group():
    service = FakeImageService(
        [MISSING], after_create=lambda _creates: ["CREATING", "CREATE_FAILED"]
    )

    run_failing(service, f"/aws/lambda-microvms/{NAME}")

    assert service.creates == 2
    assert service.deletes == 1


def test_waits_while_a_deleted_failed_image_is_still_reported():
    service = FakeImageService(
        ["CREATE_FAILED"],
        after_delete=lambda: ["CREATE_FAILED", "DELETING", MISSING],
    )

    result, _, _ = run(service)

    assert result == "1.0"
    assert service.deletes == 1
    assert service.creates == 1


@pytest.mark.parametrize("state", ["DELETE_FAILED", "UPDATE_FAILED", "SOMETHING_NEW"])
def test_raises_on_a_state_it_cannot_clear_without_a_delete_or_a_create(state):
    service = FakeImageService([state])

    run_failing(service, f"is {state}")

    assert service.creates == 0
    assert service.deletes == 0


def test_does_not_create_again_while_a_new_image_is_not_visible_yet():
    service = FakeImageService(
        [MISSING],
        after_create=lambda _creates: [MISSING, MISSING, "CREATING", "CREATED"],
    )

    result, _, _ = run(service)

    assert result == "1.0"
    assert service.creates == 1


def test_creates_again_after_the_visibility_window():
    # The image stays missing for 7 polls of 10 seconds after the first
    # create. The window is 60 seconds. So the second create comes at the
    # first poll after the window.
    service = FakeImageService(
        [MISSING],
        after_create=lambda creates: (
            [MISSING] * 7 + ["CREATING", "CREATED"]
            if creates == 1
            else ["CREATING", "CREATED"]
        ),
    )

    result, _, clock = run(service)

    assert result == "1.0"
    assert service.creates == 2
    assert clock.time >= ensure_microvm_image.CREATE_VISIBILITY_SECONDS


def test_rebuilds_at_once_when_the_build_fails_inside_the_visibility_window():
    # The first create runs at 0 s. The build fails at 10 s, inside the
    # 60-second window. The delete finishes at 30 s. The delete clears the
    # create time. So the rebuild starts at 30 s, not at 60 s.
    clock = FakeClock()
    create_times = []

    def after_create(creates):
        create_times.append(clock.now())
        return ["CREATE_FAILED"] if creates == 1 else ["CREATING", "CREATED"]

    service = FakeImageService([MISSING], after_create=after_create)

    result = ensure_microvm_image.ensure_image(
        service, NAME, now=clock.now, sleep=clock.sleep, log=lambda _m: None
    )

    assert result == "1.0"
    assert service.deletes == 1
    assert create_times == [0.0, 30.0]


def test_raises_when_the_image_stays_missing_after_two_creates():
    service = FakeImageService([MISSING], after_create=lambda _creates: [MISSING])

    run_failing(service, "after 2 CreateMicrovmImage calls")

    assert service.creates == 2


def test_raises_when_the_build_does_not_finish_in_time():
    service = FakeImageService(["CREATING"])

    clock = run_failing(service, "still CREATING after 20 minutes")

    assert clock.time > 20 * 60


def test_polls_every_ten_seconds():
    service = FakeImageService(["CREATING", "CREATING", "CREATED"])

    _, _, clock = run(service)

    assert service.gets == 3
    assert clock.time == 20


# BotoImageService


def client_error(code, message="error"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "Operation")


class FakeMicrovmsClient:
    """A lambda-microvms client whose methods run ``handler`` with the call."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def _call(self, operation, kwargs):
        self.calls.append((operation, kwargs))
        return self.handler(operation)

    def get_microvm_image(self, **kwargs):
        return self._call("GetMicrovmImage", kwargs)

    def create_microvm_image(self, **kwargs):
        return self._call("CreateMicrovmImage", kwargs)

    def delete_microvm_image(self, **kwargs):
        return self._call("DeleteMicrovmImage", kwargs)


class FakeS3Client:
    def __init__(self, error=None):
        self.error = error
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        if self.error:
            raise self.error
        return {}


def raise_(error):
    raise error


SETTINGS = ensure_microvm_image.image_settings("us-west-2", "123456789012")
IMAGE_ARN = f"arn:aws:lambda:us-west-2:123456789012:microvm-image:{NAME}"


def boto_service(microvms_handler, s3_error=None):
    microvms = FakeMicrovmsClient(microvms_handler)
    s3 = FakeS3Client(s3_error)
    service = ensure_microvm_image.BotoImageService(
        microvms=microvms,
        s3=s3,
        name=NAME,
        image_arn=IMAGE_ARN,
        bucket="test-bucket",
        key=f"examples/{NAME}.zip",
        zip_bytes=b"zip",
        settings=SETTINGS,
        log=lambda _message: None,
    )
    return service, microvms, s3


def test_get_returns_none_for_a_missing_image():
    service, microvms, _ = boto_service(
        lambda _op: raise_(client_error("ResourceNotFoundException"))
    )

    assert service.get() is None
    assert microvms.calls == [("GetMicrovmImage", {"imageIdentifier": IMAGE_ARN})]


def test_get_rethrows_other_errors():
    service, _, _ = boto_service(
        lambda _op: raise_(client_error("AccessDeniedException"))
    )

    with pytest.raises(ClientError):
        service.get()


def test_create_uploads_the_zip_and_sends_the_request():
    service, microvms, s3 = boto_service(lambda _op: {})

    service.create()

    assert s3.puts == [
        {"Bucket": "test-bucket", "Key": f"examples/{NAME}.zip", "Body": b"zip"}
    ]
    assert microvms.calls == [
        (
            "CreateMicrovmImage",
            {
                "name": NAME,
                "description": (
                    "The MicroVM image that the durable execution SDK examples run"
                ),
                "codeArtifact": {"uri": f"s3://test-bucket/examples/{NAME}.zip"},
                "baseImageArn": "arn:aws:lambda:us-west-2:aws:microvm-image:al2023-1",
                "buildRoleArn": "arn:aws:iam::123456789012:role/microvm-image-build",
                "egressNetworkConnectors": [
                    "arn:aws:lambda:us-west-2:aws:network-connector:"
                    "aws-network-connector:INTERNET_EGRESS"
                ],
                "hooks": {
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
                    "microvmImageHooks": {
                        "ready": "ENABLED",
                        "readyTimeoutInSeconds": 60,
                    },
                },
            },
        )
    ]


def test_create_request_matches_the_botocore_model():
    import botocore.session

    model = botocore.session.get_session().get_service_model("lambda-microvms")
    request = ensure_microvm_image.create_image_request(
        NAME, "test-bucket", "key.zip", SETTINGS
    )

    # The parameter validator raises on an unknown or mistyped member.
    from botocore.validate import ParamValidator

    report = ParamValidator().validate(
        request, model.operation_model("CreateMicrovmImage").input_shape
    )
    assert not report.has_errors(), report.generate_report()


def test_carries_on_when_another_run_deleted_the_image_first():
    service, microvms, _ = boto_service(
        lambda _op: raise_(client_error("ResourceNotFoundException"))
    )

    service.delete()

    assert [call[0] for call in microvms.calls] == [
        "DeleteMicrovmImage",
        "GetMicrovmImage",
    ]


def test_carries_on_when_another_run_is_deleting_the_image():
    def handler(operation):
        if operation == "DeleteMicrovmImage":
            raise client_error("ConflictException", "delete in progress")
        return {"state": "DELETING"}

    service, _, _ = boto_service(handler)

    service.delete()


def test_rethrows_a_delete_error_when_the_failed_image_is_still_there():
    def handler(operation):
        if operation == "DeleteMicrovmImage":
            raise client_error("AccessDeniedException", "access denied")
        return {"state": "CREATE_FAILED"}

    service, _, _ = boto_service(handler)

    with pytest.raises(ClientError, match="access denied"):
        service.delete()


def test_treats_a_duplicate_name_as_a_build_by_another_run():
    def handler(operation):
        if operation == "CreateMicrovmImage":
            raise client_error(
                "ValidationException",
                f"A MicroVM image with the name '{NAME}' already exists in "
                "this account",
            )
        raise client_error("ResourceNotFoundException")

    service, _, _ = boto_service(handler)

    service.create()


def test_treats_a_create_error_as_a_build_by_another_run_when_the_image_exists():
    def handler(operation):
        if operation == "CreateMicrovmImage":
            raise client_error("ConflictException", "conflict")
        return {"state": "CREATING"}

    service, _, _ = boto_service(handler)

    service.create()


def test_rethrows_a_create_error_when_no_image_exists():
    def handler(operation):
        if operation == "CreateMicrovmImage":
            raise client_error("ServiceQuotaExceededException", "quota exceeded")
        raise client_error("ResourceNotFoundException")

    service, _, _ = boto_service(handler)

    with pytest.raises(ClientError, match="quota exceeded"):
        service.create()


def test_names_the_override_variables_when_the_bucket_is_missing():
    service, microvms, _ = boto_service(
        lambda _op: {}, s3_error=client_error("NoSuchBucket")
    )

    with pytest.raises(
        RuntimeError, match=r"test-bucket does not exist.*MICROVM_ARTIFACT_BUCKET"
    ):
        service.create()
    assert microvms.calls == []


# Settings, endpoint warning


def test_settings_use_the_build_role_override():
    settings = ensure_microvm_image.image_settings(
        "us-east-1", "111122223333", "arn:aws:iam::111122223333:role/other"
    )

    assert settings["buildRoleArn"] == "arn:aws:iam::111122223333:role/other"
    assert settings["baseImageArn"] == (
        "arn:aws:lambda:us-east-1:aws:microvm-image:al2023-1"
    )


@pytest.mark.parametrize(
    ("endpoint", "warns"),
    [
        (None, False),
        ("", False),
        ("https://lambda.us-west-2.amazonaws.com", False),
        ("https://lambda.us-west-2.amazonaws.com/", False),
        ("https://beta.lambda.example.com", True),
    ],
)
def test_warns_only_on_a_custom_lambda_endpoint(endpoint, warns, capsys):
    environ = {} if endpoint is None else {"LAMBDA_ENDPOINT": endpoint}

    assert (
        ensure_microvm_image.warn_on_custom_lambda_endpoint("us-west-2", environ)
        is warns
    )
    stderr = capsys.readouterr().err
    assert ("WARNING" in stderr) is warns
    # CI passes the endpoint as a secret. So the warning omits it.
    assert "example.com" not in stderr


# Content hash and zip


@pytest.fixture
def inputs(tmp_path):
    """Copies of the real worker and image inputs, which a test can change."""
    worker_dir = tmp_path / "worker"
    shutil.copytree(
        ensure_microvm_image.WORKER_DIR / "src",
        worker_dir / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copy(ensure_microvm_image.WORKER_DIR / "pyproject.toml", worker_dir)
    image_dir = tmp_path / "image"
    shutil.copytree(ensure_microvm_image.IMAGE_DIR, image_dir)
    return worker_dir, image_dir


def hash_of(worker_dir, image_dir, settings=SETTINGS):
    return ensure_microvm_image.content_hash(
        ensure_microvm_image.hash_inputs(worker_dir, image_dir, settings)
    )


def fake_wheel_builder(counter):
    """Build a fake wheel whose bytes differ on each build."""

    def build(worker_dir, wheel_dir):
        counter.append(worker_dir)
        wheel = wheel_dir / "worker-0.1.0-py3-none-any.whl"
        wheel.write_bytes(f"build {len(counter)}".encode())
        return wheel

    return build


def test_two_builds_of_the_same_inputs_give_the_same_hash(inputs, tmp_path):
    worker_dir, image_dir = inputs
    builds = []

    first, second = (
        ensure_microvm_image.build_artifact(
            SETTINGS,
            worker_dir=worker_dir,
            image_dir=image_dir,
            out_dir=tmp_path / "out",
            build_wheel=fake_wheel_builder(builds),
        )
        for _ in range(2)
    )

    # The wheel bytes differ between the builds. The hash covers the inputs,
    # not the wheel. So the hash is the same.
    assert len(builds) == 2
    assert first.zip_bytes != second.zip_bytes
    assert first.content_hash == second.content_hash
    assert len(first.content_hash) == 12
    int(first.content_hash, 16)


def test_hash_ignores_bytecode(inputs):
    worker_dir, image_dir = inputs
    before = hash_of(worker_dir, image_dir)
    package_dir = next((worker_dir / "src").iterdir())
    (package_dir / "__pycache__").mkdir()
    (package_dir / "__pycache__" / "worker.cpython-313.pyc").write_bytes(b"x")
    (package_dir / "stray.pyc").write_bytes(b"x")

    assert hash_of(worker_dir, image_dir) == before


def _worker_source(worker_dir):
    return next((worker_dir / "src").rglob("worker.py"))


@pytest.mark.parametrize(
    "change",
    [
        "worker source",
        "new worker file",
        "worker pyproject",
        "app",
        "dockerfile",
        "constraints",
    ],
)
def test_changing_an_input_changes_the_hash(inputs, change):
    worker_dir, image_dir = inputs
    before = hash_of(worker_dir, image_dir)
    targets = {
        "worker source": _worker_source(worker_dir),
        "worker pyproject": worker_dir / "pyproject.toml",
        "app": image_dir / "app.py",
        "dockerfile": image_dir / "Dockerfile",
        "constraints": image_dir / "constraints.txt",
    }
    if change == "new worker file":
        (_worker_source(worker_dir).parent / "extra.py").write_text("x = 1\n")
    else:
        path = targets[change]
        path.write_bytes(path.read_bytes() + b"\n# changed\n")

    assert hash_of(worker_dir, image_dir) != before


def test_changing_a_setting_changes_the_hash(inputs):
    worker_dir, image_dir = inputs
    other_role = ensure_microvm_image.image_settings(
        "us-west-2", "123456789012", "arn:aws:iam::123456789012:role/other"
    )
    other_region = ensure_microvm_image.image_settings("us-east-1", "123456789012")

    hashes = {
        hash_of(worker_dir, image_dir, settings)
        for settings in (SETTINGS, other_role, other_region)
    }

    assert len(hashes) == 3


def test_hash_does_not_depend_on_settings_key_order(inputs):
    worker_dir, image_dir = inputs
    reordered = json.loads(json.dumps(SETTINGS))
    reordered = dict(reversed(list(reordered.items())))

    assert hash_of(worker_dir, image_dir, reordered) == hash_of(worker_dir, image_dir)


def test_hash_separates_entry_names_from_bytes():
    assert ensure_microvm_image.content_hash(
        [("a", b"bc")]
    ) != ensure_microvm_image.content_hash([("ab", b"c")])


def test_zip_holds_the_dockerfile_at_its_root(inputs, tmp_path):
    worker_dir, image_dir = inputs

    artifact = ensure_microvm_image.build_artifact(
        SETTINGS,
        worker_dir=worker_dir,
        image_dir=image_dir,
        out_dir=tmp_path / "out",
        build_wheel=fake_wheel_builder([]),
    )

    with zipfile.ZipFile(artifact.zip_path) as archive:
        assert sorted(archive.namelist()) == [
            "Dockerfile",
            "app.py",
            "constraints.txt",
            "wheels/worker-0.1.0-py3-none-any.whl",
        ]
        assert archive.read("Dockerfile") == (image_dir / "Dockerfile").read_bytes()


# main


def test_main_writes_the_image_and_role_arns(monkeypatch, tmp_path):
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("TEST_ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("MICROVM_EXECUTION_ROLE_ARN", "")
    monkeypatch.delenv("LAMBDA_ENDPOINT", raising=False)
    monkeypatch.setattr(
        ensure_microvm_image,
        "build_artifact",
        lambda settings: ensure_microvm_image.ImageArtifact(
            zip_path=tmp_path / "z.zip", zip_bytes=b"zip", content_hash="0123456789ab"
        ),
    )
    created = []

    def fake_client(service_name, region_name):
        assert region_name == "us-west-2"
        return service_name

    monkeypatch.setattr(ensure_microvm_image.boto3, "client", fake_client)

    def fake_ensure_image(service, name):
        created.append((service.bucket, service.key, service.image_arn, name))
        return "1"

    monkeypatch.setattr(ensure_microvm_image, "ensure_image", fake_ensure_image)
    output = tmp_path / "out" / "image.json"

    assert ensure_microvm_image.main(["--output", str(output)]) == 0

    name = "sdk-python-examples-0123456789ab"
    image_arn = f"arn:aws:lambda:us-west-2:123456789012:microvm-image:{name}"
    assert created == [
        (
            "microvm-image-artifacts-123456789012-us-west-2",
            f"examples/{name}.zip",
            image_arn,
            name,
        )
    ]
    # An empty override counts as unset.
    assert json.loads(output.read_text()) == {
        "imageArn": image_arn,
        "executionRoleArn": "arn:aws:iam::123456789012:role/microvm-execution",
    }


OVERRIDES = [
    "MICROVM_ARTIFACT_BUCKET",
    "MICROVM_IMAGE_BUILD_ROLE_ARN",
    "MICROVM_EXECUTION_ROLE_ARN",
]


@pytest.mark.parametrize("empty", OVERRIDES)
def test_main_treats_an_empty_override_as_unset(empty, monkeypatch, tmp_path):
    # GitHub passes an unset secret to a step as an empty string. So main()
    # must use the default resource name for an empty override.
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("TEST_ACCOUNT_ID", "123456789012")
    monkeypatch.delenv("LAMBDA_ENDPOINT", raising=False)
    for variable in OVERRIDES:
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv(empty, "")
    monkeypatch.setattr(
        ensure_microvm_image,
        "build_artifact",
        lambda settings: ensure_microvm_image.ImageArtifact(
            zip_path=tmp_path / "z.zip", zip_bytes=b"zip", content_hash="0123456789ab"
        ),
    )
    monkeypatch.setattr(
        ensure_microvm_image.boto3, "client", lambda service_name, region_name: None
    )
    services = []
    monkeypatch.setattr(
        ensure_microvm_image,
        "ensure_image",
        lambda service, name: services.append(service),
    )
    output = tmp_path / "image.json"

    assert ensure_microvm_image.main(["--output", str(output)]) == 0

    [service] = services
    assert service.bucket == "microvm-image-artifacts-123456789012-us-west-2"
    assert service.settings["buildRoleArn"] == (
        "arn:aws:iam::123456789012:role/microvm-image-build"
    )
    assert json.loads(output.read_text())["executionRoleArn"] == (
        "arn:aws:iam::123456789012:role/microvm-execution"
    )


def test_main_requires_the_account(monkeypatch, tmp_path):
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.delenv("TEST_ACCOUNT_ID", raising=False)

    with pytest.raises(SystemExit, match="TEST_ACCOUNT_ID is not set"):
        ensure_microvm_image.main(["--output", str(tmp_path / "x.json")])
