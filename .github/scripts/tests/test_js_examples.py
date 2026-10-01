"""Unit tests for js_examples/run.py and invoke_proxy.py. They need no JS SDK or Node.js."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

import pytest

# run.py imports invoke_proxy as a top-level module, as it does when run as a
# script. So the tests import both modules the same way.
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "js_examples")
)

import invoke_proxy  # noqa: E402
import run  # noqa: E402

TEMPLATE = textwrap.dedent(
    """\
    AWSTemplateFormatVersion: "2010-09-09"
    Transform:
      - AWS::Serverless-2016-10-31
    Resources:
      DurableFunctionRole:
        Type: AWS::IAM::Role
        Properties: {}
      HelloWorld:
        Type: AWS::Serverless::Function
        Properties:
          FunctionName: HelloWorld-22x-NodeJS-Local
          Handler: hello-world.handler
          Timeout: 60
          DurableConfig:
            ExecutionTimeout: 60
            RetentionPeriodInDays: 7
          Role:
            Fn::GetAtt:
              - DurableFunctionRole
              - Arn
          Environment:
            Variables:
              DURABLE_VERBOSE_MODE: "false"
              AWS_ENDPOINT_URL_LAMBDA: http://host.docker.internal:5000
      NonDurable:
        Type: AWS::Serverless::Function
        Properties:
          FunctionName: NonDurable-22x-NodeJS-Local
          Handler: non-durable.handler
          Timeout: 5
    """
)


@pytest.fixture
def examples(tmp_path: Path) -> Path:
    """A minimal examples package: a template and some test files."""
    (tmp_path / "template.yml").write_text(TEMPLATE)
    for rel in (
        "hello-world/hello-world.test.ts",
        "non-durable/non-durable.test.ts",
        "pause-resume/pause-resume.test.ts",
        "otel/basic/otel-basic.test.ts",
    ):
        path = tmp_path / "src/examples" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
    return tmp_path


def test_function_maps_from_template(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")

    assert maps.name_map == {
        "hello-world": "HelloWorld-22x-NodeJS-Local",
        "non-durable": "NonDurable-22x-NodeJS-Local",
    }
    # The SAM Docker endpoint is dropped; the shim sets the runner address.
    assert maps.shim_map["HelloWorld-22x-NodeJS-Local"] == {
        "file": "hello-world",
        "export": "handler",
        "timeoutSeconds": 60,
        "environment": {"DURABLE_VERBOSE_MODE": "false"},
    }
    assert maps.shim_map["NonDurable-22x-NodeJS-Local"]["timeoutSeconds"] == 5
    assert maps.function_configs == {
        "HelloWorld-22x-NodeJS-Local": {
            "DurableConfig": {"ExecutionTimeout": 60, "RetentionPeriodInDays": 7}
        },
        "NonDurable-22x-NodeJS-Local": {},
    }


def test_template_without_functions_is_an_error(tmp_path: Path) -> None:
    (tmp_path / "template.yml").write_text("Resources: {}\n")
    with pytest.raises(run.HarnessError, match="no AWS::Serverless::Function"):
        run.build_function_maps(tmp_path / "template.yml")


def test_select_skips_undeployed_and_otel(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")

    tests, not_deployed = run.select_tests(examples, [], None, maps)

    assert tests == [
        "src/examples/hello-world/hello-world.test.ts",
        "src/examples/non-durable/non-durable.test.ts",
    ]
    assert not_deployed == ["src/examples/pause-resume/pause-resume.test.ts"]


def test_select_by_pattern_and_shard(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")

    assert run.select_tests(examples, ["non-"], None, maps)[0] == [
        "src/examples/non-durable/non-durable.test.ts"
    ]
    first, _ = run.select_tests(examples, [], (1, 2), maps)
    second, _ = run.select_tests(examples, [], (2, 2), maps)
    assert first == ["src/examples/hello-world/hello-world.test.ts"]
    assert second == ["src/examples/non-durable/non-durable.test.ts"]


def test_pattern_does_not_bring_back_excluded_tests(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")
    # Give the otel test a deployed function, so only the exclusion drops it.
    maps.name_map["otel-basic"] = "OtelBasic-22x-NodeJS-Local"

    assert run.select_tests(examples, ["otel"], None, maps) == ([], [])


def jest_result(examples: Path, suites: dict[str, list[str]]) -> dict[str, object]:
    """A jest --json result: test file -> the status of each test in it."""
    return {
        "testResults": [
            {
                "name": str(examples / rel),
                "status": "failed" if "failed" in statuses else "passed",
                "assertionResults": [{"status": s} for s in statuses],
            }
            for rel, statuses in suites.items()
        ]
    }


A = "src/examples/a.test.ts"
B = "src/examples/b.test.ts"
C = "src/examples/c.test.ts"


def test_outcomes_classify_and_retry_wins(examples: Path) -> None:
    first = jest_result(examples, {A: ["passed"], B: ["failed"], C: ["pending"]})
    retry = jest_result(examples, {B: ["passed"]})

    assert run.outcomes([first], examples) == {
        A: "passed",
        B: "failed",
        C: "skipped",
    }
    assert run.outcomes([first, retry], examples)[B] == "passed"


def test_a_skipped_retry_does_not_erase_a_failure(examples: Path) -> None:
    first = jest_result(examples, {A: ["failed"]})
    retry = jest_result(examples, {A: ["pending"]})

    assert run.outcomes([first, retry], examples) == {A: "failed"}
    assert run.failed_tests([first, retry], examples) == [A]


def test_summary_fails_on_a_jest_error_no_suite_explains(
    examples: Path, tmp_path: Path
) -> None:
    result = jest_result(examples, {A: ["passed"]})

    result[run.EXIT_STATUS_KEY] = 1
    status, report = summarize(examples, tmp_path, [result], [A])
    assert status == 1
    assert report["jest_exit_errors"] == [1]

    # A non-zero exit that a failed suite explains is reported as that failure.
    failed = jest_result(examples, {A: ["failed"]})
    failed[run.EXIT_STATUS_KEY] = 1
    assert run.unexplained_exits([failed]) == []


def summarize(
    examples: Path,
    tmp_path: Path,
    results: list[dict[str, object]],
    selected: list[str],
    retried: list[str] | None = None,
) -> tuple[int, dict[str, list[Any]]]:
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    status = run.summarize(results, examples, selected, out, retried or [])
    return status, json.loads((out / "report.json").read_text())


def test_summary_passes_and_reports_flaky(examples: Path, tmp_path: Path) -> None:
    results = [
        jest_result(examples, {A: ["passed"], B: ["failed"]}),
        jest_result(examples, {B: ["passed"]}),
    ]

    status, report = summarize(examples, tmp_path, results, [A, B], retried=[B])

    assert status == 0
    assert report["flaky"] == [B]


def test_summary_fails_on_unexpected_failure(examples: Path, tmp_path: Path) -> None:
    results = [jest_result(examples, {A: ["passed"], B: ["failed"]})]

    status, report = summarize(examples, tmp_path, results, [A, B])

    assert status == 1
    assert report["unexpected_failures"] == [B]


def test_summary_fails_when_a_file_did_not_run(examples: Path, tmp_path: Path) -> None:
    results = [jest_result(examples, {A: ["passed"]})]

    status, report = summarize(examples, tmp_path, results, [A, B])

    assert status == 1
    assert report["not_run"] == [B]


def test_summary_fails_when_nothing_passed(examples: Path, tmp_path: Path) -> None:
    results = [jest_result(examples, {C: ["pending"]})]

    status, _ = summarize(examples, tmp_path, results, [C])

    assert status == 1


def test_known_failures(
    examples: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    here = tmp_path / "here"
    here.mkdir()
    (here / "known-failures.txt").write_text(f"# header\n{B}  # tracked in #1\n")
    monkeypatch.setattr(run, "HERE", here)

    # A listed failure does not fail the run.
    results = [jest_result(examples, {A: ["passed"], B: ["failed"]})]
    assert summarize(examples, tmp_path, results, [A, B])[0] == 0

    # A listed file that passes fails the run, so the list stays accurate.
    results = [jest_result(examples, {A: ["passed"], B: ["passed"]})]
    status, report = summarize(examples, tmp_path, results, [A, B])
    assert status == 1
    assert report["known_failures_now_passing"] == [B]


@pytest.mark.parametrize(
    ("installed_in", "accepted"),
    [
        # An editable install imports the package from its source directory.
        ("checkout/packages/aws-durable-execution-sdk-python-testing/src", True),
        # A wheel in a virtual environment inside the checkout is not it.
        ("checkout/.venv/lib/python3.14/site-packages", False),
        ("elsewhere", False),
        (None, False),
    ],
)
def test_check_python_requires_this_checkouts_testing_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    installed_in: str | None,
    accepted: bool,
) -> None:
    spec = None
    if installed_in is not None:
        origin = tmp_path / installed_in / run.TESTING_PACKAGE / "__init__.py"
        spec = importlib.machinery.ModuleSpec(
            run.TESTING_PACKAGE, None, origin=str(origin)
        )
    monkeypatch.setattr(
        run,
        "TESTING_SOURCE",
        tmp_path / "checkout/packages/aws-durable-execution-sdk-python-testing/src",
    )
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: spec)

    if accepted:
        run.check_python()
    else:
        with pytest.raises(run.HarnessError, match="does not have this checkout"):
            run.check_python()


def test_js_ref_and_js_dir_are_exclusive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JS_SDK_DIR", raising=False)
    with pytest.raises(SystemExit):
        run.parse_args(["--js-dir", "/tmp/js", "--js-ref", "main"])
    monkeypatch.setenv("JS_SDK_DIR", "/tmp/js")
    assert run.parse_args([]).js_dir == Path("/tmp/js")


def test_jest_args_are_split(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JS_SDK_DIR", raising=False)
    args = run.parse_args(["invoke", "--jest-args=--verbose --testTimeout=5000"])
    assert args.patterns == ["invoke"]
    assert args.jest_args == ["--verbose", "--testTimeout=5000"]


class FakeRunner(BaseHTTPRequestHandler):
    """Answers like the local runner: 201 on start, 200 elsewhere."""

    requests: ClassVar[list[tuple[str, str, bytes]]] = []

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return

    def do_GET(self) -> None:  # noqa: N802
        FakeRunner.requests.append(("GET", self.path, b""))
        self.reply(200, b'{"Events": []}')

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers["Content-Length"]))
        FakeRunner.requests.append(("POST", self.path, body))
        if self.path == "/start-durable-execution":
            self.reply(201, b'{"ExecutionArn": "arn:aws:lambda:us-west-2:123:exec"}')
        else:
            self.reply(404, b'{"message": "no route"}')

    def reply(self, status: int, data: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture
def proxy_url() -> Iterator[str]:
    FakeRunner.requests = []
    runner = ThreadingHTTPServer(("127.0.0.1", 0), FakeRunner)
    proxy = invoke_proxy.ProxyServer(
        0,
        f"http://127.0.0.1:{runner.server_port}",
        None,
        {
            "Configured": {
                "DurableConfig": {"ExecutionTimeout": 42, "RetentionPeriodInDays": 3}
            },
            "Partial": {"DurableConfig": {"ExecutionTimeout": 9}},
        },
    )
    for server in (runner, proxy):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{proxy.server_port}"
    for server in (runner, proxy):
        server.shutdown()
        server.server_close()


def test_proxy_turns_invoke_into_start(proxy_url: str) -> None:
    request = Request(
        f"{proxy_url}/2015-03-31/functions/Fn/invocations", data=b"", method="POST"
    )
    with urlopen(request) as response:  # noqa: S310
        assert response.status == 200
        assert response.headers["X-Amz-Durable-Execution-Arn"] == (
            "arn:aws:lambda:us-west-2:123:exec"
        )

    method, path, body = FakeRunner.requests[0]
    start = json.loads(body)
    assert (method, path) == ("POST", "/start-durable-execution")
    assert start["FunctionName"] == "Fn"
    # An Invoke with no payload gives the handler an empty object.
    assert start["Input"] == "{}"
    # Fn has no configuration, so the defaults apply.
    assert (
        start["ExecutionTimeoutSeconds"]
        == invoke_proxy.DEFAULT_EXECUTION_TIMEOUT_SECONDS
    )
    assert start["ExecutionRetentionPeriodDays"] == invoke_proxy.DEFAULT_RETENTION_DAYS


@pytest.mark.parametrize(
    ("identifier", "timeout", "retention"),
    [
        ("Configured", 42, 3),
        ("Configured:7", 42, 3),
        ("arn:aws:lambda:us-west-2:123456789012:function:Configured", 42, 3),
        ("Partial", 9, invoke_proxy.DEFAULT_RETENTION_DAYS),
    ],
)
def test_proxy_starts_with_the_functions_durable_config(
    proxy_url: str, identifier: str, timeout: int, retention: int
) -> None:
    request = Request(
        f"{proxy_url}/2015-03-31/functions/{quote(identifier, safe='')}/invocations",
        data=b"{}",
        method="POST",
    )
    with urlopen(request):  # noqa: S310
        pass

    start = json.loads(FakeRunner.requests[0][2])
    assert start["ExecutionTimeoutSeconds"] == timeout
    assert start["ExecutionRetentionPeriodDays"] == retention


def test_proxy_forwards_other_requests(proxy_url: str) -> None:
    path = "/2025-12-01/durable-executions/arn/history"
    with urlopen(f"{proxy_url}{path}") as response:  # noqa: S310
        assert json.loads(response.read()) == {"Events": []}
    assert FakeRunner.requests == [("GET", path, b"")]


def test_proxy_relays_runner_errors(proxy_url: str) -> None:
    request = Request(f"{proxy_url}/unknown", data=b"{}", method="POST")
    with pytest.raises(HTTPError) as exc:
        urlopen(request)  # noqa: S310
    assert exc.value.code == 404


def test_prepare_out_creates_and_reuses_its_own_directory(tmp_path: Path) -> None:
    out = tmp_path / "out"
    run.prepare_out(out)
    (out / "old.log").write_text("x")

    run.prepare_out(out)

    assert sorted(p.name for p in out.iterdir()) == [run.OUT_MARKER]


def test_prepare_out_accepts_an_empty_directory(tmp_path: Path) -> None:
    run.prepare_out(tmp_path)
    assert (tmp_path / run.OUT_MARKER).is_file()


def test_prepare_out_refuses_a_directory_it_did_not_create(tmp_path: Path) -> None:
    (tmp_path / "precious.txt").write_text("keep me")

    with pytest.raises(run.HarnessError, match="not created by this harness"):
        run.prepare_out(tmp_path)

    assert (tmp_path / "precious.txt").read_text() == "keep me"


def test_prepare_out_refuses_a_file(tmp_path: Path) -> None:
    path = tmp_path / "file"
    path.write_text("keep me")
    with pytest.raises(run.HarnessError, match="not a directory"):
        run.prepare_out(path)


def git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def test_source_fingerprint_changes_with_local_edits(tmp_path: Path) -> None:
    git(tmp_path, "init", "-q")
    (tmp_path / ".gitignore").write_text("dist/\n")
    (tmp_path / "a.ts").write_text("one")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "init")
    clean = run.source_fingerprint(tmp_path)

    # Build output is ignored, so it does not change the fingerprint.
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "a.js").write_text("built")
    assert run.source_fingerprint(tmp_path) == clean

    (tmp_path / "a.ts").write_text("two")
    edited = run.source_fingerprint(tmp_path)
    assert edited != clean

    (tmp_path / "b.ts").write_text("new")
    untracked = run.source_fingerprint(tmp_path)
    assert untracked != edited

    (tmp_path / "b.ts").write_text("newer")
    assert run.source_fingerprint(tmp_path) != untracked


def test_proxy_creates_the_dump_directory(tmp_path: Path) -> None:
    dump_dir = tmp_path / "new" / "dump"
    proxy = invoke_proxy.ProxyServer(0, "http://127.0.0.1:1", dump_dir)
    proxy.server_close()
    assert dump_dir.is_dir()


# ---------------------------------------------------------------------------
# lambda-shim.cjs, with small fake bundles. Skipped when Node.js is missing.
# ---------------------------------------------------------------------------

SHIM = Path(__file__).resolve().parents[1] / "js_examples" / "lambda-shim.cjs"

BUNDLES = {
    # Returns the event, the Lambda context fields the shim sets, and a
    # module-level counter that shows whether a worker was reused.
    "echo": """
let calls = 0;
exports.handler = async (event, context) => ({
  event,
  calls: ++calls,
  functionName: context.functionName,
  functionVersion: context.functionVersion,
  arn: context.invokedFunctionArn,
});
""",
    "sleep": """
exports.handler = async (event) => {
  await new Promise((resolve) => setTimeout(resolve, event.ms));
  return "done";
};
""",
    "boom": """
exports.handler = async () => {
  throw new TypeError("boom");
};
""",
}


@pytest.fixture(scope="module")
def shim_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    if shutil.which("node") is None:
        pytest.skip("Node.js is not installed")
    root = tmp_path_factory.mktemp("shim")
    dist = root / "dist"
    dist.mkdir()
    shim_map = {}
    for name, source in BUNDLES.items():
        (dist / f"{name}.js").write_text(source)
        shim_map[name.capitalize()] = {
            "file": name,
            "export": "handler",
            "timeoutSeconds": 1,
            "environment": {},
        }
    (root / "map.json").write_text(json.dumps(shim_map))
    port = run.free_port()
    proc = subprocess.Popen(
        [
            "node",
            str(SHIM),
            "--port",
            str(port),
            "--map",
            str(root / "map.json"),
            "--dist",
            str(dist),
            "--runner",
            "http://127.0.0.1:1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 20
    while True:
        try:
            with urlopen(f"{url}/health", timeout=1):  # noqa: S310
                break
        except OSError:
            if time.monotonic() > deadline or proc.poll() is not None:
                proc.kill()
                pytest.fail("the shim did not start")
            time.sleep(0.1)
    yield url
    proc.terminate()
    proc.wait(5)


def invoke(
    shim_url: str, identifier: str, event: object
) -> tuple[int, dict[str, str], object]:
    request = Request(
        f"{shim_url}/2015-03-31/functions/{quote(identifier, safe='')}/invocations",
        data=json.dumps(event).encode(),
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310
            return response.status, dict(response.headers), json.loads(response.read())
    except HTTPError as exc:
        return exc.code, dict(exc.headers), json.loads(exc.read())


def test_shim_runs_a_handler_and_reuses_its_worker(shim_url: str) -> None:
    status, headers, first = invoke(shim_url, "Echo", {"x": 1})
    _, _, second = invoke(shim_url, "Echo", {"x": 2})

    assert status == 200
    assert "X-Amz-Function-Error" not in headers
    assert isinstance(first, dict) and isinstance(second, dict)
    assert first["event"] == {"x": 1}
    assert first["functionVersion"] == "$LATEST"
    # The second call ran in the same warm worker.
    assert second["calls"] == first["calls"] + 1


@pytest.mark.parametrize(
    ("identifier", "version", "arn_suffix"),
    [
        ("Echo:7", "7", "Echo:7"),
        ("Echo:live", "$LATEST", "Echo:live"),
        ("arn:aws:lambda:us-west-2:123456789012:function:Echo:3", "3", "Echo:3"),
        ("arn:aws:lambda:us-west-2:123456789012:function:Echo", "$LATEST", "Echo"),
    ],
)
def test_shim_resolves_qualified_identifiers(
    shim_url: str, identifier: str, version: str, arn_suffix: str
) -> None:
    status, _, body = invoke(shim_url, identifier, {})

    assert status == 200
    assert isinstance(body, dict)
    assert body["functionName"] == "Echo"
    assert body["functionVersion"] == version
    assert body["arn"].endswith(f":function:{arn_suffix}")


def test_shim_reports_an_unknown_function(shim_url: str) -> None:
    status, headers, _ = invoke(shim_url, "Missing", {})
    assert status == 404
    assert headers["x-amzn-ErrorType"] == "ResourceNotFoundException"


def test_shim_reports_a_handler_error(shim_url: str) -> None:
    status, headers, body = invoke(shim_url, "Boom", {})
    assert status == 200
    assert headers["X-Amz-Function-Error"] == "Unhandled"
    assert isinstance(body, dict)
    assert (body["errorType"], body["errorMessage"]) == ("TypeError", "boom")


def test_shim_ends_an_invocation_at_the_timeout(shim_url: str) -> None:
    started = time.monotonic()
    status, headers, body = invoke(shim_url, "Sleep", {"ms": 5000})

    assert status == 200
    assert headers["X-Amz-Function-Error"] == "Unhandled"
    assert isinstance(body, dict)
    assert body["errorType"] == "Sandbox.Timedout"
    # The function's Timeout is 1 second, and the handler sleeps for 5.
    assert time.monotonic() - started < 3
    # The worker was terminated. The next invocation gets a new one and works.
    assert invoke(shim_url, "Sleep", {"ms": 10})[2] == "done"


def test_shim_runs_concurrent_invocations_in_parallel(shim_url: str) -> None:
    started = time.monotonic()
    with ThreadPoolExecutor(4) as pool:
        results = list(
            pool.map(lambda _: invoke(shim_url, "Sleep", {"ms": 500}), range(4))
        )

    assert [body for _, _, body in results] == ["done"] * 4
    # Run one after another, four calls would take at least 2 seconds.
    assert time.monotonic() - started < 1.5


def test_each_run_gets_its_own_output_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    first = run.new_run_dir()
    (first / "runner.log").write_text("first run")

    second = run.new_run_dir()

    assert first != second
    assert (first / "runner.log").read_text() == "first run"
    assert (run.runs_dir() / "latest").resolve() == second.resolve()


def test_old_run_directories_are_pruned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    monkeypatch.setattr(run, "KEEP_RUNS", 2)
    run.runs_dir().mkdir(parents=True)
    # A directory without the marker is not the harness's, so it stays.
    foreign = run.runs_dir() / "00000000-000000-mine"
    foreign.mkdir()
    old = []
    for stamp in ("20260101-000000-a", "20260102-000000-b"):
        directory = run.runs_dir() / stamp
        directory.mkdir()
        (directory / run.OUT_MARKER).write_text("")
        old.append(directory)

    newest = run.new_run_dir()

    assert not old[0].exists()
    assert old[1].exists()
    assert newest.exists()
    assert foreign.exists()
