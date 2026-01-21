"""Tests for DurableExecutionChecker."""

import ast

import pytest

from flake8_durable_execution import DurableExecutionChecker


@pytest.fixture
def check():
    """Return a function that checks code and returns error codes."""
    def _check(code: str) -> list[str]:
        tree = ast.parse(code)
        checker = DurableExecutionChecker(tree)
        return [msg.split()[0] for _, _, msg, _ in checker.run()]
    return _check


@pytest.fixture
def durable(check):
    """Check code wrapped in @durable_execution."""
    def _durable(body: str) -> list[str]:
        lines = body.strip().split("\n")
        indented = "\n".join("    " + line for line in lines)
        code = f"@durable_execution\ndef handler(event, ctx):\n{indented}\n"
        return check(code)
    return _durable


@pytest.fixture
def step(check):
    """Check code wrapped in @durable_step."""
    def _step(body: str) -> list[str]:
        lines = body.strip().split("\n")
        indented = "\n".join("    " + line for line in lines)
        code = f"@durable_step\ndef my_step(ctx):\n{indented}\n"
        return check(code)
    return _step


# DAR001: Random

def test_dar001_random_random(durable):
    assert "DAR001" in durable("x = random.random()")


def test_dar001_random_randint(durable):
    assert "DAR001" in durable("x = random.randint(1, 10)")


def test_dar001_random_choice(durable):
    assert "DAR001" in durable("x = random.choice([1, 2, 3])")


# DAR002: Datetime

def test_dar002_datetime_now(durable):
    assert "DAR002" in durable("x = datetime.now()")


def test_dar002_datetime_utcnow(durable):
    assert "DAR002" in durable("x = datetime.utcnow()")


def test_dar002_datetime_today(durable):
    assert "DAR002" in durable("x = datetime.today()")


# DAR003: UUID

def test_dar003_uuid4(durable):
    assert "DAR003" in durable("x = uuid.uuid4()")


def test_dar003_uuid1(durable):
    assert "DAR003" in durable("x = uuid.uuid1()")


# DAR004: Time

def test_dar004_time_time(durable):
    assert "DAR004" in durable("x = time.time()")


def test_dar004_time_monotonic(durable):
    assert "DAR004" in durable("x = time.monotonic()")


# DAR005: os.environ

def test_dar005_os_environ(durable):
    assert "DAR005" in durable('x = os.environ["KEY"]')


# DAR006: Network

def test_dar006_requests_get(durable):
    assert "DAR006" in durable('x = requests.get("http://example.com")')


def test_dar006_requests_post(durable):
    assert "DAR006" in durable('x = requests.post("http://example.com")')


# DAR007: Durable ops in @durable_step

def test_dar007_step_in_durable_step(step):
    assert "DAR007" in step("ctx.step(lambda: 1)")


def test_dar007_wait_in_durable_step(step):
    assert "DAR007" in step("ctx.wait(duration=5)")


def test_dar007_invoke_in_durable_step(step):
    assert "DAR007" in step("ctx.invoke(fn)")


# DAR008: Closure mutation

def test_dar008_augmented_assign(durable):
    assert "DAR008" in durable("count += 1")


def test_dar008_subscript_assign(durable):
    assert "DAR008" in durable('data["key"] = value')


# Safe patterns - code inside ctx.step() is OK

def test_safe_random_inside_step(durable):
    assert "DAR001" not in durable("x = ctx.step(lambda: random.random())")


def test_safe_datetime_inside_step(durable):
    assert "DAR002" not in durable("x = ctx.step(lambda: datetime.now())")


def test_safe_uuid_inside_step(durable):
    assert "DAR003" not in durable("x = ctx.step(lambda: uuid.uuid4())")


def test_safe_network_inside_step(durable):
    assert "DAR006" not in durable('x = ctx.step(lambda: requests.get("url"))')


# No false positives

def test_no_errors_without_decorator(check):
    code = "def normal():\n    x = random.random()\n"
    assert check(code) == []


def test_clean_durable_code(durable):
    assert durable("x = 1 + 2") == []
