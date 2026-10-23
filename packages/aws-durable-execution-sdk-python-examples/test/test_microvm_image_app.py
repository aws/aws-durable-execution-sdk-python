"""Tests for the handlers of microvm-image/app.py.

The app imports the MicroVM worker. The examples package does not depend on
the worker. So an environment without the worker skips these tests. The
dev-examples environment is one such environment.
"""

import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


pytest.importorskip("aws_durable_execution_sdk_python_microvm_worker")

APP_PATH = Path(__file__).resolve().parents[1] / "microvm-image" / "app.py"
SPEC = importlib.util.spec_from_file_location("microvm_image_app", APP_PATH)
assert SPEC is not None
assert SPEC.loader is not None
app = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(app)

CONTEXT = SimpleNamespace(microvm_id="mv-1")


def test_count_words_returns_the_count_hash_and_microvm_id():
    text = "the quick  brown fox\njumps"

    assert app.count_words({"text": text}, CONTEXT) == {
        "wordCount": 5,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "microvmId": "mv-1",
    }


def test_read_returns_what_write_wrote(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "SESSION_DIR", tmp_path / "session")

    assert app.write_value({"key": "k1", "value": "v1"}, CONTEXT) == {
        "microvmId": "mv-1"
    }
    assert app.read_value({"key": "k1"}, CONTEXT) == {
        "microvmId": "mv-1",
        "value": "v1",
    }


@pytest.mark.parametrize("key", ["", ".", "..", "../x", "a/b", "/etc/passwd"])
def test_rejects_a_key_that_is_not_a_plain_file_name(monkeypatch, tmp_path, key):
    monkeypatch.setattr(app, "SESSION_DIR", tmp_path / "session")

    with pytest.raises(ValueError, match="invalid key"):
        app.write_value({"key": key, "value": "v"}, CONTEXT)


def test_the_worker_accepts_the_handlers():
    # The worker rejects an async handler when it starts. This builds the
    # listener with the app's handlers, without a server.
    from aws_durable_execution_sdk_python_microvm_worker import (
        MicrovmWorkerListener,
    )

    listener = MicrovmWorkerListener.create(
        handler=app.count_words,
        routes={"/write": app.write_value, "/read": app.read_value},
    )
    listener.close()
