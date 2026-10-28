"""Keep Linux-only resource sampling outside portable harness unit tests."""

import sys
import threading

import pytest

from lmi_tests import fixture


@pytest.fixture(autouse=True)
def portable_unit_resource_snapshot(monkeypatch):
    if sys.platform == "linux":
        return

    def snapshot():
        # Real thread lifetime remains observable. RSS/FD cloud assertions still
        # use /proc in the deployed Linux fixture; these tests do not measure it.
        return {
            "threads": [
                {"id": thread.ident, "name": thread.name}
                for thread in threading.enumerate()
            ],
            "fds": 0,
            "rss": 0,
        }

    monkeypatch.setattr(fixture, "resources", snapshot)
    monkeypatch.setattr("lmi_tests.tests.unit.test_java_parity.resources", snapshot)
