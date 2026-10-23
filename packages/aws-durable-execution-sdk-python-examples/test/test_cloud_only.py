"""Tests for the cloud_only keyword of the durable_execution marker."""

from test.conftest import (
    CLOUD_ONLY_SKIP_REASON,
    RunnerMode,
    cloud_only_skip_reason,
)


def test_cloud_only_test_is_skipped_in_local_mode():
    assert (
        cloud_only_skip_reason({"cloud_only": True}, RunnerMode.LOCAL)
        == CLOUD_ONLY_SKIP_REASON
    )


def test_cloud_only_test_runs_in_cloud_mode():
    assert cloud_only_skip_reason({"cloud_only": True}, RunnerMode.CLOUD) is None


def test_test_without_cloud_only_runs_in_local_mode():
    assert cloud_only_skip_reason({}, RunnerMode.LOCAL) is None
    assert cloud_only_skip_reason({"cloud_only": False}, RunnerMode.LOCAL) is None
