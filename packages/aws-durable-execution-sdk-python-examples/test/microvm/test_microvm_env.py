"""Tests for the MicroVM settings that the MicroVM examples read."""

import pytest


# The examples import the extras. So an environment without the extras skips
# this module.
pytest.importorskip("aws_durable_execution_sdk_python_extras")

from src.microvm import (  # noqa: E402
    microvm_run_job,
    microvm_session,
    microvm_session_suspend,
    microvm_session_suspend_after,
)


EXAMPLES = [
    microvm_run_job,
    microvm_session,
    microvm_session_suspend,
    microvm_session_suspend_after,
]
IMAGE_ARN = "arn:aws:lambda:us-west-2:123456789012:microvm-image:img"
ROLE_ARN = "arn:aws:iam::123456789012:role/microvm-execution"


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda module: module.__name__)
def test_returns_the_image_and_the_role(example, monkeypatch):
    monkeypatch.setenv("MICROVM_IMAGE_ARN", IMAGE_ARN)
    monkeypatch.setenv("MICROVM_EXECUTION_ROLE_ARN", ROLE_ARN)

    assert example._require_microvm_env() == (IMAGE_ARN, ROLE_ARN)


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda module: module.__name__)
@pytest.mark.parametrize("missing", ["MICROVM_IMAGE_ARN", "MICROVM_EXECUTION_ROLE_ARN"])
@pytest.mark.parametrize("unset", [True, False], ids=["unset", "empty"])
def test_names_both_variables_when_one_is_missing(example, missing, unset, monkeypatch):
    monkeypatch.setenv("MICROVM_IMAGE_ARN", IMAGE_ARN)
    monkeypatch.setenv("MICROVM_EXECUTION_ROLE_ARN", ROLE_ARN)
    if unset:
        monkeypatch.delenv(missing)
    else:
        monkeypatch.setenv(missing, "")

    with pytest.raises(RuntimeError) as error:
        example._require_microvm_env()

    assert str(error.value) == (
        "MICROVM_IMAGE_ARN and MICROVM_EXECUTION_ROLE_ARN must be set. "
        "Build the image with scripts/ensure_microvm_image.py."
    )
