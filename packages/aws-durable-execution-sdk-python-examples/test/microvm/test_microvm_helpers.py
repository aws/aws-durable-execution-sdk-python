"""Tests for expect_microvm_terminated with a fake client and a fake clock."""

from typing import Any

import pytest
from botocore.exceptions import ClientError

from test.microvm.microvm_helpers import (
    POLL_INTERVAL_SECONDS,
    TIMEOUT_SECONDS,
    expect_microvm_terminated,
)


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetMicrovm")


class FakeClock:
    """A monotonic clock that advances only when the check sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakeMicrovmsClient:
    """Answers GetMicrovm from a list of states or exceptions, in order.

    After the list ends, it repeats the last answer.
    """

    def __init__(self, answers: list[Any]) -> None:
        self._answers = answers
        self.requests: list[dict[str, Any]] = []

    def get_microvm(self, **request: Any) -> dict[str, Any]:
        self.requests.append(request)
        index = min(len(self.requests), len(self._answers)) - 1
        answer = self._answers[index]
        if isinstance(answer, BaseException):
            raise answer
        return {"microvmId": request["microvmIdentifier"], "state": answer}


def check(client: FakeMicrovmsClient, clock: FakeClock) -> None:
    expect_microvm_terminated("mvm-1", client=client, sleep=clock.sleep, clock=clock)


@pytest.mark.parametrize("state", ["TERMINATING", "TERMINATED"])
def test_returns_at_once_for_an_ended_microvm(state):
    client = FakeMicrovmsClient([state])
    clock = FakeClock()

    check(client, clock)

    assert client.requests == [{"microvmIdentifier": "mvm-1"}]
    assert clock.sleeps == []


def test_returns_for_a_microvm_that_get_microvm_does_not_find():
    client = FakeMicrovmsClient([client_error("ResourceNotFoundException")])
    clock = FakeClock()

    check(client, clock)

    assert clock.sleeps == []


def test_polls_every_two_seconds_until_the_microvm_ends():
    client = FakeMicrovmsClient(["RUNNING", "RUNNING", "TERMINATING"])
    clock = FakeClock()

    check(client, clock)

    assert len(client.requests) == 3
    assert clock.sleeps == [POLL_INTERVAL_SECONDS, POLL_INTERVAL_SECONDS]


def test_fails_for_a_microvm_that_still_runs_after_30_seconds():
    client = FakeMicrovmsClient(["RUNNING"])
    clock = FakeClock()

    with pytest.raises(AssertionError, match="MicroVM mvm-1 is RUNNING 30 seconds"):
        check(client, clock)

    # The check reads the state at 0, 2, ..., 30 seconds.
    assert len(client.requests) == TIMEOUT_SECONDS / POLL_INTERVAL_SECONDS + 1
    assert sum(clock.sleeps) == TIMEOUT_SECONDS


def test_raises_another_get_microvm_error():
    client = FakeMicrovmsClient([client_error("AccessDeniedException")])
    clock = FakeClock()

    with pytest.raises(ClientError, match="AccessDeniedException"):
        check(client, clock)
