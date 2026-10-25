"""Tests for the MicroVM cloud-test helpers, with fake clients and a fake clock."""

from typing import Any

import pytest
from botocore.exceptions import ClientError

from test.microvm.microvm_helpers import (
    HISTORY_TIMEOUT_SECONDS,
    POLL_INTERVAL_SECONDS,
    STATE_TIMEOUT_SECONDS,
    TEST_FAILED_ERROR,
    TIMEOUT_SECONDS,
    expect_microvm_terminated,
    fail_callback_after_test_failure,
    find_launched_microvm_id,
    wait_for_history_event,
    wait_for_microvm_state,
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


def wait(client: FakeMicrovmsClient, clock: FakeClock) -> list[str]:
    return wait_for_microvm_state(
        "mvm-1", "SUSPENDED", client=client, sleep=clock.sleep, clock=clock
    )


def test_wait_returns_the_states_seen_until_the_expected_state():
    client = FakeMicrovmsClient(
        ["RUNNING", "RUNNING", "SUSPENDING", "SUSPENDED", "RUNNING"]
    )
    clock = FakeClock()

    assert wait(client, clock) == ["RUNNING", "SUSPENDING", "SUSPENDED"]
    assert len(client.requests) == 4
    assert clock.sleeps == [POLL_INTERVAL_SECONDS] * 3


@pytest.mark.parametrize("state", ["TERMINATING", "TERMINATED"])
def test_wait_fails_at_once_for_an_ended_microvm(state):
    client = FakeMicrovmsClient(["RUNNING", state])
    clock = FakeClock()

    with pytest.raises(AssertionError, match=f"is {state} before it was SUSPENDED"):
        wait(client, clock)

    assert len(client.requests) == 2


def test_wait_fails_at_once_for_a_microvm_that_get_microvm_does_not_find():
    client = FakeMicrovmsClient([client_error("ResourceNotFoundException")])
    clock = FakeClock()

    with pytest.raises(AssertionError, match="did not find MicroVM mvm-1"):
        wait(client, clock)

    assert clock.sleeps == []


def test_wait_fails_after_the_timeout():
    client = FakeMicrovmsClient(["RUNNING"])
    clock = FakeClock()

    with pytest.raises(AssertionError, match="was not SUSPENDED within 150 seconds"):
        wait(client, clock)

    assert sum(clock.sleeps) == STATE_TIMEOUT_SECONDS


def test_wait_raises_another_get_microvm_error():
    client = FakeMicrovmsClient([client_error("AccessDeniedException")])
    clock = FakeClock()

    with pytest.raises(ClientError, match="AccessDeniedException"):
        wait(client, clock)


def launch_event(name: str, microvm_id: str) -> dict[str, Any]:
    return {
        "EventType": "StepSucceeded",
        "Name": name,
        "StepSucceededDetails": {
            "Result": {
                "Payload": (
                    f'{{"microvmId": "{microvm_id}", "delivery": "http", '
                    '"resumeBeforeDelivery": true}'
                )
            }
        },
    }


class FakeLambdaClient:
    """Answers GetDurableExecutionHistory from a list of answers, in order.

    Each answer is a list of pages, or an exception. After the list ends, it
    repeats the last answer. Each page is a list of events. Every page but the
    last carries a NextMarker.
    """

    def __init__(self, answers: list[Any]) -> None:
        self._answers = answers
        self._polls = 0
        self.requests: list[dict[str, Any]] = []

    def get_durable_execution_history(self, **request: Any) -> dict[str, Any]:
        self.requests.append(request)
        if "Marker" not in request:
            self._polls += 1
        answer = self._answers[min(self._polls, len(self._answers)) - 1]
        if isinstance(answer, BaseException):
            raise answer
        index = int(request.get("Marker", "0"))
        page: dict[str, Any] = {"Events": answer[index]}
        if index + 1 < len(answer):
            page["NextMarker"] = str(index + 1)
        return page


def find(client: FakeLambdaClient, clock: FakeClock) -> str:
    return find_launched_microvm_id(
        "arn:execution", "s.launch", client=client, sleep=clock.sleep, clock=clock
    )


def test_find_reads_the_microvm_id_from_a_later_history_page():
    client = FakeLambdaClient(
        [[[{"EventType": "ExecutionStarted"}], [launch_event("s.launch", "mvm-1")]]]
    )
    clock = FakeClock()

    assert find(client, clock) == "mvm-1"
    assert client.requests == [
        {"DurableExecutionArn": "arn:execution", "IncludeExecutionData": True},
        {
            "DurableExecutionArn": "arn:execution",
            "IncludeExecutionData": True,
            "Marker": "1",
        },
    ]
    assert clock.sleeps == []


def test_find_reads_again_until_the_launch_step_succeeds():
    client = FakeLambdaClient(
        [
            client_error("ResourceNotFoundException"),
            [[launch_event("s.session", "ignored")]],
            [[launch_event("s.session", "ignored"), launch_event("s.launch", "mvm-1")]],
        ]
    )
    clock = FakeClock()

    assert find(client, clock) == "mvm-1"
    assert clock.sleeps == [POLL_INTERVAL_SECONDS] * 2


def test_find_fails_after_the_timeout():
    client = FakeLambdaClient([[[{"EventType": "ExecutionStarted"}]]])
    clock = FakeClock()

    with pytest.raises(
        AssertionError, match="no StepSucceeded event named s.launch within 120"
    ):
        find(client, clock)

    assert sum(clock.sleeps) == HISTORY_TIMEOUT_SECONDS


def test_find_raises_another_history_error():
    client = FakeLambdaClient([client_error("AccessDeniedException")])
    clock = FakeClock()

    with pytest.raises(ClientError, match="AccessDeniedException"):
        find(client, clock)


WAIT_STARTED = {"EventType": "WaitStarted", "Name": "pause", "Id": "w-1"}


def wait_for_pause(client: FakeLambdaClient, clock: FakeClock) -> dict[str, Any]:
    return wait_for_history_event(
        "arn:execution",
        "WaitStarted",
        "pause",
        client=client,
        sleep=clock.sleep,
        clock=clock,
    )


def test_wait_for_event_returns_the_event_from_a_later_history_page():
    client = FakeLambdaClient(
        [
            [
                [{"EventType": "ExecutionStarted"}],
                # Same type with another name, and same name with another type.
                [
                    {"EventType": "WaitStarted", "Name": "other"},
                    {"EventType": "StepStarted", "Name": "pause"},
                ],
                [WAIT_STARTED],
            ]
        ]
    )
    clock = FakeClock()

    assert wait_for_pause(client, clock) == WAIT_STARTED
    assert [request.get("Marker") for request in client.requests] == [
        None,
        "1",
        "2",
    ]
    assert clock.sleeps == []


def test_wait_for_event_reads_again_after_resource_not_found():
    client = FakeLambdaClient(
        [
            client_error("ResourceNotFoundException"),
            client_error("ResourceNotFoundException"),
            [[{"EventType": "ExecutionStarted"}, WAIT_STARTED]],
        ]
    )
    clock = FakeClock()

    assert wait_for_pause(client, clock) == WAIT_STARTED
    assert clock.sleeps == [POLL_INTERVAL_SECONDS] * 2


def test_wait_for_event_fails_after_the_timeout():
    client = FakeLambdaClient([[[{"EventType": "ExecutionStarted"}]]])
    clock = FakeClock()

    with pytest.raises(
        AssertionError, match="no WaitStarted event named pause within 120 seconds"
    ):
        wait_for_pause(client, clock)

    # The check reads the history at 0, 2, ..., 120 seconds.
    assert len(client.requests) == HISTORY_TIMEOUT_SECONDS / POLL_INTERVAL_SECONDS + 1
    assert sum(clock.sleeps) == HISTORY_TIMEOUT_SECONDS


class FakeRunner:
    """Records send_callback_failure calls, and raises ``error`` if set."""

    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.failures: list[dict[str, Any]] = []

    def send_callback_failure(self, **request: Any) -> None:
        self.failures.append(request)
        if self._error is not None:
            raise self._error


def test_fail_callback_sends_a_callback_failure():
    runner = FakeRunner()

    fail_callback_after_test_failure(runner, "cb-1")

    assert runner.failures == [{"callback_id": "cb-1", "error": TEST_FAILED_ERROR}]


def test_fail_callback_logs_an_error_instead_of_raising_it(caplog):
    runner = FakeRunner(RuntimeError("callback already completed"))

    fail_callback_after_test_failure(runner, "cb-1")

    assert len(runner.failures) == 1
    assert "Could not fail callback cb-1" in caplog.text
    assert "callback already completed" in caplog.text
