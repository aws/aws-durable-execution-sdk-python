"""Synchronous propagation collection without generated client dependencies."""

import asyncio
import logging
import threading
from dataclasses import FrozenInstanceError
from typing import Any
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    PluginExecutor,
    PropagationInput,
    PropagationMetadata,
)


INFO = PropagationInput("execution", "invoke-1", "target:1", "parent-1")


class _Alpha(DurableInstrumentationPlugin):
    def __init__(self, value: str | None) -> None:
        self.value = value

    def provide_propagation_metadata(
        self, info: PropagationInput
    ) -> PropagationMetadata:
        return PropagationMetadata(self.value)


class _Beta(_Alpha):
    pass


class _Gamma(_Alpha):
    pass


class _Broken(DurableInstrumentationPlugin):
    def provide_propagation_metadata(
        self, info: PropagationInput
    ) -> PropagationMetadata:
        raise ValueError("broken hook")


class _BrokenHookGetter(DurableInstrumentationPlugin):
    def __getattribute__(self, name: str) -> Any:
        if name == "provide_propagation_metadata":
            raise ValueError("broken hook lookup")
        return super().__getattribute__(name)


class _BrokenMetadata(PropagationMetadata):
    def __getattribute__(self, name: str) -> Any:
        if name == "x_amzn_trace_id":
            raise ValueError("broken result getter")
        return super().__getattribute__(name)


class _InvalidResult(DurableInstrumentationPlugin):
    def __init__(self, result: Any) -> None:
        self.result = result

    def provide_propagation_metadata(self, info: PropagationInput) -> Any:
        return self.result


def test_contract_is_immutable_and_existing_plugins_default_to_no_metadata() -> None:
    with pytest.raises(FrozenInstanceError):
        INFO.operation_id = "changed"  # type: ignore[misc]
    metadata = PropagationMetadata("header")
    with pytest.raises(FrozenInstanceError):
        metadata.x_amzn_trace_id = "changed"  # type: ignore[misc]
    legacy = DurableInstrumentationPlugin()
    assert legacy.provide_propagation_metadata(INFO) is None
    assert (
        PluginExecutor([legacy]).provide_propagation_metadata(INFO)
        == PropagationMetadata()
    )


def test_first_non_blank_wins_and_equal_values_do_not_conflict(
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = PluginExecutor(
        [_Alpha(None), _Beta("first"), _Gamma("first")]
    ).provide_propagation_metadata(INFO)
    assert result == PropagationMetadata("first")
    assert "conflict" not in caplog.text
    assert PluginExecutor([_Alpha(""), _Beta("later")]).provide_propagation_metadata(
        INFO
    ) == PropagationMetadata("later")


def test_conflicts_name_both_plugins_and_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = PluginExecutor(
        [_Alpha("first"), _Beta("second"), _Gamma("third")]
    ).provide_propagation_metadata(INFO)
    assert result == PropagationMetadata("first")
    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    assert "_Alpha" in warnings[0] and "_Beta" in warnings[0]
    assert "conflict_count=1" in warnings[0]
    assert "_Alpha" in warnings[1] and "_Gamma" in warnings[1]
    assert "conflict_count=2" in warnings[1]


@pytest.mark.parametrize(
    "broken",
    [
        _Broken(),
        _BrokenHookGetter(),
        _InvalidResult({"x_amzn_trace_id": "unsupported"}),
        _InvalidResult(PropagationMetadata(123)),  # type: ignore[arg-type]
        _InvalidResult(_BrokenMetadata("header")),
    ],
)
def test_ordinary_failures_leave_healthy_plugins_runnable(
    broken: DurableInstrumentationPlugin,
) -> None:
    result = PluginExecutor([broken, _Alpha("healthy")]).provide_propagation_metadata(
        INFO
    )
    assert result == PropagationMetadata("healthy")


def test_collection_is_synchronous_and_uses_readonly_input() -> None:
    calls: list[tuple[int, PropagationInput]] = []

    class Capturing(DurableInstrumentationPlugin):
        def provide_propagation_metadata(
            self, info: PropagationInput
        ) -> PropagationMetadata:
            calls.append((threading.get_ident(), info))
            return PropagationMetadata("header")

    assert PluginExecutor([Capturing()]).provide_propagation_metadata(
        INFO
    ) == PropagationMetadata("header")
    assert calls == [(threading.get_ident(), INFO)]


def test_diagnostic_failures_do_not_replace_healthy_results() -> None:
    with patch(
        "aws_durable_execution_sdk_python.plugin.logger.log",
        side_effect=ValueError("broken log handler"),
    ):
        result = PluginExecutor(
            [_Broken(), _Alpha("first"), _Beta("second")]
        ).provide_propagation_metadata(INFO)
    assert result == PropagationMetadata("first")


def test_accidental_async_return_is_rejected_and_closed() -> None:
    async def metadata() -> PropagationMetadata:
        return PropagationMetadata("async")

    coroutine = metadata()
    result = PluginExecutor(
        [_InvalidResult(coroutine), _Alpha("sync")]
    ).provide_propagation_metadata(INFO)
    assert result == PropagationMetadata("sync")
    assert getattr(coroutine, "cr_frame") is None


@pytest.mark.parametrize(
    "failure", [KeyboardInterrupt, SystemExit, GeneratorExit, asyncio.CancelledError]
)
def test_existing_fatal_and_cancellation_policy_is_preserved(
    failure: type[BaseException],
) -> None:
    class Fatal(DurableInstrumentationPlugin):
        def provide_propagation_metadata(
            self, info: PropagationInput
        ) -> PropagationMetadata:
            raise failure()

    with pytest.raises(failure):
        PluginExecutor([Fatal(), _Alpha("later")]).provide_propagation_metadata(INFO)


def test_string_subclass_comparison_cannot_break_aggregation() -> None:
    class StringWithHooks(str):
        def __ne__(self, other: object) -> bool:
            raise ValueError("unexpected comparison hook")

        def __str__(self) -> str:
            return self

    result = PluginExecutor(
        [
            _Alpha(StringWithHooks("first")),
            _Beta(StringWithHooks("second")),
        ]
    ).provide_propagation_metadata(INFO)
    assert result == PropagationMetadata("first")
    assert type(result.x_amzn_trace_id) is str


@pytest.mark.parametrize("empty", ["", " ", "\t\n"])
def test_blank_contribution_does_not_block_later_opaque_value(empty: str) -> None:
    value = "  opaque-header  "
    assert PluginExecutor([_Alpha(empty), _Beta(value)]).provide_propagation_metadata(
        INFO
    ) == PropagationMetadata(value)
    assert (
        PluginExecutor([_Alpha(empty)]).provide_propagation_metadata(INFO)
        == PropagationMetadata()
    )
