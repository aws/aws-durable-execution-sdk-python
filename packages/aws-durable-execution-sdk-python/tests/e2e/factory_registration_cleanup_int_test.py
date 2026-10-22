"""Failed entry-point registration releases every loaded factory exactly once."""

from collections.abc import Callable
from importlib.metadata import EntryPoint
import sys
from tempfile import TemporaryFile
from types import ModuleType

import pytest

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)
from aws_durable_execution_sdk_python.plugin_discovery import PLUGIN_ENTRY_POINT_GROUP


class _ResourceOwner:
    __durable_registration_api__ = 1

    def __init__(self) -> None:
        self.resource = TemporaryFile()
        self.notifications: list[bool] = []

    def on_registration_result(self, registered: bool) -> None:
        self.notifications.append(registered)
        if not registered:
            self.resource.close()


class _HealthyFactory(_ResourceOwner):
    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        raise AssertionError("registration must not construct a plugin")


class _InvalidSignatureFactory(_ResourceOwner):
    def create_plugin(self) -> DurableInstrumentationPlugin:
        raise AssertionError("registration must not construct a plugin")


class _RaisingDescriptorFactory(_ResourceOwner):
    @property
    def create_plugin(
        self,
    ) -> Callable[[InvocationStartInfo], DurableInstrumentationPlugin]:
        raise RuntimeError("create_plugin descriptor failed")


@pytest.mark.parametrize(
    "invalid_type", [_InvalidSignatureFactory, _RaisingDescriptorFactory]
)
def test_rejected_entry_point_releases_candidate_and_deduplicates_cleanup(
    monkeypatch: pytest.MonkeyPatch, invalid_type: type[_ResourceOwner]
) -> None:
    healthy = _HealthyFactory()
    invalid = invalid_type()
    module = ModuleType("registration_cleanup_probe")
    setattr(module, "healthy", healthy)
    setattr(module, "invalid", invalid)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    entries = [
        EntryPoint(
            name=name,
            value=f"{module.__name__}:{attribute}",
            group=PLUGIN_ENTRY_POINT_GROUP,
        )
        for name, attribute in [
            ("first", "healthy"),
            ("alias", "healthy"),
            ("bad", "invalid"),
        ]
    ]
    monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", "first,alias,bad")
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        lambda **kwargs: entries,
    )

    def handler(event: object, context: DurableContext) -> None:
        raise AssertionError("registration must not invoke the handler")

    try:
        # Real EntryPoint.load() returns imported singleton factories. The same
        # healthy instance appears explicitly and under two environment aliases.
        with pytest.raises((PluginLoadError, RuntimeError)):
            durable_execution(handler, plugins=[healthy])
        assert healthy.notifications == [False] and healthy.resource.closed
        assert invalid.notifications == [False] and invalid.resource.closed
    finally:
        healthy.resource.close()
        invalid.resource.close()
