"""Registration failures release factory resources through the public decorator."""

from collections.abc import Callable
from importlib import metadata
from io import StringIO
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStartInfo,
)
from aws_durable_execution_sdk_python.plugin_discovery import (
    PLUGIN_ENTRY_POINT_GROUP,
    PLUGIN_ENVIRONMENT_VARIABLE,
)


@pytest.mark.parametrize(
    "registration", ["explicit-first", "explicit-last", "environment"]
)
def test_factory_descriptor_failure_releases_constructor_resources(
    monkeypatch: pytest.MonkeyPatch, registration: str
) -> None:
    failure = RuntimeError("cannot inspect create_plugin")

    class ResourceFactory:
        __durable_registration_api__ = 1

        def __init__(self) -> None:
            self.resource = StringIO()
            self.notifications: list[bool] = []

        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            raise AssertionError("registration must not construct invocation plugins")

        def on_registration_result(self, registered: bool) -> None:
            self.notifications.append(registered)
            if not registered:
                self.resource.close()

    class BrokenFactory:
        @property
        def create_plugin(
            self,
        ) -> Callable[[InvocationStartInfo], DurableInstrumentationPlugin]:
            raise failure

    def handler(event: object, context: DurableContext) -> None:
        raise AssertionError("registration must not run the handler")

    explicit = ResourceFactory()
    discovered = ResourceFactory()
    broken = BrokenFactory()
    entries: list[DurableInstrumentationPluginFactory] = [explicit]
    configured_names = ""
    entry_points: list[metadata.EntryPoint] = []
    if registration == "environment":
        configured_names = "healthy,broken"
        entry_points = [
            metadata.EntryPoint(
                name=name, value=f"factories:{name}", group=PLUGIN_ENTRY_POINT_GROUP
            )
            for name in ("healthy", "broken")
        ]
    elif registration == "explicit-first":
        entries.insert(0, broken)
    else:
        entries.append(broken)
    monkeypatch.setenv(PLUGIN_ENVIRONMENT_VARIABLE, configured_names)

    try:
        with (
            patch(
                "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
                return_value=entry_points,
            ),
            patch.object(metadata.EntryPoint, "load", side_effect=[discovered, broken]),
            pytest.raises(RuntimeError) as error,
        ):
            durable_execution(handler, plugins=entries)

        assert error.value is failure
        assert explicit.notifications == [False]
        assert explicit.resource.closed
        if registration == "environment":
            assert discovered.notifications == [False]
            assert discovered.resource.closed
    finally:
        explicit.resource.close()
        discovered.resource.close()
