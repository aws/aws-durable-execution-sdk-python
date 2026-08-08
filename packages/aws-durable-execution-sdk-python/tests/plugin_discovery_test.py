from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import cast
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.plugin import (
    DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginProvider,
)
from aws_durable_execution_sdk_python.plugin_discovery import (
    PLUGIN_ENTRY_POINT_GROUP,
    PLUGIN_ENVIRONMENT_VARIABLE,
    load_configured_plugins,
)


class _PluginA(DurableInstrumentationPlugin):
    pass


class _PluginB(DurableInstrumentationPlugin):
    pass


class _FakeDistribution:
    def __init__(self, name: str) -> None:
        self.metadata = {"Name": name}


class _FakeEntryPoint:
    def __init__(
        self,
        name: str,
        loaded_value: object,
        *,
        distribution_name: str | None = "test-plugin-package",
        load_error: Exception | None = None,
    ) -> None:
        self.name = name
        self.value = f"test_plugins:{name}"
        self.dist = (
            _FakeDistribution(distribution_name)
            if distribution_name is not None
            else None
        )
        self._loaded_value = loaded_value
        self._load_error = load_error

    def load(self) -> object:
        if self._load_error is not None:
            raise self._load_error
        return self._loaded_value


def _provider(
    factory: Callable[[], object],
    *,
    plugin_type: type[DurableInstrumentationPlugin] = _PluginA,
    plugin_api_version: int = DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
) -> DurableInstrumentationPluginProvider:
    return DurableInstrumentationPluginProvider(
        plugin_type=plugin_type,
        factory=cast(Callable[[], DurableInstrumentationPlugin], factory),
        plugin_api_version=plugin_api_version,
    )


@pytest.mark.parametrize("configured_value", [None, "", "   "])
def test_unconfigured_discovery_preserves_explicit_plugins(
    configured_value: str | None,
) -> None:
    explicit_plugin = _PluginA()
    environment = (
        {}
        if configured_value is None
        else {PLUGIN_ENVIRONMENT_VARIABLE: configured_value}
    )

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points"
    ) as entry_points:
        result = load_configured_plugins(
            [explicit_plugin],
            environment=environment,
        )

    assert result == [explicit_plugin]
    entry_points.assert_not_called()


def test_discovery_uses_process_environment_by_default() -> None:
    entry_point = _FakeEntryPoint("a", _provider(_PluginA))

    with (
        patch.dict(
            os.environ,
            {PLUGIN_ENVIRONMENT_VARIABLE: "a"},
            clear=True,
        ),
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ) as entry_points,
    ):
        result = load_configured_plugins(None)

    assert len(result) == 1
    assert isinstance(result[0], _PluginA)
    entry_points.assert_called_once_with(group=PLUGIN_ENTRY_POINT_GROUP)


def test_discovery_preserves_configured_order() -> None:
    factory_calls: list[str] = []

    def create_a() -> _PluginA:
        factory_calls.append("a")
        return _PluginA()

    def create_b() -> _PluginB:
        factory_calls.append("b")
        return _PluginB()

    entry_points = [
        _FakeEntryPoint("b", _provider(create_b, plugin_type=_PluginB)),
        _FakeEntryPoint("a", _provider(create_a)),
    ]

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=entry_points,
    ):
        result = load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: " a, b "},
        )

    assert [type(plugin) for plugin in result] == [_PluginA, _PluginB]
    assert factory_calls == ["a", "b"]


@pytest.mark.parametrize("configured_value", ["a,,b", ",a", "a,"])
def test_discovery_rejects_empty_plugin_names(configured_value: str) -> None:
    with pytest.raises(
        PluginLoadError,
        match="must contain non-empty, comma-separated plugin names",
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: configured_value},
        )


def test_discovery_rejects_duplicate_configured_names() -> None:
    with pytest.raises(
        PluginLoadError,
        match="contains duplicate plugin name 'a'",
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a,b,a"},
        )


def test_discovery_reports_missing_provider_and_available_names() -> None:
    entry_point = _FakeEntryPoint("available", _provider(_PluginA))

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(PluginLoadError) as error,
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "missing"},
        )

    assert "No durable instrumentation plugin provider named 'missing'" in str(
        error.value
    )
    assert "Installed providers: available" in str(error.value)
    assert "Lambda layer" in str(error.value)


def test_discovery_reports_when_no_providers_are_installed() -> None:
    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[],
        ),
        pytest.raises(PluginLoadError, match="Installed providers: none"),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "missing"},
        )


def test_discovery_rejects_ambiguous_provider_name() -> None:
    entry_points = [
        _FakeEntryPoint(
            "duplicate",
            _provider(_PluginA),
            distribution_name="package-a",
        ),
        _FakeEntryPoint(
            "duplicate",
            _provider(_PluginB),
            distribution_name="package-b",
        ),
    ]

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=entry_points,
        ),
        pytest.raises(PluginLoadError) as error,
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "duplicate"},
        )

    assert "Multiple durable instrumentation plugin providers" in str(error.value)
    assert "package-a, package-b" in str(error.value)


def test_discovery_wraps_entry_point_enumeration_failure() -> None:
    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            side_effect=RuntimeError("metadata unavailable"),
        ),
        pytest.raises(PluginLoadError, match="metadata unavailable"),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )


def test_discovery_wraps_provider_load_failure() -> None:
    entry_point = _FakeEntryPoint(
        "a",
        _provider(_PluginA),
        load_error=ImportError("missing dependency"),
    )

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(PluginLoadError) as error,
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert "Failed to load durable instrumentation plugin provider 'a'" in str(
        error.value
    )
    assert "test-plugin-package" in str(error.value)
    assert isinstance(error.value.__cause__, ImportError)


def test_discovery_rejects_invalid_provider_type() -> None:
    entry_point = _FakeEntryPoint("a", _PluginA)

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(
            PluginLoadError,
            match="must resolve to DurableInstrumentationPluginProvider",
        ),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )


def test_discovery_rejects_incompatible_plugin_api_version() -> None:
    entry_point = _FakeEntryPoint(
        "a",
        _provider(_PluginA, plugin_api_version=99),
    )

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(PluginLoadError) as error,
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert "declares plugin API version 99" in str(error.value)
    assert (
        f"supports plugin API version {DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION}"
        in str(error.value)
    )


def test_discovery_rejects_invalid_declared_plugin_type() -> None:
    provider = DurableInstrumentationPluginProvider(
        plugin_type=cast(type[DurableInstrumentationPlugin], object),
        factory=_PluginA,
    )
    entry_point = _FakeEntryPoint("a", provider)

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(
            PluginLoadError,
            match="declares invalid plugin type builtins.object",
        ),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )


def test_discovery_rejects_non_class_declared_plugin_type() -> None:
    provider = DurableInstrumentationPluginProvider(
        plugin_type=cast(type[DurableInstrumentationPlugin], _PluginA()),
        factory=_PluginA,
    )
    entry_point = _FakeEntryPoint("a", provider)

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(
            PluginLoadError,
            match="declares invalid plugin type .*_PluginA",
        ),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )


def test_discovery_wraps_plugin_factory_failure() -> None:
    def fail_factory() -> _PluginA:
        raise RuntimeError("factory failed")

    entry_point = _FakeEntryPoint(
        "a",
        _provider(fail_factory),
        distribution_name=None,
    )

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(PluginLoadError) as error,
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert "Failed to create durable instrumentation plugin 'a'" in str(error.value)
    assert "unknown distribution" in str(error.value)
    assert isinstance(error.value.__cause__, RuntimeError)


def test_discovery_rejects_invalid_plugin_type() -> None:
    entry_point = _FakeEntryPoint("a", _provider(lambda: object()))

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(
            PluginLoadError,
            match="expected .*_PluginA",
        ),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )


def test_explicit_plugin_registration_takes_precedence(
    caplog: pytest.LogCaptureFixture,
) -> None:
    explicit_plugin = _PluginA()
    factory = Mock(return_value=_PluginA())
    entry_point = _FakeEntryPoint("a", _provider(factory))

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        caplog.at_level(
            logging.WARNING,
            logger="aws_durable_execution_sdk_python.plugin_discovery",
        ),
    ):
        result = load_configured_plugins(
            [explicit_plugin],
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert result == [explicit_plugin]
    factory.assert_not_called()
    assert "already registered by the decorator's plugins argument" in caplog.text


def test_first_dynamic_registration_wins_for_duplicate_plugin_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    first_factory = Mock(return_value=_PluginA())
    second_factory = Mock(return_value=_PluginA())
    entry_points = [
        _FakeEntryPoint("first", _provider(first_factory)),
        _FakeEntryPoint("second", _provider(second_factory)),
    ]

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=entry_points,
        ),
        caplog.at_level(
            logging.WARNING,
            logger="aws_durable_execution_sdk_python.plugin_discovery",
        ),
    ):
        result = load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "first,second"},
        )

    assert len(result) == 1
    assert isinstance(result[0], _PluginA)
    first_factory.assert_called_once_with()
    second_factory.assert_not_called()
    assert "already registered by dynamic provider 'first'" in caplog.text
