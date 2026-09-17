from __future__ import annotations

import logging
import os
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)
from aws_durable_execution_sdk_python.plugin_discovery import (
    PLUGIN_ENTRY_POINT_GROUP,
    PLUGIN_ENVIRONMENT_VARIABLE,
    load_configured_plugins,
)


INVOCATION_START_INFO = InvocationStartInfo(
    request_id="req-1",
    execution_arn="arn:exec",
    is_first_invocation=True,
)


class _PluginA(DurableInstrumentationPlugin):
    pass


class _PluginB(DurableInstrumentationPlugin):
    pass


def _plugin_a_factory(info: InvocationStartInfo) -> _PluginA:
    return _PluginA()


def _plugin_b_factory(info: InvocationStartInfo) -> _PluginB:
    return _PluginB()


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


@pytest.mark.parametrize("configured_value", [None, "", "   "])
def test_unconfigured_discovery_preserves_explicit_factories(
    configured_value: str | None,
) -> None:
    environment = (
        {}
        if configured_value is None
        else {PLUGIN_ENVIRONMENT_VARIABLE: configured_value}
    )

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points"
    ) as entry_points:
        result = load_configured_plugins(
            [_plugin_a_factory],
            environment=environment,
        )

    assert result == [_plugin_a_factory]
    entry_points.assert_not_called()


def test_discovery_uses_process_environment_by_default() -> None:
    entry_point = _FakeEntryPoint("a", _plugin_a_factory)

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

    assert result == [_plugin_a_factory]
    entry_points.assert_called_once_with(group=PLUGIN_ENTRY_POINT_GROUP)


def test_discovery_returns_factories_without_calling_them() -> None:
    """Discovery resolves factories only; instances belong to an invocation.

    Nothing is constructed at load time, so no plugin instance exists outside
    the invocation that will use it.
    """
    factory = Mock(return_value=_PluginA())
    entry_point = _FakeEntryPoint("a", factory)

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=[entry_point],
    ):
        result = load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert result == [factory]
    factory.assert_not_called()


def test_discovery_preserves_configured_order() -> None:
    entry_points = [
        _FakeEntryPoint("b", _plugin_b_factory),
        _FakeEntryPoint("a", _plugin_a_factory),
    ]

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=entry_points,
    ):
        result = load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: " a, b "},
        )

    assert result == [_plugin_a_factory, _plugin_b_factory]
    assert [type(factory(INVOCATION_START_INFO)) for factory in result] == [
        _PluginA,
        _PluginB,
    ]


def test_explicit_factories_precede_discovered_factories() -> None:
    entry_point = _FakeEntryPoint("b", _plugin_b_factory)

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=[entry_point],
    ):
        result = load_configured_plugins(
            [_plugin_a_factory],
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "b"},
        )

    assert result == [_plugin_a_factory, _plugin_b_factory]


def test_explicit_registration_wins_over_the_same_discovered_factory(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The same callable passed explicitly and named in the env registers once.

    This is the narrowed form of the old type-based precedence rule. Dedup by
    declared plugin type is gone with the provider object; identity still covers
    the documented double-registration case.
    """
    entry_point = _FakeEntryPoint("a", _plugin_a_factory)

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
            [_plugin_a_factory],
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert result == [_plugin_a_factory]
    assert "already registered" in caplog.text


def test_distinct_factories_for_one_plugin_type_are_both_registered() -> None:
    """Type-level dedup is gone: two distinct factories both register.

    Recorded deliberately. The provider object declared a ``plugin_type`` that
    discovery could compare without constructing anything; a factory is opaque
    until called, and calling it at load time would build an instance outside any
    invocation. Callers that both pass a factory and name a different one in the
    environment now get both plugins.
    """

    def another_plugin_a_factory(info: InvocationStartInfo) -> _PluginA:
        return _PluginA()

    entry_point = _FakeEntryPoint("a", another_plugin_a_factory)

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=[entry_point],
    ):
        result = load_configured_plugins(
            [_plugin_a_factory],
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    assert result == [_plugin_a_factory, another_plugin_a_factory]


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
    entry_point = _FakeEntryPoint("available", _plugin_a_factory)

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
            _plugin_a_factory,
            distribution_name="package-a",
        ),
        _FakeEntryPoint(
            "duplicate",
            _plugin_b_factory,
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


def test_discovery_wraps_factory_load_failure() -> None:
    entry_point = _FakeEntryPoint(
        "a",
        _plugin_a_factory,
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

    assert "Failed to load durable instrumentation plugin factory 'a'" in str(
        error.value
    )
    assert "test-plugin-package" in str(error.value)
    assert isinstance(error.value.__cause__, ImportError)


def test_discovery_names_unknown_distribution_in_load_failure() -> None:
    entry_point = _FakeEntryPoint(
        "a",
        _plugin_a_factory,
        distribution_name=None,
        load_error=ImportError("missing dependency"),
    )

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ),
        pytest.raises(PluginLoadError, match="unknown distribution"),
    ):
        load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )


@pytest.mark.parametrize(
    ("resolved_value", "expected_type_name"),
    [
        (_PluginA(), "_PluginA"),
        (object(), "builtins.object"),
        ("not-a-factory", "builtins.str"),
        (None, "builtins.NoneType"),
    ],
)
def test_discovery_rejects_non_callable_entry_point(
    resolved_value: object,
    expected_type_name: str,
) -> None:
    """A plugin *instance* at the entry point is now the common mistake.

    The old shape resolved to a provider object, so this replaces the
    provider-type check with the only check that still means something: the
    resolved value has to be callable. The message names what it actually was.
    """
    entry_point = _FakeEntryPoint("a", resolved_value)

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

    assert "must resolve to a callable plugin factory, but resolved to" in str(
        error.value
    )
    assert expected_type_name in str(error.value)


def test_discovery_accepts_a_plugin_class_as_factory() -> None:
    """A class taking the info is callable, so it is a factory in its own right."""

    class _InfoAwarePlugin(DurableInstrumentationPlugin):
        def __init__(self, info: InvocationStartInfo) -> None:
            self.info = info

    entry_point = _FakeEntryPoint("a", _InfoAwarePlugin)

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=[entry_point],
    ):
        result = load_configured_plugins(
            None,
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    plugin = result[0](INVOCATION_START_INFO)
    assert isinstance(plugin, _InfoAwarePlugin)
    assert plugin.info is INVOCATION_START_INFO
