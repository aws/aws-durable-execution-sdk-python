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


def test_plugin_provider_requires_authored_api_version() -> None:
    with pytest.raises(TypeError, match="plugin_api_version"):
        DurableInstrumentationPluginProvider(
            plugin_type=_PluginA,
            factory=_PluginA,
        )  # type: ignore[call-arg]


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
        plugin_api_version=DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
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
        plugin_api_version=DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
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


class _ExclusivePluginA(DurableInstrumentationPlugin):
    __durable_registration_api__ = 1
    exclusive_group = "test-telemetry"


class _ExclusivePluginB(DurableInstrumentationPlugin):
    __durable_registration_api__ = 1
    exclusive_group = "test-telemetry"


@pytest.mark.parametrize("reverse", [False, True])
def test_explicit_plugins_in_same_group_are_rejected(reverse: bool) -> None:
    plugins = [_ExclusivePluginA(), _ExclusivePluginB()]
    if reverse:
        plugins.reverse()
    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins(plugins, environment={})
    assert "_ExclusivePluginA" in str(error.value)
    assert "_ExclusivePluginB" in str(error.value)
    assert "Keep only one" in str(error.value)


@pytest.mark.parametrize("same_instance", [False, True])
def test_repeated_explicit_exclusive_plugin_type_is_rejected(
    same_instance: bool,
) -> None:
    plugin = _ExclusivePluginA()
    duplicate = plugin if same_instance else _ExclusivePluginA()

    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([plugin, duplicate], environment={})

    message = str(error.value)
    assert "_ExclusivePluginA" in message
    assert "registered more than once in exclusive group 'test-telemetry'" in message
    assert "Register this plugin only once" in message


@pytest.mark.parametrize("explicit", [False, True])
def test_discovery_preserves_first_registration_of_exclusive_type(
    explicit: bool,
) -> None:
    first_plugin = _ExclusivePluginA()
    first_factory = Mock(return_value=first_plugin)
    second_factory = Mock(side_effect=_ExclusivePluginA)
    entries = [
        _FakeEntryPoint(
            "first", _provider(first_factory, plugin_type=_ExclusivePluginA)
        ),
        _FakeEntryPoint(
            "second", _provider(second_factory, plugin_type=_ExclusivePluginA)
        ),
    ]

    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=entries,
    ):
        result = load_configured_plugins(
            [first_plugin] if explicit else None,
            environment={
                PLUGIN_ENVIRONMENT_VARIABLE: "second" if explicit else "first,second"
            },
        )

    assert result == [first_plugin]
    second_factory.assert_not_called()
    if explicit:
        first_factory.assert_not_called()
    else:
        first_factory.assert_called_once_with()


def test_unrelated_plugins_can_accompany_exclusive_plugin() -> None:
    plugins = [_PluginA(), _ExclusivePluginA(), _PluginB()]
    assert load_configured_plugins(plugins, environment={}) == plugins


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_exclusive_groups_validated_before_any_factory(
    reverse: bool, mixed: bool
) -> None:
    factory_a = Mock(side_effect=_ExclusivePluginA)
    factory_b = Mock(side_effect=_ExclusivePluginB)
    providers = [
        _FakeEntryPoint("a", _provider(factory_a, plugin_type=_ExclusivePluginA)),
        _FakeEntryPoint("b", _provider(factory_b, plugin_type=_ExclusivePluginB)),
    ]
    names = ["a", "b"]
    types = [_ExclusivePluginA, _ExclusivePluginB]
    if reverse:
        names.reverse()
        types.reverse()
    explicit = [types[0]()] if mixed else []
    configured = names[1:] if mixed else names
    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=providers,
        ),
        pytest.raises(PluginLoadError, match="mutually exclusive"),
    ):
        load_configured_plugins(
            explicit, environment={PLUGIN_ENVIRONMENT_VARIABLE: ",".join(configured)}
        )
    factory_a.assert_not_called()
    factory_b.assert_not_called()


@pytest.mark.parametrize("group", [[], {}, 123, True, "", "   "])
@pytest.mark.parametrize("discovered", [False, True])
def test_invalid_exclusive_group_is_a_clear_load_error(
    monkeypatch: pytest.MonkeyPatch,
    group: object,
    discovered: bool,
) -> None:
    monkeypatch.setattr(_PluginA, "__durable_registration_api__", 1, raising=False)
    monkeypatch.setattr(_PluginA, "exclusive_group", group, raising=False)
    factory = Mock(side_effect=_PluginA)
    entry = _FakeEntryPoint("a", _provider(factory))
    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry],
        ),
        pytest.raises(
            PluginLoadError, match="_PluginA.*exclusive_group.*non-empty string"
        ),
    ):
        load_configured_plugins(
            None if discovered else [_PluginA()],
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"} if discovered else {},
        )
    factory.assert_not_called()


class _RegistrationObserver(_PluginA):
    __durable_registration_api__ = 1

    def __init__(self) -> None:
        self.results: list[bool] = []

    def on_registration_result(self, registered: bool) -> None:
        self.results.append(registered)


def test_registration_result_notifies_acceptance_and_later_rejection() -> None:
    observer = _RegistrationObserver()
    assert load_configured_plugins([observer], environment={}) == [observer]
    with pytest.raises(PluginLoadError, match="non-empty"):
        load_configured_plugins(
            [observer], environment={PLUGIN_ENVIRONMENT_VARIABLE: ","}
        )
    assert observer.results == [True, False]


def test_factory_failure_rejects_already_constructed_plugins() -> None:
    observer = _RegistrationObserver()
    failing = Mock(side_effect=ValueError("factory failed"))
    entries = [
        _FakeEntryPoint(
            "a", _provider(lambda: observer, plugin_type=_RegistrationObserver)
        ),
        _FakeEntryPoint("b", _provider(failing, plugin_type=_PluginB)),
    ]
    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=entries,
        ),
        pytest.raises(PluginLoadError, match="factory failed"),
    ):
        load_configured_plugins(None, environment={PLUGIN_ENVIRONMENT_VARIABLE: "a,b"})
    assert observer.results == [False]


def test_registration_callback_failure_does_not_break_valid_plugins() -> None:
    class Broken(_PluginA):
        __durable_registration_api__ = 1

        def on_registration_result(self, registered: bool) -> None:
            raise ValueError("cleanup notification failed")

    observer = _RegistrationObserver()
    result = load_configured_plugins([Broken(), observer], environment={})
    assert result[-1] is observer
    assert observer.results == [True]


def test_optional_registration_metadata_preserves_legacy_explicit_objects() -> None:
    class LegacyPlugin:
        pass

    legacy = cast(DurableInstrumentationPlugin, LegacyPlugin())
    assert load_configured_plugins([legacy], environment={}) == [legacy]


def test_registration_diagnostic_failure_is_isolated() -> None:
    class Broken(_PluginA):
        __durable_registration_api__ = 1

        def on_registration_result(self, registered: bool) -> None:
            raise ValueError("callback failed")

    observer = _RegistrationObserver()
    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.logger.exception",
        side_effect=ValueError("diagnostic failed"),
    ):
        assert (
            load_configured_plugins([Broken(), observer], environment={})[-1]
            is observer
        )
    assert observer.results == [True]


def test_wrong_type_factory_result_receives_rejection_cleanup() -> None:
    actual = _RegistrationObserver()
    entry = _FakeEntryPoint("wrong", _provider(lambda: actual, plugin_type=_PluginB))
    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry],
        ),
        pytest.raises(
            PluginLoadError, match="returned.*_RegistrationObserver.*expected.*_PluginB"
        ),
    ):
        load_configured_plugins(
            None, environment={PLUGIN_ENVIRONMENT_VARIABLE: "wrong"}
        )
    assert actual.results == [False]


@pytest.mark.parametrize("discovered", [False, True])
@pytest.mark.parametrize(
    "legacy_value", [42, property(lambda _: "business"), "business-group"]
)
def test_unopted_legacy_metadata_and_helpers_are_never_interpreted(
    discovered: bool,
    legacy_value: object,
) -> None:
    calls: list[bool] = []

    class Legacy(DurableInstrumentationPlugin):
        exclusive_group = legacy_value

        def on_registration_result(self, registered: bool) -> None:
            calls.append(registered)

    instance = Legacy()
    entry = _FakeEntryPoint("legacy", _provider(lambda: instance, plugin_type=Legacy))
    with patch(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        return_value=[entry],
    ):
        result = load_configured_plugins(
            None if discovered else [instance],
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "legacy"} if discovered else {},
        )
    assert result == [instance]
    with pytest.raises(PluginLoadError, match="non-empty"):
        load_configured_plugins(
            [instance], environment={PLUGIN_ENVIRONMENT_VARIABLE: ","}
        )
    assert calls == []


def test_legacy_descriptors_are_not_read_without_class_local_opt_in() -> None:
    reads: list[str] = []

    class LegacyMeta(type):
        def __getattribute__(cls, name: str):
            if name in {"exclusive_group", "__durable_registration_api__"}:
                reads.append(name)
                raise RuntimeError("legacy descriptor is not registration metadata")
            return super().__getattribute__(name)

    class Legacy(DurableInstrumentationPlugin, metaclass=LegacyMeta):
        @property
        def on_registration_result(self):
            reads.append("on_registration_result")
            raise RuntimeError("legacy helper is not a hook")

    instance = Legacy()
    assert load_configured_plugins([instance], environment={}) == [instance]
    assert reads == []


def test_registration_opt_in_is_not_inherited_by_existing_subclasses() -> None:
    class LegacyChild(_RegistrationObserver):
        exclusive_group = 42

    old = LegacyChild()
    assert load_configured_plugins([old], environment={}) == [old]
    assert old.results == []

    class DeliberateChild(_RegistrationObserver):
        __durable_registration_api__ = 1

    enabled = DeliberateChild()
    assert load_configured_plugins([enabled], environment={}) == [enabled]
    assert enabled.results == [True]


@pytest.mark.parametrize("marker", [None, True, "1", 1.0, property(lambda _: 1)])
def test_only_literal_registration_api_version_opts_in(
    marker: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Legacy(_RegistrationObserver):
        exclusive_group = 42

    monkeypatch.setattr(Legacy, "__durable_registration_api__", marker)
    instance = Legacy()
    assert load_configured_plugins([instance], environment={}) == [instance]
    assert instance.results == []


def test_base_class_does_not_shadow_legacy_dynamic_attributes() -> None:
    class Legacy(DurableInstrumentationPlugin):
        def __getattr__(self, name: str) -> str:
            return "legacy-" + name

    assert Legacy().exclusive_group == "legacy-exclusive_group"
    assert Legacy().on_registration_result == "legacy-on_registration_result"
    assert "__durable_registration_api__" not in DurableInstrumentationPlugin.__dict__


def test_legacy_metaclass_dict_property_is_not_executed() -> None:
    def namespace(_cls: object) -> None:
        raise RuntimeError("legacy metaclass property")

    def helper(_self: object, _registered: bool) -> None:
        raise AssertionError("legacy helper must not run")

    # Dynamically authored plugin classes can legally shadow type.__dict__ at
    # runtime, even though static stubs mark that attribute final.
    legacy_meta = type("LegacyMeta", (type,), {"__dict__": property(namespace)})
    legacy_type = legacy_meta(
        "Legacy",
        (DurableInstrumentationPlugin,),
        {
            "exclusive_group": 42,
            "on_registration_result": helper,
        },
    )
    legacy = legacy_type()
    assert load_configured_plugins([legacy], environment={}) == [legacy]
