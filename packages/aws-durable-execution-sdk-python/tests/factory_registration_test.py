"""Registration metadata remains opt-in under the invocation factory API."""

import inspect
from collections.abc import Callable
from io import StringIO
from tempfile import TemporaryFile
from typing import cast
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStartInfo,
)
from aws_durable_execution_sdk_python.plugin_discovery import (
    PLUGIN_ENVIRONMENT_VARIABLE,
    load_configured_plugins,
)


class _FactoryBase:
    @classmethod
    def create_plugin(cls, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        raise AssertionError("registration must not construct invocation plugins")


class _PluginA(_FactoryBase):
    pass


class _PluginB(_FactoryBase):
    pass


class _ExclusivePluginA(_PluginA):
    __durable_registration_api__ = 1
    exclusive_group = "test-telemetry"


class _ExclusivePluginB(_PluginB):
    __durable_registration_api__ = 1
    exclusive_group = "test-telemetry"


class _CustomExclusivePlugin(_FactoryBase):
    __durable_registration_api__ = 1
    exclusive_group = "custom"


class _RegistrationObserver(_PluginA):
    __durable_registration_api__ = 1

    def __init__(self) -> None:
        self.results: list[bool] = []

    def on_registration_result(self, registered: bool) -> None:
        self.results.append(registered)


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


def test_unrelated_plugins_can_accompany_exclusive_plugin() -> None:
    plugins = [_PluginA(), _ExclusivePluginA(), _PluginB()]
    assert load_configured_plugins(plugins, environment={}) == plugins


def test_registration_result_notifies_acceptance_and_later_rejection() -> None:
    observer = _RegistrationObserver()
    assert load_configured_plugins([observer], environment={}) == [observer]
    with pytest.raises(PluginLoadError, match="non-empty"):
        load_configured_plugins(
            [observer], environment={PLUGIN_ENVIRONMENT_VARIABLE: ","}
        )
    assert observer.results == [True, False]


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
    class LegacyPlugin(_FactoryBase):
        pass

    legacy = LegacyPlugin()
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


def test_legacy_descriptors_are_not_read_without_registration_opt_in() -> None:
    reads: list[str] = []

    class LegacyMeta(type):
        def __getattribute__(cls, name: str):
            if name in {"exclusive_group", "__durable_registration_api__"}:
                reads.append(name)
                raise RuntimeError("legacy descriptor is not registration metadata")
            return super().__getattribute__(name)

    class Legacy(_FactoryBase, metaclass=LegacyMeta):
        @property
        def on_registration_result(self):
            reads.append("on_registration_result")
            raise RuntimeError("legacy helper is not a hook")

    instance = Legacy()
    assert load_configured_plugins([instance], environment={}) == [instance]
    assert reads == []


def test_inherited_registration_preserves_legacy_attribute_meaning() -> None:
    calls: list[bool] = []

    class LegacyChild(_RegistrationObserver):
        exclusive_group = 42

        def on_registration_result(self, registered: bool) -> None:
            calls.append(registered)

    old = LegacyChild()
    assert load_configured_plugins([old], environment={}) == [old]
    assert old.results == [True]
    with pytest.raises(PluginLoadError, match="non-empty"):
        load_configured_plugins([old], environment={PLUGIN_ENVIRONMENT_VARIABLE: ","})
    assert old.results == [True, False]
    assert calls == []
    assert old.exclusive_group == 42

    class DeliberateChild(_RegistrationObserver):
        __durable_registration_api__ = 1

    enabled = DeliberateChild()
    assert load_configured_plugins([enabled], environment={}) == [enabled]
    assert enabled.results == [True]


def test_subclass_can_explicitly_customize_registration_contract() -> None:
    class Child(_RegistrationObserver):
        __durable_registration_api__ = 1
        exclusive_group = "custom"

        def on_registration_result(self, registered: bool) -> None:
            self.results.append(not registered)

    child = Child()
    assert load_configured_plugins([child], environment={}) == [child]
    assert child.results == [False]
    with pytest.raises(PluginLoadError, match="exclusive group 'custom'"):
        load_configured_plugins([child, Child()], environment={})


def test_redeclared_none_does_not_erase_inherited_exclusion() -> None:
    class Child(_ExclusivePluginA):
        __durable_registration_api__ = 1

    with patch.object(Child, "exclusive_group", None):
        with pytest.raises(PluginLoadError, match="mutually exclusive"):
            load_configured_plugins([Child(), _ExclusivePluginB()], environment={})


@pytest.mark.parametrize("legacy_group", [42, property(lambda _: "business-group")])
def test_group_accumulation_preserves_unmarked_legacy_intermediate(
    legacy_group: object,
) -> None:
    class LegacyMiddle(_ExclusivePluginA):
        pass

    class Child(LegacyMiddle):
        __durable_registration_api__ = 1
        exclusive_group = "custom"

    with patch.object(LegacyMiddle, "exclusive_group", legacy_group):
        child = Child()
        assert load_configured_plugins([child], environment={}) == [child]
        assert vars(LegacyMiddle)["exclusive_group"] is legacy_group
        with pytest.raises(PluginLoadError, match="mutually exclusive"):
            load_configured_plugins([child, _ExclusivePluginB()], environment={})


@pytest.mark.parametrize("redeclare_same_group", [False, True])
def test_inherited_group_is_counted_once_per_registration(
    redeclare_same_group: bool,
) -> None:
    namespace: dict[str, object] = {"__durable_registration_api__": 1}
    if redeclare_same_group:
        namespace["exclusive_group"] = "test-telemetry"
    child_type = type("Child", (_ExclusivePluginA,), namespace)
    child = child_type()
    assert load_configured_plugins([child], environment={}) == [child]
    with pytest.raises(PluginLoadError, match="registered more than once"):
        load_configured_plugins([child, child_type()], environment={})


def test_multiple_inheritance_retains_each_opted_in_group() -> None:
    class Child(_ExclusivePluginA, _CustomExclusivePlugin):
        pass

    child = Child()
    assert load_configured_plugins([child], environment={}) == [child]
    for competitor in [_ExclusivePluginB(), _CustomExclusivePlugin()]:
        with pytest.raises(PluginLoadError, match="mutually exclusive"):
            load_configured_plugins([child, competitor], environment={})


@pytest.mark.parametrize("marker", [None, 2, True])
def test_invalid_marker_still_disables_all_inherited_groups(marker: object) -> None:
    class Child(_ExclusivePluginA):
        exclusive_group = "custom"

    with patch.object(Child, "__durable_registration_api__", marker):
        plugins = [Child(), _ExclusivePluginB(), _CustomExclusivePlugin()]
        assert load_configured_plugins(plugins, environment={}) == plugins


def test_multiple_groups_do_not_duplicate_registration_callbacks() -> None:
    class Child(_RegistrationObserver, _ExclusivePluginA):
        __durable_registration_api__ = 1
        exclusive_group = "custom"

    child = Child()
    assert load_configured_plugins([child], environment={}) == [child]
    assert child.results == [True]
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        load_configured_plugins([child, _ExclusivePluginB()], environment={})
    assert child.results == [True, False]


@pytest.mark.parametrize("marker", [None, True, "1", 1.0, property(lambda _: 1)])
def test_invalid_registration_marker_shadows_inherited_opt_in(
    marker: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Legacy(_RegistrationObserver):
        exclusive_group = 42

    monkeypatch.setattr(Legacy, "__durable_registration_api__", marker)
    instance = Legacy()
    assert load_configured_plugins([instance], environment={}) == [instance]
    assert instance.results == []


def test_base_class_does_not_shadow_legacy_dynamic_attributes() -> None:
    class Legacy(_FactoryBase):
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
    legacy_meta = type(
        "LegacyMeta",
        (type,),
        {"__dict__": property(namespace), "__mro__": property(namespace)},
    )
    legacy_type = legacy_meta(
        "Legacy",
        (_FactoryBase,),
        {
            "exclusive_group": 42,
            "on_registration_result": helper,
        },
    )
    legacy = legacy_type()
    assert load_configured_plugins([legacy], environment={}) == [legacy]


def test_inherited_callback_on_factory_class_binds_the_concrete_factory() -> None:
    observed: list[tuple[type, bool]] = []

    class Base(_FactoryBase):
        __durable_registration_api__ = 1

        @classmethod
        def on_registration_result(cls, registered: bool) -> None:
            observed.append((cls, registered))

    class Derived(Base):
        pass

    assert load_configured_plugins([Derived], environment={}) == [Derived]
    assert observed == [(Derived, True)]


@pytest.mark.parametrize("invalid_first", [False, True])
def test_decorator_rejection_cleans_valid_explicit_factories(
    monkeypatch: pytest.MonkeyPatch, invalid_first: bool
) -> None:
    """An invalid neighbor cannot strand a registered factory's resources."""
    monkeypatch.delenv(PLUGIN_ENVIRONMENT_VARIABLE, raising=False)
    created: list[InvocationStartInfo] = []
    notifications: list[bool] = []

    class ResourceFactory:
        __durable_registration_api__ = 1

        def __init__(self) -> None:
            self.resource = StringIO()

        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            created.append(info)
            return DurableInstrumentationPlugin()

        def on_registration_result(self, registered: bool) -> None:
            notifications.append(registered)
            if not registered:
                self.resource.close()

    factory = ResourceFactory()
    entries: list[object] = [factory, object()]
    if invalid_first:
        entries.reverse()

    def handler(event: object, context: DurableContext) -> None:
        raise AssertionError("registration must not run the handler")

    try:
        with pytest.raises(PluginLoadError, match="must be a plugin factory"):
            durable_execution(
                handler,
                plugins=cast(list[DurableInstrumentationPluginFactory], entries),
            )
        assert notifications == [False]
        assert factory.resource.closed
        assert created == []
    finally:
        factory.resource.close()


@pytest.mark.parametrize("invalid_first", [False, True])
@pytest.mark.parametrize("inspection", ["create_plugin", "signature"])
@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_factory_inspection_failure_preserves_error_and_rejects_registration(
    invalid_first: bool, inspection: str, error_type: type[BaseException]
) -> None:
    failure = error_type("factory inspection failed")

    class BrokenCallable:
        @property
        def __signature__(self) -> inspect.Signature:
            raise failure

        def __call__(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
            raise AssertionError("registration must not construct invocation plugins")

    class BrokenFactory:
        @property
        def create_plugin(
            self,
        ) -> Callable[[InvocationStartInfo], DurableInstrumentationPlugin]:
            if inspection == "create_plugin":
                raise failure
            return BrokenCallable()

    observer = _RegistrationObserver()
    entries: list[DurableInstrumentationPluginFactory] = [observer, BrokenFactory()]
    if invalid_first:
        entries.reverse()

    with pytest.raises(error_type) as error:
        load_configured_plugins(entries, environment={})

    assert error.value is failure
    assert observer.results == ([False] if isinstance(failure, Exception) else [])


@pytest.mark.parametrize("invalid_first", [False, True])
def test_class_inspection_failure_cleans_every_factory_and_preserves_error(
    invalid_first: bool,
) -> None:
    class ResourceFactory(_RegistrationObserver):
        def __init__(self) -> None:
            super().__init__()
            self.resource = TemporaryFile()

        # Repeat the marker to opt this cleanup implementation into the contract.
        __durable_registration_api__ = 1

        def on_registration_result(self, registered: bool) -> None:
            super().on_registration_result(registered)
            if not registered:
                self.resource.close()

    class BrokenClassFactory(ResourceFactory):
        def __init__(self) -> None:
            super().__init__()
            self.inspection_errors: list[RuntimeError] = []

        @property  # type: ignore[misc]  # Deliberately malformed customer descriptor.
        def __class__(self) -> type:  # type: ignore[override]
            error = RuntimeError(f"class inspection {len(self.inspection_errors) + 1}")
            self.inspection_errors.append(error)
            raise error

    broken = BrokenClassFactory()
    healthy = ResourceFactory()
    entries = [broken, healthy] if invalid_first else [healthy, broken]
    try:
        with pytest.raises(RuntimeError) as error:
            load_configured_plugins(entries, environment={})
        assert error.value is broken.inspection_errors[0]
        assert len(broken.inspection_errors) == 1
        assert broken.results == healthy.results == [False]
        assert broken.resource.closed and healthy.resource.closed
    finally:
        broken.resource.close()
        healthy.resource.close()
