from __future__ import annotations

import inspect
import logging
import os
import time
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


class _PluginAFactory:
    def create_plugin(self, info: InvocationStartInfo) -> _PluginA:
        return _PluginA()


class _PluginBFactory:
    def create_plugin(self, info: InvocationStartInfo) -> _PluginB:
        return _PluginB()


class _DefaultedArgumentFactory:
    """Instance method whose info parameter has a default, so one argument binds."""

    def create_plugin(self, info: InvocationStartInfo | None = None) -> _PluginA:
        return _PluginA()


class _VariadicFactory:
    """Instance method taking ``*args``, so any argument count binds."""

    def create_plugin(self, *args: object) -> _PluginA:
        return _PluginA()


_plugin_a_factory = _PluginAFactory()
_plugin_b_factory = _PluginBFactory()


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
    factory = Mock()
    factory.create_plugin = Mock(return_value=_PluginA())
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
    factory.create_plugin.assert_not_called()


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
    assert [
        type(factory.create_plugin(INVOCATION_START_INFO)) for factory in result
    ] == [
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
    """The same factory passed explicitly and named in the env registers once.

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
    discovery could compare without constructing anything; what a factory builds
    is unknown until ``create_plugin`` is called, and calling it at load time
    would build an instance outside any invocation. Callers that both pass a
    factory and name a different one in the environment now get both plugins.
    """
    another_plugin_a_factory = _PluginAFactory()

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
    ("resolved_value", "expected_description"),
    [
        (_PluginA(), "_PluginA"),
        (object(), "builtins.object"),
        ("not-a-factory", "builtins.str"),
        (None, "builtins.NoneType"),
        # A function is named by its own qualified name, not by its type. Its type
        # is ``builtins.function`` for every function ever written, so naming the
        # type would identify no particular one.
        (lambda info: _PluginA(), "<lambda>"),
    ],
)
def test_discovery_rejects_entry_point_without_create_plugin(
    resolved_value: object,
    expected_description: str,
) -> None:
    """A plugin *instance* at the entry point is now the common mistake.

    A bare callable is the other one, and it is rejected too: the registration
    type is an object with ``create_plugin``, so a function that builds a plugin
    no longer satisfies it. The message names what the target actually was.
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

    assert "must resolve to a plugin factory" in str(error.value)
    assert "create_plugin(info) method" in str(error.value)
    assert expected_description in str(error.value)


def test_discovery_rejects_a_plugin_class_at_the_entry_point() -> None:
    """A plugin class carries no ``create_plugin``, so it is not a factory."""

    class _InfoAwarePlugin(DurableInstrumentationPlugin):
        def __init__(self, info: InvocationStartInfo) -> None:
            self.info = info

    entry_point = _FakeEntryPoint("a", _InfoAwarePlugin)

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

    assert "must resolve to a plugin factory" in str(error.value)
    assert "not a plugin class" in str(error.value)


def test_explicit_plugin_instance_is_rejected_with_its_position() -> None:
    """A plugin instance in ``plugins`` fails configuration, not every invocation.

    An instance has no ``create_plugin``, so the per-invocation factory call
    raises ``AttributeError``, which the executor logs and swallows -- the plugin
    silently never runs. The position is asserted because a caller passing several
    entries has no other way to tell which one is wrong.
    """
    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins(
            [_plugin_a_factory, _PluginB(), _plugin_b_factory],  # type: ignore[list-item]
            environment={},
        )

    assert "plugins[1]" in str(error.value)
    assert "must be a plugin factory" in str(error.value)
    assert "_PluginB" in str(error.value)
    # An instance and the class it was built from share one qualified name, so the
    # message states which of the two was passed.
    assert "an instance of" in str(error.value)


@pytest.mark.parametrize(
    ("invalid_entry", "expected_type_name"),
    [
        (_PluginA(), "_PluginA"),
        (object(), "builtins.object"),
        ("not-a-factory", "builtins.str"),
        (None, "builtins.NoneType"),
    ],
)
def test_explicit_entries_without_create_plugin_are_rejected(
    invalid_entry: object,
    expected_type_name: str,
) -> None:
    """One callable member is all that is checkable without building an instance."""
    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([invalid_entry], environment={})  # type: ignore[list-item]

    assert "plugins[0]" in str(error.value)
    assert expected_type_name in str(error.value)


def test_explicit_non_callable_create_plugin_is_rejected() -> None:
    """The attribute is tested for callability, not merely for presence.

    An object whose ``create_plugin`` is data would otherwise pass here and raise
    ``TypeError`` on every invocation, where it is logged and swallowed.
    """

    class _NotAFactory:
        create_plugin = "not callable"

    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([_NotAFactory()], environment={})  # type: ignore[list-item]

    assert "plugins[0]" in str(error.value)
    assert "_NotAFactory" in str(error.value)


@pytest.mark.parametrize(
    "bare_callable",
    [
        lambda info: _PluginA(),
        _PluginAFactory.create_plugin,
    ],
)
def test_explicit_bare_callable_is_rejected(bare_callable: object) -> None:
    """A callable is no longer a factory, which reverses the previous rule.

    The registration type was ``Callable[[InvocationStartInfo],
    DurableInstrumentationPlugin]``, so a lambda or a plain function was a valid
    factory. It is now an object with ``create_plugin``, and no compatibility
    path accepts both: a bare callable fails at handler initialization with
    guidance naming the replacement.
    """
    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([bare_callable], environment={})  # type: ignore[list-item]

    assert "plugins[0]" in str(error.value)
    assert "must be a plugin factory" in str(error.value)
    assert "plugins=[MyPluginFactory(exporter)]" in str(error.value)


def test_explicit_plugin_class_is_rejected_as_a_factory() -> None:
    """A plugin class is not a factory, reversing what this branch documented.

    Calling a class constructs an instance, so a class satisfied the previous
    ``Callable`` registration type and ``plugins=[MyPlugin]`` was accepted. A
    class carries no ``create_plugin`` attribute, so it is now rejected at
    handler initialization. The replacement is a factory class whose
    ``create_plugin`` constructs the plugin, which also keeps setup work out of
    the plugin's ``__init__``.
    """

    class _InfoAwarePlugin(DurableInstrumentationPlugin):
        def __init__(self, info: InvocationStartInfo) -> None:
            self.info = info

    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([_InfoAwarePlugin], environment={})  # type: ignore[list-item]

    assert "plugins[0]" in str(error.value)
    assert "a plugin class" in str(error.value)


def test_explicit_class_declaring_create_plugin_is_accepted() -> None:
    """The requirement is the member, not the kind of object.

    A class that declares ``create_plugin`` as a ``@classmethod`` carries the
    attribute, so the class object itself is a valid factory. Nothing in the
    contract requires a factory to be an instance.
    """

    class _ClassFactoryPlugin(DurableInstrumentationPlugin):
        def __init__(self, info: InvocationStartInfo) -> None:
            self.info = info

        @classmethod
        def create_plugin(cls, info: InvocationStartInfo) -> _ClassFactoryPlugin:
            return cls(info)

    result = load_configured_plugins([_ClassFactoryPlugin], environment={})

    assert result == [_ClassFactoryPlugin]
    plugin = result[0].create_plugin(INVOCATION_START_INFO)
    assert isinstance(plugin, _ClassFactoryPlugin)
    assert plugin.info is INVOCATION_START_INFO


@pytest.mark.parametrize(
    "factory_class",
    [
        _PluginAFactory,
        _DefaultedArgumentFactory,
        _VariadicFactory,
    ],
    ids=["plain", "defaulted", "variadic"],
)
def test_explicit_factory_class_with_an_instance_method_is_rejected(
    factory_class: type,
) -> None:
    """The factory class is not the factory, and no signature shape rescues it.

    ``MyFactory.create_plugin`` read off the class is a plain function whose first
    parameter is ``self``, so the per-invocation call binds the info to ``self``
    and the factory never sees it. A signature bind alone does not catch every
    such shape: ``create_plugin(self, info=None)`` and
    ``create_plugin(self, *args)`` both bind one argument to ``self`` and leave
    the rest satisfied, so they used to pass and then fail on every invocation
    where the error is swallowed. The kind of the member decides instead.
    """
    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([factory_class], environment={})  # type: ignore[list-item]

    assert "plugins[0]" in str(error.value)
    assert "a factory instance rather than the factory class" in str(error.value)


@pytest.mark.parametrize(
    "factory_class",
    [
        _PluginAFactory,
        _DefaultedArgumentFactory,
        _VariadicFactory,
    ],
    ids=["plain", "defaulted", "variadic"],
)
def test_discovery_rejects_a_factory_class_at_the_entry_point(
    factory_class: type,
) -> None:
    """The entry-point path applies the same rule."""
    entry_point = _FakeEntryPoint("a", factory_class)

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

    assert "must resolve to a plugin factory" in str(error.value)
    assert "not the factory class" in str(error.value)


@pytest.mark.parametrize(
    "rejected_class",
    [_PluginA, _PluginAFactory],
    ids=["plugin-class", "factory-class"],
)
def test_a_rejected_class_is_named_by_itself_not_by_its_metaclass(
    rejected_class: type,
) -> None:
    """The message has to name the class that was passed.

    The type of a class is its metaclass, which is ``builtins.type`` for both
    classes here. Both are rejected shapes a caller reaches by accident:
    ``plugins=[MyPlugin]`` is what the previous major accepted, and
    ``plugins=[MyPluginFactory]`` is this major's shape with the parentheses left
    off. Naming the type would report ``builtins.type`` for either one and
    distinguish neither. So the class is named directly, and the message says it
    was a class rather than an instance.
    """
    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([rejected_class], environment={})  # type: ignore[list-item]

    message = str(error.value)
    qualified = f"{rejected_class.__module__}.{rejected_class.__qualname__}"
    assert f"the class {qualified}" in message
    assert "builtins.type" not in message


@pytest.mark.parametrize(
    "rejected_class",
    [_PluginA, _PluginAFactory],
    ids=["plugin-class", "factory-class"],
)
def test_a_rejected_class_at_the_entry_point_is_named_by_itself(
    rejected_class: type,
) -> None:
    """The entry-point path applies the same naming rule."""
    entry_point = _FakeEntryPoint("a", rejected_class)

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

    message = str(error.value)
    qualified = f"{rejected_class.__module__}.{rejected_class.__qualname__}"
    assert f"the class {qualified}" in message
    assert "builtins.type" not in message


def test_explicit_class_holding_a_callable_create_plugin_is_accepted() -> None:
    """A class attribute holding a callable takes no implicit first argument.

    Reading it off the class produces the callable itself, so the info reaches it.
    """

    class _CallableMember:
        def __call__(self, info: InvocationStartInfo) -> _PluginA:
            return _PluginA()

    class _MemberFactory:
        create_plugin = _CallableMember()

    result = load_configured_plugins([_MemberFactory], environment={})  # type: ignore[list-item]

    assert result == [_MemberFactory]
    assert isinstance(
        _MemberFactory.create_plugin(INVOCATION_START_INFO),
        _PluginA,
    )


def test_explicit_class_declaring_a_static_create_plugin_is_accepted() -> None:
    """A ``@staticmethod`` presents the signature the SDK calls, so it binds."""

    class _StaticFactoryPlugin(DurableInstrumentationPlugin):
        def __init__(self, info: InvocationStartInfo) -> None:
            self.info = info

        @staticmethod
        def create_plugin(info: InvocationStartInfo) -> _StaticFactoryPlugin:
            return _StaticFactoryPlugin(info)

    result = load_configured_plugins([_StaticFactoryPlugin], environment={})

    assert result == [_StaticFactoryPlugin]
    assert isinstance(
        result[0].create_plugin(INVOCATION_START_INFO), _StaticFactoryPlugin
    )


def test_explicit_factory_without_an_introspectable_signature_is_accepted() -> None:
    """A signature that cannot be read is not evidence of a broken factory.

    ``inspect.signature`` raises ``ValueError`` for some C-implemented callables,
    ``time.strftime`` among them. Rejecting such a factory would refuse a usable
    one over a missing description of it, so the member alone decides.
    """

    class _UnreadableSignatureFactory:
        create_plugin = staticmethod(time.strftime)

    factory = _UnreadableSignatureFactory()

    with pytest.raises(ValueError, match="no signature"):
        inspect.signature(time.strftime)

    assert load_configured_plugins([factory], environment={}) == [factory]  # type: ignore[list-item, comparison-overlap]


def test_explicit_factory_taking_no_argument_is_rejected() -> None:
    """A ``create_plugin`` that takes nothing cannot receive the info."""

    class _NoArgumentFactory:
        def create_plugin(self) -> _PluginA:
            return _PluginA()

    with pytest.raises(PluginLoadError) as error:
        load_configured_plugins([_NoArgumentFactory()], environment={})  # type: ignore[list-item]

    assert "plugins[0]" in str(error.value)
    assert "create_plugin(info) method" in str(error.value)


def test_explicit_factory_object_is_accepted() -> None:
    result = load_configured_plugins([_plugin_a_factory], environment={})

    assert result == [_plugin_a_factory]
    assert isinstance(result[0].create_plugin(INVOCATION_START_INFO), _PluginA)


def test_explicit_factory_holds_handler_lifetime_state() -> None:
    """A factory instance is where state spanning invocations belongs."""

    class _StatefulFactory:
        def __init__(self) -> None:
            self.calls: list[InvocationStartInfo] = []

        def create_plugin(self, info: InvocationStartInfo) -> _PluginA:
            self.calls.append(info)
            return _PluginA()

    factory = _StatefulFactory()

    result = load_configured_plugins([factory], environment={})

    assert result == [factory]
    assert isinstance(result[0].create_plugin(INVOCATION_START_INFO), _PluginA)
    assert factory.calls == [INVOCATION_START_INFO]


def test_explicit_entries_are_validated_before_entry_points_are_imported() -> None:
    """Explicit entries are checked first, so a valid provider is not imported.

    Importing a provider runs third-party module code. A configuration that is
    already invalid should fail before that happens, and the failure should name
    the invalid entry rather than whatever the import did.
    """
    entry_point = _FakeEntryPoint("a", _plugin_a_factory)

    with (
        patch(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            return_value=[entry_point],
        ) as entry_points,
        patch.object(
            _FakeEntryPoint, "load", side_effect=AssertionError("must not import")
        ) as load,
        pytest.raises(PluginLoadError, match=r"plugins\[0\]"),
    ):
        load_configured_plugins(
            [_PluginA()],  # type: ignore[list-item]
            environment={PLUGIN_ENVIRONMENT_VARIABLE: "a"},
        )

    entry_points.assert_not_called()
    load.assert_not_called()
