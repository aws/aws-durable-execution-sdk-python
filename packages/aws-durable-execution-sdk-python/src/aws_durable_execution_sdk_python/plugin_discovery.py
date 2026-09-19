from __future__ import annotations

import inspect
import logging
import os
from collections.abc import Mapping, Sequence
from importlib import metadata
from typing import cast

from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPluginFactory,
)


logger = logging.getLogger(__name__)

PLUGIN_ENTRY_POINT_GROUP = "aws_durable_execution.plugins"
PLUGIN_ENVIRONMENT_VARIABLE = "DURABLE_EXECUTION_PLUGINS"

# Stands in for the InvocationStartInfo when a factory's signature is checked at
# registration. Only the bind is performed, so nothing reads it.
_ARGUMENT_PROBE = object()


def _parse_configured_plugin_names(environment: Mapping[str, str]) -> list[str]:
    configured_plugins = environment.get(PLUGIN_ENVIRONMENT_VARIABLE)
    if configured_plugins is None or not configured_plugins.strip():
        return []

    plugin_names = [name.strip() for name in configured_plugins.split(",")]
    if any(not name for name in plugin_names):
        raise PluginLoadError(
            f"{PLUGIN_ENVIRONMENT_VARIABLE} must contain non-empty, "
            "comma-separated plugin names."
        )

    seen_names: set[str] = set()
    for plugin_name in plugin_names:
        if plugin_name in seen_names:
            raise PluginLoadError(
                f"{PLUGIN_ENVIRONMENT_VARIABLE} contains duplicate plugin name "
                f"'{plugin_name}'."
            )
        seen_names.add(plugin_name)

    return plugin_names


def _distribution_name(entry_point: metadata.EntryPoint) -> str:
    distribution = getattr(entry_point, "dist", None)
    if distribution is None:
        return "unknown distribution"
    return distribution.metadata.get("Name", "unknown distribution")


def _module_qualified_name(named: object) -> str:
    """Join a class's or function's module and qualified name.

    Both attributes are read with a default, because a rejected entry can be any
    object at all. A missing name must not replace the configuration error with an
    ``AttributeError``.
    """
    module = getattr(named, "__module__", None) or "unknown module"
    qualified = getattr(named, "__qualname__", None) or getattr(named, "__name__", None)
    return f"{module}.{qualified or '<unnamed>'}"


def _describe_value(value: object) -> str:
    """Describe a rejected registration entry so the caller can identify it.

    Two kinds of value are named by themselves rather than by their type. A
    class's type is its metaclass, which is ``builtins.type`` for an ordinary
    class. A function's type is ``builtins.function``. Neither of those two names
    says which value was passed.

    Both of the likeliest migration mistakes are classes: ``plugins=[MyPlugin]``
    is the shape the previous major accepted, and ``plugins=[MyPluginFactory]`` is
    this major's shape with the parentheses left off. Naming the type would report
    ``builtins.type`` for both. So a class and a function are named directly, and
    every other value is named by its type.

    The kind is named alongside the name -- "the class", "the function", "an
    instance of" -- because a factory class and an instance of that factory class
    share one qualified name, and passing the class where an instance is required
    is itself one of the rejected shapes.
    """
    if isinstance(value, type):
        return f"the class {_module_qualified_name(value)}"
    if inspect.isroutine(value):
        return f"the function {_module_qualified_name(value)}"
    return f"an instance of {_module_qualified_name(type(value))}"


def _is_plugin_factory(value: object) -> bool:
    """Report whether a value has the shape of a plugin factory.

    :class:`DurableInstrumentationPluginFactory` declares one method, so the
    shape is one member: a ``create_plugin`` that can be called with one
    positional argument. The attribute is fetched and tested rather than merely
    checked for presence, because an object carrying a non-callable
    ``create_plugin`` would otherwise pass here and fail at invocation time.

    Callability alone is not enough. ``plugins=[MyFactory]`` -- the factory
    *class* rather than an instance of it -- resolves ``create_plugin`` to a
    plain function whose first parameter is ``self``, which is callable. The
    per-invocation call supplies only the info, Python binds it to ``self``, and
    the resulting :exc:`TypeError` is contained like any other factory failure:
    telemetry is silently absent for the lifetime of the function. Two checks
    reject that at registration instead. For a class, the *kind* of the member
    decides, because a signature bind cannot tell ``create_plugin(self, info)``
    from ``create_plugin(info)``: see :func:`_is_unbound_instance_method`. For
    everything else, binding one positional argument to the signature rejects a
    member that cannot receive the info. Neither check runs factory code.

    A callable with no introspectable signature -- a C-implemented callable, for
    example -- is accepted on the member alone. ``inspect.signature`` raises for
    it, and refusing a factory because its signature could not be read would
    reject a usable factory over a missing description of it.

    Structural rather than nominal, so a factory need not import the SDK
    protocol to satisfy it. The protocol is deliberately not
    ``@runtime_checkable``; see its docstring.

    The check stops there. Whether ``create_plugin`` returns a plugin is only
    knowable by calling it, and calling it at load time is what the
    per-invocation factory design avoids: there is no invocation yet. That case
    is checked per invocation by :meth:`PluginExecutor._create_plugins`.
    """
    create_plugin = getattr(value, "create_plugin", None)
    if not callable(create_plugin):
        return False
    if isinstance(value, type) and _is_unbound_instance_method(value, create_plugin):
        return False
    return _accepts_one_positional_argument(create_plugin)


def _is_unbound_instance_method(cls: type, create_plugin: object) -> bool:
    """Report whether a class's ``create_plugin`` is an instance method.

    Read off the class, an instance method is a plain function whose first
    parameter is ``self``, so the per-invocation call binds the info to ``self``
    and the factory never sees it. The signature bind below cannot catch every
    such shape: ``create_plugin(self, info=None)`` and
    ``create_plugin(self, *args)`` both bind one argument to ``self`` and leave
    the rest satisfied. The kind of the member decides it instead.

    Three shapes read off a class are usable and none of them is a plain
    function. A ``@classmethod`` is already bound to the class, so it carries
    ``__self__``. A ``@staticmethod`` is a plain function, but its descriptor says
    it takes no implicit first argument. And an attribute holding a callable
    object -- ``create_plugin = SomeCallable()`` -- is not a function at all and
    takes no implicit first argument either. Anything else read off a class takes
    ``self`` and cannot serve.

    :func:`inspect.getattr_static` is what distinguishes the ``@staticmethod``,
    because it returns the descriptor rather than what reading the attribute
    produces. It walks the MRO without running any descriptor, so no factory code
    runs here.
    """
    if getattr(create_plugin, "__self__", None) is not None:
        return False
    if not inspect.isfunction(create_plugin):
        return False
    try:
        declared = inspect.getattr_static(cls, "create_plugin")
    except AttributeError:
        return False
    return not isinstance(declared, staticmethod)


def _accepts_one_positional_argument(create_plugin: object) -> bool:
    """Report whether one positional argument can be bound to a callable.

    A bound method, a ``@classmethod`` or ``@staticmethod`` read off a class, and
    a ``__call__`` on an instance all present the signature the SDK calls, so all
    three bind. An instance method read off the class does not: its first
    parameter is ``self``, so one argument leaves the info unbound.
    """
    try:
        signature = inspect.signature(create_plugin)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return True
    try:
        signature.bind(_ARGUMENT_PROBE)
    except TypeError:
        return False
    return True


def _load_factory(
    plugin_name: str, entry_point: metadata.EntryPoint
) -> DurableInstrumentationPluginFactory:
    """Resolve an entry point to a plugin factory.

    Only two things can still be checked here. The entry point has to import,
    and what it resolves to has to have the factory shape. Nothing more is
    knowable without calling the factory, and calling it at load time is
    precisely what this design avoids: the instance belongs to an invocation,
    and there is no invocation yet. A factory that then misbehaves at invocation
    time is contained by :meth:`PluginExecutor._create_plugins`.
    """
    try:
        factory = entry_point.load()
    except Exception as error:
        raise PluginLoadError(
            f"Failed to load durable instrumentation plugin factory "
            f"'{plugin_name}' from '{entry_point.value}' "
            f"({_distribution_name(entry_point)}): {error}"
        ) from error

    if not _is_plugin_factory(factory):
        raise PluginLoadError(
            f"Durable instrumentation plugin entry point '{plugin_name}' must "
            "resolve to a plugin factory -- an object with a "
            "create_plugin(info) method returning a "
            "DurableInstrumentationPlugin -- but resolved to "
            f"{_describe_value(factory)}. Name the factory instance, not a "
            "plugin, not a plugin class, and not the factory class."
        )

    return cast(DurableInstrumentationPluginFactory, factory)


def _validate_explicit_factories(
    explicit_plugins: Sequence[DurableInstrumentationPluginFactory] | None,
) -> list[DurableInstrumentationPluginFactory]:
    """Check that every explicitly passed plugin entry has the factory shape.

    Each entry's ``create_plugin`` is called once per invocation to build that
    invocation's plugin instance. An entry without one can never be called, so
    :meth:`PluginExecutor._create_plugins` raises ``AttributeError`` on every
    invocation, logs it and continues without that plugin -- telemetry is lost
    for the lifetime of the function, and nothing fails. Raising here converts
    that into one configuration failure while the handler is being initialized.
    The position is named because a caller passing several entries cannot
    otherwise tell which one is wrong. The entry itself is named too, by
    :func:`_describe_value`, which names a class and a function directly rather
    than by type: the type of a class is ``builtins.type``, and that would
    identify no particular class.

    A plugin *class* is rejected, and so is any bare callable. Both were
    accepted while the registration type was ``Callable``: a lambda satisfied it
    directly, and a class satisfied it because calling a class constructs an
    instance. Neither carries a ``create_plugin`` the SDK can call, so
    ``plugins=[MyPlugin]`` and ``plugins=[lambda info: MyPlugin()]`` now fail
    here. A *factory* class passed instead of an instance of it fails here too:
    ``MyFactory.create_plugin`` is callable, but its first parameter is ``self``,
    so the per-invocation call binds the info to ``self``. The replacement is a
    small factory class, instantiated, which is also where setup work that can
    fail belongs. A class that declares ``create_plugin`` as a ``@classmethod``
    or a ``@staticmethod`` is accepted, because that member presents the
    signature the SDK calls.
    """
    factories = list(explicit_plugins or [])
    for index, factory in enumerate(factories):
        if not _is_plugin_factory(factory):
            raise PluginLoadError(
                f"Durable instrumentation plugin at plugins[{index}] must be a "
                "plugin factory -- an object with a create_plugin(info) method "
                "returning a DurableInstrumentationPlugin -- but is "
                f"{_describe_value(factory)}. Pass a factory rather than a "
                "plugin, a plugin class, or a plain callable, and pass a factory "
                "instance rather than the factory class, for example "
                "plugins=[MyPluginFactory(exporter)]."
            )
    return factories


def load_configured_plugins(
    explicit_plugins: Sequence[DurableInstrumentationPluginFactory] | None,
    *,
    environment: Mapping[str, str] | None = None,
) -> list[DurableInstrumentationPluginFactory]:
    """Combine explicit plugin factories with those selected through the environment.

    Explicit factories retain their order. Dynamically selected factories follow
    in configured order. Every returned factory has its ``create_plugin`` called
    once per invocation.

    A factory already registered explicitly is not registered a second time
    through the environment. The check is by factory identity, which is what is
    knowable here: the old shape declared a ``plugin_type`` and could dedup on
    it, but what a factory builds is unknown until ``create_plugin`` is called,
    and calling it at load time is what this design avoids. Identity still covers
    the case the plugin packages document -- the same factory object both passed
    to the decorator and named in ``DURABLE_EXECUTION_PLUGINS``. Two *different*
    factories that happen to build the same plugin type will now both be
    registered.
    """

    resolved_factories = _validate_explicit_factories(explicit_plugins)
    resolved_environment = os.environ if environment is None else environment
    plugin_names = _parse_configured_plugin_names(resolved_environment)
    if not plugin_names:
        return resolved_factories

    try:
        discovered_entry_points = list(
            metadata.entry_points(group=PLUGIN_ENTRY_POINT_GROUP)
        )
    except Exception as error:
        raise PluginLoadError(
            "Failed to inspect installed durable instrumentation plugin "
            f"providers in entry-point group '{PLUGIN_ENTRY_POINT_GROUP}': {error}"
        ) from error

    entry_points_by_name: dict[str, list[metadata.EntryPoint]] = {}
    for entry_point in discovered_entry_points:
        entry_points_by_name.setdefault(entry_point.name, []).append(entry_point)

    for plugin_name in plugin_names:
        matching_entry_points = entry_points_by_name.get(plugin_name, [])
        if not matching_entry_points:
            available_names = ", ".join(sorted(entry_points_by_name)) or "none"
            raise PluginLoadError(
                f"No durable instrumentation plugin provider named "
                f"'{plugin_name}' was found in entry-point group "
                f"'{PLUGIN_ENTRY_POINT_GROUP}'. Installed providers: "
                f"{available_names}. Ensure the provider package is installed "
                "in the function artifact or an attached Lambda layer."
            )

        if len(matching_entry_points) > 1:
            distributions = ", ".join(
                _distribution_name(entry_point) for entry_point in matching_entry_points
            )
            raise PluginLoadError(
                f"Multiple durable instrumentation plugin providers named "
                f"'{plugin_name}' were found in entry-point group "
                f"'{PLUGIN_ENTRY_POINT_GROUP}': {distributions}. Remove the "
                "duplicate provider package."
            )

        factory = _load_factory(plugin_name, matching_entry_points[0])
        if any(factory is registered for registered in resolved_factories):
            logger.warning(
                "Skipping dynamically configured plugin '%s' because the same "
                "plugin factory is already registered.",
                plugin_name,
            )
            continue

        resolved_factories.append(factory)

    return resolved_factories
