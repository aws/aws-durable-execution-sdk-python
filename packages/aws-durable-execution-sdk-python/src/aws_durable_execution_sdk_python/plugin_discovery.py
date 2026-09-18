from __future__ import annotations

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


def _qualified_type_name(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _load_factory(
    plugin_name: str, entry_point: metadata.EntryPoint
) -> DurableInstrumentationPluginFactory:
    """Resolve an entry point to a plugin factory.

    Only two things can still be checked here. The entry point has to import,
    and what it resolves to has to be callable. Nothing more is knowable without
    calling the factory, and calling it at load time is precisely what this
    design avoids: the instance belongs to an invocation, and there is no
    invocation yet. A factory that then misbehaves at invocation time is
    contained by :meth:`PluginExecutor._create_plugins`.
    """
    try:
        factory = entry_point.load()
    except Exception as error:
        raise PluginLoadError(
            f"Failed to load durable instrumentation plugin factory "
            f"'{plugin_name}' from '{entry_point.value}' "
            f"({_distribution_name(entry_point)}): {error}"
        ) from error

    if not callable(factory):
        raise PluginLoadError(
            f"Durable instrumentation plugin entry point '{plugin_name}' must "
            "resolve to a callable plugin factory, but resolved to "
            f"{_qualified_type_name(factory)}."
        )

    return cast(DurableInstrumentationPluginFactory, factory)


def _validate_explicit_factories(
    explicit_plugins: Sequence[DurableInstrumentationPluginFactory] | None,
) -> list[DurableInstrumentationPluginFactory]:
    """Check that every explicitly passed plugin entry is callable.

    Each entry is called once per invocation to build that invocation's plugin
    instance. An entry that is not callable can never be called, so
    :meth:`PluginExecutor._create_plugins` raises ``TypeError`` on every
    invocation, logs it and continues without that plugin -- telemetry is lost
    for the lifetime of the function, and nothing fails. Raising here converts
    that into one configuration failure while the handler is being initialized.
    The position is named because a caller passing several entries cannot
    otherwise tell which one is wrong.

    A plugin *class* is callable and stays valid: calling it constructs an
    instance, so ``plugins=[MyPlugin]`` is accepted whenever ``MyPlugin`` accepts
    the info argument. It is permitted rather than recommended, because a
    constructor should only assign fields and a class used directly as a factory
    invites setup work into ``__init__``. ``plugins=[lambda info: MyPlugin(...)]``
    or a ``@classmethod`` factory keeps that work out of the constructor. Only a
    plugin *instance*, or any other non-callable value, is rejected.
    """
    factories = list(explicit_plugins or [])
    for index, factory in enumerate(factories):
        if not callable(factory):
            raise PluginLoadError(
                f"Durable instrumentation plugin at plugins[{index}] must be a "
                "callable plugin factory taking an InvocationStartInfo, but is "
                f"{_qualified_type_name(factory)}. Pass a factory rather than a "
                "plugin instance, for example "
                "plugins=[lambda info: MyPlugin(...)]."
            )
    return factories


def load_configured_plugins(
    explicit_plugins: Sequence[DurableInstrumentationPluginFactory] | None,
    *,
    environment: Mapping[str, str] | None = None,
) -> list[DurableInstrumentationPluginFactory]:
    """Combine explicit plugin factories with those selected through the environment.

    Explicit factories retain their order. Dynamically selected factories follow
    in configured order. Every returned factory is called once per invocation.

    A factory already registered explicitly is not registered a second time
    through the environment. The check is by factory identity, which is what is
    knowable here: the old shape declared a ``plugin_type`` and could dedup on
    it, but a factory is opaque until called, and calling it at load time is what
    this design avoids. Identity still covers the case the plugin packages
    document -- the same provider callable both passed to the decorator and named
    in ``DURABLE_EXECUTION_PLUGINS``. Two *different* factories that happen to
    build the same plugin type will now both be registered.
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
