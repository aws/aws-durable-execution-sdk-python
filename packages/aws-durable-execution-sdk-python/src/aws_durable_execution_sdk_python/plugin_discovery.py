from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from importlib import metadata

from aws_durable_execution_sdk_python.__about__ import __version__
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.plugin import (
    DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginProvider,
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


def _qualified_class_name(value_type: type[object]) -> str:
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _load_provider(
    plugin_name: str, entry_point: metadata.EntryPoint
) -> DurableInstrumentationPluginProvider:
    try:
        provider = entry_point.load()
    except Exception as error:
        raise PluginLoadError(
            f"Failed to load durable instrumentation plugin provider "
            f"'{plugin_name}' from '{entry_point.value}' "
            f"({_distribution_name(entry_point)}): {error}"
        ) from error

    if not isinstance(provider, DurableInstrumentationPluginProvider):
        raise PluginLoadError(
            f"Durable instrumentation plugin entry point '{plugin_name}' must "
            "resolve to DurableInstrumentationPluginProvider, but resolved to "
            f"{_qualified_type_name(provider)}."
        )

    if provider.plugin_api_version != DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION:
        raise PluginLoadError(
            f"Durable instrumentation plugin provider '{plugin_name}' declares "
            f"plugin API version {provider.plugin_api_version}, but "
            f"aws-durable-execution-sdk-python {__version__} supports plugin API "
            f"version {DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION}. Install "
            "compatible SDK and plugin package versions."
        )

    declared_plugin_type: object = provider.plugin_type
    if not isinstance(declared_plugin_type, type) or not issubclass(
        declared_plugin_type, DurableInstrumentationPlugin
    ):
        declared_type_name = (
            _qualified_class_name(declared_plugin_type)
            if isinstance(declared_plugin_type, type)
            else _qualified_type_name(declared_plugin_type)
        )
        raise PluginLoadError(
            f"Durable instrumentation plugin provider '{plugin_name}' declares "
            f"invalid plugin type {declared_type_name}; "
            "expected a DurableInstrumentationPlugin subclass."
        )

    return provider


def _create_plugin(
    plugin_name: str,
    entry_point: metadata.EntryPoint,
    provider: DurableInstrumentationPluginProvider,
) -> DurableInstrumentationPlugin:
    try:
        plugin = provider.factory()
    except Exception as error:
        raise PluginLoadError(
            f"Failed to create durable instrumentation plugin '{plugin_name}' "
            f"from '{entry_point.value}' ({_distribution_name(entry_point)}): "
            f"{error}"
        ) from error

    if type(plugin) is not provider.plugin_type:
        raise PluginLoadError(
            f"Durable instrumentation plugin provider '{plugin_name}' returned "
            f"{_qualified_type_name(plugin)}; expected "
            f"{_qualified_class_name(provider.plugin_type)}."
        )

    return plugin


def load_configured_plugins(
    explicit_plugins: Sequence[DurableInstrumentationPlugin] | None,
    *,
    environment: Mapping[str, str] | None = None,
) -> list[DurableInstrumentationPlugin]:
    """Combine explicit plugins with providers selected through the environment.

    Explicit plugins retain their order. Dynamically selected plugins follow in
    configured order. When discovery creates a plugin whose concrete type is
    already registered, the first registration wins, so explicit registration
    takes precedence.
    """

    resolved_plugins = list(explicit_plugins or [])
    resolved_environment = os.environ if environment is None else environment
    plugin_names = _parse_configured_plugin_names(resolved_environment)
    if not plugin_names:
        return resolved_plugins

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

    registered_types: dict[type[DurableInstrumentationPlugin], str] = {
        type(plugin): "the decorator's plugins argument" for plugin in resolved_plugins
    }

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

        entry_point = matching_entry_points[0]
        provider = _load_provider(plugin_name, entry_point)
        if existing_registration := registered_types.get(provider.plugin_type):
            logger.warning(
                "Skipping dynamically configured plugin '%s' because %s is "
                "already registered by %s.",
                plugin_name,
                _qualified_class_name(provider.plugin_type),
                existing_registration,
            )
            continue

        plugin = _create_plugin(plugin_name, entry_point, provider)
        resolved_plugins.append(plugin)
        registered_types[provider.plugin_type] = f"dynamic provider '{plugin_name}'"

    return resolved_plugins
