/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef PLUGIN_TYPE
#error PLUGIN_TYPE() macro not defined
#define PLUGIN_TYPE(...)
#endif

// Args are (type_name, id, display_name, allow_multiple_active)
// `id` should never change for a given type after being defined here.
// `allow_multiple_active` specifies whether this type of plugin supports
// having multiple active plugins (e.g. there could be several plugin providers
// at the same time) or there could only be one active plugin (e.g. only one
// implementation of PrincipalParser can be active).

// Provides other plugins. Use this if your plugin implements more than one
// type of plugin or for plugin loaders. See PluginProvider.h
PLUGIN_TYPE(PLUGIN_PROVIDER, 1, "Plugin provider", true)
// See StatsPublisherFactory.h
PLUGIN_TYPE(STATS_PUBLISHER_FACTORY,
            2,
            "Stats publisher factory",
            false)
// See BuildInfo.h
PLUGIN_TYPE(BUILD_INFO, 3, "Build Info", false)
// See LocationProvider.h
PLUGIN_TYPE(LOCATION_PROVIDER, 4, "Location provider", false)
// See TraceLoggerFactory.h
PLUGIN_TYPE(TRACE_LOGGER_FACTORY, 5, "TraceLogger factory", false)
// See PrincipalParserFactory.h
PLUGIN_TYPE(PRINCIPAL_PARSER_FACTORY, 6, "PrincipalParser factory", false)
// See PermissionCheckerFactory.h
PLUGIN_TYPE(PERMISSION_CHECKER_FACTORY, 7, "PermissionChecker factory", false)
// See ConfigSourceFactory.h
PLUGIN_TYPE(CONFIG_SOURCE_FACTORY, 8, "ConfigSource factory", true)
// See BacktraceRunner.h
PLUGIN_TYPE(BACKTRACE_RUNNER, 9, "Backtrace runner", false)
// See SequencerLocatorFactory.h
PLUGIN_TYPE(SEQUENCER_LOCATOR_FACTORY, 10, "SequencerLocator factory", false)
// See OpenTracerFactory.h
PLUGIN_TYPE(OPEN_TRACER_FACTORY, 11, "Open Tracing tracer factory", false)
// See HotTextOptimizerPlugin.h
PLUGIN_TYPE(HOT_TEXT_OPTIMIZER, 12, "HotTextOptimizer", false)
// See AdminServerFactory.h
PLUGIN_TYPE(ADMIN_SERVER_FACTORY, 13, "AdminServer factory", false)
// See ZookeeperClientPlugin.h
PLUGIN_TYPE(ZOOKEEPER_CLIENT_FACTORY, 14, "ZookeeperClient factory", false)
// See Logger.h
PLUGIN_TYPE(LOGGER, 15, "Logger plugin", false)
// See DefaultSettingsProvider.h
PLUGIN_TYPE(DEFAULT_SETTINGS_PROVIDER, 16, "Default settings provider", false)

#undef PLUGIN_TYPE
