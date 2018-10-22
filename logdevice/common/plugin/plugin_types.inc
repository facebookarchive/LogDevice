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

// TODO: add more plugin types

// Plugin type used for legacy client plugin bundle
PLUGIN_TYPE(LEGACY_CLIENT_PLUGIN, 100, "Legacy client plugin", false)

// Plugin type used for legacy server plugin bundle
PLUGIN_TYPE(LEGACY_SERVER_PLUGIN, 101, "Legacy server plugin", false)

#undef PLUGIN_TYPE
