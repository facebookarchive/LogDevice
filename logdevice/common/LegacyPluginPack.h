/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <boost/program_options.hpp>
#include <folly/Format.h>
#include <opentracing/noop.h>
#include <opentracing/tracer.h>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/StatsPublisher.h"
#include "logdevice/common/admin/AdminServer.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

class Processor;
class SequencerLocator;
class ServerConfig;
struct Settings;
struct ServerSettings;
class TextConfigUpdater;
class TraceLogger;
class UpdateableConfig;

/**
 * NOTE: this interface is now deprecated. If you want to create a new type of
 * plugin, look into common/plugin/
 *
 * Interface for pluggable common components of LogDevice.  Subclasses may
 * override some or all methods. Most of the methods are invoked at various
 * points during initialization.  Default implementations typically produce null
 * pointers or no-op instances where appropriate.
 *
 * The server and client will keep the Plugin instance alive throughout their
 * lifetime, allowing LegacyPluginPack subclasses to be stateful.
 *
 * This is a base class that contains the list of accessible plugins by both
 * server and clients. If you want to define a server-specific or
 * client-specific plugin see server/ServerPluginPack.h or
 * lib/ClientPluginPack.h respectively.
 */
class LegacyPluginPack {
 public:
  virtual const char* description() const {
    return "default LegacyPluginPack";
  };

  virtual ~LegacyPluginPack() {}
};

}} // namespace facebook::logdevice
