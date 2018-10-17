/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/LegacyPluginPack.h"

/**
 * @file Server Plugin pack for LogDevice
 *
 * To enable LogDevice plugin capabilities, subclass ServerPluginPack,
 * define a function named "logdevice_server_plugin" and link it with the
 * LogDevice library or server.
 *
 * extern "C" __attribute__((__used__)) facebook::logdevice::ServerPluginPack*
 * logdevice_server_plugin() { facebook::logdevice::ServerPluginPack* plugin =
 * new PluginImpl;
 *   ...
 *   return plugin;
 * }
 *
 * LogDevice will use dynamic symbol lookup via dlsym() to find the plugin.
 *
 * Note that the plugin interface is currently *not* designed for binary
 * compatibility.  It is not safe to independently build and ship a plugin and
 * a client (or server).
 */
namespace facebook { namespace logdevice {

class Server;

class ServerPluginPack : public virtual LegacyPluginPack,
                         public virtual Plugin {
 public:
  Type type() const override {
    return Type::LEGACY_SERVER_PLUGIN;
  }

  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  std::string displayName() const override {
    return description();
  }

  virtual const char* description() const override {
    return "built-in server plugin";
  }
  /**
   * Creates a new AdminServer instance that will be managed by this
   * server. If there is no implementation available for this
   * interface (e.g., returned nullptr), we
   * will not start the admin server.
   */
  virtual std::unique_ptr<AdminServer> createAdminServer(Server* server);

  /**
   * Places hot text on large pages to improve performance.
   */
  virtual void optimizeHotText() {}
};

}} // namespace facebook::logdevice
