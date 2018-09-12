/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/program_options.hpp>
#include <chrono>
#include <dlfcn.h>
#include <folly/Format.h>
#include <memory>
#include <opentracing/noop.h>
#include <opentracing/tracer.h>
#include <string>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/StatsPublisher.h"
#include "logdevice/common/admin/AdminServer.h"
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
 * Interface for pluggable common components of LogDevice.  Subclasses may
 * override some or all methods. Most of the methods are invoked at various
 * points during initialization.  Default implementations typically produce null
 * pointers or no-op instances where appropriate.
 *
 * The server and client will keep the Plugin instance alive throughout their
 * lifetime, allowing PluginPack subclasses to be stateful.
 *
 * This is a base class that contains the list of accessible plugins by both
 * server and clients. If you want to define a server-specific or
 * client-specific plugin see server/ServerPluginPack.h or
 * lib/ClientPluginPack.h respectively.
 */
class PluginPack {
 public:
  virtual const char* description() const {
    return "default PluginPack";
  };

  /**
   * Invoked by the server before parsing its command line.  Allows the
   * plugin to define additional groups of options for the parser. Doesn't
   * store pointer to SettingsUpdater
   */
  virtual void addOptions(SettingsUpdater*) {}

  /**
   * Allows the plugin to register additional ConfigSource instances with the
   * TextConfigUpdater.  Invoked by the server before fetching its config.
   */
  virtual void
  registerConfigSources(TextConfigUpdater&,
                        std::chrono::milliseconds /* zk_polling_interval */) {}

  /**
   * If this returns non-null, the client/server will also create a
   * StatsPublishingThread, periodically collect them and push to the
   * StatsPublisher object.
   */
  virtual std::unique_ptr<StatsPublisher>
  createStatsPublisher(StatsPublisherScope,
                       UpdateableSettings<Settings>,
                       int /* num_db_shards */) {
    return nullptr;
  }

  /**
   * Creates a TraceLogger to which trace samples are pushed if tracing is on.
   * The default implementation creates a NoopTraceLogger.
   */
  virtual std::unique_ptr<TraceLogger>
  createTraceLogger(const std::shared_ptr<UpdateableConfig>&);

  virtual std::unique_ptr<PrincipalParser>
  createPrincipalParser(AuthenticationType type) {
    ld_check(type == AuthenticationType::NONE);
    return nullptr;
  }

  virtual std::shared_ptr<PermissionChecker>
  createPermissionChecker(PermissionCheckerType type,
                          const std::unordered_set<std::string>& /*domains*/) {
    ld_check(type == PermissionCheckerType::NONE);
    return nullptr;
  }

  virtual std::unique_ptr<SequencerLocator>
  createSequencerLocator(const std::shared_ptr<UpdateableConfig>&);

  virtual std::unique_ptr<BuildInfo> createBuildInfo() {
    return std::unique_ptr<BuildInfo>(new BuildInfo());
  }

  // Called by watchdog thread for each stalled worker thread, if
  // Settings::watchdog_print_bt_on_stall is true.  Called with the thread id of
  // the worker, as returned by gettid(2), which is different than
  // pthread_self().  Intended for printing the stack trace of the given thread.
  virtual void watchdogPrintBacktraceOnStall(int /*pid*/) {}

  // Called by watchdog thread once, if Settings::watchdog_print_bt_on_stall is
  // true.  Intended to call 'kernelctl walker' which outputs stack traces of
  // threads in UNINTERRUPTIBLE state.
  virtual void watchdogKernelStacktrace() {}

  virtual std::shared_ptr<opentracing::Tracer> createOTTracer() {
    return opentracing::MakeNoopTracer();
  }

  virtual ~PluginPack() {}
};

/**
 * A plugin pack loader, this takes the name (just for logging) and a
 * constructor symbol name that it will lookup on the symbol table and
 * dynamically load it if found. If the ctor_symbol was not found we load an
 * instance of the type T instead
 *
 * If `logstr_out' is null (default), the function logs the outcome
 * of plugin loading using the standard LogDevice logging framework.
 * If non-null, the debug info is saved into the string instead (
 * useful because this function can be called very early on, before
 * the logging framework is initialised.)
 */
template <class T>
std::unique_ptr<T> load_plugin_pack(const char* name,
                                    const char* ctor_symbol,
                                    std::string* logstr_out) {
  std::string logstr;
  void* ptr = dlsym(RTLD_DEFAULT, ctor_symbol);
  std::unique_ptr<T> rv;
  if (ptr != nullptr) {
    auto fnptr = reinterpret_cast<T* (*)()>(ptr);
    rv.reset(fnptr());
    logstr =
        folly::format("{} plugin loaded: {}", name, rv->description()).str();
  } else {
    rv.reset(new T());
    logstr = folly::format("No plugin found for {}", std::string(name)).str();
  }
  if (logstr_out != nullptr) {
    *logstr_out = std::move(logstr);
  } else {
    ld_info("%s", logstr.c_str());
  }
  return rv;
}

}} // namespace facebook::logdevice
