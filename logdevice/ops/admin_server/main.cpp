/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <iostream>
#include <signal.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <folly/Singleton.h>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/commandline_util.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/DynamicPluginLoader.h"
#include "logdevice/common/plugin/Logger.h"
#include "logdevice/common/plugin/StaticPluginLoader.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/ops/admin_server/StandaloneAdminServer.h"
#include "logdevice/server/fatalsignal.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::admin;

std::unique_ptr<StandaloneAdminServer> admin_server;

static void signal_shutdown() {
  if (admin_server) {
    admin_server->shutdown();
  }
}

static void shutdown_signal_handler(int sig) {
  const char* name;
  switch (sig) {
    case SIGINT:
      name = "SIGINT";
      break;
    case SIGTERM:
      name = "SIGTERM";
      break;
    default:
      name = "unexpected signal";
      ld_check(false);
  }
  ld_info("caught %s, shutting down", name);
  signal_shutdown();
}

static void watchdog_stall_handler(int sig) {
  if (sig == SIGUSR1) {
    abort();
  }
}

static void setup_signal_handler(int signum, void (*handler)(int)) {
  struct sigaction sa;
  sa.sa_handler = handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  int rv;
  rv = sigaction(signum, &sa, nullptr);
  ld_check(rv == 0);
}

static boost::program_options::options_description
get_options(const char* program_name) {
  using boost::program_options::bool_switch;
  using boost::program_options::value;

  boost::program_options::options_description all_options(
      folly::format("Usage: {} [options]\n\nOptions", program_name).str());

  // clang-format off
  all_options.add_options()
      ("help,h",
       "produce this help message and exit")

      ("version,V",
       "output version information and exit");
  // clang-format on

  return all_options;
}

int main(int argc, const char* argv[]) {
  logdeviceInit();
  dbg::currentLevel = dbg::Level::INFO;
  const char* program_name = argv[0];

  ThreadID::set(ThreadID::Type::UTILITY, "logdevice-admin");

  std::shared_ptr<PluginRegistry> plugin_registry =
      std::make_shared<PluginRegistry>(
          createPluginVector<DynamicPluginLoader,
                             StaticPluginLoader,
                             BuiltinPluginProvider>());

  {
    std::shared_ptr<Logger> logger_plugin =
        plugin_registry->getSinglePlugin<Logger>(PluginType::LOGGER);
    if (logger_plugin) {
      dbg::external_logger_plugin.swap(logger_plugin);
    }
  }

  auto settings_updater = std::make_shared<SettingsUpdater>();
  plugin_registry->addOptions(settings_updater.get());

  try {
    auto fallback_fn = [&](int ac, const char* av[]) {
      auto opts = get_options(program_name);

      boost::program_options::variables_map parsed =
          program_options_parse_no_positional(ac, av, opts);

      // Check for --help before calling notify(), so that required options
      // aren't required.
      if (parsed.count("help")) {
        std::cout << "Standalone LogDevice Admin API Server.\n\n" << opts;
        std::cout << std::endl;
        std::cout << "LogDevice Admin Server settings:" << std::endl
                  << std::endl;
        std::cout << settings_updater->help(SettingFlag::SERVER);
        exit(0);
      }

      if (parsed.count("version")) {
        auto build_info =
            plugin_registry->getSinglePlugin<BuildInfo>(PluginType::BUILD_INFO);
        ld_check(build_info);
        std::cout << "version " << build_info->version() << '\n';
        std::string package = build_info->packageNameWithVersion();
        if (!package.empty()) {
          std::cout << "package " << package << '\n';
        }
        exit(0);
      }

      // Surface any errors
      boost::program_options::notify(parsed);
    };

    admin_server = std::make_unique<StandaloneAdminServer>(
        plugin_registry, settings_updater);
    ld_check(admin_server);
    admin_server->getSettingsUpdater()->parseFromCLI(
        argc, argv, &SettingsUpdater::eitherClientOrServerOption, fallback_fn);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }

  try {
    admin_server->start();
    setup_signal_handler(SIGINT, shutdown_signal_handler);
    setup_signal_handler(SIGTERM, shutdown_signal_handler);
    setup_signal_handler(SIGUSR1, watchdog_stall_handler);
    admin_server->waitForShutdown();
    setup_signal_handler(SIGINT, SIG_DFL);
    setup_signal_handler(SIGHUP, SIG_DFL);
  } catch (StandaloneAdminServerFailed& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(2);
  }

  return 0;
}
