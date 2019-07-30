/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <fcntl.h>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <pwd.h>
#include <signal.h>
#include <unistd.h>

#include <folly/Optional.h>
#include <folly/Singleton.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>

#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/StatsCollectionThread.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/commandline_util.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/DynamicPluginLoader.h"
#include "logdevice/common/plugin/HotTextOptimizerPlugin.h"
#include "logdevice/common/plugin/Logger.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/StaticPluginLoader.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/fatalsignal.h"
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"
#include "logdevice/server/util.h"

using namespace facebook::logdevice;

namespace {

// Our executable is 1.3 GB, and the shared libraries we use are small, so this
// should be enough, at least as of March 2017.
constexpr size_t MEMLOCK_MEM = 2ULL * 1024 * 1024 * 1024;

} // anonymous namespace

// After initializing all threads, the main thread will wait on this semaphore
// for SIGINT, SIGHUP or for the admin thread to signal shutdown
Semaphore main_thread_sem;
std::atomic<bool> shutdown_requested{false};

// only used from main thread
bool rotate_logs_requested{false};

static void signal_shutdown() {
  shutdown_requested.store(true);
  main_thread_sem.post();
  ld_info("shutting down");
}

static void watchdog_stall_handler(int sig) {
  if (sig == SIGUSR1) {
    abort();
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

static void coredump_signal_handler(int sig) {
  handle_fatal_signal(sig);
}

static void rotate_logs_handler(int sig) {
  ld_check(sig == SIGHUP);
  rotate_logs_requested = true;
  main_thread_sem.post();
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

static rlim_t set_r_limit(int resource, folly::Optional<rlim_t> desired) {
  struct rlimit rlim;
  int rv = getrlimit(resource, &rlim);
  ld_check(rv == 0);
  if (rv != 0) {
    ld_error("getrlimit() failed, errno=%d (%s)", errno, strerror(errno));
    exit(1);
  }

  if (desired) {
    rlim.rlim_cur = desired.value();
    rlim.rlim_max = std::max(rlim.rlim_cur, rlim.rlim_max);
    rv = setrlimit(resource, &rlim);
    if (rv != 0) {
      ld_error("setrlimit() failed, errno=%d (%s)", errno, strerror(errno));
      exit(1);
    }
  }

  return rlim.rlim_cur;
}

static void set_fd_limit(UpdateableSettings<ServerSettings> server_settings) {
  rlim_t num_fds;
  if (server_settings->fd_limit > 0) {
    num_fds = set_r_limit(RLIMIT_NOFILE, server_settings->fd_limit);
  } else {
    num_fds = set_r_limit(RLIMIT_NOFILE, folly::none);
  }
  ld_info("Limit on number of file descriptors is %ld", long(num_fds));

  if (server_settings->eagerly_allocate_fdtable) {
    // Eagerly allocate the kernel's fdtable by using a high fd now.
    int fd = num_fds - 1;
    if (dup2(2, fd) >= 0) {
      // success
      close(fd);
    } else {
      ld_error("Unable to expand kernel fdtable with dup2, "
               "continuing: errno=%d (%s)",
               errno,
               strerror(errno));
    }
  }
}

static void
possibly_lock_mem(UpdateableSettings<ServerSettings> server_settings) {
  if (server_settings->lock_memory) {
    // The default amount of lockable memory is 64 kB, and our executable is
    // currently 1.3 GB...
    rlim_t cur = set_r_limit(RLIMIT_MEMLOCK, MEMLOCK_MEM);
    ld_info("Limit on amount of memory we can lock is %ld bytes", long(cur));

    // This will lock the executable & all shared libraries. Any files memory
    // mapped after this point could be dropped.  If swap is enabled, any memory
    // allocated after this point, including the stacks of threads created after
    // this point, could be swapped out.
    int rc = mlockall(MCL_CURRENT);
    if (rc != 0) {
      ld_error("mlockall() failed, errno=%d (%s)", errno, strerror(errno));
      exit(1);
    }
    ld_info("Current virtual memory locked into RAM.");
  }
}

/**
 * If running as root, attempt to switch to a different user specified on the
 * command line.
 */
static void drop_root(UpdateableSettings<ServerSettings> server_settings) {
  if (getuid() == 0 || geteuid() == 0) {
    if (server_settings->user.empty()) {
      ld_warning("Running under root, but no --user command line option "
                 "specified. Will keep running as root");
      return;
    }

    struct passwd* pw = getpwnam(server_settings->user.c_str());
    if (pw == nullptr) {
      ld_error("Cannot find user \"%s\" to switch to",
               server_settings->user.c_str());
      exit(1);
    }

    if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
      ld_error("Failed to assume identity of user %s",
               server_settings->user.c_str());
      exit(1);
    }
#ifdef __linux__
    /* re-enable coredumps after setuid() */
    prctl(PR_SET_DUMPABLE, 1);
#endif
  } else if (!server_settings->user.empty()) {
    ld_error("Specified --user on the command line but did not start as root");
    exit(1);
  }
}

static void set_log_file(UpdateableSettings<ServerSettings> server_settings) {
  static std::string prev;
  if (prev == server_settings->log_file) {
    // This setting did not change.
    return;
  }

  ld_info("Logging to %s",
          server_settings->log_file.empty()
              ? "stderr"
              : server_settings->log_file.c_str());

  if (!server_settings->log_file.empty()) {
    int log_file_fd = open(
        server_settings->log_file.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0666);
    if (log_file_fd >= 0) {
      dbg::useFD(log_file_fd);
    } else {
      ld_error("Failed to open error log file %s. Will keep logging to %s",
               server_settings->log_file.c_str(),
               prev.empty() ? "stderr" : prev.c_str());
    }
  } else {
    dbg::useFD(STDERR_FILENO);
  }

  dbg::enableNonblockingPipe();

  prev = server_settings->log_file;
}

static std::string build_info_string(BuildInfo& build_info) {
  std::string rv;
  for (const auto& pair : build_info.fullMap()) {
    if (!rv.empty()) {
      rv += '\n';
    }
    rv += pair.first + ": " + pair.second;
  }
  return rv;
}

static void
on_server_settings_changed(UpdateableSettings<ServerSettings> server_settings) {
  dbg::assertOnData = server_settings->assert_on_data;
  dbg::currentLevel = server_settings->loglevel;
  dbg::externalLoggerLogLevel = server_settings->external_loglevel;
  ZookeeperClient::setDebugLevel(server_settings->loglevel);
  dbg::setLogLevelOverrides(server_settings->loglevel_overrides);

  // If `unmap_caches' is true, install our coredump handler.  If false
  // restore the default handler.
  void (*handler)(int) =
      server_settings->unmap_caches ? coredump_signal_handler : SIG_DFL;
  for (int signum : {SIGSEGV, SIGABRT, SIGBUS, SIGQUIT, SIGILL, SIGFPE}) {
    setup_signal_handler(signum, handler);
  }

  set_log_file(server_settings);
}

int main(int argc, const char** argv) {
  logdeviceInit();

  ThreadID::set(ThreadID::Type::UTILITY, "logdeviced-main");

  std::shared_ptr<PluginRegistry> plugin_registry =
      std::make_shared<PluginRegistry>(
          createPluginVector<DynamicPluginLoader,
                             StaticPluginLoader,
                             BuiltinPluginProvider>());
  auto ht_plugin = plugin_registry->getSinglePlugin<HotTextOptimizerPlugin>(
      PluginType::HOT_TEXT_OPTIMIZER);
  if (ht_plugin) {
    (*ht_plugin)();
  }

  {
    std::shared_ptr<Logger> logger_plugin =
        plugin_registry->getSinglePlugin<Logger>(PluginType::LOGGER);
    if (logger_plugin) {
      dbg::external_logger_plugin.swap(logger_plugin);
    }
  }

  UpdateableSettings<ServerSettings> server_settings;
  UpdateableSettings<RebuildingSettings> rebuilding_settings;
  UpdateableSettings<LocalLogStoreSettings> locallogstore_settings;
  UpdateableSettings<GossipSettings> gossip_settings;
  UpdateableSettings<Settings> settings;
  UpdateableSettings<RocksDBSettings> rocksdb_settings;
  UpdateableSettings<AdminServerSettings> admin_server_settings;

  auto settings_updater = std::make_shared<SettingsUpdater>();
  settings_updater->registerSettings(server_settings);
  settings_updater->registerSettings(rebuilding_settings);
  settings_updater->registerSettings(locallogstore_settings);
  settings_updater->registerSettings(gossip_settings);
  settings_updater->registerSettings(settings);
  settings_updater->registerSettings(rocksdb_settings);
  settings_updater->registerSettings(admin_server_settings);

  plugin_registry->addOptions(settings_updater.get());

  settings_updater->setInternalSetting("server", "true");

  auto fallback_parser = [&](int argc2, const char** argv2) {
    boost::program_options::options_description opts;
    opts.add_options()("help,h", "produce this help message and exit");
    opts.add_options()("verbose,v",
                       "by default --help only prints the most important "
                       "settings, use this option to print all settings");
    opts.add_options()("version,V", "output version information and exit");
    opts.add_options()(
        "markdown-settings",
        "output settings documentation in Markdown format and exit");
    opts.add_options()(
        "include-deprecated", "include deprecated settings in Markdown output");

    boost::program_options::variables_map parsed =
        program_options_parse_no_positional(argc2, argv2, opts);

    if (parsed.count("help")) {
      folly::Optional<std::string> bundle;
      if (!parsed.count("verbose")) {
        bundle = server_settings->getName();
      }
      std::cout << "LogDevice server." << std::endl << std::endl;
      std::cout << "Use --verbose/-v to see all settings." << std::endl
                << std::endl;
      std::cout << settings_updater->help(SettingFlag::SERVER, bundle)
                << std::endl;
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

    if (parsed.count("markdown-settings")) {
      std::cout << settings_updater->markdownDoc(
          parsed.count("include-deprecated"));
      exit(0);
    }
  };

  try {
    settings_updater->parseFromCLI(
        argc, argv, &SettingsUpdater::mustBeServerOption, fallback_parser);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }

  // With initial settings parsed, subscribe to changes to allow runtime
  // changes of particular settings that use global variables etc
  auto server_settings_subscription = server_settings.callAndSubscribeToUpdates(
      std::bind(on_server_settings_changed, server_settings));

  // Now that the logging framework is initialised, log plugin info
  ld_info(
      "Plugins loaded: %s", plugin_registry->getStateDescriptionStr().c_str());

  ld_info("server starting");
  {
    auto build_info =
        plugin_registry->getSinglePlugin<BuildInfo>(PluginType::BUILD_INFO);
    ld_check(build_info);
    ld_info("version %s", build_info->version().c_str());
    std::string str = build_info_string(*build_info);
    if (!str.empty()) {
      ld_info("Build Info\n%s", str.c_str());
    }
  }

  if (!folly::kIsDebug) {
    ld_info("asserts off (NDEBUG set)");
  } else {
    ld_info("asserts on (NDEBUG not set)");
  }

  std::unique_ptr<ServerParameters> params;
  try {
    params = std::make_unique<ServerParameters>(settings_updater,
                                                server_settings,
                                                rebuilding_settings,
                                                locallogstore_settings,
                                                gossip_settings,
                                                settings,
                                                rocksdb_settings,
                                                admin_server_settings,
                                                plugin_registry,
                                                signal_shutdown);
  } catch (const ConstructorFailed&) {
    return 1;
  }

  set_fd_limit(server_settings);
  possibly_lock_mem(server_settings);
  drop_root(server_settings);

  // Run the StatsCollectionThread
  std::unique_ptr<StatsCollectionThread> stats_thread =
      StatsCollectionThread::maybeCreate(
          settings,
          params.get()->getUpdateableConfig()->get()->serverConfig(),
          plugin_registry,
          params->getNumDBShards(),
          params.get()->getStats());

  Server server(params.get());

  // We get here only if all subsystems initialized successfully.
  // Otherwsie Server constructor calls _exit(1).

  if (!server.startListening()) {
    ld_error("Failed to start listening");
    return 1;
  }

  ld_info("Listeners initialized");

  setup_signal_handler(SIGINT, shutdown_signal_handler);
  setup_signal_handler(SIGTERM, shutdown_signal_handler);
  setup_signal_handler(SIGUSR1, watchdog_stall_handler);
  setup_signal_handler(SIGHUP, rotate_logs_handler);
  for (;;) {
    main_thread_sem.wait();
    if (shutdown_requested.load()) {
      break;
    }
    if (rotate_logs_requested) {
      server.rotateLocalLogs();
      rotate_logs_requested = false;
      continue;
    }
    ld_check(false);
  }
  setup_signal_handler(SIGINT, SIG_DFL);
  setup_signal_handler(SIGHUP, SIG_DFL);

  ld_info("shutting down");

  return 0;
}
