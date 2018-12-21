/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/ops/admin_server/StandaloneAdminServer.h"

#include <iostream>

#include <folly/futures/Future.h>

#include "logdevice/common/ConfigInit.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/plugin/AdminServerFactory.h"
#include "logdevice/common/plugin/LocationProvider.h"
#include "logdevice/common/plugin/TraceLoggerFactory.h"
#include "logdevice/common/request_util.h"

namespace facebook { namespace logdevice { namespace admin {
StandaloneAdminServer::StandaloneAdminServer(
    std::shared_ptr<PluginRegistry> plugins,
    std::shared_ptr<SettingsUpdater> settings_updater)
    : plugin_registry_(std::move(plugins)),
      settings_updater_(std::move(settings_updater)) {
  settings_updater_->registerSettings(admin_settings_);
  settings_updater_->registerSettings(server_settings_);
  settings_updater_->registerSettings(rebuilding_settings_);
  settings_updater_->registerSettings(locallogstore_settings_);
  settings_updater_->registerSettings(gossip_settings_);
  settings_updater_->registerSettings(settings_);
  settings_updater_->registerSettings(rocksdb_settings_);

  plugin_registry_->addOptions(settings_updater_.get());
  server_settings_subscription_ = server_settings_.subscribeToUpdates(
      std::bind(&StandaloneAdminServer::onSettingsUpdate, this));
}

void StandaloneAdminServer::start() {
  // ASCII ART
  std::cout <<
      R"(
   __                ___           _
  / /  ___   __ _   /   \_____   _(_) ___ ___
 / /  / _ \ / _` | / /\ / _ \ \ / / |/ __/ _ \
/ /__| (_) | (_| |/ /_//  __/\ V /| | (_|  __/
\____/\___/ \__, /___,' \___| \_/ |_|\___\___|   Admin Server!
            |___/

)" << std::endl;
  ld_info("Starting Standalone Admin Server");

#ifdef NDEBUG
  ld_info("asserts off (NDEBUG set)");
#else
  ld_info("asserts on (NDEBUG not set)");
#endif

  ld_info("Config path: %s", server_settings_->config_path.c_str());
  std::string socket_addr;
  if (!admin_settings_->admin_unix_socket.empty()) {
    socket_addr = admin_settings_->admin_unix_socket;
  } else {
    socket_addr = std::to_string(admin_settings_->admin_port);
  }

  ld_info("Listening on: %s", socket_addr.c_str());
  ld_info(
      "Plugins loaded: %s", plugin_registry_->getStateDescriptionStr().c_str());

  std::shared_ptr<LocationProvider> location_plugin =
      plugin_registry_->getSinglePlugin<LocationProvider>(
          PluginType::LOCATION_PROVIDER);
  std::string plugin_location =
      location_plugin ? location_plugin->getMyLocation() : "";
  auto location = settings_->client_location;
  if (!location.hasValue() && !plugin_location.empty()) {
    // if my-location was not specified, set the value to what the plugin
    // provides.
    folly::Optional<NodeLocation> res;
    res.assign(NodeLocation());
    if (res->fromDomainString(plugin_location) != 0) {
      // TODO
      /*
       *throw boost::program_options::error(
       *    "Invalid value for --my-location. Expecting valid location "
       *    "string: \"{region}.{dc}.{cluster}.{row}.{rack}\"");
       */
    }
    // settings_->client_location = res;
  }
  // Loading the config
  updateable_config_ = std::make_shared<UpdateableConfig>();
  std::unique_ptr<LogsConfig> logs_cfg;

  server_config_subscription_ =
      updateable_config_->updateableServerConfig()->addHook(std::bind(
          &StandaloneAdminServer::onConfigUpdate, this, std::placeholders::_1));

  ConfigParserOptions options;
  options.alternative_layout_property = settings_->alternative_layout_property;

  ConfigInit config_init(settings_->initial_config_load_timeout);

  int rv = config_init.attach(server_settings_->config_path,
                              plugin_registry_,
                              updateable_config_,
                              nullptr /* RemoteLogsConfig*/,
                              settings_);
  if (rv != 0) {
    ld_critical("Could not load the config file.");
    throw StandaloneAdminServerFailed();
  }
  initStatsCollection();
  initProcessor();
  initClusterStateRefresher();
  initEventLog();
  initAdminServer();
}

void StandaloneAdminServer::initProcessor() {
  std::shared_ptr<TraceLogger> trace_logger;
  std::shared_ptr<TraceLoggerFactory> trace_logger_factory =
      plugin_registry_->getSinglePlugin<TraceLoggerFactory>(
          PluginType::TRACE_LOGGER_FACTORY);
  if (!trace_logger_factory || settings_->trace_logger_disabled) {
    trace_logger = std::make_shared<NoopTraceLogger>(updateable_config_);
  } else {
    trace_logger = (*trace_logger_factory)(updateable_config_);
  }

  processor_ = ClientProcessor::create(updateable_config_,
                                       std::move(trace_logger),
                                       settings_,
                                       stats_.get(),
                                       plugin_registry_,
                                       /* credentials= */ "",
                                       "admin-server");

  if (!LogsConfigManager::createAndAttach(
          *processor_, false /* is_writable */)) {
    err = E::INVALID_CONFIG;
    ld_critical("Internal LogsConfig Manager could not be started in Client. "
                "LogsConfig will not be available!");
    throw StandaloneAdminServerFailed();
  }
}

void StandaloneAdminServer::initAdminServer() {
  // Figure out the socket address for the admin server.
  auto server_config = updateable_config_->getServerConfig();
  ld_check(server_config);

  auto adm_plugin = plugin_registry_->getSinglePlugin<AdminServerFactory>(
      PluginType::ADMIN_SERVER_FACTORY);
  if (adm_plugin) {
    admin_server_ = (*adm_plugin)(processor_.get(),
                                  settings_updater_,
                                  server_settings_,
                                  admin_settings_,
                                  stats_.get());
    ld_check(admin_server_);
    admin_server_->start();
  } else {
    ld_critical(
        "Not initializing Admin API, since there are no implementations "
        "available.");
    throw StandaloneAdminServerFailed();
  }
}

void StandaloneAdminServer::initClusterStateRefresher() {
  if (processor_ && processor_->cluster_state_) {
    ld_info("Triggering an initial refresh of ClusterState");
    processor_->cluster_state_->refreshClusterStateAsync();
  }
  // TODO: Create a polling timer to refresh cluster state, or move the
  // (currently-broken) per-worker one to processor
  // processor_->activateClusterStatePolling();
}

void StandaloneAdminServer::initStatsCollection() {
  if (settings_->stats_collection_interval.count() > 0) {
    auto params = StatsParams().setIsServer(false);
    // avoid instantianting thread-local Stats unnecessarily
    stats_ = std::make_unique<StatsHolder>(std::move(params));
  }
  // TODO: Validate that SSL Certificates exist
  stats_thread_ = StatsCollectionThread::maybeCreate(
      settings_,
      updateable_config_->get()->serverConfig(),
      plugin_registry_,
      StatsPublisherScope::CLIENT,
      /* num_shards */ 0,
      stats_.get());
}

void StandaloneAdminServer::initEventLog() {
  auto event_log = std::make_unique<EventLogStateMachine>(settings_);
  event_log->enableSendingUpdatesToWorkers();
  std::unique_ptr<Request> req =
      std::make_unique<StartEventLogStateMachineRequest>(
          std::move(event_log), 0);

  const int rv = processor_->postRequest(req);
  if (rv != 0) {
    ld_error("Cannot post request to start event log state machine: %s (%s)",
             error_name(err),
             error_description(err));
    throw StandaloneAdminServerFailed();
  }
}

void StandaloneAdminServer::shutdown() {
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Shutting down took%lums", msec_since(start_ts.timePoint()));
  };
  ld_info("Initiating shutdown");
  server_config_subscription_.deregister();
  ld_info("Stopping AdminServer, no new requests after this point.");
  if (admin_server_) {
    admin_server_->stop();
    admin_server_.reset();
  }
  if (processor_) {
    ld_info("Stopping accepting work on all workers.");
    std::vector<folly::SemiFuture<folly::Unit>> futures =
        fulfill_on_all_workers<folly::Unit>(
            processor_.get(), [](folly::Promise<folly::Unit> p) -> void {
              auto* worker = Worker::onThisThread();
              worker->stopAcceptingWork();
              p.setValue();
            });
    ld_info("Waiting for workers to acknowledge.");

    folly::collectAllSemiFuture(futures.begin(), futures.end()).get();
    ld_info("Workers acknowledged stopping accepting new work");

    ld_info("Finishing work and closing sockets on all workers.");
    futures = fulfill_on_all_workers<folly::Unit>(
        processor_.get(), [](folly::Promise<folly::Unit> p) -> void {
          auto* worker = Worker::onThisThread();
          worker->finishWorkAndCloseSockets();
          p.setValue();
        });
    ld_info("Waiting for workers to acknowledge.");

    folly::collectAllSemiFuture(futures.begin(), futures.end()).get();
    ld_info("Workers finished all works.");

    ld_info("Stopping Processor");
    processor_->waitForWorkers();
    processor_->shutdown();
  }

  shutdown_requested_.store(true);
  main_thread_sem_.post();
}

void StandaloneAdminServer::onSettingsUpdate() {
  dbg::assertOnData = server_settings_->assert_on_data;
  dbg::currentLevel = server_settings_->loglevel;
  dbg::externalLoggerLogLevel = server_settings_->external_loglevel;
  ZookeeperClient::setDebugLevel(server_settings_->loglevel);
  dbg::setLogLevelOverrides(server_settings_->loglevel_overrides);
}

bool StandaloneAdminServer::onConfigUpdate(ServerConfig& config) {
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Updating settings from config took %lums",
            msec_since(start_ts.timePoint()));
  };
  auto settings = config.getServerSettingsConfig();
  if (settings_updater_) {
    // Ensure that settings are updated when we receive new config.
    settings_updater_->setFromConfig(settings);
  }
  return true;
}

void StandaloneAdminServer::waitForShutdown() {
  for (;;) {
    main_thread_sem_.wait();
    if (shutdown_requested_.load()) {
      break;
    }
    ld_check(false);
  }
}
}}} // namespace facebook::logdevice::admin
