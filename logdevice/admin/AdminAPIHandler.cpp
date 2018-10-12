/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIHandler.h"

#include <array>
#include <folly/MoveWrapper.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/Conv.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/LogGroupThroughput.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

/* Example on fulfilling a promise on a worker
 *  ld_check(processor_);
 *  return fulfill_on_worker<int32_t>(
 *      processor_, // processor
 *      folly::none, // worker_id if you want to pin it
 *      WorkerType::BACKGROUND, [](folly::Promise<Unit> p) mutable {
 *        Worker* w = Worker::onThisThread();
 *        auto config = w->getConfig();
 *        auto logsconfig = config->localLogsConfig();
 *        p.setValue(logsconfig->getVersion());
 *      });
 */

void AdminAPIHandler::getLogTreeInfo(thrift::LogTreeInfo& response) {
  auto logsconfig = processor_->config_->getLocalLogsConfig();
  ld_check(logsconfig);
  response.set_version(std::to_string(logsconfig->getVersion()));
  response.set_num_logs(logsconfig->size());
  response.set_max_backlog_seconds(logsconfig->getMaxBacklogDuration().count());
  response.set_is_fully_loaded(logsconfig->isFullyLoaded());
}

void AdminAPIHandler::getReplicationInfo(thrift::ReplicationInfo& response) {
  auto logsconfig = processor_->config_->getLocalLogsConfig();
  ld_check(logsconfig);
  // Tolerable Failure Domain : {RACK: 2} which means that you can take
  // down 2 racks and we _should_ be still read-available. This is only an
  // approximation and is not guaranteed since you might have older data
  // that was replicated with an old replication policy that is more
  // restrictive.
  ld_check(logsconfig != nullptr);

  ReplicationProperty repl = logsconfig->getNarrowestReplication();
  // create the narrowest replication property json
  std::map<std::string, int32_t> narrowest_replication;
  for (auto item : repl.getDistinctReplicationFactors()) {
    narrowest_replication[NodeLocation::scopeNames()[item.first]] = item.second;
  }
  response.set_narrowest_replication(std::move(narrowest_replication));

  thrift::TolerableFailureDomain tfd;
  tfd.set_domain(NodeLocation::scopeNames()[repl.getBiggestReplicationScope()]);
  tfd.set_count(repl.getReplication(repl.getBiggestReplicationScope()) - 1);

  response.set_smallest_replication_factor(repl.getReplicationFactor());
  response.set_tolerable_failure_domains(tfd);
  response.set_version(std::to_string(logsconfig->getVersion()));
}

void AdminAPIHandler::getSettings(
    thrift::SettingsResponse& response,
    std::unique_ptr<thrift::SettingsRequest> request) {
  const auto& settings = ld_server_->getSettings();

  auto requested_settings = request->get_settings();

  for (const auto& setting : settings.getState()) {
    // Filter settings by name (if provided)
    if (requested_settings != nullptr &&
        requested_settings->find(setting.first) == requested_settings->end()) {
      continue;
    }

    auto get = [&](SettingsUpdater::Source src) {
      return settings.getValueFromSource(setting.first, src).value_or("");
    };
    thrift::Setting s;
    s.currentValue = get(SettingsUpdater::Source::CURRENT);
    s.defaultValue = folly::join(" ", setting.second.descriptor.default_value);

    std::string cli = get(SettingsUpdater::Source::CLI);
    std::string config = get(SettingsUpdater::Source::CONFIG);
    std::string admin_cmd = get(SettingsUpdater::Source::ADMIN_CMD);

    if (!cli.empty()) {
      s.sources[thrift::SettingSource::CLI] = std::move(cli);
    }
    if (!config.empty()) {
      s.sources[thrift::SettingSource::CONFIG] = std::move(config);
    }
    if (!admin_cmd.empty()) {
      s.sources[thrift::SettingSource::ADMIN_OVERRIDE] = std::move(admin_cmd);
    }

    response.settings[setting.first] = std::move(s);
  }
}

folly::Future<folly::Unit>
AdminAPIHandler::future_takeLogTreeSnapshot(thrift::unsigned64 min_version) {
  folly::Promise<folly::Unit> p;

  // Are we running with LCM?
  if (!processor_->settings()->enable_logsconfig_manager) {
    thrift::NotSupported error;
    error.set_message("LogsConfigManager is disabled in settings on this node");
    p.setException(std::move(error));
    return p.getFuture();
  } else if (!processor_->settings()->logsconfig_snapshotting) {
    thrift::NotSupported error;
    error.set_message("LogsConfigManager snapshotting is not enabled");
    p.setException(std::move(error));
    return p.getFuture();
  }

  auto logsconfig_worker_type =
      LogsConfigManager::workerType(ld_server_->getProcessor());
  auto logsconfig_owner_worker =
      worker_id_t{LogsConfigManager::getLogsConfigManagerWorkerIdx(
          ld_server_->getProcessor()->getWorkerCount(logsconfig_worker_type))};
  // Because thrift does not support u64, we encode it in a i64.
  uint64_t minimum_version = to_unsigned(min_version);

  auto cb = [minimum_version](folly::Promise<folly::Unit> promise) mutable {
    // This is needed because we want to move this into a lambda, an
    // std::function<> does not allow capturing move-only objects, so here we
    // are!
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3610.html
    folly::MoveWrapper<folly::Promise<folly::Unit>> mpromise(
        std::move(promise));
    Worker* w = Worker::onThisThread(true /* enforce_worker */);
    auto config = w->getConfig();
    ld_check(config);
    auto logsconfig = config->localLogsConfig();
    if (minimum_version > 0 && logsconfig->getVersion() < minimum_version) {
      thrift::StaleVersion error;
      error.set_message(
          folly::format("LogTree version on this node is {} which is lower "
                        "than the minimum requested {}",
                        logsconfig->getVersion(),
                        minimum_version)
              .str());
      error.set_server_version(
          static_cast<std::int64_t>(logsconfig->getVersion()));
      mpromise->setException(std::move(error));
      return;
    }
    // LogsConfigManager must exist on this worker, even if the RSM is not
    // started.
    ld_check(w->logsconfig_manager_);

    if (!w->logsconfig_manager_->isLogsConfigFullyLoaded()) {
      thrift::NodeNotReady error;
      error.set_message("LogsConfigManager has not fully replayed yet");
      mpromise->setException(error);
      return;
    } else {
      // LogsConfig is has fully replayed. Let's take a snapshot.
      auto snapshot_cb = [=](Status st) mutable {
        if (st == E::OK) {
          ld_info("A LogTree snapshot has been taken based on an Admin API "
                  "request");
          mpromise->setValue(folly::Unit());
        } else {
          thrift::OperationError error;
          error.set_message(
              folly::format("Cannot take a snapshot: {}", error_name(st))
                  .str());
          mpromise->setException(std::move(error));
        }
      };
      ld_check(w->logsconfig_manager_->getStateMachine());
      // Actually take the snapshot, the callback will fulfill the promise.
      w->logsconfig_manager_->getStateMachine()->snapshot(
          std::move(snapshot_cb));
    }
  };
  return fulfill_on_worker<folly::Unit>(
      processor_,
      folly::Optional<worker_id_t>(logsconfig_owner_worker),
      logsconfig_worker_type,
      cb);
}

void AdminAPIHandler::getLogGroupThroughput(
    thrift::LogGroupThroughputResponse& response,
    std::unique_ptr<thrift::LogGroupThroughputRequest> request) {
  ld_check(request != nullptr);

  auto operation =
      thrift::LogGroupOperation(thrift::LogGroupOperation::APPENDS);
  if (request->__isset.operation) {
    operation = request->operation;
  }

  using apache::thrift::util::enumName;
  std::string time_series = lowerCase(enumName(operation));

  std::vector<Duration> query_intervals;
  if (request->__isset.time_period) {
    auto time_period = request->time_period;
    for (const auto t : time_period) {
      query_intervals.push_back(std::chrono::seconds(t));
    }
  }
  if (query_intervals.empty()) {
    query_intervals.push_back(std::chrono::seconds(60));
  }

  std::string msg;
  if (!verifyIntervals(ld_server_, time_series, query_intervals, msg)) {
    thrift::InvalidRequest err;
    err.set_message(msg);
    throw err;
  }

  StatsHolder* stats = ld_server_->getParameters()->getStats();
  if (!stats) {
    return;
  } else {
    AggregateMap agg = doAggregate(
        stats,
        time_series,
        query_intervals,
        ld_server_->getParameters()->getUpdateableConfig()->getLogsConfig());

    std::string req_log_group;
    if (request->__isset.log_group_name) {
      req_log_group = request->log_group_name;
    }

    for (const auto& entry : agg) {
      std::string log_group_name = folly::to<std::string>(entry.first);
      if (!req_log_group.empty() && log_group_name != req_log_group) {
        continue;
      }

      thrift::LogGroupThroughput lg_throughput;
      lg_throughput.operation = operation;

      const OneGroupResults& results = entry.second;
      std::vector<int64_t> log_results;
      for (auto result : results) {
        log_results.push_back(int64_t(result));
      }
      lg_throughput.results = std::move(log_results);
      response.throughput[log_group_name] = std::move(lg_throughput);
    }
  }
}
}} // namespace facebook::logdevice
