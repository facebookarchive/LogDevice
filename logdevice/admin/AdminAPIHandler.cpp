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
#include "logdevice/admin/safety/SafetyChecker.h"
#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/LogGroupCustomCounters.h"
#include "logdevice/server/LogGroupThroughput.h"

namespace facebook { namespace logdevice {

AdminAPIHandler::AdminAPIHandler(
    Processor* processor,
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> updateable_server_settings,
    UpdateableSettings<AdminServerSettings> updateable_admin_server_settings,
    StatsHolder* stats_holder)
    : AdminAPIHandlerBase(processor,
                          std::move(settings_updater),
                          std::move(updateable_server_settings),
                          std::move(updateable_admin_server_settings),
                          stats_holder),
      facebook::fb303::FacebookBase2("LogDevice Admin API Service") {
  start_time_ = std::chrono::steady_clock::now();
  safety_checker_ = std::make_shared<SafetyChecker>(processor_);
  safety_checker_->useAdminSettings(updateable_admin_server_settings_);
}

facebook::fb303::cpp2::fb_status AdminAPIHandler::getStatus() {
  // Given that this thrift / Admin API service is started as soon as we
  // start accepting connections, the service can only be:
  // - ALIVE
  // - STOPPING
  if (processor_->isShuttingDown()) {
    return facebook::fb303::cpp2::fb_status::STOPPING;
  } else if (!processor_->isLogsConfigLoaded()) {
    return facebook::fb303::cpp2::fb_status::STARTING;
  } else {
    return facebook::fb303::cpp2::fb_status::ALIVE;
  }
}

void AdminAPIHandler::getVersion(std::string& _return) {
  auto build_info = processor_->getPluginRegistry()->getSinglePlugin<BuildInfo>(
      PluginType::BUILD_INFO);
  _return = build_info->version();
}

int64_t AdminAPIHandler::aliveSince() {
  auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - start_time_);
  return uptime.count();
}

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
  std::map<thrift::LocationScope, int32_t> narrowest_replication;
  std::map<std::string, int32_t> narrowest_replication_legacy;
  for (auto item : repl.getDistinctReplicationFactors()) {
    narrowest_replication[toThrift<thrift::LocationScope>(item.first)] =
        item.second;
    narrowest_replication_legacy[NodeLocation::scopeNames()[item.first]] =
        item.second;
  }
  response.set_narrowest_replication(std::move(narrowest_replication));
  response.set_narrowest_replication_legacy(
      std::move(narrowest_replication_legacy));

  thrift::TolerableFailureDomain tfd;

  const auto biggest_replication_scope = repl.getBiggestReplicationScope();
  tfd.set_domain(toThrift<thrift::LocationScope>(biggest_replication_scope));
  tfd.set_domain_legacy(NodeLocation::scopeNames()[biggest_replication_scope]);
  tfd.set_count(repl.getReplication(biggest_replication_scope) - 1);

  response.set_smallest_replication_factor(repl.getReplicationFactor());
  response.set_tolerable_failure_domains(tfd);
  response.set_version(std::to_string(logsconfig->getVersion()));
}

void AdminAPIHandler::getSettings(
    thrift::SettingsResponse& response,
    std::unique_ptr<thrift::SettingsRequest> request) {
  auto requested_settings = request->get_settings();

  for (const auto& setting : settings_updater_->getState()) {
    // Filter settings by name (if provided)
    if (requested_settings != nullptr &&
        requested_settings->find(setting.first) == requested_settings->end()) {
      continue;
    }

    auto get = [&](SettingsUpdater::Source src) {
      return settings_updater_->getValueFromSource(setting.first, src)
          .value_or("");
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

folly::SemiFuture<folly::Unit> AdminAPIHandler::semifuture_takeLogTreeSnapshot(
    thrift::unsigned64 min_version) {
  folly::Promise<folly::Unit> p;
  auto future = p.getSemiFuture();

  // Are we running with LCM?
  if (!processor_->settings()->enable_logsconfig_manager) {
    p.setException(thrift::NotSupported(
        "LogsConfigManager is disabled in settings on this node"));
    return future;
  } else if (!processor_->settings()->logsconfig_snapshotting) {
    p.setException(
        thrift::NotSupported("LogsConfigManager snapshotting is not enabled"));
    return future;
  }

  auto logsconfig_worker_type = LogsConfigManager::workerType(processor_);
  auto logsconfig_owner_worker =
      worker_id_t{LogsConfigManager::getLogsConfigManagerWorkerIdx(
          processor_->getWorkerCount(logsconfig_worker_type))};
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
      thrift::StaleVersion error(
          folly::format("LogTree version on this node is {} which is lower "
                        "than the minimum requested {}",
                        logsconfig->getVersion(),
                        minimum_version)
              .str());
      error.set_server_version(static_cast<int64_t>(logsconfig->getVersion()));
      mpromise->setException(std::move(error));
      return;
    }
    // LogsConfigManager must exist on this worker, even if the RSM is not
    // started.
    ld_check(w->logsconfig_manager_);

    if (!w->logsconfig_manager_->isLogsConfigFullyLoaded()) {
      mpromise->setException(
          thrift::NodeNotReady("LogsConfigManager has not fully replayed yet"));
      return;
    } else {
      // LogsConfig is has fully replayed. Let's take a snapshot.
      auto snapshot_cb = [=](Status st) mutable {
        if (st == E::OK) {
          ld_info("A LogTree snapshot has been taken based on an Admin API "
                  "request");
          mpromise->setValue(folly::Unit());
        } else {
          mpromise->setException(thrift::OperationError(
              folly::format("Cannot take a snapshot: {}", error_name(st))
                  .str()));
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
      cb,
      RequestType::ADMIN_CMD_UTIL_INTERNAL);
}

void setLogGroupCustomCountersResponse(
    std::string log_group_name,
    GroupResults counters,
    thrift::LogGroupCustomCountersResponse& response,
    std::vector<uint16_t> keys_filter) {
  std::vector<thrift::LogGroupCustomCounter> results;
  for (const auto& result : counters) {
    if (!keys_filter.empty()) {
      auto key_it =
          std::find(keys_filter.begin(), keys_filter.end(), result.first);
      if (key_it == keys_filter.end()) {
        continue;
      }
    }
    thrift::LogGroupCustomCounter counter;
    counter.key = static_cast<int16_t>(result.first);
    counter.val = static_cast<int64_t>(result.second);
    results.push_back(counter);
  }

  response.counters[log_group_name] = std::move(results);
}

void AdminAPIHandler::getLogGroupCustomCounters(
    thrift::LogGroupCustomCountersResponse& response,
    std::unique_ptr<thrift::LogGroupCustomCountersRequest> request) {
  ld_check(request != nullptr);

  if (!stats_holder_) {
    thrift::NotSupported err;
    err.set_message("This admin server cannot provide stats");
    throw err;
  }

  Duration query_interval = std::chrono::seconds(60);
  if (request->time_period != 0) {
    query_interval = std::chrono::seconds(request->time_period);
  }

  CustomCountersAggregateMap agg =
      doAggregateCustomCounters(stats_holder_, query_interval);

  std::string req_log_group = request->log_group_path;

  std::vector<u_int16_t> keys_filter;
  for (const uint8_t& key : request->keys) {
    if (key > std::numeric_limits<uint8_t>::max() || key < 0) {
      thrift::InvalidRequest err;
      std::ostringstream error_message;
      error_message << "key " << key << " is not within the limits 0-"
                    << std::numeric_limits<uint8_t>::max();

      err.set_message(error_message.str());
      throw err;
    }
    keys_filter.push_back(key);
  }

  if (!req_log_group.empty()) {
    if (agg.find(req_log_group) == agg.end()) {
      return;
    }
    auto log_group = agg[req_log_group];

    setLogGroupCustomCountersResponse(
        req_log_group, log_group, response, keys_filter);
    return;
  }

  for (const auto& entry : agg) {
    setLogGroupCustomCountersResponse(
        entry.first, entry.second, response, keys_filter);
  }
}

void AdminAPIHandler::getLogGroupThroughput(
    thrift::LogGroupThroughputResponse& response,
    std::unique_ptr<thrift::LogGroupThroughputRequest> request) {
  ld_check(request != nullptr);

  if (!stats_holder_) {
    thrift::NotSupported err;
    err.set_message(
        "This admin server cannot provide per-log-throughtput stats");
    throw err;
  }
  auto operation = request->operation_ref().value_or(
      thrift::LogGroupOperation(thrift::LogGroupOperation::APPENDS));

  using apache::thrift::util::enumName;
  std::string time_series = lowerCase(enumName(operation));

  std::vector<Duration> query_intervals;
  if (request->time_period_ref().has_value()) {
    auto time_period = request->time_period_ref().value();
    for (const auto t : time_period) {
      query_intervals.push_back(std::chrono::seconds(t));
    }
  }
  if (query_intervals.empty()) {
    query_intervals.push_back(std::chrono::seconds(60));
  }

  std::string msg;
  if (!verifyIntervals(stats_holder_, time_series, query_intervals, msg)) {
    thrift::InvalidRequest err;
    err.set_message(msg);
    throw err;
  }

  AggregateMap agg = doAggregate(stats_holder_,
                                 time_series,
                                 query_intervals,
                                 processor_->config_->getLogsConfig());

  std::string req_log_group = request->log_group_name_ref().value_or("");

  for (const auto& entry : agg) {
    std::string log_group_name = entry.first;
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
}} // namespace facebook::logdevice
