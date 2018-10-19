/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/LogsConfigRequestOrigin.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/logs/LogsConfigApiTracer.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {
/** Forward Declarations **/
namespace logsconfig {
class LogsConfigTree;
}
namespace configuration {
class LocalLogsConfig;
}

class LogsConfigManagerRequest;

struct LogsConfigManagerRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<LogsConfigManagerRequest>,
                     request_id_t::Hash>
      map;
};

class LogsConfigManagerReply;

struct LogsConfigManagerReplyMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<LogsConfigManagerReply>,
                     request_id_t::Hash>
      map;
};

/**
 * LogsConfigManager is the owner of the LogsConfigStateMachine object, it also
 * publishes the updates coming from the RSM to UpdateableLogsConfig. This is
 * the component also responsible for taking snapshots of the state machine.
 */
class LogsConfigManager {
 public:
  LogsConfigManager(UpdateableSettings<Settings> updateable_settings,
                    std::shared_ptr<UpdateableConfig> updateable_config,
                    WorkerType lcm_worker_type_,
                    bool is_writable);

  static bool createAndAttach(Processor& processor, bool is_writable);
  /**
   * Starts the underlying ReplicatedStateMachine and subscribes to its updates
   */
  void start();

  /**
   * Stops the underlying ReplicatedStateMachine
   */
  void stop();

  /**
   * returns the underlying state machine, note that the state machine is not
   * thread-safe and must be used from the same worker as LogsConfigManager.
   */
  LogsConfigStateMachine* getStateMachine() const {
    return state_machine_.get();
  }

  /**
   * Returns true if enable-logsconfig-manager is enabled in UpdateableSettings
   */
  bool isEnabledInSettings() const {
    auto settings = Worker::settings();
    // on Client we cannot start logsconfig manager if on-demand-logs-config
    // is enabled we can't enable logsconfig manager.
    if (!settings.server &&
        (settings.on_demand_logs_config ||
         settings.force_on_demand_logs_config)) {
      return false;
    }
    return settings.enable_logsconfig_manager;
  }

  /**
   * Returns the WorkerType that LogsConfigManager should be working on
   */
  static WorkerType workerType(Processor* processor) {
    // This returns either WorkerType::BACKGROUND or WorkerType::GENERAL based
    // on whether we have background workers.
    if (processor->getWorkerCount(WorkerType::BACKGROUND) > 0) {
      return WorkerType::BACKGROUND;
    }
    return WorkerType::GENERAL;
  }

  /**
   * Returns the WorkerType that LogsConfigManager should be working on
   */
  WorkerType getWorkerTypeAffinity() const {
    return lcm_worker_type_;
  }

  bool isLogsConfigFullyLoaded() const {
    return is_fully_loaded_;
  }

  /**
   * Callback that gets called with UpdateableSettings gets updated. This will
   * dispatch a RefreshLogsConfigManagerRequest which might start/stop the RSM
   * based on the new settings
   * This must be called on the Worker thread.
   */
  void onSettingsUpdated();

  /**
   * Decides on which worker the LogsConfigManager and LogsConfigStateMachine
   * should bind to.
   */
  static int getLogsConfigManagerWorkerIdx(int nthreads);

 private:
  StatsHolder* getStats();
  void cancelPublishTimer();
  void activatePublishTimer();
  void updateLogsConfig(const logsconfig::LogsConfigTree& tree);

  /**
   * This is a timer that is used to delay publishing the LogsConfig into the
   * UpdateableLogsConfig. This is configurable via the
   * logsconfig-manager-grace-period setting. If the grace period is set to zero
   * then we publish the config on every change.
   */
  Timer publish_timer_;
  UpdateableSettings<Settings> settings_;
  std::shared_ptr<UpdateableConfig> updateable_config_;
  std::unique_ptr<LogsConfigStateMachine> state_machine_;
  std::unique_ptr<LogsConfigStateMachine::SubscriptionHandle>
      config_updates_handle_;
  WorkerType lcm_worker_type_;
  bool is_writable_;
  // Is the RSM running? Only updated and checked in Worker thread.
  bool is_running_ = false;
  bool is_fully_loaded_ = false;
  std::chrono::milliseconds publish_grace_period_{0};

  UpdateableSettings<Settings>::SubscriptionHandle settings_update_handle_;
};

/**
 * A Request for creating the LogsConfigManager, This does not mean that it will
 * always start the underlying RSM and start publishing new LogsConfig. It will
 * be listening on UpdateableSettings changes for this. If the
 * enable-logsconfig-manager setting is already enabled, It will be start the
 * RSM immediately.
 */
class StartLogsConfigManagerRequest : public Request {
 public:
  explicit StartLogsConfigManagerRequest(
      std::unique_ptr<LogsConfigManager> manager)
      : Request(RequestType::START_LOGS_CONFIG_MANAGER),
        manager_(std::move(manager)) {}

  ~StartLogsConfigManagerRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int nthreads) override {
    return LogsConfigManager::getLogsConfigManagerWorkerIdx(nthreads);
  }

  WorkerType getWorkerTypeAffinity() override {
    return manager_->getWorkerTypeAffinity();
  }

 private:
  std::unique_ptr<LogsConfigManager> manager_;
};

/**
 * A request for applying a delta on the RSM
 */
class LogsConfigManagerRequest : public Request {
 public:
  LogsConfigManagerRequest(const LOGS_CONFIG_API_Header& request_header,
                           const Address& to,
                           std::string blob,
                           int respond_to_worker,
                           WorkerType respond_to_worker_type,
                           WorkerType lcm_worker_type,
                           std::unique_ptr<LogsConfigApiTracer> tracer)
      : Request(RequestType::LOGS_CONFIG_MANAGER_REQUEST),
        request_header_(request_header),
        to_(to),
        blob_(std::move(blob)),
        respond_to_worker_(respond_to_worker),
        respond_to_worker_type_(respond_to_worker_type),
        lcm_worker_type_(lcm_worker_type),
        tracer_(std::move(tracer)) {}

  Execution execute() override;

  int getThreadAffinity(int nthreads) override {
    return LogsConfigManager::getLogsConfigManagerWorkerIdx(nthreads);
  }

  WorkerType getWorkerTypeAffinity() override {
    return lcm_worker_type_;
  }

  Execution executeGetDirectoryOrLogGroup(
      const logsconfig::LogsConfigTree& state_machine,
      const std::shared_ptr<LogsConfig::LogGroupNode>& metadata_log);

  /*
   * This gets called after we hear back from the RSM (writeDelta's callback)
   * and based on the Delta type it queries (or not) the current LogsConfigTree
   * and serializes the response to be sent back to the requester.
   */
  std::string
  calculateResponsePayload(const logsconfig::Delta& delta,
                           const logsconfig::LogsConfigTree& tree) const;
  ~LogsConfigManagerRequest() override {}

 private:
  void postRequest(Status st,
                   lsn_t config_version = 0,
                   std::string response_blob = std::string());
  void deleteThis();

  /* This is used to serialize a log group + parent name in the
   * LogsConfigApiRequest
   */
  std::string
  serializeLogGroupInDirectory(const logsconfig::LogGroupWithParentPath& lid);

  LOGS_CONFIG_API_Header request_header_;
  Address to_;
  std::string blob_;
  int respond_to_worker_;
  WorkerType respond_to_worker_type_;
  WorkerType lcm_worker_type_;
  std::unique_ptr<LogsConfigApiTracer> tracer_;
};

/**
 * Replies from the LogsConfigManager
 */
class LogsConfigManagerReply : public Request {
 public:
  LogsConfigManagerReply(Status st,
                         const Address& to,
                         uint64_t config_version,
                         std::string response_blob,
                         request_id_t client_rqid,
                         int worker,
                         WorkerType worker_type,
                         LogsConfigRequestOrigin origin,
                         std::unique_ptr<LogsConfigApiTracer> tracer);

  Execution execute() override;

  int getThreadAffinity(int /* unused */) override {
    return worker_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

  ~LogsConfigManagerReply() override {}

 private:
  enum class SendResult { ERROR, SENT_NONE, SENT_SOME, SENT_ALL };

  void onRetry();
  LogsConfigManagerReply::SendResult sendChunks(Worker* w);

  Status status_;
  Address to_;
  uint64_t config_version_;
  std::string response_blob_;
  request_id_t client_rqid_;
  // Worker on which we received the corresponding LogsConfigManagerRequest.
  // We want this request to execute on the same worker so we can respond to
  // the client on the same socket.
  int worker_;
  WorkerType worker_type_;
  std::unique_ptr<LogsConfigApiTracer> tracer_;

  // Timer used to retry send of chunks part of a large message
  // Chunked messages could cause the socket layer to congest and return
  // with E::NOBUFS
  std::unique_ptr<ExponentialBackoffTimer> retry_timer_;

  // Offset within response_blob_ that we were able to send
  size_t response_offset_;

  LogsConfigRequestOrigin origin_;
};

}} // namespace facebook::logdevice
