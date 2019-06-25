/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Worker.h"

#include <algorithm>
#include <pthread.h>
#include <string>
#include <unistd.h>

#include <folly/Memory.h>
#include <folly/stats/BucketedTimeSeries.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "logdevice/common/AbortAppendersEpochRequest.h"
#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/Appender.h"
#include "logdevice/common/AppenderBuffer.h"
#include "logdevice/common/CheckNodeHealthRequest.h"
#include "logdevice/common/CheckSealRequest.h"
#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/ConfigurationFetchRequest.h"
#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/DataSizeRequest.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/EventLoopTaskQueue.h"
#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/FindKeyRequest.h"
#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/GetEpochRecoveryMetadataRequest.h"
#include "logdevice/common/GetHeadAttributesRequest.h"
#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/GetTrimPointRequest.h"
#include "logdevice/common/GraylistingTracker.h"
#include "logdevice/common/IsLogEmptyRequest.h"
#include "logdevice/common/LogIDUniqueQueue.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/LogsConfigUpdatedRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodesConfigurationUpdatedRequest.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/SequencerBackgroundActivator.h"
#include "logdevice/common/ServerConfigUpdatedRequest.h"
#include "logdevice/common/ShapingContainer.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/TimeoutMap.h"
#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/WorkerTimeoutStats.h"
#include "logdevice/common/WriteMetaDataRecord.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/protocol/MessageTracer.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

namespace {
node_index_t getMyNodeIndex(const Worker* worker) {
  if (worker->immutable_settings_->server) {
    return worker->processor_->getMyNodeID().index();
  }
  return NODE_INDEX_INVALID;
}

folly::Optional<NodeLocation>
getMyLocation(const std::shared_ptr<UpdateableConfig>& config,
              const Worker* worker) {
  if (worker->immutable_settings_->server) {
    std::shared_ptr<ServerConfig> cfg(config->get()->serverConfig());
    ld_check(worker->processor_->hasMyNodeID());
    auto my_node_index = worker->processor_->getMyNodeID().index();
    auto nodes = config->getNodesConfiguration();
    auto node = nodes->getNodeServiceDiscovery(my_node_index);
    ld_check(node);
    return node->location;
  }
  return worker->immutable_settings_->client_location;
}
} // namespace

// the size of the bucket array of activeAppenders_ map
static constexpr size_t N_APPENDER_MAP_BUCKETS = 128 * 1024;
// Definition of static constexpr to make pre C++17 compilers happy.
constexpr int Worker::kHiPriTaskExecDistribution;
constexpr int Worker::kMidPriTaskExecDistribution;
constexpr int Worker::kLoPriTaskExecDistribution;
constexpr folly::StringPiece Worker::kWorkerDataID;

thread_local Worker* Worker::on_this_thread_{nullptr};
// This pimpl class is a container for all classes that would normally be
// members of Worker but we don't want to have to include them in Worker.h.
class WorkerImpl {
 public:
  WorkerImpl(Worker* w,
             const std::shared_ptr<UpdateableConfig>& config,
             StatsHolder* stats)
      : sender_(w->immutable_settings_,
                w->getEventBase(),
                config->get()->serverConfig()->getTrafficShapingConfig(),
                &w->processor_->clientIdxAllocator(),
                w->worker_type_ == WorkerType::FAILURE_DETECTOR,
                config->getServerConfig()
                    ->getNodesConfigurationFromServerConfigSource(),
                getMyNodeIndex(w),
                getMyLocation(config, w),
                stats),
        activeAppenders_(w->immutable_settings_->server ? N_APPENDER_MAP_BUCKETS
                                                        : 1),
        // AppenderBuffer queue capacity is the system-wide per-log limit
        // divided by the number of Workers
        appenderBuffer_(w->immutable_settings_->appender_buffer_queue_cap /
                        w->immutable_settings_->num_workers),
        // TODO: Make this configurable
        previously_redirected_appends_(1024),
        sslFetcher_(w->immutable_settings_->ssl_cert_path,
                    w->immutable_settings_->ssl_key_path,
                    w->immutable_settings_->ssl_ca_path,
                    w->immutable_settings_->ssl_cert_refresh_interval),

        graylistingTracker_(std::make_unique<GraylistingTracker>())

  {
    const auto& read_shaping_cfg =
        config->get()->serverConfig()->getReadIOShapingConfig();
    read_shaping_container_ = std::make_unique<ShapingContainer>(
        1,
        w->getEventBase(),
        read_shaping_cfg,
        std::make_shared<ReadShapingFlowGroupDeps>(stats));
    read_shaping_container_->getFlowGroup(NodeLocationScope::NODE)
        .setScope(nullptr, NodeLocationScope::NODE);
  }

  ShardAuthoritativeStatusManager shardStatusManager_;
  Sender sender_;
  LogRebuildingMap runningLogRebuildings_;
  FindKeyRequestMap runningFindKey_;
  FireAndForgetRequestMap runningFireAndForgets_;
  TrimRequestMap runningTrimRequests_;
  GetTrimPointRequestMap runningGetTrimPoint_;
  IsLogEmptyRequestMap runningIsLogEmpty_;
  DataSizeRequestMap runningDataSize_;
  GetHeadAttributesRequestMap runningGetHeadAttributes_;
  GetClusterStateRequestMap runningGetClusterState_;
  GetEpochRecoveryMetadataRequestMap runningGetEpochRecoveryMetadata_;
  LogsConfigApiRequestMap runningLogManagementReqs_;
  LogsConfigManagerRequestMap runningLogsConfigManagerReqs_;
  LogsConfigManagerReplyMap runningLogsConfigManagerReplies_;
  AppendRequestMap runningAppends_;
  CheckSealRequestMap runningCheckSeals_;
  ConfigurationFetchRequestMap runningConfigurationFetches_;
  GetSeqStateRequestMap runningGetSeqState_;
  AppenderMap activeAppenders_;
  GetLogInfoRequestMaps runningGetLogInfo_;
  ClusterStateSubscriptionList clusterStateSubscriptions_;
  LogRecoveryRequestMap runningLogRecoveries_;
  SyncSequencerRequestList runningSyncSequencerRequests_;
  AppenderBuffer appenderBuffer_;
  AppenderBuffer previously_redirected_appends_;
  LogIDUniqueQueue recoveryQueueDataLog_;
  LogIDUniqueQueue recoveryQueueMetaDataLog_;
  AllClientReadStreams clientReadStreams_;
  WriteMetaDataRecordMap runningWriteMetaDataRecords_;
  AppendRequestEpochMap appendRequestEpochMap_;
  CheckNodeHealthRequestSet pendingHealthChecks_;
  SSLFetcher sslFetcher_;
  std::unique_ptr<SequencerBackgroundActivator> sequencerBackgroundActivator_;
  std::unique_ptr<GraylistingTracker> graylistingTracker_;
  std::unique_ptr<ShapingContainer> read_shaping_container_;
};

std::string Worker::makeThreadName(Processor* processor,
                                   WorkerType type,
                                   worker_id_t idx) {
  const std::string& processor_name = processor->getName();
  std::array<char, 16> name_buf;
  snprintf(name_buf.data(),
           name_buf.size(),
           "%s:%s",
           processor_name.c_str(),
           Worker::getName(type, idx).c_str());
  return name_buf.data();
}

Worker::Worker(WorkContext::KeepAlive event_loop,
               Processor* processor,
               worker_id_t idx,
               const std::shared_ptr<UpdateableConfig>& config,
               StatsHolder* stats,
               WorkerType worker_type,
               ThreadID::Type thread_type)
    : WorkContext(std::move(event_loop)),
      processor_(processor),
      updateable_settings_(processor->updateableSettings()),
      immutable_settings_(processor->updateableSettings().get()),
      idx_(idx),
      worker_type_(worker_type),
      impl_(new WorkerImpl(this, config, stats)),
      config_(config),
      stats_(stats),
      shutting_down_(false),
      accepting_work_(true),
      worker_timeout_stats_(std::make_unique<WorkerTimeoutStats>()) {}

Worker::~Worker() {
  shutting_down_ = true;
  stopAcceptingWork();

  server_config_update_sub_.unsubscribe();
  logs_config_update_sub_.unsubscribe();
  updateable_settings_subscription_.unsubscribe();

  // BufferedWriter must have cleared this map by now
  ld_check(active_buffered_writers_.empty());

  dispose_metareader_timer_.reset();

  for (auto it = runningWriteMetaDataRecords().map.begin();
       it != runningWriteMetaDataRecords().map.end();) {
    WriteMetaDataRecord* write = it->second;
    it = runningWriteMetaDataRecords().map.erase(it);
    write->getDriver()->onWorkerShutdown();
  }

  // Free all ExponentialBackoffTimerNode objects in timers_
  for (auto it = timers_.begin(); it != timers_.end();) {
    ExponentialBackoffTimerNode& node = *it;
    it = timers_.erase(it);
    delete &node;
  }

  // destroy all Appenders to free up zero copied records.
  // Note: on graceful shutdown there should be no active appenders and
  // this is mostly used in situations where full graceful shutdown is not
  // used (e.g., tests)
  activeAppenders().map.clearAndDispose();
}

Worker* FOLLY_NULLABLE Worker::onThisThread(bool enforce_worker) {
  if (Worker::on_this_thread_) {
    return Worker::on_this_thread_;
  }

  if (enforce_worker) {
    // Not on a worker == assert failure
    ld_check(false);
  }
  return nullptr;
}

std::string Worker::getName(WorkerType type, worker_id_t idx) {
  return std::string("W") + workerTypeChar(type) + std::to_string(idx.val_);
}

std::shared_ptr<Configuration> Worker::getConfiguration() const {
  ld_check((bool)config_);
  return config_->get();
}

std::shared_ptr<ServerConfig> Worker::getServerConfig() const {
  ld_check((bool)config_);
  return config_->getServerConfig();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Worker::getNodesConfiguration() const {
  return config_->getNodesConfiguration();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Worker::getNodesConfigurationFromNCMSource() const {
  return config_->getNodesConfigurationFromNCMSource();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Worker::getNodesConfigurationFromServerConfigSource() const {
  return config_->getNodesConfigurationFromServerConfigSource();
}

std::shared_ptr<LogsConfig> Worker::getLogsConfig() const {
  ld_check((bool)config_);
  return config_->getLogsConfig();
}

std::shared_ptr<configuration::ZookeeperConfig>
Worker::getZookeeperConfig() const {
  ld_check((bool)config_);
  return config_->getZookeeperConfig();
}

std::shared_ptr<UpdateableConfig> Worker::getUpdateableConfig() {
  ld_check((bool)config_);
  return config_;
}

void Worker::onLogsConfigUpdated() {
  clientReadStreams().noteConfigurationChanged();
  if (rebuilding_coordinator_) {
    rebuilding_coordinator_->noteConfigurationChanged();
  }
}

void Worker::onServerConfigUpdated() {
  ld_check(Worker::onThisThread() == this);

  dbg::thisThreadClusterName() =
      config_->get()->serverConfig()->getClusterName();

  sender().noteConfigurationChanged(getNodesConfiguration());

  clientReadStreams().noteConfigurationChanged();
  // propagate the config change to metadata sequencer
  runningWriteMetaDataRecords().noteConfigurationChanged();

  if (event_log_) {
    event_log_->noteConfigurationChanged();
  }

  if (idx_.val_ == 0 && worker_type_ == WorkerType::GENERAL) {
    // running this operation on worker 0 only. the cluster state
    // should be updated/resized only once since it is shared among workers.
    auto cs = getClusterState();
    if (cs) {
      cs->noteConfigurationChanged();
    }
  }
}

void Worker::onNodesConfigurationUpdated() {
  if (folly::kIsDebug) {
    ld_info("Worker %d (type %s) notified of NodesConfiguration update",
            idx_.val(),
            workerTypeStr(worker_type_));
  }
  // note: onServerConfigUpdated() is a virtual function and this may calls
  // derived function from subclass (e.g., ServerWorker)
  onServerConfigUpdated();
}

namespace {
class SettingsUpdatedRequest : public Request {
 public:
  explicit SettingsUpdatedRequest(worker_id_t worker_id, WorkerType worker_type)
      : Request(RequestType::SETTINGS_UPDATED),
        worker_id_(worker_id),
        worker_type_(worker_type) {}
  Request::Execution execute() override {
    Worker::onThisThread()->onSettingsUpdated();
    return Execution::COMPLETE;
  }
  int getThreadAffinity(int) override {
    return worker_id_.val_;
  }
  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  worker_id_t worker_id_;
  WorkerType worker_type_;
};
} // namespace

const Settings& Worker::settings() {
  Worker* w = onThisThread();
  ld_check(w->immutable_settings_);
  return *w->immutable_settings_;
}

void Worker::onSettingsUpdated() {
  // If SettingsUpdatedRequest are posted faster than they're processed,
  // each request will pick up multiple settings updates. This would mean
  // that after the first SettingsUpdatedRequest is processed, we already
  // have an uptodate setting and no further processing is necessary.
  std::shared_ptr<const Settings> new_settings = updateable_settings_.get();
  if (new_settings == immutable_settings_) {
    return;
  }

  immutable_settings_ = new_settings;
  auto event_loop = checked_downcast<EventLoop*>(getExecutor());
  event_loop->getTaskQueue().setDequeuesPerIteration(
      {immutable_settings_->hi_requests_per_iteration,
       immutable_settings_->mid_requests_per_iteration,
       immutable_settings_->lo_requests_per_iteration});
  clientReadStreams().noteSettingsUpdated();
  if (logsconfig_manager_) {
    // LogsConfigManager might want to start or stop the underlying RSM if
    // the enable-logsconfig-manager setting is changed.
    logsconfig_manager_->onSettingsUpdated();
  }

  if (!new_settings->enable_store_histogram_calculations) {
    getWorkerTimeoutStats().clear();
  }

  if (impl_->graylistingTracker_) {
    impl_->graylistingTracker_->onSettingsUpdated();
  }

  if (event_log_) {
    event_log_->onSettingsUpdated();
  }

  if (impl_->sequencerBackgroundActivator_) {
    impl_->sequencerBackgroundActivator_->onSettingsUpdated();
  }

  impl_->sender_.onSettingsUpdated(immutable_settings_);
}

void Worker::initializeSubscriptions() {
  enum class ConfigType { SERVER, LOGS, NODES };
  Processor* processor = processor_;
  worker_id_t idx = idx_;
  WorkerType worker_type = worker_type_;

  auto configUpdateCallback = [processor, idx, worker_type](ConfigType type) {
    return [processor, idx, worker_type, type]() {
      // callback runs on unspecified thread so we need to post a Request
      // through the processor
      std::unique_ptr<Request> req;
      switch (type) {
        case ConfigType::SERVER:
          req = std::make_unique<ServerConfigUpdatedRequest>(idx, worker_type);
          break;
        case ConfigType::LOGS:
          req = std::make_unique<LogsConfigUpdatedRequest>(
              processor->settings()->configuration_update_retry_interval,
              idx,
              worker_type);
          break;
        case ConfigType::NODES:
          req = std::make_unique<NodesConfigurationUpdatedRequest>(
              idx, worker_type);
          break;
      }

      int rv = processor->postWithRetrying(req);

      if (rv != 0) {
        ld_error("error processing %s config update on worker #%d (%s): "
                 "postWithRetrying() failed with status %s",
                 type == ConfigType::SERVER
                     ? "server"
                     : type == ConfigType::LOGS ? "logs" : "nodes",
                 idx.val_,
                 workerTypeStr(worker_type),
                 error_description(err));
      }
    };
  };

  // Subscribe to config updates
  server_config_update_sub_ =
      config_->updateableServerConfig()->subscribeToUpdates(
          configUpdateCallback(ConfigType::SERVER));
  logs_config_update_sub_ = config_->updateableLogsConfig()->subscribeToUpdates(
      configUpdateCallback(ConfigType::LOGS));
  nodes_configuration_update_sub_ =
      config_->updateableNodesConfiguration()->subscribeToUpdates(
          configUpdateCallback(ConfigType::NODES));

  // Pretend we got the config update - to make sure we didn't miss anything
  // before we subscribed
  onServerConfigUpdated();
  onLogsConfigUpdated();
  onNodesConfigurationUpdated();

  auto settingsUpdateCallback = [processor, idx, worker_type]() {
    std::unique_ptr<Request> request =
        std::make_unique<SettingsUpdatedRequest>(idx, worker_type);

    int rv = processor->postWithRetrying(request);

    if (rv != 0) {
      ld_error("error processing settings update on worker #%d: "
               "postWithRetrying() failed with status %s",
               idx.val_,
               error_description(err));
    }
  };
  // Subscribe to settings update
  updateable_settings_subscription_ =
      updateable_settings_.raw()->subscribeToUpdates(settingsUpdateCallback);
  // Ensuring that we didn't miss any updates by updating the local copy of
  // settings from the global one
  onSettingsUpdated();
}

void Worker::setupWorker() {
  requests_stuck_timer_ = std::make_unique<Timer>(
      std::bind(&Worker::reportOldestRecoveryRequest, this));
  load_timer_ = std::make_unique<Timer>(std::bind(&Worker::reportLoad, this));
  isolation_timer_ = std::make_unique<Timer>(
      std::bind(&Worker::disableSequencersDueIsolationTimeout, this));
  cluster_state_polling_ = std::make_unique<Timer>(
      []() { getClusterState()->refreshClusterStateAsync(); });

  // Now that virtual calls are available (unlike in the constructor),
  // initialise `message_dispatch_'
  message_dispatch_ = createMessageDispatch();

  if (stats_ && worker_type_ == WorkerType::GENERAL) {
    // Imprint the thread-local Stats object with our ID
    stats_->get().worker_id = idx_;
  }

  // Subscribe to config updates and setting updates
  initializeSubscriptions();

  clientReadStreams().registerForShardAuthoritativeStatusUpdates();

  // Start the graylisting tracker
  if (!settings().disable_outlier_based_graylisting) {
    impl_->graylistingTracker_->start();
  }

  // Initialize load reporting and start timer
  reportLoad();
  reportOldestRecoveryRequest();
}

const std::shared_ptr<TraceLogger> Worker::getTraceLogger() const {
  return processor_->getTraceLogger();
}

// Consider changing value of derived value time_delay_before_force_abort when
// changing value of this poll delay.
static std::chrono::milliseconds PENDING_REQUESTS_POLL_DELAY{50};

void Worker::stopAcceptingWork() {
  accepting_work_ = false;
}

void Worker::finishWorkAndCloseSockets() {
  ld_check(!accepting_work_);

  subclassFinishWork();

  size_t c = runningSyncSequencerRequests().getList().size();
  if (c) {
    runningSyncSequencerRequests().terminateRequests();
    ld_info("Aborted %lu sync sequencer requests", c);
  }

  // Abort all LogRebuilding state machines.
  std::vector<LogRebuildingInterface*> to_abort;
  for (auto& it : runningLogRebuildings().map) {
    to_abort.push_back(it.second.get());
  }
  if (!to_abort.empty()) {
    for (auto l : to_abort) {
      l->abort(false /* notify_complete */);
    }
    ld_info("Aborted %lu log rebuildings", to_abort.size());
  }
  if (rebuilding_coordinator_) {
    rebuilding_coordinator_->shutdown();
  }

  if (event_log_ && !event_log_stopped_) {
    event_log_->stop();
    event_log_stopped_ = true;
  }

  if (logsconfig_manager_) {
    logsconfig_manager_->stop();
    logsconfig_manager_.reset();
  }

  // abort all fire-and-forget requests
  if (!runningFireAndForgets().map.empty()) {
    c = runningFireAndForgets().map.size();
    runningFireAndForgets().map.clear();
    ld_info("Aborted %lu fire-and-forget requests", c);
  }

  // abort get-trim-point requests
  if (!runningGetTrimPoint().map.empty()) {
    c = runningGetTrimPoint().map.size();
    runningGetTrimPoint().map.clear();
    ld_info("Aborted %lu get-trim-point requests", c);
  }

  // abort configuration-fetch requests
  if (!runningConfigurationFetches().map.empty()) {
    c = runningConfigurationFetches().map.size();
    runningConfigurationFetches().map.clear();
    ld_info("Aborted %lu configuration-fetch requests", c);
  }

  // Kick off the following async sequence:
  //  1) wait for requestsPending() to become zero
  //  2) tear down state machines such as read streams
  //  3) shut down the communication layer
  //  4) processor_->noteWorkerQuiescent(idx_);

  if (requestsPending() == 0 && !waiting_for_sockets_to_close_) {
    // Destructors for ClientReadStream, ServerReadStream and
    // PerWorkerStorageTaskQueue may involve communication, so take care of
    // that before we tear down the messaging fabric.
    subclassWorkFinished();
    clientReadStreams().clear();
    noteShuttingDownNoPendingRequests();

    ld_info("Shutting down Sender");
    sender().beginShutdown();
    waiting_for_sockets_to_close_ = true;
    // Initialize timer to force close sockets.
    force_close_sockets_counter_ = settings().time_delay_before_force_abort;
  }

  if (requestsPending() == 0 && sender().isClosed()) {
    // already done
    ld_info("Worker finished closing sockets");
    processor_->noteWorkerQuiescent(idx_, worker_type_);

    ld_check(requests_pending_timer_ == nullptr ||
             !requests_pending_timer_->isActive());
  } else {
    // start a timer that'll periodically do the check
    if (requests_pending_timer_ == nullptr) {
      if (requestsPending() > 0) {
        ld_info("Waiting for requests to finish");
      }
      // Initialize the force abort timer.
      force_abort_pending_requests_counter_ =
          settings().time_delay_before_force_abort;
      requests_pending_timer_.reset(new Timer([this] {
        finishWorkAndCloseSockets();
        if (force_abort_pending_requests_counter_ > 0) {
          --force_abort_pending_requests_counter_;
          if (force_abort_pending_requests_counter_ == 0) {
            forceAbortPendingWork();
          }
        }

        if (force_close_sockets_counter_ > 0) {
          --force_close_sockets_counter_;
          if (force_close_sockets_counter_ == 0) {
            forceCloseSockets();
          }
        }
      }));
    }
    requests_pending_timer_->activate(PENDING_REQUESTS_POLL_DELAY);
  }
}

void Worker::forceAbortPendingWork() {
  {
    // Find if the appender map is non-empty. If it is non-empty send
    // AbortAppenderRequest aborting all the appenders on this worker.
    const auto& map = activeAppenders().map;
    ld_info("Is appender map empty %d", map.empty());
    if (!map.empty()) {
      std::unique_ptr<Request> req =
          std::make_unique<AbortAppendersEpochRequest>(idx_);
      processor_->postImportant(req);
    }
  }
}

void Worker::forceCloseSockets() {
  {
    // Close all sockets irrespective of pending work on them.
    auto sockets_closed = sender().closeAllSockets();

    ld_info("Num server sockets closed: %u, Num clients sockets closed: %u",
            sockets_closed.first,
            sockets_closed.second);
  }
}

void Worker::disableSequencersDueIsolationTimeout() {
  Worker::onThisThread(false)
      ->processor_->allSequencers()
      .disableAllSequencersDueToIsolation();
}

void Worker::reportOldestRecoveryRequest() {
  auto now = SteadyTimestamp::now();
  auto min = now;
  LogRecoveryRequest* oldest = nullptr;
  for (const auto& request : impl_->runningLogRecoveries_.map) {
    auto created = request.second->getCreationTimestamp();
    if (created < min) {
      min = created;
      oldest = request.second.get();
    }
  }
  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - min);
  WORKER_STAT_SET(oldest_recovery_request, diff.count());

  if (diff > std::chrono::minutes(10) && oldest != nullptr) {
    auto describe_oldest_recovery = [&] {
      InfoRecoveriesTable table(getInfoRecoveriesTableColumns(), true);
      oldest->getDebugInfo(table);
      return table.toString();
    };
    RATELIMIT_WARNING(
        std::chrono::minutes(10),
        2,
        "Oldest log recovery is %ld seconds old:\n%s",
        std::chrono::duration_cast<std::chrono::seconds>(diff).count(),
        describe_oldest_recovery().c_str());
  }

  requests_stuck_timer_->activate(std::chrono::minutes(1));
}

EpochRecovery* Worker::findActiveEpochRecovery(logid_t logid) const {
  ld_check(logid != LOGID_INVALID);

  auto it = runningLogRecoveries().map.find(logid);
  if (it == runningLogRecoveries().map.end()) {
    err = E::NOTFOUND;
    return nullptr;
  }

  LogRecoveryRequest* log_recovery = it->second.get();

  ld_check(log_recovery);

  EpochRecovery* erm = log_recovery->getActiveEpochRecovery();

  if (!erm) {
    err = E::NOTFOUND;
  }

  return erm;
}

bool Worker::requestsPending() const {
  std::vector<std::string> counts;
#define PROCESS(x, name)                                     \
  if (!x.empty()) {                                          \
    counts.push_back(std::to_string(x.size()) + " " + name); \
  }

  PROCESS(activeAppenders().map, "appenders");
  PROCESS(runningFindKey().map, "findkeys");
  PROCESS(runningFireAndForgets().map, "fire and forgets");
  PROCESS(runningGetLogInfo().gli_map, "get log infos");
  PROCESS(runningGetLogInfo().per_node_map, "per-node get log infos");
  PROCESS(runningGetTrimPoint().map, "get log trim points");
  PROCESS(runningTrimRequests().map, "trim requests");
  PROCESS(runningLogRebuildings().map, "log rebuildings");
  PROCESS(runningSyncSequencerRequests().getList(), "sync sequencer requests");
  PROCESS(runningConfigurationFetches().map, "configuration fetch requests");
#undef PROCESS

  if (counts.empty()) {
    return false;
  }
  RATELIMIT_INFO(std::chrono::seconds(5),
                 5,
                 "Pending requests: %s",
                 folly::join(", ", counts).c_str());
  return true;
}

void Worker::popRecoveryRequest() {
  auto& metadataqueue_index =
      recoveryQueueMetaDataLog().q.get<LogIDUniqueQueue::FIFOIndex>();
  auto& dataqueue_index =
      recoveryQueueDataLog().q.get<LogIDUniqueQueue::FIFOIndex>();

  while (true) {
    auto& index =
        metadataqueue_index.size() > 0 ? metadataqueue_index : dataqueue_index;

    if (index.size() == 0) {
      return;
    }

    ld_check(index.size() > 0);
    auto& seqmap = processor_->allSequencers();

    auto it = index.begin();
    std::shared_ptr<Sequencer> seq = seqmap.findSequencer(*it);

    if (!seq) {
      // Sequencer went away.
      if (ld_catch(MetaDataLog::isMetaDataLog(*it) && err == E::NOSEQUENCER,
                   "INTERNAL ERROR: couldn't find sequencer for queued "
                   "recovery for log %lu: %s",
                   it->val_,
                   error_name(err))) {
        ld_info("No sequencer for queued recovery for log %lu", it->val_);
      }
      WORKER_STAT_INCR(recovery_completed);
    } else if (seq->startRecovery() != 0) {
      ld_error("Failed to start recovery for log %lu: %s",
               it->val_,
               error_description(err));
      return;
    }

    WORKER_STAT_DECR(recovery_enqueued);
    index.erase(it);
    return;
  }
}

ExponentialBackoffTimerNode* Worker::registerTimer(
    std::function<void(ExponentialBackoffTimerNode*)> callback,
    const chrono_expbackoff_t<ExponentialBackoffTimer::Duration>& settings) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      std::function<void()>(), settings);
  ExponentialBackoffTimerNode* node =
      new ExponentialBackoffTimerNode(std::move(timer));

  node->timer->setCallback([node, cb = std::move(callback)]() { cb(node); });

  timers_.push_back(*node);
  // The node (and timer) is semi-owned by the Worker now.  It can get deleted
  // by the caller; if not, it will get destroyed in our destructor.
  return node;
}

void Worker::disposeOfMetaReader(std::unique_ptr<MetaDataLogReader> reader) {
  ld_check(reader);
  if (shutting_down_) {
    // we are in the destructor of the Worker and members of the Worker may
    // have already gotten destroyed. destroy the reader directly and return.
    return;
  }

  // create the timer if not exist
  if (accepting_work_ && dispose_metareader_timer_ == nullptr) {
    dispose_metareader_timer_.reset(new Timer([this] {
      while (!finished_meta_readers_.empty()) {
        finished_meta_readers_.pop();
      }
    }));
  }

  reader->finalize();
  finished_meta_readers_.push(std::move(reader));
  if (accepting_work_ && dispose_metareader_timer_) {
    dispose_metareader_timer_->activate(std::chrono::milliseconds::zero());
  }
}

void Worker::reportLoad() {
  if (worker_type_ != WorkerType::GENERAL) {
    // specialised workers are not meant to take normal worker load,
    // therefore skipping it from load balancing calculations
    return;
  }

  using namespace std::chrono;
  auto now = steady_clock::now();

  struct rusage usage;
  int rv = getrusage(RUSAGE_THREAD, &usage);
  ld_check(rv == 0);

  int64_t now_load = (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000 +
      (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);

  if (last_load_ >= 0) {
    // We'll report load in CPU microseconds per wall clock second.  This
    // should be a number on the order of millions, which should be
    // comfortable for subsequent handling.
    int64_t load_delta = (now_load - last_load_) /
        duration_cast<duration<double>>(now - last_load_time_).count();

    PER_WORKER_STAT_ADD_SAMPLE(Worker::stats(), idx_, load_delta);

    ld_spew("%s reporting load %ld", getName().c_str(), load_delta);
    processor_->reportLoad(idx_, load_delta, worker_type_);
  }

  last_load_ = now_load;
  last_load_time_ = now;
  load_timer_->activate(seconds(10));
}

EventLogStateMachine* Worker::getEventLogStateMachine() {
  return event_log_;
}

void Worker::setEventLogStateMachine(EventLogStateMachine* event_log) {
  ld_check(event_log_ == nullptr);
  event_log_ = event_log;
  event_log_->setWorkerId(idx_);
}

void Worker::setLogsConfigManager(std::unique_ptr<LogsConfigManager> manager) {
  ld_check(logsconfig_manager_ == nullptr);
  logsconfig_manager_ = std::move(manager);
}

LogsConfigManager* Worker::getLogsConfigManager() {
  return logsconfig_manager_.get();
}

void Worker::setRebuildingCoordinator(
    RebuildingCoordinatorInterface* rebuilding_coordinator) {
  rebuilding_coordinator_ = rebuilding_coordinator;
}

void Worker::setClusterMaintenanceStateMachine(
    maintenance::ClusterMaintenanceStateMachine* sm) {
  cluster_maintenance_state_machine_ = sm;
}

ClusterState* Worker::getClusterState() {
  return Worker::onThisThread()->processor_->cluster_state_.get();
}

void Worker::onStoppedRunning(RunContext prev_context) {
  Worker* w = Worker::onThisThread();
  ld_check(w);
  std::chrono::steady_clock::time_point start_time;
  start_time = w->currentlyRunningStart_;

  setCurrentlyRunningContext(RunContext(), prev_context);

  auto end_time = w->currentlyRunningStart_;
  // Bumping the counters
  if (end_time - start_time >= settings().request_execution_delay_threshold) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      2,
                      "Slow request/timer callback: %.3fs, source: %s",
                      std::chrono::duration_cast<std::chrono::duration<double>>(
                          end_time - start_time)
                          .count(),
                      prev_context.describe().c_str());
    WORKER_STAT_INCR(worker_slow_requests);
  }

  auto usec = std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                    start_time)
                  .count();
  switch (prev_context.type_) {
    case RunContext::MESSAGE: {
      auto msg_type = static_cast<int>(prev_context.subtype_.message);
      ld_check(msg_type < static_cast<int>(MessageType::MAX));
      MESSAGE_TYPE_STAT_ADD(
          Worker::stats(), msg_type, message_worker_usec, usec);
      HISTOGRAM_ADD(Worker::stats(), message_callback_duration[msg_type], usec);
      break;
    }
    case RunContext::REQUEST: {
      int rqtype = static_cast<int>(prev_context.subtype_.request);
      ld_check(rqtype < static_cast<int>(RequestType::MAX));
      REQUEST_TYPE_STAT_ADD(Worker::stats(), rqtype, request_worker_usec, usec);
      HISTOGRAM_ADD(Worker::stats(), request_execution_duration[rqtype], usec);
      break;
    }
    case RunContext::STORAGE_TASK_RESPONSE: {
      int task_type = static_cast<int>(prev_context.subtype_.storage_task);
      ld_check(task_type < static_cast<int>(StorageTaskType::MAX));
      STORAGE_TASK_TYPE_STAT_ADD(
          Worker::stats(), task_type, storage_task_response_worker_usec, usec);
      HISTOGRAM_ADD(
          Worker::stats(), storage_task_response_duration[task_type], usec);
      break;
    }
    case RunContext::NONE: {
      REQUEST_TYPE_STAT_ADD(
          Worker::stats(), RequestType::INVALID, request_worker_usec, usec);
      HISTOGRAM_ADD(
          Worker::stats(),
          request_execution_duration[static_cast<int>(RequestType::INVALID)],
          usec);
      break;
    }
  }
}

void Worker::onStartedRunning(RunContext new_context) {
  setCurrentlyRunningContext(new_context, RunContext());
}

void Worker::activateIsolationTimer() {
  isolation_timer_->activate(immutable_settings_->isolated_sequencer_ttl);
}

void Worker::activateClusterStatePolling() {
  cluster_state_polling_->activate(std::chrono::seconds(600));
}

void Worker::deactivateIsolationTimer() {
  isolation_timer_->cancel();
}

void Worker::setCurrentlyRunningContext(RunContext new_context,
                                        RunContext prev_context) {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Attempting to set worker context while not on a worker. "
                    "New context: %s, expected context: %s.",
                    new_context.describe().c_str(),
                    prev_context.describe().c_str());
    ld_check(false);
    return;
  }
  ld_check(w->currentlyRunning_ == prev_context);
  w->currentlyRunning_ = new_context;
  w->currentlyRunningStart_ = std::chrono::steady_clock::now();
}

std::unique_ptr<MessageDispatch> Worker::createMessageDispatch() {
  // We create a vanilla MessageDispatch instance; ServerWorker and
  // ClientWorker create more specific ones
  return std::make_unique<MessageDispatch>();
}

// Stashes current RunContext and pauses its timer. Returns everything needed to
// restore it. Use it for nesting RunContexts.
std::tuple<RunContext, std::chrono::steady_clock::duration>
Worker::packRunContext() {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Attempting to pack worker context while not on a worker.");
    ld_check(false);
    return std::make_tuple(
        RunContext(), std::chrono::steady_clock::duration(0));
  }
  auto res = std::make_tuple(
      w->currentlyRunning_,
      std::chrono::steady_clock::now() - w->currentlyRunningStart_);
  w->currentlyRunning_ = RunContext();
  w->currentlyRunningStart_ = std::chrono::steady_clock::now();
  return res;
}

void Worker::unpackRunContext(
    std::tuple<RunContext, std::chrono::steady_clock::duration> s) {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Attempting to pack worker context while not on a worker.");
    ld_check(false);
    return;
  }
  ld_check(w->currentlyRunning_.type_ == RunContext::Type::NONE);
  w->currentlyRunning_ = std::get<0>(s);
  w->currentlyRunningStart_ = std::chrono::steady_clock::now() - std::get<1>(s);
}

//
// Pimpl getters
//

Sender& Worker::sender() const {
  return impl_->sender_;
}

LogRebuildingMap& Worker::runningLogRebuildings() const {
  return impl_->runningLogRebuildings_;
}

FindKeyRequestMap& Worker::runningFindKey() const {
  return impl_->runningFindKey_;
}

FireAndForgetRequestMap& Worker::runningFireAndForgets() const {
  return impl_->runningFireAndForgets_;
}

TrimRequestMap& Worker::runningTrimRequests() const {
  return impl_->runningTrimRequests_;
}

GetTrimPointRequestMap& Worker::runningGetTrimPoint() const {
  return impl_->runningGetTrimPoint_;
}

IsLogEmptyRequestMap& Worker::runningIsLogEmpty() const {
  return impl_->runningIsLogEmpty_;
}

DataSizeRequestMap& Worker::runningDataSize() const {
  return impl_->runningDataSize_;
}

GetHeadAttributesRequestMap& Worker::runningGetHeadAttributes() const {
  return impl_->runningGetHeadAttributes_;
}

GetClusterStateRequestMap& Worker::runningGetClusterState() const {
  return impl_->runningGetClusterState_;
}

LogsConfigApiRequestMap& Worker::runningLogsConfigApiRequests() const {
  return impl_->runningLogManagementReqs_;
}

LogsConfigManagerRequestMap& Worker::runningLogsConfigManagerRequests() const {
  return impl_->runningLogsConfigManagerReqs_;
}

LogsConfigManagerReplyMap& Worker::runningLogsConfigManagerReplies() const {
  return impl_->runningLogsConfigManagerReplies_;
}

AppenderMap& Worker::activeAppenders() const {
  return impl_->activeAppenders_;
}

AppendRequestMap& Worker::runningAppends() const {
  return impl_->runningAppends_;
}

CheckSealRequestMap& Worker::runningCheckSeals() const {
  return impl_->runningCheckSeals_;
}

ConfigurationFetchRequestMap& Worker::runningConfigurationFetches() const {
  return impl_->runningConfigurationFetches_;
}

ShapingContainer& Worker::readShapingContainer() const {
  return *impl_->read_shaping_container_;
}

GetSeqStateRequestMap& Worker::runningGetSeqState() const {
  return impl_->runningGetSeqState_;
}

GetLogInfoRequestMaps& Worker::runningGetLogInfo() const {
  return impl_->runningGetLogInfo_;
}

ClusterStateSubscriptionList& Worker::clusterStateSubscriptions() const {
  return impl_->clusterStateSubscriptions_;
}

AppenderBuffer& Worker::appenderBuffer() const {
  return impl_->appenderBuffer_;
}

AppenderBuffer& Worker::previouslyRedirectedAppends() const {
  return impl_->previously_redirected_appends_;
}

LogRecoveryRequestMap& Worker::runningLogRecoveries() const {
  return impl_->runningLogRecoveries_;
}

SyncSequencerRequestList& Worker::runningSyncSequencerRequests() const {
  return impl_->runningSyncSequencerRequests_;
}

LogIDUniqueQueue& Worker::recoveryQueueDataLog() const {
  return impl_->recoveryQueueDataLog_;
}

LogIDUniqueQueue& Worker::recoveryQueueMetaDataLog() const {
  return impl_->recoveryQueueMetaDataLog_;
}

ShardAuthoritativeStatusManager& Worker::shardStatusManager() const {
  return impl_->shardStatusManager_;
}

AllClientReadStreams& Worker::clientReadStreams() const {
  return impl_->clientReadStreams_;
}

WriteMetaDataRecordMap& Worker::runningWriteMetaDataRecords() const {
  return impl_->runningWriteMetaDataRecords_;
}

AppendRequestEpochMap& Worker::appendRequestEpochMap() const {
  return impl_->appendRequestEpochMap_;
}

CheckNodeHealthRequestSet& Worker::pendingHealthChecks() const {
  return impl_->pendingHealthChecks_;
}

GetEpochRecoveryMetadataRequestMap&
Worker::runningGetEpochRecoveryMetadata() const {
  return impl_->runningGetEpochRecoveryMetadata_;
}

SSLFetcher& Worker::sslFetcher() const {
  return impl_->sslFetcher_;
}

std::unique_ptr<SequencerBackgroundActivator>&
Worker::sequencerBackgroundActivator() const {
  return impl_->sequencerBackgroundActivator_;
}

const std::unordered_set<node_index_t>& Worker::getGraylistedNodes() const {
  return impl_->graylistingTracker_->getGraylistedNodes();
}

void Worker::resetGraylist() {
  return impl_->graylistingTracker_->resetGraylist();
}

std::string Worker::describeMyNode() {
  auto worker = Worker::onThisThread(false);
  if (!worker) {
    return "Can't find Processor";
  }
  return worker->processor_->describeMyNode();
}

// Per request common execute logic after request has been picked for execution.
void Worker::processRequest(std::unique_ptr<Request> rq) {
  using namespace std::chrono;

  ld_check(rq);
  if (UNLIKELY(!rq)) {
    RATELIMIT_CRITICAL(
        1s, 1, "INTERNAL ERROR: got a NULL request pointer from RequestPump");
    return;
  }

  RunContext run_context = rq->getRunContext();

  auto priority = rq->getExecutorPriority();

  auto queue_time{
      duration_cast<microseconds>(steady_clock::now() - rq->enqueue_time_)};
  if (queue_time > 20ms) {
    auto priority_to_str = [](int8_t priority) {
      switch (priority) {
        case folly::Executor::LO_PRI:
          return "LO_PRI";
        case folly::Executor::MID_PRI:
          return "MID_PRI";
        case folly::Executor::HI_PRI:
          return "HI_PRI";
      }
      return "";
    };
    RATELIMIT_WARNING(5s,
                      10,
                      "Request queued for %g msec: %s (id: %lu), p :%s",
                      queue_time.count() / 1000.0,
                      rq->describe().c_str(),
                      rq->id_.val(),
                      priority_to_str(priority));
  }

  Worker::onStartedRunning(run_context);

  // rq should not be accessed after execute, as it may have been deleted.
  Request::Execution status = rq->execute();

  switch (status) {
    case Request::Execution::COMPLETE:
      rq.reset();
      break;
    case Request::Execution::CONTINUE:
      rq.release();
      break;
  }

  Worker::onStoppedRunning(run_context);
  WORKER_STAT_INCR(worker_requests_executed);
}

int Worker::tryPost(std::unique_ptr<Request>& req) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return -1;
  }
  if (!req) {
    err = E::INVALID_PARAM;
    return -1;
  }

  if (num_requests_enqueued_.load(std::memory_order_relaxed) >
      updateable_settings_->worker_request_pipe_capacity) {
    err = E::NOBUFS;
    return -1;
  }

  return forcePost(req);
}

void Worker::add(folly::Func func) {
  addWithPriority(std::move(func), folly::Executor::LO_PRI);
}

void Worker::addWithPriority(folly::Func func, int8_t priority) {
  switch (priority) {
    case folly::Executor::HI_PRI:
      STAT_INCR(processor_->stats_, worker_enqueued_hi_pri_work);
      break;
    case folly::Executor::MID_PRI:
      STAT_INCR(processor_->stats_, worker_enqueued_mid_pri_work);
      break;
    case folly::Executor::LO_PRI:
      STAT_INCR(processor_->stats_, worker_enqueued_lo_pri_work);
      break;
    default:
      break;
  }

  num_requests_enqueued_.fetch_add(1, std::memory_order_relaxed);
  WorkContext::addWithPriority(
      [this,
       func = std::move(func),
       priority,
       enqueue_time = std::chrono::steady_clock::now()]() mutable {
        WorkerContextScopeGuard g(this);
        num_requests_enqueued_.fetch_sub(1, std::memory_order_relaxed);

        const auto queue_time =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - enqueue_time);

        HISTOGRAM_ADD(stats_, requests_queue_latency, queue_time.count());
        switch (priority) {
          case folly::Executor::HI_PRI:
            HISTOGRAM_ADD(stats_, hi_pri_requests_latency, queue_time.count());
            STAT_INCR(processor_->stats_, worker_executed_hi_pri_work);
            break;
          case folly::Executor::MID_PRI:
            HISTOGRAM_ADD(stats_, mid_pri_requests_latency, queue_time.count());
            STAT_INCR(processor_->stats_, worker_executed_mid_pri_work);
            break;
          case folly::Executor::LO_PRI:
            HISTOGRAM_ADD(stats_, lo_pri_requests_latency, queue_time.count());
            STAT_INCR(processor_->stats_, worker_executed_lo_pri_work);
            break;
          default:
            break;
        }
        func();
      },
      priority);
}

int Worker::forcePost(std::unique_ptr<Request>& req) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return -1;
  }

  if (!req) {
    err = E::INVALID_PARAM;
    return -1;
  }

  req->enqueue_time_ = std::chrono::steady_clock::now();
  auto priority = req->getExecutorPriority();
  folly::Func func = [rq = std::move(req), this]() mutable {
    processRequest(std::move(rq));
  };
  addWithPriority(std::move(func), priority);

  return 0;
}
}} // namespace facebook::logdevice
