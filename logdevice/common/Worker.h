/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include <folly/IntrusiveList.h>
#include <folly/Random.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/io/async/Request.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/RunContext.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/TimeoutMap.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/common/work_model/SerialWorkContext.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"
// Think twice before adding new includes here!  This file is included in many
// translation units and increasing its transitive dependency footprint will
// slow down the build.  We use forward declaration and the pimpl idiom to
// offload most header inclusion to the .cpp file; scroll down for details.

namespace testing {
// test classes that will have access to this class' internals
struct SocketConnectRequest;
} // namespace testing

// helper macros to increment/decrement stats on worker threads
// NOTE: Also need to include Stats.h to use them; this header doesn't to
// reduce transitive dependencies
#define WORKER_STAT_ADD(name, val) STAT_ADD(Worker::stats(), name, val)
#define WORKER_STAT_SUB(name, val) STAT_SUB(Worker::stats(), name, val)
#define WORKER_STAT_INCR(name) STAT_INCR(Worker::stats(), name)
#define WORKER_STAT_DECR(name) STAT_DECR(Worker::stats(), name)
#define WORKER_STAT_SET(name, val) STAT_SET(Worker::stats(), name, val)

// helper macros to increment/decrement per worker stats on worker threads
// ignore stale logs and logs with empty group name. The versions prefixed with
// "LOG_STAT_" can be used to make the code compatible with unit tests where
// there is actually no worker thread.
// Otherwise, the versions prefixed with "WORKER_LOG_STAT_" are more convenient.
// This is a no-op if per-log configuration is not available locally which may
// be the case in clients.
#define LOG_STAT_ADD(stats, cluster_config, log_id, name, val)       \
  do {                                                               \
    const auto config_ = cluster_config;                             \
    if (config_ && config_->logsConfig()->isLocal()) {               \
      const auto log_path_opt_ = config_->getLogGroupPath(log_id);   \
      if (log_path_opt_) {                                           \
        LOG_GROUP_STAT_ADD(stats, log_path_opt_.value(), name, val); \
      }                                                              \
    }                                                                \
  } while (0)

#define LOG_STAT_SUB(stats, cluster_config, log_id, name, val) \
  LOG_STAT_ADD(stats, cluster_config, log_id, name, -(val))
#define LOG_STAT_INCR(stats, cluster_config, log_id, name) \
  LOG_STAT_ADD(stats, cluster_config, log_id, name, 1)
#define LOG_STAT_DECR(stats, cluster_config, log_id, name) \
  LOG_STAT_SUB(stats, cluster_config, log_id, name, 1)

#define WORKER_LOG_STAT_ADD(log_id, name, val) \
  LOG_STAT_ADD(Worker::stats(), Worker::getConfig(false), log_id, name, val)
#define WORKER_LOG_STAT_SUB(log_id, name, val) \
  WORKER_LOG_STAT_ADD(log_id, name, -(val))
#define WORKER_LOG_STAT_INCR(log_id, name) WORKER_LOG_STAT_ADD(log_id, name, 1)
#define WORKER_LOG_STAT_DECR(log_id, name) WORKER_LOG_STAT_SUB(log_id, name, 1)

#define WORKER_TRAFFIC_CLASS_STAT_ADD(traffic_class, name, val) \
  TRAFFIC_CLASS_STAT_ADD(Worker::stats(), traffic_class, name, val)
#define WORKER_TRAFFIC_CLASS_STAT_SUB(traffic_class, name, val) \
  WORKER_TRAFFIC_CLASS_STAT_ADD(traffic_class, name, -(val))
#define WORKER_TRAFFIC_CLASS_STAT_INCR(traffic_class, name) \
  WORKER_TRAFFIC_CLASS_STAT_ADD(traffic_class, name, 1)
#define WORKER_TRAFFIC_CLASS_STAT_DECR(traffic_class, name) \
  WORKER_TRAFFIC_CLASS_STAT_SUB(traffic_class, name, 1)

namespace facebook { namespace logdevice {

/**
 * @file A Worker executes LogDevice requests, keeps track of active Request
 *       objects, and manages a collection of Sockets. All LogDevice requests
 *       are executed on Executor threads. Worker objects directly receive and
 *       execute requests from client threads, the listener thread, and the
 *       command port thread. These other threads use Processor object to
 *       pass the requests to a Worker.
 */

class AllClientReadStreams;
class AppenderBuffer;
class BufferedWriterShard;
class ClusterState;
class Configuration;
class EpochRecovery;
class EventLogStateMachine;
class GetSeqStateRequestMap;
class LogRebuildingInterface;
class LogStorageState;
class LogsConfig;
class LogsConfigManager;
class MessageDispatch;
class MetaDataLogReader;
class Mutator;
class Processor;
class RebuildingCoordinatorInterface;
class Request;
class SSLFetcher;
class Sender;
class SequencerBackgroundActivator;
class ServerConfig;
class ShapingContainer;
class ShardAuthoritativeStatusManager;
class SocketCallback;
class StatsHolder;
class SyncSequencerRequestList;
class TraceLogger;
class UpdateableConfig;
class WorkerImpl;
class WorkerTimeoutStats;

namespace maintenance {
class ClusterMaintenanceStateMachine;
}

struct AppendRequestEpochMap;
struct AppendRequestMap;
struct AppenderMap;
struct CheckNodeHealthRequestSet;
struct CheckSealRequestMap;
struct ConfigurationFetchRequestMap;
struct ClusterStateSubscriptionList;
struct DataSizeRequestMap;
struct ExponentialBackoffTimerNode;
struct FindKeyRequestMap;
struct FireAndForgetRequestMap;
struct GetClusterStateRequestMap;
struct GetEpochRecoveryMetadataRequestMap;
struct GetHeadAttributesRequestMap;
struct GetLogInfoRequestMaps;
struct GetTrimPointRequestMap;
struct IsLogEmptyRequestMap;
struct LogIDUniqueQueue;
struct LogRebuildingMap;
struct LogRecoveryRequestMap;
struct LogsConfigApiRequestMap;
struct LogsConfigManagerReplyMap;
struct LogsConfigManagerRequestMap;
struct MUTATED_Header;
struct TrimRequestMap;
struct WriteMetaDataRecordMap;

namespace configuration {
class ZookeeperConfig;
namespace nodes {
class NodesConfiguration;
}
} // namespace configuration

template <typename Duration>
class ChronoExponentialBackoffAdaptiveVariable;

class Worker : public WorkContext {
 public:
  /**
   * Creates and starts a Worker thread.
   *
   * @param processor         processor that created this Worker, required.
   * @param idx               see .idx_
   * @param config            cluster configuration that Sockets and other
   *                          objects running on this Worker thread will use
   *
   * @throws ConstructorFailed on error, sets err to
   *   NOMEM    if a libevent call failed because malloc() failed
   *   INTERNAL if a libevent call fails unexpectedly
   *   SYSLIMIT if system limits on thread stack sizes or total number of
   *            threads (if any) are reached
   *
   * NOTE: since pthread_create() is called in the parent (EventLoop)
   *       constructor, a Worker potentially may start receiving requests
   *       before the thread that is running its constructor is done
   *       initializing Worker fields (sender(), etc), which happen after
   *       EventLoop constructor returns control. The code creating Workers
   *       must take this into account. For now we are safe because Processor
   *       creates all Workers before it starts sending them any requests.
   */
  Worker(WorkContext::KeepAlive event_loop,
         Processor* processor,
         worker_id_t idx,
         const std::shared_ptr<UpdateableConfig>& config,
         StatsHolder* stats = nullptr,
         WorkerType worker_type = WorkerType::GENERAL,
         ThreadID::Type thread_type = ThreadID::UNKNOWN_WORKER);
  ~Worker() override;

  static std::string getName(WorkerType type, worker_id_t idx);

  static std::string makeThreadName(Processor* processor,
                                    WorkerType type,
                                    worker_id_t idx);

  /**
   * @return the identifier of this worker, in format W<type><idx>, e.g. "WG13".
   */
  std::string getName() const {
    return getName(worker_type_, idx_);
  }

  /**
   * @return struct event_base of the EventLoop associated with this worker.
   * Deprecated. It is advised that new code should not use this.
   */
  struct event_base* getEventBase() {
    auto event_loop = checked_downcast<EventLoop*>(getExecutor());
    return event_loop->getEventBase();
  }

  /**
   * @return Thread id of thread that executes work given to this worker.
   * Deprecated. new code should not use this.
   */
  int getThreadId() {
    auto event_loop = checked_downcast<EventLoop*>(getExecutor());
    return event_loop->getThreadId();
  }

  /**
   * @return cluster configuration object cached on this Worker and
   *         auto updated
   */
  std::shared_ptr<Configuration> getConfiguration() const;

  /**
   * @return server configuration object cached on this Worker and
   *         auto updated
   */
  std::shared_ptr<ServerConfig> getServerConfig() const;

  /**
   * @return  NodesConfiguraton object cached on this worker, depending on
   *          processor settings, the source can be server config or NCM.
   */
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  /**
   * get the NodesConfiguration updated by NodesConfigurationManager.
   * Note: only used during NCM migration period, will be removed later
   */
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfigurationFromNCMSource() const;

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfigurationFromServerConfigSource() const;

  /**
   * @return logs configuration object cached on this Worker and
   *         auto updated
   */
  std::shared_ptr<LogsConfig> getLogsConfig() const;

  /**
   * @return zookeeper configuration object cached on this Worker and
   *         auto updated
   */
  std::shared_ptr<configuration::ZookeeperConfig> getZookeeperConfig() const;

  /**
   * Gets the UpdateableConfig object - used to force a reload of the config
   */
  std::shared_ptr<UpdateableConfig> getUpdateableConfig();

  /**
   * @return   whether the destruction of this Worker has started
   */
  bool shuttingDown() const {
    return shutting_down_;
  }

  /**
   * @param enforce_worker  set this to false if you're fine with getting back
   *                        a nullptr if this isn't called on a worker thread.
   * @return                a pointer to the Worker object running on this
   *                        thread, or nullptr if this thread is not running a
   *                        Worker.
   */
  static Worker* onThisThread(bool enforce_worker = true);

  static void onSet(Worker* w) {
    on_this_thread_ = w;
  }

  static void onUnset() {
    on_this_thread_ = nullptr;
  }

  /**
   * @return   a pointer to the stats object of the Worker running on this
   *           thread
   */
  static StatsHolder* stats() {
    Worker* w = Worker::onThisThread(false);
    return w ? w->stats_ : nullptr;
  }

  // Slightly faster than the static stats() because it doesn't look up the
  // thread-local Worker. Use on very hot paths.
  StatsHolder* getStats() {
    return stats_;
  }

  static ClusterState* getClusterState();

  /**
   * This is a convenience function that objects running on a Worker can
   * use to get the cluster config without getting their Worker object first.
   *
   * @return  if this function is called on a Worker thread, return that
   *          Worker's config. Otherwise assert and crash.
   */
  static std::shared_ptr<Configuration> getConfig(bool enforce_worker = true) {
    Worker* w = Worker::onThisThread(enforce_worker);
    return w ? w->getConfiguration() : nullptr;
  }

  // Parent pointer to the owning processor. Requests running on a Worker can
  // call back into the processor to hand off a request to another Worker.
  Processor* const processor_;

  // configuration settings to use
  UpdateableSettings<Settings> updateable_settings_;

  // local copy of last settings fetched
  std::shared_ptr<const Settings> immutable_settings_;

  const std::shared_ptr<TraceLogger> getTraceLogger() const;

  // Socket close callbacks for each storage node used by PeriodicReleases.
  // These are used to invalidate last released lsn to a storage node
  // when the connection b/w sequencer and storage node breaks.
  std::vector<std::unique_ptr<SocketCallback>>
      periodic_releases_socket_callbacks_;

  // unique numeric index that Processor assigned to this Worker. 0-based.
  // This can be used in Request::getThreadAffinity() to send a Request to a
  // specific Worker.
  const worker_id_t idx_;

  // The worker type of this particular worker. This defines at which worker
  // pool this worker lives.
  const WorkerType worker_type_{WorkerType::GENERAL};

  // MessageDispatch instance which the Socket layer uses to dispatch message
  // events.  Subclasses of Worker can override createMessageDispatch() to
  // make this a MessageDispatch subclass.  Intentionally here to outlive
  // `impl_' (Sender, importantly).
  std::unique_ptr<MessageDispatch> message_dispatch_;

  // ID used to fetch worker reference when work is posted and scheduled on some
  // thread.
  constexpr static folly::StringPiece kWorkerDataID{"WORKER"};

  // Set of active BufferedWriterShard instances on this worker.  They are
  // managed by BufferedWriter.
  // Values are raw pointers so that a forward declaration suffices and we
  // don't have to pull a file from lib/ in Worker.cpp.
  // Needs to outlive impl_->runningAppends_ because buffered writer append
  // callbacks access this map.
  using ActiveBufferedWritersMap =
      std::unordered_map<buffered_writer_id_t,
                         BufferedWriterShard*,
                         buffered_writer_id_t::Hash>;
  ActiveBufferedWritersMap active_buffered_writers_;

  // Pimpl, contains most of the objects we provide getters for
  friend class WorkerImpl;
  std::unique_ptr<WorkerImpl> impl_;

  // An interface for sending Messages on this Worker. This object owns all
  // Sockets on this Worker.
  Sender& sender() const;

  // a map of all currently running LogRebuildings.
  LogRebuildingMap& runningLogRebuildings() const;

  // a map of all currently running FindKeyRequests
  FindKeyRequestMap& runningFindKey() const;

  // a map of all currently running FireAndForgetRequest
  FireAndForgetRequestMap& runningFireAndForgets() const;

  // a map of all currently running TrimRequests
  TrimRequestMap& runningTrimRequests() const;

  // a map of all currently running GetTrimPointRequest
  GetTrimPointRequestMap& runningGetTrimPoint() const;

  // a map of all currently running IsLogEmptyRequests
  IsLogEmptyRequestMap& runningIsLogEmpty() const;

  // a map of all currently running dataSizeRequests
  DataSizeRequestMap& runningDataSize() const;

  // a map of all currently running GetHeadAttributesRequest
  GetHeadAttributesRequestMap& runningGetHeadAttributes() const;

  // a map of all currently running GetClusterStateRequests
  GetClusterStateRequestMap& runningGetClusterState() const;

  // a map for all currently running LogsConfigApiRequest
  LogsConfigApiRequestMap& runningLogsConfigApiRequests() const;

  // Internal LogsConfigManager requests map
  LogsConfigManagerRequestMap& runningLogsConfigManagerRequests() const;

  // Internal LogsConfigManager reply map
  LogsConfigManagerReplyMap& runningLogsConfigManagerReplies() const;

  // a map of GetEpochRecoveryMetadataRequest that are running on this
  // worker
  GetEpochRecoveryMetadataRequestMap& runningGetEpochRecoveryMetadata() const;

  // a map of all currently running ByteOffsetRequests.
  // ByteOffsetRequestMap runningByteOffset_;

  // a map of all currently running AppendRequests
  AppendRequestMap& runningAppends() const;

  // a map of all currently running CheckSealRequest
  CheckSealRequestMap& runningCheckSeals() const;
  ShapingContainer& readShapingContainer() const;

  ConfigurationFetchRequestMap& runningConfigurationFetches() const;

  // a map of all currently running GetSeqStateRequests
  GetSeqStateRequestMap& runningGetSeqState() const;

  // a map of all currently active Appenders created by this Worker
  AppenderMap& activeAppenders() const;

  // a map of all currently running GetLogInfoRequests
  GetLogInfoRequestMaps& runningGetLogInfo() const;

  // A list of clients that have subscribed to be notified when the config
  // is updated
  std::set<ClientID> configChangeSubscribers_;

  // list of subscriptions to cluster state changes
  ClusterStateSubscriptionList& clusterStateSubscriptions() const;

  // We keep track of the total size of allocated Appenders together with
  // corresponding APPEND_Messages and payloads.
  // Note: this is an atomic because Appenders can be destroyed on another
  // Worker thread than the worker thread that created it.
  // This is used for soft and hard limit on the total size of appenders.
  // When the soft limit is reached Appenders will stop going into the Leader
  // Redundancy stage. When the hard limit is reached Sequencers will start
  // rejecting appends.
  std::atomic<size_t> totalSizeOfAppenders_{0};

  // A per-logid buffer for incoming appenders while sequencers are being
  // reactivated (e.g., getting a new epoch due to ESN exhaustion)
  AppenderBuffer& appenderBuffer() const;

  // If we get an append that was previously sent to another sequencer, and has
  // an LSN from that sequencer, we don't know whether recovery will replicate &
  // release it.  So we hold on to it until recovery is done, and only execute
  // the append if it wasn't recovered.  This buffer holds those redirected
  // appends during recovery.
  AppenderBuffer& previouslyRedirectedAppends() const;

  // a map of all LogRecoveryRequests currently running (active) on this Worker
  LogRecoveryRequestMap& runningLogRecoveries() const;

  // a list of all SyncSequencerRequests currently running on this Worker.
  SyncSequencerRequestList& runningSyncSequencerRequests() const;

  // If a recovery ID is present in this map, MUTATED_Message will call the
  // corresponding callback instead of looking up an epoch recovery through
  // runningLogRecoveries().map. If someone who is not an EpochRecovery sends a
  // MUTATE_Message, it can use this map to get a response.
  using MutationCallback =
      std::function<void(const MUTATED_Header&, ShardID from)>;
  std::unordered_map<recovery_id_t, MutationCallback, recovery_id_t::Hash>
      customMutationCallbacks_;

  // Only a limited number of LogRecoveryRequest objects can be active on a
  // Worker at the same time. For others, we add their log ids to this queue
  // and kick off recovery once the size of runningLogRecoveries().map drops
  // below Settings::recovery_requests_per_worker. A hashed_unique index is used
  // to ensure that we never add the same log more than once to the queue.
  // Currently we prioritize recovery for metadata logs over recovery for data
  // logs, so two queues are maintained for data logs and metadata logs,
  // respectively.
  LogIDUniqueQueue& recoveryQueueDataLog() const;
  LogIDUniqueQueue& recoveryQueueMetaDataLog() const;

  // EventLogStateMachine only exists on Worker 0 of a server. It provides an
  // interface for listening to updates arriving on the event log.
  EventLogStateMachine* event_log_{nullptr};

  // LogsConfigManager owns the replicated state machine LogsConfigStateMachine
  // and ensures that UpdatebleLogsConfig gets updated when we have a new
  // version of logs config. only exists on Worker 0 of a server. It
  // provides an interface to the RSM controlling LogsConfig
  std::unique_ptr<LogsConfigManager> logsconfig_manager_;

  // Only set on the first worker thread of a server node.
  // Owned by Server.
  RebuildingCoordinatorInterface* rebuilding_coordinator_{nullptr};

  // Set on the worker running this state machine
  // The object itself is owned by server
  maintenance::ClusterMaintenanceStateMachine*
      cluster_maintenance_state_machine_{nullptr};

  // A wrapper around ShardAuthoritativeStatusMap which maps a
  // (node_index_t, uint32_t) to a AuthoritativeStatus.
  // This map is used by Recovery and ClientReadStream state machines to adjust
  // their fmajority detection mechanisms based on the authoritativeness of the
  //   shards that are part of the nodeset they are working on.
  ShardAuthoritativeStatusManager& shardStatusManager() const;

  AllClientReadStreams& clientReadStreams() const;

  // a map of running WriteMetaDataRecord state machines, noted that we store
  // raw pointers in the map. The state machine is owned by their parent driver,
  // MetaDataLogWriter, which guarantees that it can outlive Workers and its
  // child state machine is removed from this map before destruction.
  WriteMetaDataRecordMap& runningWriteMetaDataRecords() const;

  // For each log, contains the largest epoch number of a record that was
  // successfully appended by this Worker thread to that log. Included in
  // subsequent append requests to prevent out-of-order LSN assignment.
  AppendRequestEpochMap& appendRequestEpochMap() const;

  // Outstanding health check requests
  CheckNodeHealthRequestSet& pendingHealthChecks() const;

  // SSL context fetcher, used to refresh certificate data
  SSLFetcher& sslFetcher() const;

  // Sequencer background activator, only runs on one worker
  std::unique_ptr<SequencerBackgroundActivator>&
  sequencerBackgroundActivator() const;

  /**
   * Returns true if there are still some requests pending on this Worker.
   */
  bool requestsPending() const;

  /**
   * Picks a log from recoveryQueue_ and posts a LogRecoveryRequest for it.
   * Called when another LogRecoveryRequest is finished executing.
   */
  void popRecoveryRequest();

  /**
   * If false, incoming communication that causes new work to be queued will be
   * ignored. This is used during shutdown to allow workers to finish existing
   * work without taking more.
   */
  bool isAcceptingWork() const {
    return accepting_work_;
  }

  /**
   * Sets accepting_work_ to false.
   * Should be called only from a worker thread. Used during shutdown.
   */
  void stopAcceptingWork();

  /**
   * This method asynchronously destroys all read streams and flushes output
   * buffers once there are no more requests in flight (@see requestsPending()).
   * This method is used during shutdown. Once invoked it is going to be
   * executed recursively every PENDING_REQUESTS_POLL_DELAY milliseconds using
   * timer until there's no more work pending and output evbuffers are empty.
   * Then it calls Processor::noteWorkerQuiescent().
   */
  void finishWorkAndCloseSockets();

  /**
   * This method is called to abort pending work if it does finish by itself in
   * given amount of time after first invocation of finishWorkAndCloseSockets.
   */
  void forceAbortPendingWork();

  /**
   * This method closes all the sockets during shutdown if the sockets don't
   * drain in given time.
   */
  void forceCloseSockets();

  virtual void subclassFinishWork() {}
  virtual void subclassWorkFinished() {}

  /**
   * This method finds the oldest running recovery request
   * and update a stat.
   */
  void reportOldestRecoveryRequest();

  /**
   * @return if there is an active Mutator (see Mutator.h) on this Worker for
   *         a log record identified by @param rid, returns a pointer to that
   *         Mutator. Otherwise returns nullptr.
   */
  Mutator* findMutator(RecordID rid) const;

  /**
   * @return if this Worker is running a LogRecoveryRequest for @param
   *         log, return a pointer to its currently active EpochRecovery
   *         machine (there will be always be one). Otherwise return nullptr.
   */
  EpochRecovery* findActiveEpochRecovery(logid_t logid) const;

  /**
   * A utility method to register a new ExponentialBackoffTimer with this
   * Worker. The newly created timer is owned by this Worker and is destroyed
   * during its shutdown.
   *
   * @return Pointer to the created ExponentialBackoffTimerNode object.
   * Deleting the node object cancels the timer.
   */
  ExponentialBackoffTimerNode* registerTimer(
      std::function<void(ExponentialBackoffTimerNode*)> callback,
      const chrono_expbackoff_t<ExponentialBackoffTimer::Duration>& settings);

  ExponentialBackoffTimerNode* registerTimer(
      std::function<void(ExponentialBackoffTimerNode*)> callback,
      ExponentialBackoffTimer::Duration initial_delay,
      ExponentialBackoffTimer::Duration max_delay,
      ExponentialBackoffTimer::Duration::rep multiplier = chrono_expbackoff_t<
          ExponentialBackoffTimer::Duration>::DEFAULT_MULTIPLIER) {
    return registerTimer(std::move(callback),
                         chrono_expbackoff_t<ExponentialBackoffTimer::Duration>(
                             initial_delay, max_delay, multiplier));
  }

  /**
   * Take the ownership of a MetaDataLogReader object and dispose it later.
   * Can be used when owner of a MetaDataLogReader object wants to destroy the
   * object but it is in the context of its metadata callback.
   */
  void disposeOfMetaReader(std::unique_ptr<MetaDataLogReader> reader);

  EventLogStateMachine* getEventLogStateMachine();

  /**
   * Register the given EventLogStateMachine to this Worker.
   */
  void setEventLogStateMachine(EventLogStateMachine* event_log);

  /**
   * Pass ownership of the LogsConfigManager to this Worker.
   */
  void setLogsConfigManager(std::unique_ptr<LogsConfigManager> manager);

  LogsConfigManager* getLogsConfigManager();

  /**
   * Register a RebuildingCoordinator to the Worker thread on which it runs.
   */
  void setRebuildingCoordinator(
      RebuildingCoordinatorInterface* rebuilding_coordinator);

  /**
   * Register ClusterMaintenanceStateMachine to the worker thread on which
   * it runs.
   */
  void setClusterMaintenanceStateMachine(
      maintenance::ClusterMaintenanceStateMachine* sm);

  // The currently running request / message callback
  RunContext currentlyRunning_;

  // Time when currentlyRunning_ was set
  std::chrono::steady_clock::time_point currentlyRunningStart_;

  // This should be called whenever the ServerConfig  has been updated.
  // Has to be called from the worker thread
  virtual void onServerConfigUpdated();
  // This should be called whenever the LogsConfig gets updated.
  // Has to be called from the worker thread
  virtual void onLogsConfigUpdated();
  // This should be called whenever the NodesConfiguration gets updated.
  // Has to be called from the worker thread.
  // There's not gurantee that it will be called only once per config change.
  // Subscribers should react to the change in config, not to the fact this
  // function is called.
  virtual void onNodesConfigurationUpdated();

  // Sets currently running request. Verifies that we are on a worker and that
  // the current context is NONE
  static void onStartedRunning(RunContext new_context);

  // Resets the currently running context to NONE and bumps counters for
  // prev_context
  static void onStoppedRunning(RunContext prev_context);

  // Packs the current RunContext and returns it along with how long it ran for,
  // sets the current RunContext to NONE
  static std::tuple<RunContext, std::chrono::steady_clock::duration>
  packRunContext();

  // Unpacks the given RunContext, sets the current worker's RunContext to the
  // one supplied and adds the supplied duration to the duration of the current
  // RunContext.
  static void unpackRunContext(
      std::tuple<RunContext, std::chrono::steady_clock::duration> s);

  // For debugging.
  static std::string describeMyNode();

  /**
   * @return a pointer to the Settings object of the calling thread's EventLoop.
   *         assert and crash if the calling thread is not running an EventLoop.
   */
  static const Settings& settings();

  // This overrides EventLoop::onSettingsUpdated() but calls it first thing
  virtual void onSettingsUpdated();

  void activateIsolationTimer();

  // This ensures that ClusterState is refreshed periodically.
  void activateClusterStatePolling();

  void deactivateIsolationTimer();

  WorkerTimeoutStats& getWorkerTimeoutStats() {
    return *worker_timeout_stats_;
  }

  const std::unordered_set<node_index_t>& getGraylistedNodes() const;
  void resetGraylist();

  // Common request processing logic, after request is picked up by the worker
  // or cputhreadpool for execution.
  void processRequest(std::unique_ptr<Request> req);
  // Methods used by processor to post requests to worker.
  int tryPost(std::unique_ptr<Request>& req);

  // Sets up WorkerContext so that executor can save it before posting the
  // function for execution.
  // Add lo_pri work into the executor.
  void add(folly::Func func) override;

  // Add to the executor with a given priority.
  void addWithPriority(folly::Func func, int8_t priority) override;

  /**
   * Post a request into Worker for execution. The post cannot fail with NOBUFS.
   * The priority of the request is embedded in the request instance. As of
   * today, high priority is not implemented any different than low priority.
   * The only difference between the two is high priority queue depth is small
   * compared to more general low priority queue. A new request getting
   * scheduled as high priority should not delay other tasks like storage
   * responses and timer callbacks who are the primary consumers of this
   * mechanism.
   *
   * @return 0 if a request was posted. Otherwise it returns -1 and err contains
   * the actual error_code.
   */
  int forcePost(std::unique_ptr<Request>& req);

  virtual void setupWorker();

  // Execution probability distribution of different tasks. Hi Priority tasks
  // are called such because they have a higher chance of getting executed.
  static constexpr int kHiPriTaskExecDistribution = 70;
  static constexpr int kMidPriTaskExecDistribution = 0;
  static constexpr int kLoPriTaskExecDistribution = 30;

 private:
  // Periodically called to report load to Processor for load-aware work
  // assignment
  void reportLoad();

  void disableSequencersDueIsolationTimeout();

  // Initializes subscriptions to config and setting updates
  void initializeSubscriptions();

  // Helper used by onStartedRunning() and onStoppedRunning()
  static void setCurrentlyRunningContext(RunContext new_context,
                                         RunContext prev_context);

  // Subclasses can override to create a MessageDispatch subclass during
  // initialisation
  virtual std::unique_ptr<MessageDispatch> createMessageDispatch();
  // Called during shutdown when there are no more pending requests; allows
  // subclasses to clear state machines that were waiting for that
  virtual void noteShuttingDownNoPendingRequests() {}

  std::shared_ptr<UpdateableConfig> config_; // cluster config to use for
  // all ops on this thread

  // Handles for our subscriptions to config updates, used in destructor to
  // unsubscribe
  ConfigSubscriptionHandle server_config_update_sub_;
  ConfigSubscriptionHandle logs_config_update_sub_;
  ConfigSubscriptionHandle nodes_configuration_update_sub_;

  // subscription for changes in UpdateableSettings<Settings>
  UpdateableSettings<Settings>::SubscriptionHandle
      updateable_settings_subscription_;

  // Used to update counters (e.g. number of connections, etc.)
  StatsHolder* stats_;

  // Storage for getTimeoutCommon().
  mutable struct timeval get_common_tv_buf_;

  // Methods of Sockets and other objects managed by this Worker check
  // this to see if they are executing in a Worker destructor where
  // some members of Worker may have already been destroyed.
  bool shutting_down_;

  // See isAcceptingWork()
  bool accepting_work_;

  static thread_local Worker* on_this_thread_;

  // timer that checks requestsPending() == 0 and !sender().isConnected() and
  // calls Processor's noteWorkerQuiescent() once both are true.
  std::unique_ptr<Timer> requests_pending_timer_;

  // Force abort pending requests. This is a derived timer based on
  // requests_pending_timer to force abort the requests.
  // The shutdown code in finishWorkAndCloseSockets tries to
  // shutdown everything and waits for everything running on the worker to
  // quiesce. At some point the work happening on the worker needs to stop so
  // that we are still within the shutdown timeout budget. This variable keeps
  // track of how many times we tried to quiesce ( approx 20s ) before shutting
  // down work forcefully.
  size_t force_abort_pending_requests_counter_{0};

  // Close open sockets. This is a derived timer based on requests_pending_timer
  // to close sockets if they don't drain in given time. The shutdown code
  // starts flush on all sockets. If the sockets are taking too long to drain,
  // instead of allowing them to continue forever, close the sockets right away.
  // This is initialized once we start waiting for sockets to close.
  size_t force_close_sockets_counter_{0};

  // timer that checks stuck requests and update counter if there are some
  // stuck requests
  std::unique_ptr<Timer> requests_stuck_timer_;

  // true iff we've called flushOutputAndClose() on sender().
  bool waiting_for_sockets_to_close_{false};

  // used to dispose concluded MetaDataLogReader objects and their associated
  // ClientReadStream objects
  std::unique_ptr<Timer> dispose_metareader_timer_;

  // finished MetaDataLogReader objects to be disposed
  std::queue<std::unique_ptr<MetaDataLogReader>> finished_meta_readers_;

  // List of timers associated with this Worker object. Owned by Worker so
  // that they're destroyed when the worker thread is shutting down. Note that
  // ExponentialBackoffTimerNode objects are not owned by this list (it's an
  // intrusive list), so care should be taken to delete the elements after
  // unlinking and during destruction of this Worker object.
  folly::IntrusiveList<ExponentialBackoffTimerNode,
                       &ExponentialBackoffTimerNode::list_hook>
      timers_;

  // Timer for the worker to periodically report its load, and associated
  // state required to calculate recent load. See reportLoad().
  std::unique_ptr<Timer> load_timer_;
  int64_t last_load_ = -1;
  std::chrono::steady_clock::time_point last_load_time_;
  std::unique_ptr<Timer> isolation_timer_;

  std::unique_ptr<Timer> cluster_state_polling_;

  std::unique_ptr<WorkerTimeoutStats> worker_timeout_stats_;

  // Counts the number of requests enqueued into the Worker for processing.
  // Used to return NOBUFS when count goes above worker_request_pipe_capacity.
  std::atomic<size_t> num_requests_enqueued_{0};

  // Stop on EventLogStateMachine should only be called once.
  // Set to true once stop has been called
  bool event_log_stopped_{false};

  friend struct ::testing::SocketConnectRequest;
};

class WorkerContextScopeGuard {
 public:
  WorkerContextScopeGuard(const WorkerContextScopeGuard&) = delete;
  WorkerContextScopeGuard& operator=(const WorkerContextScopeGuard&) = delete;
  WorkerContextScopeGuard(WorkerContextScopeGuard&&) = delete;
  WorkerContextScopeGuard& operator=(WorkerContextScopeGuard&&) = delete;

  explicit WorkerContextScopeGuard(Worker* w) {
    unset_context_ = w && w != Worker::onThisThread(false /*enforce_worker*/);
    if (unset_context_) {
      Worker::onSet(w);
    }
  }

  ~WorkerContextScopeGuard() {
    if (unset_context_) {
      Worker::onUnset();
    }
  }

 private:
  // This guard unsets context only if it was the one which set the context.
  bool unset_context_{false};
};
}} // namespace facebook::logdevice
