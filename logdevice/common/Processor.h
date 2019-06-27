/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <folly/AtomicBitSet.h>
#include <folly/Function.h>
#include <folly/Memory.h>
#include <folly/memory/EnableSharedFromThis.h>

#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/work_model/WorkContext.h"
#include "logdevice/include/types.h"
// Think twice before adding new includes here!  This file is included in many
// translation units and increasing its transitive dependency footprint will
// slow down the build.  We use forward declaration and the pimpl idiom to
// offload most header inclusion to the .cpp file; scroll down for details.

namespace facebook { namespace logdevice {

namespace configuration { namespace nodes {
class NodesConfiguration;
class NodesConfigurationManager;
}} // namespace configuration::nodes

/**
 * @file a Processor is a pool of Worker threads running libevent 2.x
 *       event loops that collectively process Requests.
 */

class AllSequencers;
class AppendProbeController;
class ClientAPIHitsTracer;
class ClientIdxAllocator;
class ClusterState;
class EventLogRebuildingSet;
class EventLoop;
class PermissionChecker;
class PluginRegistry;
class PrincipalParser;
class ProcessorImpl;
class RebuildingSupervisor;
class Request;
class SequencerBatching;
class SequencerLocator;
class StatsHolder;
class TraceLogger;
class TrafficShaper;
class UpdateableConfig;
class UpdateableSecurityInfo;
class WatchDogThread;
class Worker;
class WheelTimer;
class Configuration;
enum class SequencerOptions : uint8_t;
using workers_t = std::vector<std::unique_ptr<Worker>>;

class Processor : public folly::enable_shared_from_this<Processor> {
 public:
  enum class Order { FORWARD, REVERSE, RANDOM };

  /**
   * Creates a new Processor.  You cannot use this directly, need to call the
   * create() factory method which performs two-step initialisation with init().
   *
   * @param cluster_config config to pass to Worker threads
   * @param Settings       common setting shared by all EventLoops in this
   *                       Processor and all the objects running on them.
   *                       Must be non-null. Is NOT owned by Processor.
   * @param rebuilding_mode             whether this Processor is part of
   *                                    rebuilding process
   * @param permission_checker          pointer to a PermissionChecker.
   *                                    Ownership is transfered to the processor
   * @param principal_parser            pointer to a PrinciplaParser. Ownership
   *                                    is transfered to the processor
   * @param stats                       object used to update various stat
   *                                    counters
   * @param sequencer_locator           used on clients and storage nodes to
   *                                    map from log ids to sequencer nodes
   * @param credentials                 an optional field used in the initial
   *                                    handshake of a Socket connection. Used
   *                                    only when the configuration file has set
   *                                    authentication_type to be
   *                                    "self_identification"
   */
  Processor(std::shared_ptr<UpdateableConfig> updateable_config,
            std::shared_ptr<TraceLogger> traceLogger,
            UpdateableSettings<Settings> settings,
            StatsHolder* stats,
            std::shared_ptr<PluginRegistry> plugin_registry,
            std::string credentials = "",
            std::string csid = "",
            std::string name = "logdevice",
            folly::Optional<NodeID> my_node_id = folly::none);

 protected:
  /**
   * Second step of construction, decoupled from constructor to allow
   * subclasses to override Worker construction.  Starts all worker threads.
   *
   * @throws ConstructorFailed if one or more EventLoops failed to start. err
   *         is set to NOMEM, SYSLIMIT or INTERNAL as defined for EventLoop
   *         constructor, or INVALID_PARAM if nworkers is outside [1,
   *         MAX_WORKERS]. err is set to INVALID_CONFIG if the
   *         PermissionChecker or PrincipalParser could not be created.
   */
  virtual void init();

  /**
   * Creates a worker pool of the supplied type and returns the vector
   */
  workers_t createWorkerPool(WorkerType type, size_t count);

  int postToWorker(std::unique_ptr<Request>& rq,
                   Worker& wh,
                   WorkerType worker_type,
                   worker_id_t worker_idx,
                   bool force);

  /**
   * Common implementation of postRequest(), postImportant() etc. Different
   * processors can decide to override this accordingly.
   */
  int postImpl(std::unique_ptr<Request>& rq,
               WorkerType worker_type,
               int target_thread,
               bool force);

  int blockingRequestImpl(std::unique_ptr<Request>& rq, bool force);

  /**
   * Decides which thread to send a request to.
   */
  int getTargetThreadForRequest(const std::unique_ptr<Request>& rq);

 public:
  /**
   * In production we actually create a subclass (ServerProcessor or
   * ClientProcessor) depending on the context, however some tests don't care
   * so we still allow creating a plain Processor.
   */
  template <typename... Args>
  static std::shared_ptr<Processor> create(Args&&... args) {
    auto p = std::make_shared<Processor>(std::forward<Args>(args)...);
    p->init();
    return p;
  }

  /**
   * A null constructor. Used for testing.
   */
  explicit Processor(std::shared_ptr<UpdateableConfig> updateable_config,
                     UpdateableSettings<Settings> settings,
                     bool fake_storage_node = false,
                     int max_logs = 1,
                     StatsHolder* stats = nullptr);

  virtual ~Processor();

  /**
   * @return the number of worker threads this Processor runs
   */
  virtual int getWorkerCount(WorkerType type) const;
  /**
   * Returns the total number of workers in all pools
   */
  int getAllWorkersCount() const;

  /**
   * Call the given function once for each Worker managed by this
   * Processor.
   *
   * @note Unlike postRequest, func runs in the current thread context
   *       and not "on the worker". Care should be taken to ensure any
   *       operations performed by func are thread safe.
   */
  void applyToWorkers(folly::Function<void(Worker&)> func,
                      Order order = Order::FORWARD);

  void applyToWorkerPool(folly::Function<void(Worker&)>&& func,
                         Order order = Order::FORWARD,
                         WorkerType worker_type = WorkerType::GENERAL) {
    applyToWorkerPool(func, order, worker_type);
  }

  void applyToWorkerPool(folly::Function<void(Worker&)>& func,
                         Order order = Order::FORWARD,
                         WorkerType worker_type = WorkerType::GENERAL);

  /**
   * Call the given function once with the worker_id_t of each Worker managed by
   * this Processor.
   *
   * @note Unlike postRequest, func runs in the current thread context
   *       and not "on the worker". Care should be taken to ensure any
   *       operations performed by func are thread safe.
   */
  template <typename F>
  void applyToWorkerIdxs(F func,
                         Order order = Order::FORWARD,
                         WorkerType type = WorkerType::GENERAL) {
    const int nworkers = getWorkerCount(type);
    switch (order) {
      default:
      case Order::FORWARD:
        for (worker_id_t idx{0}; idx.val_ < nworkers; ++idx.val_) {
          func(idx, type);
        }
        break;
      case Order::REVERSE:
        for (worker_id_t idx{nworkers - 1}; idx.val_ >= 0; --idx.val_) {
          func(idx, type);
        }
        break;
      case Order::RANDOM: {
        for (auto i : workerIdsRandomPermutation(type)) {
          func(worker_id_t(i), type);
        }
        break;
      }
    }
  }

  /**
   * Returns the reference to the worker instance with the given index and
   * worker pool.
   */
  Worker& getWorker(worker_id_t worker_id, WorkerType type);

  /**
   * Get all EventLoops owned by the Processor.
   * Used for tests.
   */
  std::vector<std::unique_ptr<EventLoop>>& getEventLoops();

  /**
   * Returns the refernce to an object which is responsible for timers
   */
  WheelTimer& getWheelTimer();

  /**
   * Schedules rq to run on one of the Workers managed by this Processor.
   *
   * @param rq  request to execute. Must not be nullptr. On success the
   *            function takes ownership of the object (and passes it on
   *            to a Worker). On failure ownership remains with the
   *            caller.
   *
   * @return 0 if request was successfully passed to a Worker thread for
   *           execution, -1 on failure. err is set to
   *     INVALID_PARAM  if rq is invalid
   *     NOBUFS         if too many requests are pending to be delivered to
   *                    Workers
   *     SHUTDOWN       Processor is shutting down
   */
  int postRequest(std::unique_ptr<Request>& rq);

  /**
   * The same as standard postRequest() but worker id is being
   * selected manually
   */
  int postRequest(std::unique_ptr<Request>& rq,
                  WorkerType worker_type,
                  int target_thread);

  /**
   * Runs a Request on one of the Workers, waiting for it to finish.
   *
   * For parameters and return values, see postRequest()/postImportant().
   */
  int blockingRequest(std::unique_ptr<Request>& rq);
  int blockingRequestImportant(std::unique_ptr<Request>& rq);

  /**
   * Similar to postRequest() but proceeds regardless of how many requests are
   * already pending on the worker (no NOBUFS error).
   *
   * This should be used when there is no avenue for pushback in case the
   * worker is overloaded.  A guideline: if the only way to handle NOBUFS from
   * the regular postRequest() would be to retry posting the request
   * individually, then using this instead is appropriate and more efficient.
   *
   * @param rq  request to execute. Must not be nullptr. On success the
   *            function takes ownership of the object (and passes it on
   *            to the Processor). On failure ownership remains with the
   *            caller.
   *
   * @return 0 if request was successfully passed to a Worker thread for
   *           execution, -1 on failure. err is set to
   *     INVALID_PARAM  if rq is invalid
   *     SHUTDOWN       this queue or processor_ is shutting down
   */
  int postImportant(std::unique_ptr<Request>& rq);
  int postImportant(std::unique_ptr<Request>& rq,
                    WorkerType worker_type,
                    int target_thread);
  // Older alias for postImportant()
  int postWithRetrying(std::unique_ptr<Request>& rq) {
    return postImportant(rq);
  }

  /**
   * Are we a storage node, able to store and deliver records?
   */
  virtual bool runningOnStorageNode() const {
    return fake_storage_node_;
  }

  /**
   * @return True if we should reject reads for shard `shard_idx` because
   *         there's evidence that some data in it was lost.
   *         (If we ship NO_RECORDS gaps for such shards, readers may
   *          erroneously report data loss.)
   *         We also reject writes for such shards.
   *         This flag is reset to false using markShardAsNotMissingData()
   *         after rebuilding finishes for this shard.
   */
  bool isDataMissingFromShard(uint32_t shard_idx);

  /**
   * @return  True if we should reject reads from down-rev clients for
   *          shard 'shard_idx' because some in-flight data for the shard
   *          may have been lost due to an unclean shutdown, and the client
   *          is unable to understand UNDER_REPLICATED gaps.
   *
   *          Dirty shards remain FULLY_AUTHORITATIVE and accept writes.
   *
   *          This flag is reset to false by markShardClean() either at
   *          startup once the RebuildingCoordinator determines that the
   *          shard is clean, or after rebuilding completes.
   */
  bool isShardDirty(uint32_t shard_idx);

  /**
   * Makes isDataMissingFromShard() return false for `shard_idx` from now on.
   * On a newly constructed Processor, isDataMissingFromShard() returns true
   * until this method is called.
   */
  void markShardAsNotMissingData(uint32_t shard_idx);

  /**
   * Makes isShardDirty() return false for `shard_idx` from now on.
   * On a newly constructed Processor, isShardDirty() returns true
   * until this method is called.
   */
  void markShardClean(uint32_t shard_idx);

  /**
   * Maps log ID to shard index. The mapping must be the same on all nodes.
   */
  int getShardForLog(logid_t log);

  /**
   * This MUST be called on ServerProcessor only, this lives here as an
   * transiet state until we have cleaner separation between server and
   * client code
   */
  virtual void getClusterDeadNodeStats(size_t* /* unused */,
                                       size_t* /* unused */) {
    // Not Supposed to be called here.
    std::abort();
  }

  /**
   * This MUST be called on ServerProcessor only, this lives here as an
   * transiet state until we have cleaner separation between server and
   * client code
   */
  virtual bool isNodeAlive(node_index_t /* unused */) const {
    // Not Supposed to be called here.
    std::abort();
    return false;
  }

  /**
   * This MUST be called on ServerProcessor only, this lives here as an
   * transient state until we have cleaner separation between server and
   * client code
   */
  virtual bool isNodeBoycotted(node_index_t /* unused */) const {
    // Not Supposed to be called here.
    std::abort();
    return false;
  }

  /**
   * This MUST be called on ServerProcessor only, this lives here as an
   * transiet state until we have cleaner separation between server and
   * client code
   */
  virtual bool isNodeIsolated() const {
    // Not Supposed to be called here.
    std::abort();
    return false;
  }

  /**
   * This MUST be called on ServerProcessor only, this lives here as an
   * transiet state until we have cleaner separation between server and
   * client code
   */
  virtual bool isFailureDetectorRunning() const {
    return false;
  }

  /**
   * Set the NodesConfigurationManager instance by having Processor owns its
   * reference. Should only be called once immediately after Processor creation.
   */
  void setNodesConfigurationManager(
      std::shared_ptr<configuration::nodes::NodesConfigurationManager> ncm);
  /**
   * Get the NodesConfiguration updated by NodesConfigurationManager.
   * Note: only used during NCM migration period, will be removed later
   */
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfigurationFromNCMSource() const;

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfigurationFromServerConfigSource() const;

  /**
   * @return  NodesConfiguraton object of the cluster, depending on
   *          processor settings, the source can be server config or NCM.
   */
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  /**
   * Get the NodesConfigurationManager instance that implements
   * NodesConfigurationAPI. When NCM is enabled in settings, should always
   * return a valid pointer after Processor init workflow in client/server.
   */
  configuration::nodes::NodesConfigurationManager*
  getNodesConfigurationManager();

  const std::shared_ptr<TraceLogger> getTraceLogger() const;

  std::shared_ptr<Configuration> getConfig();

  std::shared_ptr<UpdateableConfig> config_;

  /**
   * Get resource token for a message coming into the system. If the token is
   * not granted the message is not read from evbuffer and enqueued into the
   * task queue for processing.
   * This is a way of pushing back clients. It also restricts memory
   * consumption.
   */
  ResourceBudget::Token getIncomingMessageToken(size_t payload_size);

 private:
  // Make runningOnStorageNode() return true. Used for tests.
  bool fake_storage_node_ = false;

  // global settings shared by this Processor, EventLoops it manages, and
  // all objects running on those EventLoops
  UpdateableSettings<Settings> settings_;

  // This should be destroyed before settings_, since it accesses that.
  UpdateableSettings<Settings>::SubscriptionHandle settingsUpdateHandle_;

  std::shared_ptr<PluginRegistry> plugin_registry_;

 public:
  StatsHolder* stats_;

  friend class ProcessorImpl;
  std::unique_ptr<ProcessorImpl> impl_;

  // Pointer to the object responsible for requesting rebuilding. Unowned.
  // nullptr if we're not a server or not a storage node.
  RebuildingSupervisor* rebuilding_supervisor_ = nullptr;

  // Maintains state of nodes in the cluster
  std::unique_ptr<ClusterState> cluster_state_;

  std::shared_ptr<configuration::nodes::NodesConfigurationManager> ncm_;

  // Tracks append success and failures on the client, controlling the sending
  // of probes to save bandwidth
  AppendProbeController& appendProbeController() const;

  // a map from log ids to Sequencer objects owned by this Processor that
  // manage append requests on those logs.
  AllSequencers& allSequencers() const;

  // UpdateableSecurityInfo owned by the processor
  // encapsulates PrincipalParser and PermissionChecker
  std::unique_ptr<UpdateableSecurityInfo> security_info_;

  // Object used on clients and storage nodes to find which node runs the
  // sequencer for a particular log.
  std::unique_ptr<SequencerLocator> sequencer_locator_;

  // Orchestrates bandwidth policy and bandwidth releases to the Senders
  // in each Worker.
  std::unique_ptr<TrafficShaper> traffic_shaper_;

  // A thread running on server side to detect worker stalls
  std::unique_ptr<WatchDogThread> watchdog_thread_;

  // ResourceBudget used to limit the total number of accepted connections.
  // See Settings::max_incoming_connections_.
  ResourceBudget conn_budget_incoming_;

  // Limits accepted and established connections originating from clients.
  // See Settings::max_external_connections_.
  ResourceBudget conn_budget_external_;

  // Current rebuilding set. Unlike Worker::shard_status_, this is updated
  // immediately after receiving an event log record, without any grace period.
  // nullptr on server means that we haven't yet caught up on the event log.
  // If it's non-nullptr we don't have any guarantees about how up-to-date it
  // is either, but it's not likely to be far behind.
  UpdateableSharedPtr<EventLogRebuildingSet> rebuilding_set_;

  /**
   * Tracer for API calls.
   */
  std::unique_ptr<ClientAPIHitsTracer> api_hits_tracer_;

  // The credentials used in HELLO message if the authentication_type specified
  // in the configuration file is set to "self_identification"
  const std::string HELLOCredentials_;

  // CSID (Client Session ID) used to uniquely identify client sessions
  const std::string csid_;
  /**
   * Stops any running threads.  Normally exercised through the destructor but
   * also if the constructor fails or explicitly during shutdown.
   */
  void shutdown();

  /**
   * Called by a Worker during shutdown informing the Processor that it had
   * completed all pending requests and closed sockets.
   */
  void noteWorkerQuiescent(worker_id_t worker_id, WorkerType type);

  /**
   * Call by a shutdown thread to wait until all Workers process their active
   * requests (i.e. call noteWorkerQuiescent()) before resuming. Optional
   * argument can be used to wait only for a subset of workers.
   */
  void waitForWorkers(size_t nworkers = 0);

  /**
   * Issues sequential read stream IDs when read streams are started
   * (client/reader-side), to ensure IDs are unique across workers.
   */
  read_stream_id_t issueReadStreamID() {
    return read_stream_id_t(next_read_stream_id_.fetch_add(1));
  }

  buffered_writer_id_t issueBufferedWriterID() {
    return buffered_writer_id_t(next_buffered_writer_id_++);
  }

  /**
   * Selects a worker based on hash of the given `seed` value.
   * Seed can be e.g. request ID or a random number.
   */
  worker_id_t selectWorkerRandomly(uint64_t seed, WorkerType type);

  /**
   * Selects a worker to process a Request when the Request owner does not
   * care.  Load-aware. This only works for WorkerType::GENERAL worker-pool
   */
  worker_id_t selectWorkerLoadAware();

  /**
   * Proxy for WorkerLoadBalancing::reportLoad(), used by Worker to report
   * load.
   */
  virtual void reportLoad(worker_id_t idx,
                          int64_t load,
                          WorkerType worker_type);

  SequencerBatching& sequencerBatching();

  const std::string& getName() {
    return name_;
  }

  UpdateableSettings<Settings> updateableSettings() const {
    return settings_;
  }

  std::shared_ptr<const Settings> settings() const {
    return settings_.get();
  }

  /**
   * True if a valid NodeID was passed to the Processor during construction.
   */
  bool hasMyNodeID() const;

  /**
   * Returns the NodeID of the server that we are running. The NodeID needs
   * to have been previously passed to the constructor of the Processor.
   *
   * If the NodeID is not set (e.g on clients), it will cause ld_check failure.
   */
  NodeID getMyNodeID() const;

  /**
   * Returns the NodeID of the server that we are running on if it's set.
   */
  folly::Optional<NodeID> getOptionalMyNodeID() const;

  ClientIdxAllocator& clientIdxAllocator() const;

  std::shared_ptr<PluginRegistry> getPluginRegistry() {
    return plugin_registry_;
  }

  // Run the given function on whichever background thread gets to it first.
  // The three methods of enqueueing are just wrappers on the write*() methods
  // of MPMCQueue.  Briefly, "Blocking()" will block indefinitely until there's
  // room in the queueu, "IfNotFull()" will try very hard to enqueue right away
  // but won't block if the queue is full, and the one with no suffix will
  // enqueue if it can without blocking.  If you don't have a specific need for
  // IfNotFull, use the no suffix version.
  void enqueueToBackgroundBlocking(folly::Function<void()> fn);

  bool enqueueToBackground(folly::Function<void()> fn);

  bool enqueueToBackgroundIfNotFull(folly::Function<void()> fn);

  // For debugging.  I can't think of a good way to distinguish different client
  // instances.  Even the Processor's "this" pointer won't do: although there's
  // a 1:1 correspondence between Client and Processor, two different Processors
  // in two different processes can have the same pointer.  So I just return
  // "Client" in that case.
  std::string describeMyNode() const;

  void setServerInstanceId(ServerInstanceId serverInstanceId) {
    serverInstanceId_ = serverInstanceId;
  }

  ServerInstanceId getServerInstanceId() const {
    return serverInstanceId_;
  }

  bool isShuttingDown() const {
    return shutting_down_.load();
  }

  bool isInitialized() const {
    return initialized_.load(std::memory_order_relaxed);
  }

  bool isLogsConfigLoaded() const;

  // Reset the sequencer batching object. Not safe to use when there are appends
  // inflight. Should ONLY be used in testing.
  void
  setSequencerBatching(std::unique_ptr<SequencerBatching> sequencer_batching);

 private:
  const std::shared_ptr<TraceLogger> trace_logger_;

  // Semaphore used during shutdown. Workers post() on it once they've finished
  // processing all outstanding requests, while the shutdown thread wait()-s on
  // it once for each worker thread.
  Semaphore shutdown_sem_;

  // Used to ensure shutdown() is only called once.
  std::atomic<bool> shutting_down_{false};

  std::atomic<bool> allow_post_during_shutdown_{false};

  // Used to detect that we are in a test environment without a
  // fully initialized processor;
  std::atomic<bool> initialized_{false};

  // Keeps track of those workers that called noteWorkerFinished()
  std::array<folly::AtomicBitSet<MAX_WORKERS>,
             static_cast<uint8_t>(WorkerType::MAX)>
      workers_finished_;

  // Next ID for issueReadStreamID()
  std::atomic<read_stream_id_t::raw_type> next_read_stream_id_{1};

  // Next ID for issueBufferedWriterID()
  std::atomic<buffered_writer_id_t::raw_type> next_buffered_writer_id_{1};

  // See isDataMissingFromShard().
  folly::AtomicBitSet<MAX_SHARDS> shards_not_missing_data_;

  // See isShardDirty().
  folly::AtomicBitSet<MAX_SHARDS> clean_shards_;

  // BufferedWriter for batching by sequencers.  Initialized only on servers.
  std::unique_ptr<SequencerBatching> sequencer_batching_;

  std::string name_;

  const folly::Optional<NodeID> my_node_id_;

  ServerInstanceId serverInstanceId_{ServerInstanceId_INVALID};

  /**
   * Subclasses override to create the correct Worker subclasses;
   * ServerProcessor creates ServerWorker instances, ClientProcessor creates
   * ClientWorker.
   * Accepts KeepAlive of the executor on which the worker will execute most of
   * it's CPU logic.
   */
  virtual Worker* createWorker(WorkContext::KeepAlive executor,
                               worker_id_t,
                               WorkerType type);

  bool validateFn(const folly::Function<void()>& fn);

  std::vector<int> workerIdsRandomPermutation(WorkerType type);

  // Copying this from settings on creation, since this number is accessed a
  // lot (e.g. to route every request), and going through UpdateableSettings
  // and FastUpdateableSharedPtr and shared_ptr machinery every time we need it
  // is relatively expensive
  size_t num_general_workers_{0};
};

}} // namespace facebook::logdevice
