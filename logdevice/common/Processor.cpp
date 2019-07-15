/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Processor.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include <folly/Conv.h>
#include <folly/Hash.h>
#include <folly/MPMCQueue.h>
#include <folly/Memory.h>
#include <folly/container/Array.h>
#include <folly/synchronization/CallOnce.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/AppendProbeController.h"
#include "logdevice/common/ClientAPIHitsTracer.h"
#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/EventLoopTaskQueue.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodesConfigurationPublisher.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/SequencerBatching.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Thread.h"
#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/TrafficShaper.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/WatchDogThread.h"
#include "logdevice/common/WheelTimer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerLoadBalancing.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/SequencerLocatorFactory.h"
#include "logdevice/common/plugin/StaticPluginLoader.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace {

using namespace facebook::logdevice;

class BackgroundThread : public Thread {
 public:
  // "num" is an arbitrary number for the thread, only used in threadName().
  BackgroundThread(Processor* processor, int num)
      : processor_(processor), num_(num), stats_(processor->stats_) {}

  // Can't move or copy this once the thread is running.
  BackgroundThread(const BackgroundThread&) = delete;
  BackgroundThread& operator=(const BackgroundThread&) = delete;

  BackgroundThread(BackgroundThread&&) = delete;
  BackgroundThread& operator=(BackgroundThread&&) = delete;

 protected:
  // Main thread loop.
  void run() override;

  std::string threadName() override {
    return "ld:bckgnd" + folly::to<std::string>(num_);
  }

 private:
  Processor* const processor_;
  const int num_;
  StatsHolder* stats_;
};

} // anonymous namespace

namespace facebook { namespace logdevice {

// This pimpl class is a container for all classes that would normally be
// members of Processor but we don't want to have to include them in
// Processor.h.
class ProcessorImpl {
 public:
  ProcessorImpl(Processor* processor,
                UpdateableSettings<Settings> settings,
                std::shared_ptr<TraceLogger> trace_logger)
      : append_probe_controller_(std::chrono::seconds(10)), // TODO configurable
        worker_load_balancing_(settings->num_workers),
        incoming_message_budget_(
            settings->inline_message_execution
                ? std::numeric_limits<uint64_t>::max()
                : settings->incoming_messages_max_bytes_limit),
        background_init_flag_(),
        background_queue_(),
        nc_publisher_(processor->config_, settings, std::move(trace_logger)) {}

  ~ProcessorImpl() {
    for (auto& workers : all_workers_) {
      for (auto& worker : workers) {
        WorkerContextScopeGuard g(worker.get());
        worker.reset();
      }
    }
  }

  WheelTimer wheel_timer_;
  AppendProbeController append_probe_controller_;
  WorkerLoadBalancing worker_load_balancing_;
  ClientIdxAllocator client_idx_allocator_;
  ResourceBudget incoming_message_budget_;

  // for lazy init of background queue and threads
  folly::once_flag background_init_flag_;
  std::vector<std::unique_ptr<BackgroundThread>> background_threads_;
  // An empty function means "exit the thread."
  folly::MPMCQueue<folly::Function<void()>> background_queue_;
  NodesConfigurationPublisher nc_publisher_;
  std::vector<std::unique_ptr<EventLoop>> ev_loops_;
  std::unique_ptr<AllSequencers> allSequencers_;
  std::array<workers_t, static_cast<size_t>(WorkerType::MAX)> all_workers_;
  // If anything depends on worker make sure that it is deleted in the
  // destructor above.
};

namespace {
void settingsUpdated(const UpdateableSettings<Settings>& settings) {
  bool ch = settings->abort_on_failed_check;
  bool ca = settings->abort_on_failed_catch;
  if (dbg::abortOnFailedCheck.exchange(ch) != ch) {
    ld_info("abort-on-failed-check is %s", ch ? "on" : "off");
  }
  if (dbg::abortOnFailedCatch.exchange(ca) != ca) {
    ld_info("abort-on-failed-catch is %s", ca ? "on" : "off");
  }
}

std::unique_ptr<SequencerLocator>
get_sequencer_locator(std::shared_ptr<PluginRegistry> plugin_registry,
                      std ::shared_ptr<UpdateableConfig> config) {
  auto plugin = plugin_registry->getSinglePlugin<SequencerLocatorFactory>(
      PluginType::SEQUENCER_LOCATOR_FACTORY);
  if (plugin) {
    return (*plugin)(std::move(config));
  } else {
    return std::make_unique<HashBasedSequencerLocator>();
  }
}

} // namespace

Processor::Processor(std::shared_ptr<UpdateableConfig> updateable_config,
                     std::shared_ptr<TraceLogger> trace_logger,
                     UpdateableSettings<Settings> settings,
                     StatsHolder* stats,
                     std::shared_ptr<PluginRegistry> plugin_registry,
                     std::string credentials,
                     std::string csid,
                     std::string name,
                     folly::Optional<NodeID> my_node_id)
    :

      config_(std::move(updateable_config)),
      settings_(settings),
      plugin_registry_(std::move(plugin_registry)),
      stats_(stats),
      impl_(new ProcessorImpl(this, settings, trace_logger)),
      sequencer_locator_(get_sequencer_locator(plugin_registry_, config_)),
      conn_budget_incoming_(settings_->max_incoming_connections),
      conn_budget_external_(settings_->max_external_connections),
      api_hits_tracer_(std::make_unique<ClientAPIHitsTracer>(trace_logger)),
      HELLOCredentials_(settings_->server ? Principal::CLUSTER_NODE
                                          : std::move(credentials)),
      csid_(std::move(csid)),
      trace_logger_(trace_logger),
      name_(name),
      my_node_id_(std::move(my_node_id)) {
  settingsUpdateHandle_ = settings_.callAndSubscribeToUpdates(
      std::bind(settingsUpdated, settings_));
}

void Processor::init() {
  auto local_settings = settings_.get();
  ld_check(local_settings);
  // RWTicketSpinLockT type in LogStorageState, EpochRecordCache and
  // PartitionedRocksDBStore have to be modified if MAX_WORKERS > 255
  ld_check(MAX_WORKERS <= 255);

  if (local_settings->num_workers < 1 ||
      local_settings->num_workers > MAX_WORKERS) {
    ld_error("tried to create Processor with %d workers, max is %zu",
             local_settings->num_workers,
             MAX_WORKERS);
    err = E::INVALID_PARAM;
    throw ConstructorFailed();
  }

  num_general_workers_ = local_settings->num_workers;

  auto config = config_->get();
  // It's important to set the initial cluster state before we start the
  // workers since some of the timers in the worker (NodeStatsController) will
  // try to read it soon enough, we don't want to have a time race.
  cluster_state_ = std::make_unique<ClusterState>(
      getNodesConfiguration()->getMaxNodeIndex() + 1,
      this,
      *getNodesConfiguration()->getServiceDiscovery());

  for (int i = 0; i < numOfWorkerTypes(); i++) {
    WorkerType worker_type = workerTypeByIndex(i);
    auto count = getWorkerCount(worker_type);
    workers_t worker_pool = createWorkerPool(worker_type, count);
    ld_info(
        "Initialized %d workers of type %s", count, workerTypeStr(worker_type));
    impl_->all_workers_[i] = std::move(worker_pool);
  }

  impl_->allSequencers_ =
      std::make_unique<AllSequencers>(this, config_, settings_);

  security_info_ =
      std::make_unique<UpdateableSecurityInfo>(this, settings_->server);

  if (settings_->server) {
    traffic_shaper_ = std::make_unique<TrafficShaper>(this, stats_);

    if (getWorkerCount(WorkerType::GENERAL) != 0) {
      watchdog_thread_ =
          std::make_unique<WatchDogThread>(this,
                                           settings_->watchdog_poll_interval_ms,
                                           settings_->watchdog_bt_ratelimit);
    }

    // Now that workers are running, we can initialize SequencerBatching
    // (which waits for all workers to process a Request).  It would be nice
    // to do this lazily only when sequencer batching is actually on, however
    // because it needs to talk to all workers and wait for replies, it would
    // be suspect to deadlocks.
    sequencer_batching_.reset(new SequencerBatching(this));
  } else {
    // in the context of clients, the processor triggers a cluster state refresh
    // so that appends can be routed to nodes that are alive from the start
    if (settings_->enable_initial_get_cluster_state) {
      cluster_state_->refreshClusterStateAsync();
    }
  }

  initialized_.store(true, std::memory_order_relaxed);
}

workers_t Processor::createWorkerPool(WorkerType type, size_t count) {
  auto local_settings = settings_.get();
  ld_check(local_settings);
  workers_t workers;
  workers.reserve(count);

  for (int i = 0; i < count; i++) {
    // increment the next worker idx
    std::unique_ptr<Worker> worker;
    try {
      auto& loops = impl_->ev_loops_;
      loops.emplace_back(std::make_unique<EventLoop>(
          Worker::makeThreadName(this, type, worker_id_t(i)),
          ThreadID::CPU_EXEC,
          local_settings->worker_request_pipe_capacity,
          local_settings->enable_executor_priority_queues,
          folly::make_array<uint32_t>(
              local_settings->hi_requests_per_iteration,
              local_settings->mid_requests_per_iteration,
              local_settings->lo_requests_per_iteration)));
      auto executor = folly::getKeepAliveToken(loops.back().get());
      worker.reset(createWorker(std::move(executor), worker_id_t(i), type));
    } catch (ConstructorFailed&) {
      shutdown();
      throw ConstructorFailed();
    }
    ld_check(worker);
    workers.push_back(std::move(worker));
  }
  return workers;
}

// Testing Constructor
Processor::Processor(std::shared_ptr<UpdateableConfig> updateable_config,
                     UpdateableSettings<Settings> settings,
                     bool fake_storage_node,
                     int /*max_logs*/,
                     StatsHolder* stats)
    : config_(std::move(updateable_config)),
      fake_storage_node_(fake_storage_node),
      settings_(settings),
      plugin_registry_(std::make_shared<PluginRegistry>(
          createAugmentedCommonBuiltinPluginVector<StaticPluginLoader>())),
      stats_(stats),
      impl_(new ProcessorImpl(this,
                              settings,
                              std::make_shared<NoopTraceLogger>(config_))),
      conn_budget_incoming_(settings_.get()->max_incoming_connections),
      conn_budget_external_(settings_.get()->max_external_connections) {
  ld_check(settings.get());
  num_general_workers_ = settings_->num_workers;
}

Processor::~Processor() {
  shutdown();
}

int Processor::getWorkerCount(WorkerType type) const {
  switch (type) {
    case WorkerType::GENERAL:
      return num_general_workers_;
    default:
      return 0;
  }
}

int Processor::getAllWorkersCount() const {
  size_t nworkers = 0;
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    nworkers += getWorkerCount(workerTypeByIndex(i));
  }
  return nworkers;
}

worker_id_t Processor::selectWorkerRandomly(uint64_t seed, WorkerType type) {
  auto count = getWorkerCount(type);
  ld_check_gt(count, 0);
  return worker_id_t(folly::hash::twang_mix64(seed) % count);
}

int Processor::getTargetThreadForRequest(const std::unique_ptr<Request>& rq) {
  WorkerType worker_type = rq->getWorkerTypeAffinity();
  const int nworkers = getWorkerCount(worker_type);
  int target_thread = rq->getThreadAffinity(nworkers);
  ld_check_gt(nworkers, 0);
  // target_thread can be > nworkers if the target thread is gossip or admin
  // api worker
  ld_check_ge(target_thread, -1);

  // If the Request does not care about which thread it runs on, schedule it
  // round-robin.
  if (target_thread == -1) {
    target_thread = selectWorkerRandomly(rq->id_.val(), worker_type).val_;
  }
  return target_thread;
}

WheelTimer& Processor::getWheelTimer() {
  return impl_->wheel_timer_;
}

Worker& Processor::getWorker(worker_id_t worker_id, WorkerType worker_type) {
  ld_check(worker_id.val() >= 0);
  ld_check(worker_id.val() < getWorkerCount(worker_type));
  auto& workers = impl_->all_workers_[static_cast<uint8_t>(worker_type)];
  return *workers[worker_id.val()].get();
}

std::vector<std::unique_ptr<EventLoop>>& Processor::getEventLoops() {
  return impl_->ev_loops_;
}

void Processor::applyToWorkers(folly::Function<void(Worker&)> func,
                               Processor::Order order) {
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    WorkerType type = workerTypeByIndex(i);
    applyToWorkerPool(func, order, type);
  }
}

void Processor::applyToWorkerPool(folly::Function<void(Worker&)>& func,
                                  Processor::Order order,
                                  WorkerType worker_type) {
  applyToWorkerIdxs(
      [&func, this](
          worker_id_t idx, WorkerType type) { func(getWorker(idx, type)); },
      order,
      worker_type);
}

int Processor::postToWorker(std::unique_ptr<Request>& rq,
                            Worker& w,
                            WorkerType worker_type,
                            worker_id_t worker_idx,
                            bool force) {
  auto type = rq->type_;
  const int rv = force ? w.forcePost(rq) : w.tryPost(rq);
  // rq is (hopefully) now invalid!  Don't dereference!
  Request::bumpStatsWhenPosted(stats_, type, worker_type, worker_idx, rv == 0);
  return rv;
}

int Processor::postImpl(std::unique_ptr<Request>& rq,
                        WorkerType worker_type,
                        int target_thread,
                        bool force) {
  int nworkers = getWorkerCount(worker_type);

  if (folly::kIsDebug) {
    // Assert that we're not misdirecting the request
    int rq_thread_affinity = rq->getThreadAffinity(nworkers);
    ld_check(rq_thread_affinity == -1 || rq_thread_affinity == target_thread);
  }

  // Catch attempts to route outside of the worker index space (e.g. to
  // the Gossip thread.
  if (target_thread >= nworkers) {
    err = E::INVALID_PARAM;
    return -1;
  }
  auto& w = getWorker(worker_id_t(target_thread), worker_type);
  return postToWorker(rq, w, worker_type, worker_id_t(target_thread), force);
}

int Processor::postRequest(std::unique_ptr<Request>& rq,
                           WorkerType worker_type,
                           int target_thread) {
  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return -1;
  }
  return postImpl(rq, worker_type, target_thread, /* force */ false);
}

int Processor::postRequest(std::unique_ptr<Request>& rq) {
  return postRequest(
      rq, rq->getWorkerTypeAffinity(), getTargetThreadForRequest(rq));
}

int Processor::blockingRequestImpl(std::unique_ptr<Request>& rq, bool force) {
  Semaphore sem;
  rq->setClientBlockedSemaphore(&sem);

  int rv = force ? postImportant(rq) : postRequest(rq);
  if (rv != 0) {
    rq->setClientBlockedSemaphore(nullptr);
    return rv;
  }

  // Block until the Request has completed
  sem.wait();
  return 0;
}

int Processor::blockingRequest(std::unique_ptr<Request>& rq) {
  return blockingRequestImpl(rq, false);
}
int Processor::blockingRequestImportant(std::unique_ptr<Request>& rq) {
  return blockingRequestImpl(rq, true);
}

bool Processor::isDataMissingFromShard(uint32_t shard_idx) {
  return !shards_not_missing_data_[shard_idx];
}

void Processor::markShardAsNotMissingData(uint32_t shard_idx) {
  if (!shards_not_missing_data_.set(shard_idx)) {
    // The shard wasn't already marked as rebuilt.
    PER_SHARD_STAT_DECR(stats_, shard_missing_all_data, shard_idx);
  }
}

bool Processor::isShardDirty(uint32_t shard_idx) {
  return !clean_shards_[shard_idx];
}

void Processor::markShardClean(uint32_t shard_idx) {
  if (!clean_shards_.set(shard_idx)) {
    // The shard wasn't already marked as clean.
    PER_SHARD_STAT_DECR(stats_, shard_dirty, shard_idx);
  }
}

void Processor::setNodesConfigurationManager(
    std::shared_ptr<configuration::nodes::NodesConfigurationManager> ncm) {
  ncm_ = std::move(ncm);
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Processor::getNodesConfiguration() const {
  return config_->getNodesConfiguration();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Processor::getNodesConfigurationFromNCMSource() const {
  return config_->getNodesConfigurationFromNCMSource();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Processor::getNodesConfigurationFromServerConfigSource() const {
  return config_->getNodesConfigurationFromServerConfigSource();
}

configuration::nodes::NodesConfigurationManager*
Processor::getNodesConfigurationManager() {
  return ncm_.get();
}

const std::shared_ptr<TraceLogger> Processor::getTraceLogger() const {
  return trace_logger_;
}

void Processor::shutdown() {
  if (shutting_down_.exchange(true)) {
    // already called
    return;
  }
  allow_post_during_shutdown_.store(true);

  if (security_info_) { // can be nullptr in tests
    security_info_->shutdown();
  }

  if (sequencer_batching_) {
    // Shut down SequencerBatching to abort all outstanding batched appends and
    // send replies to the client (this requires talking to all workers)
    sequencer_batching_->shutDown();
  }

  if (cluster_state_) {
    cluster_state_->shutdown();
  }

  // First get the pthread_t for all running worker threads
  std::vector<std::thread*> event_threads;
  event_threads.reserve(getAllWorkersCount());

  if (traffic_shaper_) {
    traffic_shaper_->shutdown();
  }

  if (watchdog_thread_) {
    watchdog_thread_->shutdown();
  }

  if (impl_->allSequencers_) {
    impl_->allSequencers_->shutdown();
  }

  if (ncm_) {
    ncm_->shutdown();
  }

  // Shutdown wheeltimer so that before shutting down executor threads.
  impl_->wheel_timer_.shutdown();

  // Processor will now stop allowing any requests to workers.
  allow_post_during_shutdown_.store(false);
  // Tell all Workers to shut down and terminate their threads. This
  // also alters WorkerHandles so that further attempts to post
  // requests through them fail with E::SHUTDOWN.
  for (auto& loop : impl_->ev_loops_) {
    loop->getTaskQueue().shutdown();
    event_threads.push_back(&loop->getThread());
  }

  for (size_t i = 0; i < impl_->background_threads_.size(); ++i) {
    impl_->background_queue_.blockingWrite(folly::Function<void()>());
  }

  // Join all the pthreads to complete shutdown. This is necessary because
  // EventLoops have a pointer to this Processor, so they have to finish
  // before we're destroyed.
  for (auto& event_thread : event_threads) {
    ld_check(event_thread->joinable());
    event_thread->join();
  }

  for (auto& background_thread : impl_->background_threads_) {
    background_thread->join();
  }
}

void Processor::noteWorkerQuiescent(worker_id_t worker_id, WorkerType type) {
  bool val = workers_finished_[workerIndexByType(type)].set(
      static_cast<int>(worker_id));
  if (!val) {
    shutdown_sem_.post();
  }
}

void Processor::waitForWorkers(size_t nworkers) {
  if (nworkers == 0) {
    nworkers = getAllWorkersCount();
  }

  while (nworkers-- > 0) {
    shutdown_sem_.wait();
  }
}

worker_id_t Processor::selectWorkerLoadAware() {
  return impl_->worker_load_balancing_.selectWorker();
}

void Processor::reportLoad(worker_id_t idx,
                           int64_t load,
                           WorkerType worker_type) {
  ld_check(worker_type == WorkerType::GENERAL);
  impl_->worker_load_balancing_.reportLoad(idx, load);
}

int Processor::postImportant(std::unique_ptr<Request>& rq) {
  return postImportant(
      rq, rq->getWorkerTypeAffinity(), getTargetThreadForRequest(rq));
}

int Processor::postImportant(std::unique_ptr<Request>& rq,
                             WorkerType worker_type,
                             int target_thread) {
  if (shutting_down_.load() && !allow_post_during_shutdown_) {
    err = E::SHUTDOWN;
    return -1;
  }
  return postImpl(rq, worker_type, target_thread, /* force */ true);
}

SequencerBatching& Processor::sequencerBatching() {
  ld_check(sequencer_batching_);
  return *sequencer_batching_;
}

Worker* Processor::createWorker(WorkContext::KeepAlive executor,
                                worker_id_t idx,
                                WorkerType worker_type) {
  auto worker =
      new Worker(std::move(executor), this, idx, config_, stats_, worker_type);
  // Finish the remaining initialization on the executor.
  worker->addWithPriority(
      [worker] { worker->setupWorker(); }, folly::Executor::HI_PRI);
  return worker;
}

//
// Pimpl getters
//

AppendProbeController& Processor::appendProbeController() const {
  return impl_->append_probe_controller_;
}

AllSequencers& Processor::allSequencers() const {
  return *impl_->allSequencers_;
}

ClientIdxAllocator& Processor::clientIdxAllocator() const {
  return impl_->client_idx_allocator_;
}

bool Processor::hasMyNodeID() const {
  return my_node_id_.hasValue();
}

NodeID Processor::getMyNodeID() const {
  ld_check(hasMyNodeID() && my_node_id_.value().isNodeID());
  return my_node_id_.value();
}

folly::Optional<NodeID> Processor::getOptionalMyNodeID() const {
  return my_node_id_;
}

namespace {
// Lazily initialize the background queue and background threads.
void initBackgroundQueueAndThreads(Processor* processor) {
  ld_check(processor != nullptr);
  auto& impl = processor->impl_;
  auto settings = processor->settings();

  int num_threads = settings->num_processor_background_threads <= 0
      ? settings->num_workers
      : settings->num_processor_background_threads;
  ld_check(num_threads > 0);
  ld_info(
      "Starting the background queue (capacity: %zd) and %d background threads",
      settings->background_queue_size,
      num_threads);

  impl->background_queue_ = folly::MPMCQueue<folly::Function<void()>>(
      settings->background_queue_size);

  // Create and start all background threads.
  for (int i = 0; i < num_threads; ++i) {
    impl->background_threads_.emplace_back(new BackgroundThread(processor, i));
    int rc = impl->background_threads_.back()->start();
    ld_check(rc == 0);
  }
}
} // namespace

void Processor::enqueueToBackgroundBlocking(folly::Function<void()> fn) {
  folly::call_once(
      impl_->background_init_flag_, initBackgroundQueueAndThreads, this);
  ld_check(impl_->background_queue_.capacity() > 0);
  if (!validateFn(fn)) {
    return;
  }
  impl_->background_queue_.blockingWrite(std::move(fn));
}

bool Processor::enqueueToBackground(folly::Function<void()> fn) {
  folly::call_once(
      impl_->background_init_flag_, initBackgroundQueueAndThreads, this);
  ld_check(impl_->background_queue_.capacity() > 0);
  return validateFn(fn) && impl_->background_queue_.write(std::move(fn));
}

bool Processor::enqueueToBackgroundIfNotFull(folly::Function<void()> fn) {
  folly::call_once(
      impl_->background_init_flag_, initBackgroundQueueAndThreads, this);
  ld_check(impl_->background_queue_.capacity() > 0);
  return validateFn(fn) &&
      impl_->background_queue_.writeIfNotFull(std::move(fn));
}

bool Processor::validateFn(const folly::Function<void()>& fn) {
  if (!fn) {
    ld_error(
        "Enqueueing empty fn to Processor's background thread not allowed!");
    ld_check(false);
    return false;
  }
  return true;
}

std::vector<int> Processor::workerIdsRandomPermutation(WorkerType type) {
  folly::ThreadLocalPRNG rng;
  std::vector<int> worker_ids(
      impl_->all_workers_[static_cast<uint8_t>(type)].size());
  std::iota(worker_ids.begin(), worker_ids.end(), 0);
  std::shuffle(worker_ids.begin(), worker_ids.end(), rng);

  return worker_ids;
}

std::string Processor::describeMyNode() const {
  return settings_->server ? getMyNodeID().toString() : "Client";
}

std::shared_ptr<Configuration> Processor::getConfig() {
  return config_->get();
}

bool Processor::isLogsConfigLoaded() const {
  auto logsconfig = config_->getLogsConfig();
  if (logsconfig == nullptr) {
    return false;
  }
  // Configuration may not be local (RemoteLogsConfig).
  // In this case we always whether that config is fully loaded.
  return logsconfig->isFullyLoaded();
}

void Processor::setSequencerBatching(
    std::unique_ptr<SequencerBatching> sequencer_batching) {
  sequencer_batching_ = std::move(sequencer_batching);
}

ResourceBudget::Token Processor::getIncomingMessageToken(size_t payload_size) {
  return impl_->incoming_message_budget_.acquireToken(payload_size);
}

}} // namespace facebook::logdevice

namespace {

void BackgroundThread::run() {
  using namespace std::chrono_literals;
  using namespace std::chrono;

  for (;;) {
    folly::Function<void()> task;

    ld_check(processor_->impl_->background_queue_.capacity() > 0);
    processor_->impl_->background_queue_.blockingRead(task);

    if (!task) {
      return;
    }

    auto start_time{steady_clock::now()};
    task();
    auto duration = steady_clock::now() - start_time;
    auto usec = duration_cast<microseconds>(duration).count();

    if (duration >= 1ms &&
        duration >= processor_->settings()->slow_background_task_threshold) {
      RATELIMIT_INFO(1s, 2, "Slow background thread task: %.3fs", usec / 1e6);
      STAT_INCR(stats_, background_slow_requests);
    }
    HISTOGRAM_ADD(stats_, background_thread_duration, usec);
  }
}

} // anonymous namespace
