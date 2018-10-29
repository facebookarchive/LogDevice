/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "Processor.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include <folly/Conv.h>
#include <folly/Hash.h>
#include <folly/MPMCQueue.h>
#include <folly/Memory.h>
#include <folly/synchronization/CallOnce.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/AppendProbeController.h"
#include "logdevice/common/ClientAPIHitsTracer.h"
#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/EventLoopHandle.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/LegacyPluginPack.h"
#include "logdevice/common/MetaDataLogWriter.h"
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
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerLoadBalancing.h"
#include "logdevice/common/ZeroCopiedRecordDisposal.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
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
  ProcessorImpl(Processor* /*processor*/, UpdateableSettings<Settings> settings)
      : append_probe_controller_(std::chrono::seconds(10)), // TODO configurable
        worker_load_balancing_(settings->num_workers),
        background_init_flag_(),
        background_queue_() {}

  std::array<workers_t, static_cast<size_t>(WorkerType::MAX)> workers_;
  AppendProbeController append_probe_controller_;
  std::unique_ptr<AllSequencers> allSequencers_;
  WorkerLoadBalancing worker_load_balancing_;
  ClientIdxAllocator client_idx_allocator_;
  std::unique_ptr<ZeroCopiedRecordDisposal> record_disposal_;

  // for lazy init of background queue and threads
  folly::once_flag background_init_flag_;
  std::vector<std::unique_ptr<BackgroundThread>> background_threads_;
  // An empty function means "exit the thread."
  folly::MPMCQueue<folly::Function<void()>> background_queue_;
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
                      const std ::shared_ptr<UpdateableConfig>& config) {
  auto plugin = plugin_registry->getSinglePlugin<SequencerLocatorFactory>(
      PluginType::SEQUENCER_LOCATOR_FACTORY);
  if (plugin) {
    return (*plugin)(config);
  } else {
    return std::make_unique<HashBasedSequencerLocator>(
        config->updateableServerConfig());
  }
}

} // namespace

Processor::Processor(std::shared_ptr<UpdateableConfig> updateable_config,
                     std::shared_ptr<TraceLogger> trace_logger,
                     UpdateableSettings<Settings> settings,
                     StatsHolder* stats,
                     std::shared_ptr<LegacyPluginPack> plugin,
                     std::shared_ptr<PluginRegistry> plugin_registry,
                     std::string credentials,
                     std::string csid,
                     std::string name)
    :

      config_(std::move(updateable_config)),
      settings_(settings),
      plugin_(std::move(plugin)),
      plugin_registry_(std::move(plugin_registry)),
      stats_(stats),
      impl_(new ProcessorImpl(this, settings)),
      sequencer_locator_(get_sequencer_locator(plugin_registry_, config_)),
      conn_budget_incoming_(settings_->max_incoming_connections),
      conn_budget_backlog_(settings_->connection_backlog),
      conn_budget_external_(settings_->max_external_connections),
      api_hits_tracer_(std::make_unique<ClientAPIHitsTracer>(trace_logger)),
      HELLOCredentials_(settings_->server ? Principal::CLUSTER_NODE
                                          : std::move(credentials)),
      csid_(std::move(csid)),
      trace_logger_(trace_logger),
      name_(name) {
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
      config->serverConfig()->getMaxNodeIdx() + 1, this);

  // This needs to be initialized here to make sure that getWorkerCount()
  // is called on the sub-type of the actual processor.
  impl_->record_disposal_ = std::make_unique<ZeroCopiedRecordDisposal>(this);
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    WorkerType worker_type = workerTypeByIndex(i);
    auto count = getWorkerCount(worker_type);
    workers_t worker_pool = createWorkerPool(worker_type, count);
    ld_info(
        "Initialized %d workers of type %s", count, workerTypeStr(worker_type));
    impl_->workers_[i] = std::move(worker_pool);

    for (std::unique_ptr<EventLoopHandle>& handle : impl_->workers_[i]) {
      handle->start();
    }
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
}

workers_t Processor::createWorkerPool(WorkerType type, size_t count) {
  auto local_settings = settings_.get();
  ld_check(local_settings);
  workers_t workers;
  workers.reserve(count);

  for (int i = 0; i < count; i++) {
    // increment the next worker idx
    std::unique_ptr<EventLoopHandle> worker;
    try {
      worker = std::make_unique<EventLoopHandle>(
          createWorker(worker_id_t(i), type),
          local_settings->worker_request_pipe_capacity,
          local_settings->requests_per_iteration);
    } catch (ConstructorFailed&) {
      shutdown();
      throw ConstructorFailed();
    }
    ld_check(worker);
    workers.push_back(std::move(worker));
  }
  return workers;
}

EventLoopHandle& Processor::findWorker(WorkerType type, int worker_idx) {
  ld_check(type != WorkerType::MAX);
  auto& workers = impl_->workers_[static_cast<uint8_t>(type)];
  ld_check(worker_idx < workers.size());
  return *workers[worker_idx];
}

namespace {
class TestBuiltinPlugin : public virtual Plugin,
                          public virtual LegacyPluginPack {
  Type type() const override {
    return Type::LEGACY_CLIENT_PLUGIN;
  }

  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str() + " test";
  }

  std::string displayName() const override {
    return "Test plugin";
  }
};
} // namespace

// Testing Constructor
Processor::Processor(UpdateableSettings<Settings> settings,
                     bool fake_storage_node,
                     int /*max_logs*/,
                     StatsHolder* stats)
    : fake_storage_node_(fake_storage_node),
      settings_(settings),
      plugin_(std::make_shared<LegacyPluginPack>()),
      plugin_registry_(std::make_shared<PluginRegistry>(
          createAugmentedCommonBuiltinPluginVector<StaticPluginLoader,
                                                   TestBuiltinPlugin>())),
      stats_(stats),
      impl_(new ProcessorImpl(this, settings)),
      conn_budget_incoming_(settings_.get()->max_incoming_connections),
      conn_budget_backlog_(settings_.get()->connection_backlog),
      conn_budget_external_(settings_.get()->max_external_connections) {
  ld_check(settings.get());
  num_general_workers_ = settings_->num_workers;
  impl_->record_disposal_ = std::make_unique<ZeroCopiedRecordDisposal>(this);
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

worker_id_t Processor::selectWorkerRandomly(request_id_t rqid,
                                            WorkerType type) {
  return worker_id_t(folly::hash::twang_mix64(rqid.val()) %
                     getWorkerCount(type));
}

int Processor::getTargetThreadForRequest(const std::unique_ptr<Request>& rq) {
  WorkerType worker_type = rq->getWorkerTypeAffinity();
  const int nworkers = getWorkerCount(worker_type);
  int target_thread = rq->getThreadAffinity(nworkers);
  ld_check(nworkers > 0);
  // target_thread can be > nworkers if the target thread is gossip or admin
  // api worker
  ld_check(target_thread >= -1);

  // If the Request does not care about which thread it runs on, schedule it
  // round-robin.
  if (target_thread == -1) {
    target_thread = selectWorkerRandomly(rq->id_, worker_type).val_;
  }
  return target_thread;
}

Worker& Processor::getWorker(worker_id_t worker_id, WorkerType worker_type) {
  ld_check(worker_id.val() >= 0);
  ld_check(worker_id.val() < getWorkerCount(worker_type));
  return *static_cast<Worker*>(
      impl_->workers_[static_cast<uint8_t>(worker_type)][worker_id.val()]
          ->get());
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
                            EventLoopHandle& wh,
                            WorkerType worker_type,
                            worker_id_t worker_idx,
                            bool force) {
  auto type = rq->type_;
  const int rv = force ? wh.forcePostRequest(rq) : wh.postRequest(rq);
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
  auto& wh = findWorker(worker_type, target_thread);
  return postToWorker(rq, wh, worker_type, worker_id_t(target_thread), force);
}

int Processor::postRequest(std::unique_ptr<Request>& rq,
                           WorkerType worker_type,
                           int target_thread) {
  return postImpl(rq, worker_type, target_thread, /* force */ false);
}

int Processor::postRequest(std::unique_ptr<Request>& rq) {
  return postRequest(
      rq, rq->getWorkerTypeAffinity(), getTargetThreadForRequest(rq));
}

int Processor::blockingRequest(std::unique_ptr<Request>& rq) {
  return impl_
      ->workers_[static_cast<uint8_t>(rq->getWorkerTypeAffinity())]
                [getTargetThreadForRequest(rq)]
      ->blockingRequest(rq);
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

const std::shared_ptr<TraceLogger> Processor::getTraceLogger() const {
  return trace_logger_;
}

void Processor::shutdown() {
  if (shutting_down_.exchange(true)) {
    // already called
    return;
  }

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
  std::vector<pthread_t> pthreads;
  pthreads.reserve(getAllWorkersCount());

  if (traffic_shaper_) {
    traffic_shaper_->shutdown();
  }

  if (watchdog_thread_) {
    watchdog_thread_->shutdown();
  }

  if (impl_->allSequencers_) {
    impl_->allSequencers_->shutdown();
  }

  // Tell all Workers to shut down and terminate their threads. This
  // also alters WorkerHandles so that further attempts to post
  // requests through them fail with E::SHUTDOWN.
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    for (auto& w : impl_->workers_[i]) {
      w->dontWaitOnDestruct();
      w->shutdown();
      pthreads.push_back(w->getThread());
    }
  }

  for (size_t i = 0; i < impl_->background_threads_.size(); ++i) {
    impl_->background_queue_.blockingWrite(folly::Function<void()>());
  }

  // Join all the pthreads to complete shutdown. This is necessary because
  // EventLoops have a pointer to this Processor, so they have to finish
  // before we're destroyed.
  for (const pthread_t& pthread : pthreads) {
    int rv = pthread_join(pthread, nullptr);
    ld_check(rv == 0);
  }

  for (auto& thread : impl_->background_threads_) {
    thread->join();
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
  return postImpl(rq,
                  rq->getWorkerTypeAffinity(),
                  getTargetThreadForRequest(rq),
                  /* force */ true);
}

SequencerBatching& Processor::sequencerBatching() {
  ld_check(sequencer_batching_);
  return *sequencer_batching_;
}

Worker* Processor::createWorker(worker_id_t i, WorkerType type) {
  return new Worker(this, i, config_, stats_, type);
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

ZeroCopiedRecordDisposal& Processor::zeroCopiedRecordDisposal() const {
  return *impl_->record_disposal_;
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
      impl_->workers_[static_cast<uint8_t>(type)].size());
  std::iota(worker_ids.begin(), worker_ids.end(), 0);
  std::shuffle(worker_ids.begin(), worker_ids.end(), rng);

  return worker_ids;
}

std::string Processor::describeMyNode() const {
  return settings_->server
      ? config_->getServerConfig()->getMyNodeID().toString()
      : "Client";
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
