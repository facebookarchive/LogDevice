/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/shutdown.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "logdevice/admin/AdminServer.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/Worker.h"
#include "logdevice/server/ConnectionListener.h"
#include "logdevice/server/LogStoreMonitor.h"
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/UnreleasedRecordDetector.h"
#include "logdevice/server/admincommands/CommandListener.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/util.h"

namespace facebook { namespace logdevice {

/**
 * Request used during shutdown to set accepting_work_ to false for each
 * worker.
 */
class WorkerRequest : public Request {
 public:
  WorkerRequest(worker_id_t worker_id,
                WorkerType worker_type,
                Semaphore& sem,
                std::function<void(Worker* worker)> callback)
      : Request(RequestType::STOP_ACCEPTING_WORK),
        callback_(callback),
        worker_id_(worker_id),
        worker_type_(worker_type),
        sem_(sem) {}

  Request::Execution execute() override {
    Worker* worker = Worker::onThisThread();
    ld_check(worker != nullptr);

    callback_(worker);
    sem_.post();

    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_id_.val_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  std::function<void(Worker* worker)> callback_;
  worker_id_t worker_id_;
  WorkerType worker_type_;
  Semaphore& sem_;
};

int post_and_wait(Processor* processor,
                  std::function<void(Worker* worker)> callback) {
  return post_and_wait(
      processor, [](WorkerType /*unused*/) -> bool { return true; }, callback);
}

int post_and_wait(Processor* processor,
                  std::function<bool(WorkerType worker_type)> filter,
                  std::function<void(Worker* worker)> callback) {
  Semaphore sem;
  int workers = 0;

  for (int i = 0; i < numOfWorkerTypes(); i++) {
    WorkerType worker_type = workerTypeByIndex(i);
    // Only perform the operation on workers that pass the filter
    if (!filter(worker_type)) {
      continue;
    } else {
      for (worker_id_t idx{0};
           idx.val_ < processor->getWorkerCount(worker_type);
           ++idx.val_) {
        std::unique_ptr<Request> request =
            std::make_unique<WorkerRequest>(idx, worker_type, sem, callback);

        // use postWithRetrying() here to make sure that workers will perform
        // their graceful shutdown sequence before its destructor is called.
        // If the request is still unable to reach some worker after the
        // graceful shutdown timeout, consider it as an ungraceful shutdown and
        // logdeviced will exit after the timeout.
        int rv = processor->postWithRetrying(request);
        if (rv != 0) {
          ld_error("Failed to post WorkerRequest on worker %d (%s): %s",
                   idx.val_,
                   workerTypeStr(worker_type),
                   error_description(err));
        } else {
          ++workers;
        }
      }
    }
  }
  for (int i = 0; i < workers; ++i) {
    sem.wait();
  }

  return workers;
}

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

void shutdown_server(
    std::unique_ptr<AdminServer>& admin_server,
    std::unique_ptr<Listener>& connection_listener,
    std::unique_ptr<Listener>& command_listener,
    std::unique_ptr<Listener>& gossip_listener,
    std::unique_ptr<Listener>& ssl_connection_listener,
    std::unique_ptr<EventLoop>& connection_listener_loop,
    std::unique_ptr<EventLoop>& command_listener_loop,
    std::unique_ptr<EventLoop>& gossip_listener_loop,
    std::unique_ptr<EventLoop>& ssl_connection_listener_loop,
    std::unique_ptr<LogStoreMonitor>& logstore_monitor,
    std::shared_ptr<ServerProcessor>& processor,
    std::unique_ptr<ShardedStorageThreadPool>& storage_thread_pool,
    std::unique_ptr<ShardedRocksDBLocalLogStore>& sharded_store,
    std::shared_ptr<SequencerPlacement> sequencer_placement,
    std::unique_ptr<RebuildingCoordinator>& rebuilding_coordinator,
    std::unique_ptr<EventLogStateMachine>& event_log,
    std::unique_ptr<RebuildingSupervisor>& rebuilding_supervisor,
    std::shared_ptr<UnreleasedRecordDetector>& unreleased_record_detector,
    std::unique_ptr<maintenance::ClusterMaintenanceStateMachine>&
        cluster_maintenance_state_machine,
    bool fast_shutdown) {
  auto t1 = steady_clock::now();

  // Stop the Admin API Server
  if (admin_server) {
    ld_info("Stopping Admin API server");
    // This should gracefully finish the current pending admin requests and
    // cleanly shutdown any threads/workers managed by the Admin API server.
    admin_server->stop();
    ld_info("Admin API server stopped accepting requests");
  }

  if (sequencer_placement && !fast_shutdown) {
    // request that any logs handled by this server are moved to a different
    // machine before shutting down workers
    ld_info("Requesting sequencer placement failover");
    sequencer_placement->requestFailover();
  }

  if (unreleased_record_detector) {
    ld_info("Stopping unreleased record detector thread");
    unreleased_record_detector->stop();
    unreleased_record_detector.reset();
  }

  ld_info("Stopping logstore monitoring thread");
  logstore_monitor.reset();

  if (rebuilding_supervisor) {
    // stop rebuilding supervisor (destruction is below)
    rebuilding_supervisor->stop();
  }

  // stop accepting new connections
  ld_info("Destroying listeners");
  std::vector<folly::SemiFuture<folly::Unit>> listeners_closed;
  if (connection_listener) {
    listeners_closed.emplace_back(
        connection_listener->stopAcceptingConnections());
  }
  // Avoid waiting for command listener as it could take a long time to shutdown
  // reset it at the end Joining admin command is avoided at this time because
  // , if admin command queue is large joining the thread won't be possible in
  // shutdown timeout. Hence, at this time accepting new requests is stopped but
  // the listener thread itself is not joined. This special casing of admin
  // commands listener is not ideal. Ideally, we want same approach for all
  // listeners. But, all of the stuck shutdowns till now have been because of
  // admin command thread.
  auto command_listener_closed = folly::SemiFuture<folly::Unit>();
  ld_assert(command_listener_closed.hasValue());
  if (command_listener) {
    command_listener_closed = command_listener->stopAcceptingConnections();
  }
  if (gossip_listener) {
    listeners_closed.emplace_back(gossip_listener->stopAcceptingConnections());
  }
  if (ssl_connection_listener) {
    listeners_closed.emplace_back(
        ssl_connection_listener->stopAcceptingConnections());
  }
  folly::collectAll(listeners_closed.begin(), listeners_closed.end()).wait();

  connection_listener.reset();
  connection_listener_loop.reset();
  gossip_listener.reset();
  gossip_listener_loop.reset();
  ssl_connection_listener.reset();
  ssl_connection_listener_loop.reset();

  // set accepting_work to false
  ld_info("Stopping accepting work on all workers except FAILURE_DETECTOR");
  int workers_except_fd = 0;
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    WorkerType worker_type = workerTypeByIndex(i);
    if (worker_type != WorkerType::FAILURE_DETECTOR) {
      workers_except_fd += processor->getWorkerCount(worker_type);
    }
  }

  int nworkers =
      post_and_wait(processor.get(),
                    [](WorkerType worker_type) -> bool {
                      return worker_type != WorkerType::FAILURE_DETECTOR;
                    },
                    [](Worker* worker) { worker->stopAcceptingWork(); });

  if (nworkers < workers_except_fd) {
    ld_error("Posting stopAcceptingWork request with retrying failed on "
             "%d/%d workers",
             workers_except_fd - nworkers,
             workers_except_fd);
  }

  if (storage_thread_pool) {
    // dump last released LSNs from LogStorageStateMap to the local log store
    // (note that workers could still be processing some RELEASE messages so
    // this might not be the most recent state)
    ld_info("Dumping release states");
    dump_release_states(
        processor->getLogStorageStateMap(), *storage_thread_pool);

    ld_info("Shutting down record cache monitor thread.");
    processor->getLogStorageStateMap().shutdownRecordCacheMonitor();

    ld_info("Shutting down storage thread pool");
    bool persist_record_caches = false;
    auto local_settings = processor->settings();
    persist_record_caches = local_settings->enable_record_cache;
    // Stop accepting new storage tasks and wait for the existing ones to
    // finish. Also write record cache after all the storage tasks are done.
    storage_thread_pool->shutdown(persist_record_caches);

    // storage threads have been shut down, record cache will not get any
    // new writes, clear all existing caches so that their entries can be
    // destroyed on Worker later.
    ld_info("Shutting down record caches");
    processor->getLogStorageStateMap().shutdownRecordCaches();
  }

  std::vector<std::thread> flushing_threads;
  if (sharded_store) {
    ld_info("Spawning background threads to flush memtables");
    for (shard_index_t idx = 0; idx < sharded_store->numShards(); ++idx) {
      flushing_threads.emplace_back([&sharded_store, idx] {
        sharded_store->getByIndex(idx)->markImmutable();
        ld_info("Finished flushing memtables in shard %d", idx);
      });
    }
  }

  // after stateful requests finish, flush and close sockets
  ld_info("Finishing work and closing sockets on all workers except "
          "FAILURE_DETECTOR");
  nworkers = post_and_wait(
      processor.get(),
      [](WorkerType worker_type) -> bool {
        return worker_type != WorkerType::FAILURE_DETECTOR;
      },
      [](Worker* worker) { worker->finishWorkAndCloseSockets(); });
  if (nworkers < workers_except_fd) {
    ld_error(
        "Posting finishWorkAndCloseSockets request with retrying failed on "
        "%d/%d workers",
        workers_except_fd - nworkers,
        workers_except_fd);
  }
  // wait until workers finish outstanding requests and close sockets. This
  // waits for all workers except FAILURE_DETECTOR
  ld_info("Waiting for workers to stop");
  processor->waitForWorkers(nworkers);

  // Shutdown FAILURE_DETECTOR worker
  ld_info("Finishing work and closing sockets on FAILURE_DETECTOR");
  nworkers = post_and_wait(processor.get(),
                           [](WorkerType worker_type) -> bool {
                             return worker_type == WorkerType::FAILURE_DETECTOR;
                           },
                           [&processor](Worker* worker) {
                             FailureDetector* fd =
                                 processor.get()->failure_detector_.get();
                             if (fd) {
                               fd->shutdown();
                             }
                             worker->stopAcceptingWork();
                             worker->finishWorkAndCloseSockets();
                           });

  if (nworkers < processor->getWorkerCount(WorkerType::FAILURE_DETECTOR)) {
    ld_error("Stopping FAILURE_DETECTOR failed.");
  }
  if (nworkers > 0) {
    // Wait until failure detector is terminated
    ld_info("Waiting for FAILURE_DETECTOR to stop");
    processor->waitForWorkers(nworkers);
    ld_info("FAILURE_DETECTOR worker stopped");
  }

  command_listener_closed.wait();
  command_listener.reset();
  command_listener_loop.reset();

  // take down all worker threads
  ld_info("Shutting down worker threads");
  processor->shutdown();

  if (admin_server) {
    // Note that deallocating AdminServer might be expensive if it's holding the
    // safety checker metadata cache, hence why we are destroying it near the
    // end.
    ld_info("Admin API server destroyed");
    admin_server.reset();
  }

  // call destructors for RebuildingCoordinator, SequencerPlacement,
  // ShardedLocalLogStore, ShardedStorageThreadPool, and Processor objects.
  if (rebuilding_coordinator) {
    ld_info("Destroying rebuilding coordinator");
    rebuilding_coordinator.reset();
  }
  if (event_log) {
    ld_info("Destroying EventLogStateMachine");
    event_log.reset();
  }
  if (rebuilding_supervisor) {
    ld_info("Destroying rebuilding supervisor");
    rebuilding_supervisor.reset();
  }
  if (cluster_maintenance_state_machine) {
    ld_info("Destroying ClusterMaintenanceStateMachine");
    cluster_maintenance_state_machine.reset();
  }
  if (sequencer_placement) {
    ld_info("Destroying sequencer placement");
    sequencer_placement.reset();
  }
  if (sharded_store) {
    ld_info("Waiting for memtable flushes");
    for (auto& t : flushing_threads) {
      t.join();
    }
  }
  if (sharded_store) {
    ld_info("Destroying local log store");
    sharded_store.reset();
  }
  storage_thread_pool.reset();
  processor.reset();

  auto t2 = steady_clock::now();
  int64_t duration = duration_cast<milliseconds>(t2 - t1).count();
  ld_info("Shutdown took %ld ms", duration);
}

}} // namespace facebook::logdevice
