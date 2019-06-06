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

#include <folly/Executor.h>

#include "logdevice/common/RequestType.h"
#include "logdevice/common/RunContext.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/strong_typedef.h"

namespace facebook { namespace logdevice {

/**
 * @file  a Request is a state machine executing a LogDevice request, such as
 *        "append a record to a log", or "start delivery of records from a log".
 *        A Request executes entirely on a single EventLoop thread, as well as
 *        one or more storage threads.
 */

class StatsHolder;

class Request {
 public:
  explicit Request(RequestType type = RequestType::MISC)
      : id_(getNextRequestID()), type_(type) {}

  virtual ~Request();

  /**
   * Values of this enum class are returned by execute() in order to inform
   * the caller whether it should destroy the request object.
   */
  enum class Execution {
    COMPLETE, // Request execution is complete. The caller (RequestPump)
              // must destroy the request object.
    CONTINUE  // Request execution has been started and is continuing. The
              // request object is self-owned and will destroy itself once
              // the request has run its course. The caller must not destroy
              // the request object.
  };

  /**
   * This is currently the only visible method of a Request. This method starts
   * executing the request. It can only be called by a Worker thread and
   * is called when such a thread reads the request from its request pipe.
   *
   * @return one of the enum constants of type Execution defined above;
   *         note execute() can delete this and return Execution::CONTINUE.
   */
  virtual Execution execute() = 0;

  /**
   * A Processor calls this function in order to decide which thread to run
   * this request on.
   *
   * Note that this is the id of the worker in the worker pool as specified by
   * getWorkerTypeAffinity().
   *
   * @param  nthreads   the number of threads the calling Processor runs
   *
   * @return -1 if Processor can run this Request on a thread of its choosing.
   *         A value in the range [0, nthreads[ tells the Processor to run
   *         this Request on the thread at that offset in the vector of
   *         EventLoop threads. Positive values >= nthreads are invalid.
   */
  virtual int getThreadAffinity(int /*nthreads*/) {
    return -1;
  }

  /**
   * A processor calls this function in order to decide on which worker-pool to
   * run this request on. If the there are no workers for the returned
   * worker-pool then we fallback to the general pool anyway.
   */
  virtual WorkerType getWorkerTypeAffinity() {
    return WorkerType::GENERAL;
  }

  virtual RunContext getRunContext() const {
    return RunContext(type_);
  }

  /**
   * Returns a string describing the request.
   * The default implementation returns the name of RequestType.
   * Can be called after execute() returned Execution::COMPLETE.
   */
  virtual std::string describe() const;

  /**
   * Sets the semaphore to be posted to after the request is done processing,
   * in order to unblock a client thread waiting on it.  The semaphore will
   * get posted to exactly once during the lifetime of the request, when it is
   * appropriate to unblock the client (typically when the request is
   * finished).
   */
  void setClientBlockedSemaphore(Semaphore* sem) {
    client_blocked_sem_ = sem;
  }

  /**
   * Priority used by executor to schedule this request. Using this processor
   * enqueues the request into the right priority queue of the executor.
   */
  virtual int8_t getExecutorPriority() const {
    return folly::Executor::LO_PRI;
  }

  const request_id_t id_; // unique id of this request, used to look up
                          // request objects by id

  const RequestType type_; // Type of this request.

  /**
   * Bump stats to reflect that this request was posted to a queue.
   */
  static void bumpStatsWhenPosted(StatsHolder* stats,
                                  RequestType type,
                                  WorkerType worker_type,
                                  worker_id_t worker_idx,
                                  bool success);

  /**
   * Allocates and returns the next request ID. Each thread maintains its own
   * pool of request IDs to avoid contention on the request ID atomic, and grabs
   * them in batches of 2^32
   */
  static request_id_t getNextRequestID();

  std::chrono::steady_clock::time_point enqueue_time_;

 protected:
  /**
   * Unblock a client thread waiting on the Request, if any.  This gets called
   * in ~Request() but subclasses of Request are free to call it sooner.  Only
   * the first call actually posts to the semaphore, subsequent calls are
   * no-ops.
   */
  void unblockClient();

 private:
  static std::atomic<uint32_t>
      next_req_batch_id; // first 32 bits of the request ID

  Semaphore* client_blocked_sem_ = nullptr;
};

}} // namespace facebook::logdevice
