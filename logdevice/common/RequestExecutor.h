/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/Request.h"
#include "logdevice/common/WorkerType.h"

namespace facebook { namespace logdevice {

class Processor;

class RequestPoster {
 public:
  virtual ~RequestPoster() = default;
  /**
   * Post the request to the worker of type `worker_type` and with the specifid
   * thread ID. If target_thread is -1, then post to any thread of that type.
   * If `force` is set, it means proceed regardless of how many requests are
   * already pending on the worker (no NOBUFS error)
   *
   * TODO: Once the worker has a similar interface for enqueuing requests, this
   * .     API can become `getExecutorForWorker` instead.
   *
   * @return 0 if request was successfully passed to a Worker thread for
   *           execution, -1 on failure. err is set to
   *     INVALID_PARAM  if rq is invalid
   *     NOBUFS         if too many requests are pending to be delivered to
   *                    Workers and `force` is not set.
   *     SHUTDOWN       Processor is shutting down
   */
  virtual int postImpl(std::unique_ptr<Request>& rq,
                       WorkerType worker_type,
                       int target_thread,
                       bool force) = 0;
};

/**
 * An interface for enqueuing requests to be executed on some worker pool.
 */
class RequestExecutor {
 public:
  explicit RequestExecutor(RequestPoster* poster) : request_poster_(poster) {}

  /**
   * Schedules rq to run on one of the Workers managed by this Processor.
   *
   * @param rq  request to execute. Must not be nullptr. On success the
   *            function takes ownership of the object (and passes it on
   *            to a Worker). On failure ownership remains with the
   *            caller.
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

 private:
  int blockingRequestImpl(std::unique_ptr<Request>& rq, bool force);

  RequestPoster* request_poster_;
};

}} // namespace facebook::logdevice
