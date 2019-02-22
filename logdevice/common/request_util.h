/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <numeric>

#include <folly/Format.h>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/Random.h>
#include <folly/futures/Future.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * A utility class for running a function on some worker thread.
 */
class FuncRequest : public Request {
 public:
  FuncRequest(worker_id_t worker,
              WorkerType worker_type,
              RequestType type,
              std::function<void()>&& func)
      : Request(type),
        worker_(worker),
        worker_type_(worker_type),
        func_(std::move(func)) {}

  ~FuncRequest();

  static std::unique_ptr<Request> make(worker_id_t worker,
                                       WorkerType worker_type,
                                       RequestType type,
                                       std::function<void()>&& func);

 private:
  worker_id_t worker_;
  WorkerType worker_type_;
  std::function<void()> func_;

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

  Execution execute() override;
};

/**
 * A utility class for running a promise on some worker thread.
 */
template <typename T>
class PromiseRequest : public Request {
 public:
  PromiseRequest(folly::Optional<worker_id_t> worker,
                 WorkerType worker_type,
                 RequestType type,
                 folly::Promise<T>&& p,
                 folly::Function<void(folly::Promise<T>)> func)
      : Request(type),
        worker_(std::move(worker)),
        worker_type_(std::move(worker_type)),
        p_(std::move(p)),
        func_(std::move(func)) {}

  ~PromiseRequest() {}

  template <typename... Args>
  static std::unique_ptr<Request> make(Args&&... args) {
    return std::make_unique<PromiseRequest>(std::forward<Args>(args)...);
  }

 private:
  folly::Optional<worker_id_t> worker_;
  WorkerType worker_type_;
  folly::Promise<T> p_;
  folly::Function<void(folly::Promise<T>)> func_;

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.value_or(worker_id_t{-1}).val();
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

  Execution execute() override {
    func_(std::move(p_));
    return Request::Execution::COMPLETE;
  }
};

/**
 * A utility function for running a function on some worker thread.
 * If the request cannot be posted, logs an error.
 * If with_retrying = true, requests posting can only fail during shutdown.
 */
bool run_on_worker_nonblocking(Processor* processor,
                               worker_id_t worker_id,
                               WorkerType worker_type,
                               RequestType type,
                               std::function<void()>&& func,
                               bool with_retrying = false);

/**
 * A utility function for running a function on some worker threads and
 * retrieving the return value of all calls.
 *
 * If the request cannot be posted on some worker threads, logs an error.
 * The user of this function only gets the result for workers on which the
 * request was successfully posted.
 * If with_retrying = true, requests posting can only fail during shutdown.
 *
 * Basic example usage (run on workers 1, 2, 3):
 *
 *  std::vector<int> results;
 *  results = run_on_workers(server_->getProcessor(), {1, 2, 3}, [&]() {
 *    return Worker::onThisThreead()->idx_.val_;
 *  });
 *
 */
template <typename Func>
std::vector<typename std::result_of<Func()>::type>
run_on_workers(Processor* processor,
               std::vector<int> worker_ids,
               WorkerType worker_type,
               Func cb,
               bool with_retrying = false) {
  using R = typename std::result_of<Func()>::type;

  std::vector<folly::Optional<R>> results(worker_ids.size());
  int n_posted = 0;
  Semaphore sem;
  for (int i = 0; i < worker_ids.size(); ++i) {
    worker_id_t idx = worker_id_t(worker_ids[i]);
    bool posted =
        run_on_worker_nonblocking(processor,
                                  idx,
                                  worker_type,
                                  RequestType::ADMIN_CMD_UTIL_INTERNAL,
                                  [&sem, &cb, res = &results[i]] {
                                    *res = cb();
                                    sem.post();
                                  },
                                  with_retrying);

    if (posted) {
      ++n_posted;
    }
  }

  for (int i = 0; i < n_posted; ++i) {
    sem.wait();
  }

  std::vector<R> res;
  for (auto& r : results) {
    if (r.hasValue()) {
      res.push_back(std::move(r.value()));
    }
  }

  return res;
}

template <typename Func>
typename std::result_of<Func()>::type run_on_worker(Processor* processor,
                                                    int worker_id,
                                                    Func cb) {
  return run_on_worker(processor, worker_id, WorkerType::GENERAL, cb);
}

template <typename Func>
typename std::result_of<Func()>::type run_on_worker(Processor* processor,
                                                    int worker_id,
                                                    WorkerType worker_type,
                                                    Func cb) {
  auto res =
      run_on_workers(processor, {worker_id}, worker_type, std::move(cb), true);
  ld_check(res.size() == 1);
  return std::move(res[0]);
}

/**
 * A utility function for running a function on all worker threads and
 * retrieving the return value of all calls.
 *
 * Logs an error if the request cannot be posted on some worker threads. The
 * user of this function only gets the result for workers on which the request
 * was successfully posted.
 *
 * Basic example usage:
 *
 *  std::vector<int> results;
 *  results = run_on_all_workers(server_->getProcessor(), [&]() {
 *    return Worker::onThisThread()->idx_.val_;
 *  });
 *
 */
template <typename Func>
std::vector<typename std::result_of<Func()>::type>
run_on_all_workers(Processor* processor, Func cb) {
  std::vector<typename std::result_of<Func()>::type> results;
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    WorkerType worker_type = workerTypeByIndex(i);
    auto pool_result = run_on_worker_pool(processor, worker_type, cb);
    if (pool_result.size() > 0) {
      results.reserve(results.size() + pool_result.size());
      results.insert(results.end(), pool_result.begin(), pool_result.end());
    }
  }
  return results;
}

template <typename Func>
std::vector<typename std::result_of<Func()>::type>
run_on_worker_pool(Processor* processor, WorkerType worker_type, Func&& cb) {
  const int nworkers = processor->getWorkerCount(worker_type);
  std::vector<int> worker_ids(nworkers);
  std::iota(worker_ids.begin(), worker_ids.end(), 0);
  return run_on_workers(
      processor, worker_ids, worker_type, std::forward<Func>(cb));
}

template <typename T>
folly::SemiFuture<T>
fulfill_on_worker(Processor* processor,
                  folly::Optional<worker_id_t> worker_id,
                  WorkerType worker_type,
                  folly::Function<void(folly::Promise<T>)> cb,
                  RequestType request_type = RequestType::MISC,
                  bool with_retrying = false) {
  ld_check(processor);
  folly::Promise<T> p; // will be moved to Request;
  auto future = p.getSemiFuture();
  const int nworkers = processor->getWorkerCount(worker_type);
  if (FOLLY_UNLIKELY(nworkers <= 0)) {
    // We don't have workers in this pool
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "No workers of type %s are available",
                    workerTypeStr(worker_type));
    p.setException(
        std::runtime_error(folly::sformat("No workers of {} type are available",
                                          workerTypeStr(worker_type))
                               .c_str()));
    return future;
  }
  // It's important to not use the promise after moving, it becomes invalid.
  // This is why we have the future extracted earlier.
  auto req = PromiseRequest<T>::make(std::move(worker_id),
                                     worker_type,
                                     request_type,
                                     std::move(p),
                                     std::move(cb));

  int rv;
  if (with_retrying) {
    rv = processor->postWithRetrying(req);
  } else {
    rv = processor->postRequest(req);
  }

  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error: cannot post request of type %s: %s",
                    requestTypeNames[request_type].c_str(),
                    error_name(err));

    // creating a new promise since we moved the first promise to the Request
    return folly::makeFuture<T>(
        folly::make_exception_wrapper<std::runtime_error>(
            folly::sformat("Cannot schedule the request on the "
                           "{} workers on the server, server busy!",
                           workerTypeStr(worker_type))
                .c_str()));
  }
  return future;
}

// To run a functor on a single other worker, we allow mutable lambdas as the
// functor will be owned by the worker and it's free to modify the states
// captured by value however it pleases.
//
// However, to dispatch the same functor to multiple other workers (i.e., via
// fulfill_on_worker_pool or fulfill_on_all_workers), the cb function will be
// operating on shared state (captured by value or by reference). Hence the
// function signature contains the const qualifier.
template <typename T>
std::vector<folly::SemiFuture<T>>
fulfill_on_worker_pool(Processor* processor,
                       WorkerType worker_type,
                       folly::Function<void(folly::Promise<T>) const> cb,
                       RequestType request_type = RequestType::MISC,
                       bool with_retrying = false) {
  const int nworkers = processor->getWorkerCount(worker_type);
  std::vector<folly::SemiFuture<T>> futures;
  auto func = std::move(cb).asSharedProxy();
  for (int i = 0; i < nworkers; ++i) {
    futures.emplace_back(fulfill_on_worker<T>(processor,
                                              worker_id_t{i},
                                              worker_type,
                                              func,
                                              request_type,
                                              with_retrying));
  }
  return futures;
}

template <typename T>
std::vector<folly::SemiFuture<T>>
fulfill_on_all_workers(Processor* processor,
                       folly::Function<void(folly::Promise<T>) const> cb,
                       RequestType request_type = RequestType::MISC,
                       bool with_retrying = false) {
  ld_check(processor);
  const auto nWorkers = processor->getAllWorkersCount();
  std::vector<folly::SemiFuture<T>> futures;
  futures.reserve(nWorkers);
  auto func = std::move(cb).asSharedProxy();
  for (int i = 0; i < numOfWorkerTypes(); ++i) {
    WorkerType worker_type = workerTypeByIndex(i);
    auto pool_futures = fulfill_on_worker_pool<T>(
        processor, worker_type, func, request_type, with_retrying);
    futures.insert(futures.end(),
                   std::make_move_iterator(pool_futures.begin()),
                   std::make_move_iterator(pool_futures.end()));
  }
  return futures;
}

}} // namespace facebook::logdevice
