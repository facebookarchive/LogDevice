/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Expected.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/futures/Retrying.h>

namespace facebook { namespace logdevice {

/**
 * @file RetryHandler provides an API to retry a certain function if it fails
 * with exponential backoff and optional jitter.
 *
 * It is a thin-wrapper around folly::futures::retrying to support retrying for
 * code that's not future based.
 *
 */
template <class T>
class RetryHandler {
 public:
  using Result = folly::Expected<T, T>;

  /**
   * Retries the function until either we exhaust all the retries or
   * should_retry returns false.
   *
   * @param func: The function we want to retry, takes as a param the trial
   *              number.
   * @param should_retry: Given the output of func, returns true to retry, or
   *                      false to stop retrying.
   *
   * @returns A folly::Expected indicating either:
   *   1- Success and contains the returned value.
   *   2- Error which means that we exhausted all the retries. It will also
   *      hold the last returned value from the function.
   */
  static folly::SemiFuture<Result>
  run(folly::Function<T(size_t trial_num) const> func,
      folly::Function<bool(const T&) const> should_retry,
      size_t max_tries,
      std::chrono::milliseconds backoff_min,
      std::chrono::milliseconds backoff_max,
      double jitter_param) {
    return folly::futures::retrying(
               folly::futures::retryingPolicyCappedJitteredExponentialBackoff(
                   max_tries, backoff_min, backoff_max, jitter_param),
               [fu = std::move(func), should_retry = std::move(should_retry)](
                   size_t trial) -> folly::SemiFuture<Result> {
                 T ret = fu(trial);

                 // If it's a failure, simulate an execption for
                 // futures::retrying to retry.
                 if (should_retry(ret)) {
                   return folly::make_exception_wrapper<Failure>(
                       std::move(ret));
                 } else {
                   return folly::makeExpected<T>(std::move(ret));
                 }
               })
        // When we exhaust all the retries, return it as a folly unexpected
        // of a future carrying an exception.
        .deferError(folly::tag_t<Failure>(),
                    [](Failure f) -> folly::SemiFuture<Result> {
                      return folly::makeUnexpected(std::move(f.type));
                    });
  }

  /**
   * Synchronous version of RetryHandler<T>::run.
   */
  static Result syncRun(folly::Function<T(size_t trial_num) const> func,
                        folly::Function<bool(const T&) const> should_retry,
                        size_t max_tries,
                        std::chrono::milliseconds backoff_min,
                        std::chrono::milliseconds backoff_max,
                        double jitter_param) {
    auto& executor = folly::InlineExecutor::instance();
    return run(std::move(func),
               std::move(should_retry),
               max_tries,
               backoff_min,
               backoff_max,
               jitter_param)
        .via(&executor)
        .get();
  }

 private:
  struct Failure : std::exception {
    explicit Failure(T t) : type(std::move(t)) {}
    T type;
  };
};

}} // namespace facebook::logdevice
