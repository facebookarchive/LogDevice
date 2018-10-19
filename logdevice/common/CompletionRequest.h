/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <tuple>

#include "folly/Function.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

/**
 * @file CompletionRequest is a variadic template for defining Request types
 *       whose execute() method calls a completion function with a specified
 *       argument list. The completion function returns no result. The full
 *       list of arguments passed to the completion function consists of
 *       a logdevice::Status followed by the variadic template arguments.
 */

namespace facebook { namespace logdevice {

/**
 * Please try to use CompletionRequest below which is noncopyable and
 * uses folly::Function.
 */
template <template <class...> class FunctionType, typename... Args>
class CompletionRequestBase : public Request {
 private:
  template <int...>
  struct seq {};

  template <int N, int... S>
  struct gens : gens<N - 1, N - 1, S...> {};

  template <int... S>
  struct gens<0, S...> {
    typedef seq<S...> type;
  };

 public:
  /**
   * Type of completion function to be called.
   */
  typedef FunctionType<void(Status st, Args...)> CompletionFunction;

  /**
   * @param cf            function to call on a Worker
   * @param destination   id of worker that should run this Request, or -1
   *                      for an arbitrary Worker
   * @param status        passed to cf as first argument
   * @param ...args       remaining arguments to pass to cf
   */
  CompletionRequestBase(CompletionFunction cf,
                        worker_id_t destination,
                        Status st,
                        Args... args)
      : Request(RequestType::COMPLETION),
        params_(std::make_tuple(std::move(args)...)),
        cf_(std::move(cf)),
        status_(st),
        destination_(destination) {}

  int getThreadAffinity(int nthreads) override {
    ld_check(destination_.val_ < nthreads);
    return destination_.val_;
  }

  Execution execute() override {
    callCompletionFunction(typename gens<sizeof...(Args)>::type());
    return Execution::COMPLETE;
  }

  std::tuple<Args...> params_; // passed to cf_

 private:
  CompletionFunction cf_;         // completion function to call
  const Status status_;           // passed to cf_
  const worker_id_t destination_; // Worker thread on which to execute cf_,
                                  // or -1 for arbitrary Worker

  template <int... S>
  void callCompletionFunction(seq<S...>) {
    cf_(status_, std::get<S>(std::move(params_))...);
  }
};

template <typename... Args>
using CompletionRequest = CompletionRequestBase<folly::Function, Args...>;

}} // namespace facebook::logdevice
