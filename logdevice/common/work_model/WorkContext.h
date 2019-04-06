/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>

#include <folly/Executor.h>

namespace facebook { namespace logdevice {

class WorkContext : public folly::Executor {
 public:
  using work_context_id_t = uint64_t;
  static constexpr work_context_id_t kAnonymousId{0};
  using KeepAlive = folly::Executor::KeepAlive<folly::Executor>;
  explicit WorkContext(KeepAlive executor_keep_alive,
                       work_context_id_t id = kAnonymousId);
  WorkContext(const WorkContext&) = delete;
  WorkContext& operator=(const WorkContext&) = delete;
  ~WorkContext() override;

  void add(folly::Func func) override;

  void addWithPriority(folly::Func func, int8_t priority) override;
  work_context_id_t getId() const;
  bool anonymous() const;
  virtual Executor* getExecutor() {
    return executor_.get();
  }

 protected:
  bool keepAliveAcquire() override;
  void keepAliveRelease() override;
  /**
   * KeepAlive of underlying executor that executes added work. This keep alive
   * token provides a way to access the executor and also prevents it from going
   * out of scope.
   */
  KeepAlive executor_;

 private:
  work_context_id_t id_;

  // Number of clients that hold a reference to this work context.
  std::atomic<size_t> num_references_{0};
};

}} // namespace facebook::logdevice
