/**
 * Copyright (c) 2004-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include "folly/executors/SerialExecutor.h"
#include "logdevice/common/work_model/WorkContext.h"

namespace facebook { namespace logdevice {

/**
 * SerialWorkContext provides guarantees that work added will be executed on the
 * executor in serial order. SerialWorkContext thus, provides a way to make sure
 * work added executes in the order it was added.
 *
 * Implementation Details:
 * SerialWorkContext is constructed using Executor instance or
 * KeepAlive<Executor> on which added work will be executed. This Executor can
 * be something like EventLoop or CPUThreadPoolExecutor, or can be any other
 * Executor capable of executing work. This underlying executor does the actual
 * execution while SerialWorkContext is a decorator on top of it.
 * SerialWorkContext uses SerialExecutor to achieve this, it accepts the
 * underlying executor's KeepAlive token. The SerialExecutor captures executor's
 * KeepAlive token and expects executor should not go away while the token is
 * still alive. SerialExecutor::create returns KeepAlive of newly created
 * SerialExecutor instance. SerialExecutor and the token generated by create is
 * not visible externally. It is destroyed along with SerialWorkContext. Work
 * added into SerialWorkContext gets added to SerialExecutor, which makes sure
 * to execute it on the underlying executor in serial order.
 */
class SerialWorkContext : public WorkContext {
 public:
  explicit SerialWorkContext(WorkContext::KeepAlive executor)
      : WorkContext(folly::SerialExecutor::create(
            folly::getKeepAliveToken(executor.get()))),
        parent_(std::move(executor)) {}

  ~SerialWorkContext() override {}

  Executor* getExecutor() override {
    return parent_.get();
  }

 protected:
  // folly::SerialExecutor captures the parent and does not allow to fetch it
  // Save off the parent keep alive and create another keepAlive for the
  // SerialExecutor.
  WorkContext::KeepAlive parent_;
};

}} // namespace facebook::logdevice
