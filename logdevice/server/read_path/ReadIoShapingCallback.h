/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <mutex>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

class CatchupQueue;
class ServerReadStream;

class ReadIoShapingCallback : public BWAvailableCallback {
 public:
  explicit ReadIoShapingCallback(WeakRef<ServerReadStream> stream)
      : stream_(stream) {}

  void operator()(FlowGroup&, std::mutex&) override;
  size_t cost() const;
  void addCQRef(WeakRef<CatchupQueue> cq);
  virtual ~ReadIoShapingCallback() override;

 private:
  // 'this' ReadIoShapingCallback object is a member of ServerReadStream.
  // CatchupQueue pushes this callback in FlowGroup's queue if read bandwidth
  // is not available.
  // The weak reference prevents from cases when a connection gets closed, i.e.
  // CatchupQueue object is invalid, but FlowGroup's queue may still have
  // enqueued the ReadIoShapingCallback.
  WeakRef<CatchupQueue> catchup_queue_;
  WeakRef<ServerReadStream> stream_;
};

}} // namespace facebook::logdevice
