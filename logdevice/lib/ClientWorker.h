/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/Worker.h"

/**
 * @file Subclass of Worker containing state specific to clients.
 */

namespace facebook { namespace logdevice {

class ClientProcessor;
class ClientWorkerImpl;
class NodeStatsMessageCallback;

class ClientWorker : public Worker {
 public:
  ClientWorker(WorkContext::KeepAlive,
               ClientProcessor*,
               worker_id_t,
               const std::shared_ptr<UpdateableConfig>&,
               StatsHolder*,
               WorkerType type);

  /**
   * If current thread is running a ClientWorker, return it.  Otherwise,
   * assert false (when enforce is true) and/or return nullptr.
   */
  static ClientWorker* onThisThread(bool enforce = true) {
    if (ThreadID::getType() == ThreadID::CLIENT_WORKER ||
        ThreadID::getType() == ThreadID::CPU_EXEC) {
      return checked_downcast<ClientWorker*>(Worker::onThisThread());
    }
    if (enforce) {
      // Not on a client worker == assert failure
      ld_check(false);
    }
    return nullptr;
  }

  // Intentionally shadows `Worker::processor_' to expose a more specific
  // subclass of Processor
  ClientProcessor* const processor_;

  /**
   * If this worker is responsible for the NodeStatsHandler, return the
   * callback. Otherwise return nullptr.
   */
  NodeStatsMessageCallback* nodeStatsMessageCallback() const;

  void sampleAllReadStreamsDegubInfoToScuba() const;

  void setupWorker() override;

 private:
  // Pimpl, contains most of the objects we provide getters for
  friend class ClientWorkerImpl;
  std::unique_ptr<ClientWorkerImpl> impl_;

  std::unique_ptr<MessageDispatch> createMessageDispatch() override;

  std::unique_ptr<Timer> sample_read_streams_timer_;
};
}} // namespace facebook::logdevice
