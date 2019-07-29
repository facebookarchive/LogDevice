/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/ClientWorker.h"

#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientMessageDispatch.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/lib/NodeStatsHandler.h"

namespace facebook { namespace logdevice {

class ClientWorkerImpl {
 public:
  explicit ClientWorkerImpl(ClientWorker* /*w*/) {}

  // should only be initialized on a single worker, as defined in
  // NodeStatsHandler::getThreadAffinity
  std::unique_ptr<NodeStatsHandler> node_stats_handler_;
};

ClientWorker::ClientWorker(WorkContext::KeepAlive event_loop,
                           ClientProcessor* processor,
                           worker_id_t idx,
                           const std::shared_ptr<UpdateableConfig>& config,
                           StatsHolder* stats = nullptr,
                           WorkerType type = WorkerType::GENERAL)
    : Worker(std::move(event_loop),
             processor,
             idx,
             config,
             stats,
             type,
             ThreadID::CLIENT_WORKER),
      processor_(processor),
      impl_(new ClientWorkerImpl(this)) {}

std::unique_ptr<MessageDispatch> ClientWorker::createMessageDispatch() {
  return std::make_unique<ClientMessageDispatch>();
}

void ClientWorker::sampleAllReadStreamsDegubInfoToScuba() const {
  clientReadStreams().sampleAllReadStreamsDegubInfoToScuba();
  sample_read_streams_timer_->activate(settings().all_read_streams_rate);
}

void ClientWorker::setupWorker() {
  Worker::setupWorker();
  if (idx_.val() ==
      NodeStatsHandler::getThreadAffinity(
          processor_->getWorkerCount(WorkerType::GENERAL))) {
    ld_debug("Started node stats handler on %s", getName().c_str());
    impl_->node_stats_handler_ = std::make_unique<NodeStatsHandler>();
    impl_->node_stats_handler_->init();
    impl_->node_stats_handler_->start();
  }
  sample_read_streams_timer_ = std::make_unique<Timer>(
      std::bind(&ClientWorker::sampleAllReadStreamsDegubInfoToScuba, this));

  sampleAllReadStreamsDegubInfoToScuba();
}

NodeStatsMessageCallback* ClientWorker::nodeStatsMessageCallback() const {
  return impl_->node_stats_handler_.get();
}
}} // namespace facebook::logdevice
