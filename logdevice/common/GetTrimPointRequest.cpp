/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/GetTrimPointRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GET_TRIM_POINT_Message.h"

namespace facebook { namespace logdevice {

Request::Execution GetTrimPointRequest::execute() {
  // Insert request into map for worker to track it
  auto& map = Worker::onThisThread()->runningGetTrimPoint().map;
  if (map.find(log_id_) != map.end()) {
    // the request has been there already. Reactive it if necessary
    // destroy this request object
    map[log_id_]->start();
    return Execution::COMPLETE;
  }
  auto result = map.insert(
      std::make_pair(log_id_, std::unique_ptr<GetTrimPointRequest>(this)));
  ld_check(result.second);

  // set request timer
  request_timer_ =
      std::make_unique<Timer>([this] { this->onRequestTimeout(); });

  // broadcast the init messages
  onRequestTimeout();

  return Execution::CONTINUE;
}

void GetTrimPointRequest::start() {
  if (!request_timer_->isActive()) {
    onRequestTimeout();
  }
}

void GetTrimPointRequest::onRequestTimeout() {
  // get storage set
  auto sequencer = getSequencer();
  if (sequencer != nullptr &&
      sequencer->getState() == Sequencer::State::ACTIVE) {
    auto md_map = sequencer->getMetaDataMap();
    if (nullptr == md_map) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      2,
                      "get a null pointer of meta data map for log id %lu",
                      log_id_.val());
    } else {
      auto storage_set = md_map->getUnionStorageSet();
      if (storage_set == nullptr) {
        RATELIMIT_DEBUG(std::chrono::seconds(10),
                        2,
                        "get a null pointer of storage set for log id %lu",
                        log_id_.val());
      } else {
        // broadcast requests
        ld_debug("Sending GetTrimPointRequests for epoch = %u, logid = %lu",
                 sequencer->getCurrentEpoch().val_,
                 log_id_.val());
        for (auto shard : *storage_set) {
          sendTo(shard);
        }
      }
    }
  }
  // reset timer
  ld_debug("Activating timer for logid = %lu", log_id_.val());
  request_timer_->activate(request_interval_);
}

void GetTrimPointRequest::sendTo(ShardID shard) {
  const auto& nodes_configuration = getNodesConfiguration();
  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard.node())) {
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    2,
                    "Cannot find node at index %u for logid %lu",
                    shard.node(),
                    log_id_.val());
    return;
  }

  NodeID to(shard.node());
  GET_TRIM_POINT_Header header = {log_id_, shard.shard()};
  auto msg = std::make_unique<GET_TRIM_POINT_Message>(header);
  if (Worker::onThisThread()->sender().sendMessage(std::move(msg), to) != 0) {
    if (err == E::PROTONOSUPPORT) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "GET_TRIM_POINT is not supported by the server at %s",
                     Sender::describeConnection(to).c_str());
    }
  }
}

void GetTrimPointRequest::onReply(ShardID from,
                                  Status status,
                                  lsn_t trim_point) {
  ld_debug("Received GET_TRIM_POINT_REPLY from %s, status=%s, result=%lu for "
           "logid %lu",
           from.toString().c_str(),
           error_name(status),
           trim_point,
           log_id_.val());
  switch (status) {
    case E::OK:
      updateTrimPoint(status, trim_point);
      break;

    case E::REBUILDING:
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      10,
                      "shard %s is rebuilding.",
                      from.toString().c_str());
      break;

    case E::AGAIN:
    case E::SHUTDOWN:
    case E::NOTSTORAGE:
    case E::FAILED:
      break;

    default:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Received GET_TRIM_POINT_REPLY message from "
                      "%s with unexpected status %s",
                      from.toString().c_str(),
                      error_description(status));
      break;
  }
}

void GetTrimPointRequest::onMessageSent(ShardID to, Status status) {
  if (status != E::OK) {
    if (status == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "GET_TRIM_POINT is not supported by the server for "
                      "shard %s",
                      to.toString().c_str());
    }
  }
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GetTrimPointRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

std::shared_ptr<Sequencer> GetTrimPointRequest::getSequencer() const {
  Worker* w = Worker::onThisThread();
  auto& seqmap = w->processor_->allSequencers();
  return seqmap.findSequencer(log_id_);
}

void GetTrimPointRequest::updateTrimPoint(Status status, lsn_t tp) const {
  auto sequencer = getSequencer();
  if (sequencer != nullptr &&
      sequencer->getState() == Sequencer::State::ACTIVE) {
    sequencer->updateTrimPoint(status, tp);
  }
}

int GetTrimPointRequest::getThreadAffinity(int nthreads) {
  return folly::hash::twang_mix64(log_id_.val_) % nthreads;
}

}} // namespace facebook::logdevice
