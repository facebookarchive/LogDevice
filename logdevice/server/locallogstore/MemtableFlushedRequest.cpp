/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/MemtableFlushedRequest.h"

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

Request::Execution MemtableFlushedRequest::execute() {
  if (isLocalFlush()) {
    broadcast();
  }

  applyFlush();
  return Execution::COMPLETE;
}

void MemtableFlushedRequest::broadcast() {
  const auto& nodes_configuration = getNodesConfiguration();
  const auto& storage_membership = nodes_configuration->getStorageMembership();

  for (const node_index_t node : *storage_membership) {
    // current flexible log sharding is not supported in rebuilding, so
    // here we send to all storage node and let the recipent node do the
    // message routing
    if (node != getMyNodeID().index() && responsibleForNodesUpdates(node)) {
      MEMTABLE_FLUSHED_Header header(
          flushToken_, server_instance_id_, shard_idx_, node_index_);
      NodeID nodeId(node, nodes_configuration->getNodeGeneration(node));
      auto msg = std::make_unique<MEMTABLE_FLUSHED_Message>(header);
      int rv = sender_->sendMessage(std::move(msg), nodeId);
      if (rv != 0) {
        ld_debug("Failed to send MEMTABLE_FLUSHED_Message to %s with "
                 "{shardId:%d, FlushToken:%lu} : %s",
                 Sender::describeConnection(nodeId).c_str(),
                 shard_idx_,
                 flushToken_,
                 error_name(err));
      }
    }
  }
}

void MemtableFlushedRequest::applyFlush() {
  // send an update to all LogRebuilding state machines
  // whose log maps to the shard on which memtable was flushed.
  ServerWorker* w = ServerWorker::onThisThread();
  for (const auto& lr : w->runningLogRebuildings().map) {
    if (shard_idx_ == lr.first.second) {
      lr.second->onMemtableFlushed(
          node_index_, server_instance_id_, flushToken_);
    }
  }
}

NodeID MemtableFlushedRequest::getMyNodeID() const {
  ServerWorker* w = ServerWorker::onThisThread();
  return w->processor_->getMyNodeID();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
MemtableFlushedRequest::getNodesConfiguration() const {
  const ServerWorker* w = ServerWorker::onThisThread();
  return w->getNodesConfiguration();
}

int MemtableFlushedRequest::getThreadAffinity(int /*unused*/) {
  return int(worker_id_);
}

bool MemtableFlushedRequest::responsibleForNodesUpdates(
    node_index_t nodeIndex) {
  return (nodeIndex % Worker::settings().num_workers) == (int)worker_id_;
}

bool MemtableFlushedRequest::isLocalFlush() {
  return node_index_ == getMyNodeID().index();
}

}} // namespace facebook::logdevice
