/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/MEMTABLE_FLUSHED_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file Created by PartitionedRocksDBStore when it receives a callback on
 *       memtable being flushed to stable storage.
 */

class MemtableFlushedRequest : public Request {
 public:
  MemtableFlushedRequest(worker_id_t worker_id,
                         node_index_t node_index,
                         ServerInstanceId server_instance_id,
                         uint32_t shard_idx,
                         FlushToken token)
      : Request(RequestType::MEMTABLE_FLUSHED),
        sender_(std::make_unique<SenderProxy>()),
        worker_id_(worker_id),
        node_index_(node_index),
        server_instance_id_(server_instance_id),
        shard_idx_(shard_idx),
        flushToken_(token) {}

  ~MemtableFlushedRequest() override {}

  Request::Execution execute() override;

  int getThreadAffinity(int nthreads) override;

  virtual NodeID getMyNodeID() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;
  virtual bool responsibleForNodesUpdates(node_index_t nodeIndex);
  virtual void applyFlush();

 protected:
  std::unique_ptr<SenderBase> sender_;

 private:
  void broadcast();
  bool isLocalFlush();
  const worker_id_t worker_id_;
  const node_index_t node_index_;
  const ServerInstanceId server_instance_id_;
  const uint32_t shard_idx_;
  const FlushToken flushToken_;
};

}} // namespace facebook::logdevice
