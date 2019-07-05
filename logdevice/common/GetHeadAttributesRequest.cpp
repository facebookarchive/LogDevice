/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetHeadAttributesRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_Message.h"

namespace facebook { namespace logdevice {

Request::Execution GetHeadAttributesRequest::execute() {
  // Check parameters
  if (log_id_ == LOGID_INVALID) {
    ld_error("Invalid log ID");
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  if (MetaDataLog::isMetaDataLog(log_id_)) {
    ld_error("%lu is a metadata log ID, which is not supported "
             "by GetHeadAttributesRequest.",
             log_id_.val());
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  // Set the client timer
  client_timeout_timer_ =
      std::make_unique<Timer>([this] { this->onClientTimeout(); });
  client_timeout_timer_->activate(client_timeout_);

  // Insert request into map for worker to track it
  auto insert_result =
      Worker::onThisThread()->runningGetHeadAttributes().map.insert(
          std::make_pair(id_, std::unique_ptr<GetHeadAttributesRequest>(this)));
  ld_check(insert_result.second);

  registerForShardAuthoritativeStatusUpdates();

  initNodeSetFinder();
  return Execution::CONTINUE;
}

void GetHeadAttributesRequest::initNodeSetFinder() {
  nodeset_finder_ = makeNodeSetFinder();
  nodeset_finder_->start();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GetHeadAttributesRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

std::unique_ptr<NodeSetFinder> GetHeadAttributesRequest::makeNodeSetFinder() {
  return std::make_unique<NodeSetFinder>(
      log_id_, client_timeout_, [this](Status status) { this->start(status); });
}

void GetHeadAttributesRequest::initStorageSetAccessor() {
  ld_check(nodeset_finder_);
  auto shards = nodeset_finder_->getUnionStorageSet(*getNodesConfiguration());
  ReplicationProperty minRep = nodeset_finder_->getNarrowestReplication();

  ld_debug("Building StorageSetAccessor with nodeset %s, replication %s",
           toString(shards).c_str(),
           minRep.toString().c_str());

  StorageSetAccessor::ShardAccessFunc shard_access =
      [this](ShardID shard, const StorageSetAccessor::WaveInfo&) {
        return this->sendTo(shard);
      };

  StorageSetAccessor::CompletionFunc completion = [this](Status st) {
    finalize(st);
  };

  nodeset_accessor_ = makeStorageSetAccessor(
      getNodesConfiguration(), shards, minRep, shard_access, completion);
  ld_check(nodeset_accessor_ != nullptr);
  nodeset_accessor_->start();
};

std::unique_ptr<StorageSetAccessor>
GetHeadAttributesRequest::makeStorageSetAccessor(
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_configuration,
    StorageSet shards,
    ReplicationProperty minRep,
    StorageSetAccessor::ShardAccessFunc node_access,
    StorageSetAccessor::CompletionFunc completion) {
  return std::make_unique<StorageSetAccessor>(
      log_id_,
      shards,
      nodes_configuration,
      minRep,
      node_access,
      completion,
      StorageSetAccessor::Property::FMAJORITY,
      client_timeout_);
}

void GetHeadAttributesRequest::start(Status status) {
  if (status != E::OK) {
    ld_error("Unable to get the set of nodes to send GetHeadAttributes "
             "requests to for log %lu: %s.",
             log_id_.val_,
             error_name(status));
    finalize(status);
    return;
  }

  initStorageSetAccessor();
}

StorageSetAccessor::SendResult GetHeadAttributesRequest::sendTo(ShardID shard) {
  const auto& nodes_configuration = getNodesConfiguration();
  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard.node())) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Cannot find node at index %u in NodesConfiguration",
                    shard.node());
    return {StorageSetAccessor::Result::PERMANENT_ERROR, Status::NOTFOUND};
  }

  NodeID to(shard.node());
  GET_HEAD_ATTRIBUTES_Header header = {
      id_, log_id_, /*flags=*/0, shard.shard()};
  auto msg = std::make_unique<GET_HEAD_ATTRIBUTES_Message>(header);
  if (Worker::onThisThread()->sender().sendMessage(std::move(msg), to) != 0) {
    if (err == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "GET_HEAD_ATTRIBUTES is not supported by the server at "
                      "%s",
                      shard.toString().c_str());
      return {StorageSetAccessor::Result::PERMANENT_ERROR, err};
    } else {
      return {StorageSetAccessor::Result::TRANSIENT_ERROR, err};
    }
  }

  return {StorageSetAccessor::Result::SUCCESS, err};
}

void GetHeadAttributesRequest::finalize(Status status, bool delete_this) {
  ld_check(!callback_called_);
  callback_called_ = true;
  callback_(*this,
            status,
            std::make_unique<LogHeadAttributes>(
                LogHeadAttributes{trim_point_, trim_point_timestamp_}));
  if (delete_this) {
    deleteThis();
  }
}

GetHeadAttributesRequest::~GetHeadAttributesRequest() {
  const Worker* worker = Worker::onThisThread(false /* enforce_worker */);
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!callback_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    ld_check(worker->shuttingDown());
    ld_warning("GetHeadAttributesRequest destroyed while still processing");
    callback_(*this,
              E::SHUTDOWN,
              std::make_unique<LogHeadAttributes>(LogHeadAttributes()));
  }
}

void GetHeadAttributesRequest::onMessageSent(ShardID to, Status status) {
  if (status != E::OK) {
    if (status == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "GET_HEAD_ATTRIBUTES is not supported by the server at "
                      "of shard %s",
                      to.toString().c_str());
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::PERMANENT_ERROR, status});
    } else {
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::TRANSIENT_ERROR, status});
    }
  }
}

void GetHeadAttributesRequest::onReply(ShardID from,
                                       Status status,
                                       LogHeadAttributes attributes) {
  ld_debug("Received GET_HEAD_ATTRIBUTES_REPLY from %s, status=%s, "
           "trim point=%s, trim point timestamp=%ld",
           from.toString().c_str(),
           error_name(status),
           lsn_to_string(attributes.trim_point).c_str(),
           attributes.trim_point_timestamp.count());

  auto res = StorageSetAccessor::Result::SUCCESS;

  // See GET_HEAD_ATTRIBUTES_REPLY_Header doc block for possible statuses
  // explanation.
  switch (status) {
    case E::OK:
      trim_point_ = std::max(trim_point_, attributes.trim_point);
      trim_point_timestamp_ =
          std::min(trim_point_timestamp_, attributes.trim_point_timestamp);
      break;

    case E::REBUILDING:
    case E::DROPPED:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Failure while getting head attributes from %s node "
                        "with status %s.",
                        from.toString().c_str(),
                        error_name(status));
      res = StorageSetAccessor::Result::TRANSIENT_ERROR;
      break;

    case E::NOTSTORAGE:
    case E::NOTSUPPORTED:
    case E::FAILED:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Failure while getting head attributes from %s node "
                        "with status %s.",
                        from.toString().c_str(),
                        error_name(status));
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;

    default:
      ld_error("Received GET_HEAD_ATTRIBUTES_REPLY message from %s with "
               "unexpected status %s",
               from.toString().c_str(),
               error_name(status));
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;
  }

  nodeset_accessor_->onShardAccessed(from, {res, status});
}

void GetHeadAttributesRequest::onClientTimeout() {
  RATELIMIT_WARNING(std::chrono::seconds(1),
                    10,
                    "timed out (%ld ms) waiting for storage nodes, "
                    "assuming that the log is not empty. Shard states: %s",
                    client_timeout_.count(),
                    (nodeset_accessor_ != nullptr)
                        ? nodeset_accessor_->describeState().c_str()
                        : "");
  finalize(E::TIMEDOUT);
}

void GetHeadAttributesRequest::onShardStatusChanged() {
  // nodeset accessor may be not constructed yet.
  if (nodeset_accessor_) {
    nodeset_accessor_->onShardStatusChanged();
  }
}

void GetHeadAttributesRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningGetHeadAttributes().map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

}} // namespace facebook::logdevice
