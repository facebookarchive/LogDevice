/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DataSizeRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/DATA_SIZE_Message.h"

namespace facebook { namespace logdevice {

Request::Execution DataSizeRequest::execute() {
  // Check parameters
  if (log_id_ == LOGID_INVALID) {
    ld_error("Invalid log ID");
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  if (MetaDataLog::isMetaDataLog(log_id_)) {
    ld_error("%lu is a metadata log ID, which is not supported "
             "by DataSizeRequest.",
             log_id_.val());
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  if (start_ > end_) {
    ld_error("DATA_SIZE for log %lu: Received invalid time range [%lu,%lu]",
             log_id_.val_,
             start_.count(),
             end_.count());
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  // Insert request into map for worker to track it
  auto insert_result = Worker::onThisThread()->runningDataSize().map.insert(
      std::make_pair(id_, std::unique_ptr<DataSizeRequest>(this)));
  ld_check(insert_result.second);

  registerForShardAuthoritativeStatusUpdates();
  initNodeSetFinder();
  return callback_called_ ? Execution::COMPLETE : Execution::CONTINUE;
}

void DataSizeRequest::initNodeSetFinder() {
  nodeset_finder_ = makeNodeSetFinder();
  nodeset_finder_->start();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
DataSizeRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

std::unique_ptr<NodeSetFinder> DataSizeRequest::makeNodeSetFinder() {
  return std::make_unique<NodeSetFinder>(
      log_id_,
      client_timeout_,
      [this](Status status) { this->start(status); },
      // Currently we enforce that DataSizeRequest finds nodesets through
      // reading the metadata logs. The reason is to make sure that this client
      // eventually receives SHARD_STATUS_UPDATE messages via its metadata log
      // read streams.
      // TODO(T25938692) use metadata logs v2
      NodeSetFinder::Source::METADATA_LOG);
}

void DataSizeRequest::initStorageSetAccessor() {
  ld_check(nodeset_finder_);
  auto shards = nodeset_finder_->getUnionStorageSet(*getNodesConfiguration());
  ReplicationProperty minRep = nodeset_finder_->getNarrowestReplication();
  replication_factor_ = minRep.getReplicationFactor();
  ld_debug("Building StorageSetAccessor with %lu shards in storage set, "
           "replication %s",
           shards.size(),
           minRep.toString().c_str());

  StorageSetAccessor::ShardAccessFunc shard_access =
      [this](ShardID shard, const StorageSetAccessor::WaveInfo&) {
        return this->sendTo(shard);
      };

  StorageSetAccessor::CompletionFunc completion = [this](Status st) {
    this->status_ = st;
    finalize(st);
  };

  nodeset_accessor_ = makeStorageSetAccessor(
      getNodesConfiguration(), shards, minRep, shard_access, completion);
  failure_domain_ = makeFailureDomain(shards, getNodesConfiguration(), minRep);
}

void DataSizeRequest::onShardStatusChanged(bool initialize) {
  // nodeset accessor may be not constructed yet.
  if (nodeset_accessor_) {
    applyShardStatus(initialize);

    // A node may have become underreplicated or unavailable, such that there
    // is no longer enough nodes not in rebuilding to wait for responses from.
    // We might as well finish early in this case.
    if (haveDeadEnd()) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "DATA_SIZE[%lu] hit a dead end due to shard "
                     "authoritative status change. Shard statuses: %s",
                     log_id_.val(),
                     nodeset_accessor_->describeState().c_str());
      finalize(E::PARTIAL, /*delete_this=*/!initialize);
      return;
    }

    if (!initialize) {
      // NodeSetAccessor won't be ready when we initialize this request's
      // failure domain, but will update itself when we start it.
      nodeset_accessor_->onShardStatusChanged();
    }
  }
}

ShardAuthoritativeStatusMap& DataSizeRequest::getShardAuthoritativeStatusMap() {
  return Worker::onThisThread()
      ->shardStatusManager()
      .getShardAuthoritativeStatusMap();
}

void DataSizeRequest::applyShardStatus(bool initialize_unknown) {
  const auto& shard_status_map = getShardAuthoritativeStatusMap();
  for (const ShardID shard : nodeset_accessor_->getShards()) {
    const auto auth_st =
        shard_status_map.getShardStatus(shard.node(), shard.shard());
    setShardAuthoritativeStatus(shard, auth_st, initialize_unknown);
  }
}

void DataSizeRequest::setShardAuthoritativeStatus(ShardID shard,
                                                  AuthoritativeStatus auth_st,
                                                  bool initialize_unknown) {
  if (!failure_domain_->containsShard(shard)) {
    ld_warning("Given authoritative status for shard not known by failure "
               "detector in isLogEmpty for log %lu: %s",
               log_id_.val(),
               shard.toString().c_str());
    return; // ignore shard, might be newly added node
  }

  shard_status_t shard_st = 0;
  int rv = failure_domain_->getShardAttribute(shard, &shard_st);
  if (rv != 0 && !initialize_unknown) {
    ld_error("Failed to find status for shard %s in isLogEmpty for log %lu",
             shard.toString().c_str(),
             log_id_.val());
    ld_check(false);
    return;
  }

  shard_st = (auth_st == AuthoritativeStatus::UNDERREPLICATION ||
              auth_st == AuthoritativeStatus::UNAVAILABLE)
      ? shard_st | SHARD_IS_REBUILDING
      : shard_st & ~SHARD_IS_REBUILDING;

  // If a shard becomes AUTHORITATIVE_EMPTY, mark it as having a result, so
  // that the result is as if the node had responded 0.
  if (auth_st == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
    shard_st |= SHARD_HAS_RESULT;
  }
  failure_domain_->setShardAttribute(shard, shard_st);
  failure_domain_->setShardAuthoritativeStatus(shard, auth_st);
}

std::unique_ptr<StorageSetAccessor> DataSizeRequest::makeStorageSetAccessor(
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
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

void DataSizeRequest::start(Status status) {
  if (status != E::OK) {
    ld_error("Unable to get the set of nodes to send DataSize requests to "
             "for log %lu: %s.",
             log_id_.val_,
             error_description(status));
    finalize(status, /*delete_this=*/false);
    return;
  }

  initStorageSetAccessor();
  ld_check(nodeset_accessor_ != nullptr);
  ld_check(!callback_called_);
  onShardStatusChanged(/*initialize=*/true);
  if (!callback_called_) {
    nodeset_accessor_->start();
  }
}

StorageSetAccessor::SendResult DataSizeRequest::sendTo(ShardID shard) {
  const auto& nodes_configuration = getNodesConfiguration();
  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard.node())) {
    ld_error("Cannot find node at index %u", shard.node());
    return {StorageSetAccessor::Result::PERMANENT_ERROR, Status::NOTFOUND};
  }

  NodeID to(shard.node());
  DATA_SIZE_Header header = {
      id_, log_id_, shard.shard(), start_.count(), end_.count()};
  auto msg = std::make_unique<DATA_SIZE_Message>(header);
  if (Worker::onThisThread()->sender().sendMessage(std::move(msg), to) != 0) {
    if (err == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "DATA_SIZE is not supported by the server at %s",
                      Sender::describeConnection(to).c_str());
      return {StorageSetAccessor::Result::PERMANENT_ERROR, err};
    } else {
      return {StorageSetAccessor::Result::TRANSIENT_ERROR, err};
    }
  }

  return {StorageSetAccessor::Result::SUCCESS, Status::OK};
}

void DataSizeRequest::finalize(Status status, bool delete_this) {
  ld_check(!callback_called_);
  callback_called_ = true;

  switch (status) {
    case E::OK:
    case E::PARTIAL:
    case E::TIMEDOUT:
    case E::FAILED:
    case E::INVALID_PARAM:
    case E::ACCESS:
      break;
    default:
      ld_error("DataSize[%lu] finished with unexpected status %s; returning "
               "FAILED instead. Shard statuses: %s",
               log_id_.val(),
               error_name(status),
               nodeset_accessor_->describeState().c_str());
      ld_check(false);
      status = E::FAILED;
  }

  if (status == E::OK || status == E::PARTIAL) {
    ld_check_ne(replication_factor_, 0);

    // Below, we guess that the remaining nodes have the average size of data
    // stored for this log and time range.

    // Divide by the replication factor
    size_t result_size =
        static_cast<size_t>(data_size_ / double(replication_factor_));

    // This also includes nodes that were simply AUTHORITATIVE_EMPTY, so it's
    // as if they responded 0.
    size_t num_responses = failure_domain_->countShards(
        [](shard_status_t val) { return val & SHARD_HAS_RESULT; });

    if (num_responses == 0) {
      // We found ourselves in a dead end during start
      callback_(*this, E::FAILED, 0);
    } else {
      size_t avg_size = result_size / double(num_responses);
      size_t final_estimate =
          round(avg_size * double(failure_domain_->numShards()));
      callback_(*this, status, final_estimate);
    }
  } else {
    callback_(*this, status, 0);
  }

  if (delete_this) {
    deleteThis();
  }
}

DataSizeRequest::~DataSizeRequest() {
  const Worker* worker =
      static_cast<Worker*>(Worker::onThisThread(false /*enforce_worker*/));
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!callback_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    ld_check(worker->shuttingDown());
    ld_warning("DataSizeRequest destroyed while still processing");
    callback_(*this, E::SHUTDOWN, 0);
  }
}

void DataSizeRequest::onMessageSent(ShardID to, Status status) {
  if (status != E::OK) {
    if (status == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "DATA_SIZE is not supported by the server for shard %s",
                      to.toString().c_str());
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::PERMANENT_ERROR, status});
    } else {
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::TRANSIENT_ERROR, status});
    }
  }
}

bool DataSizeRequest::haveDeadEnd() {
  // Check if we could possibly get an f-majority of responses, i.e. there are
  // enough remaining nodes that are not underreplicated or in other transient
  // or permanent error states to get proper responses from an f-majority of
  // the nodes.
  bool res = failure_domain_->isFmajority([&](shard_status_t val) {
    return this->node_has_result_filter(val) || potential_response_filter(val);
  }) == FmajorityResult::NONE;
  return res;
}

void DataSizeRequest::onReply(ShardID from, Status status, size_t size) {
  ld_debug("Received DATA_SIZE_REPLY[%lu] from %s, status=%s, result=%lu",
           log_id_.val(),
           from.toString().c_str(),
           error_name(status),
           size);

  if (!failure_domain_->containsShard(from)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "dataSize[%lu] Shard %s not known, ignoring",
                    log_id_.val(),
                    from.toString().c_str());
    return;
  }

  shard_status_t shard_st;
  int rv = failure_domain_->getShardAttribute(from, &shard_st);
  ld_check_eq(rv, 0);

  if (shard_st & SHARD_HAS_RESULT) {
    // We already have a healthy result for it; ignore reply. This can happen
    // e.g. if we re-sent the request due to wave timeout and then the node
    // responds to both at once, or if a node became AUTHORITATIVE_EMPTY and
    // then responded.
    ld_debug("dataSize[%lu] Ignoring a reply from %s with status %s and size "
             "%lu since we already have a healthy result for it.",
             log_id_.val(),
             from.toString().c_str(),
             error_name(status),
             size);
    return;
  }

  auto res = StorageSetAccessor::Result::SUCCESS;

  switch (status) {
    case E::OK:
      // For simplicity, just count the first returned result. For instance,
      // if a wave timed out and we re-sent the request, we might see two
      // responses from the given shard, but don't want to count its answer
      // twice.
      if (~shard_st & SHARD_HAS_RESULT) {
        data_size_ += size;
      }

      shard_st |= SHARD_HAS_RESULT;
      failure_domain_->setShardAttribute(from, shard_st);
      break;

    case E::REBUILDING:
      RATELIMIT_DEBUG(std::chrono::seconds(1),
                      10,
                      "shard %s is rebuilding.",
                      from.toString().c_str());
      res = StorageSetAccessor::Result::TRANSIENT_ERROR;
      break;

    case E::AGAIN:
      res = StorageSetAccessor::Result::TRANSIENT_ERROR;
      break;

    case E::SHUTDOWN:
    case E::NOTSTORAGE:
    case E::FAILED:
    case E::NOTSUPPORTED:
    case E::NOTSUPPORTEDLOG:
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      {
        // Note that we shouldn't expect a response from this node; no need to
        // keep any previous shard status we might have stored as attribute.
        // For instance, non-partitioned log stores do not support dataSize,
        // and FAILED is returned in case the local log store has a permanent
        // error.
        failure_domain_->setShardAttribute(from, SHARD_HAS_ERROR);
      }
      break;

    default:
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "Received DATA_SIZE_REPLY message from %s with unexpected status %s "
          "for log %lu",
          from.toString().c_str(),
          error_description(status),
          log_id_.val());
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;
  }

  nodeset_accessor_->onShardAccessed(from, {res, status});
}

void DataSizeRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningDataSize().map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

}} // namespace facebook::logdevice
