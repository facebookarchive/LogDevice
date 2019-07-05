/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FindKeyRequest.h"

#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/FINDKEY_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

Request::Execution FindKeyRequest::execute() {
  // Everything up to start() runs in the context of a Worker().  Unit tests
  // call start() directly.

  // Check parameters
  if (log_id_ == LOGID_INVALID) {
    RATELIMIT_ERROR(std::chrono::seconds(10), 2, "Invalid log ID");
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  if (MetaDataLog::isMetaDataLog(log_id_)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "%lu is a metadata log ID, which is not supported "
                    "by FindKeyRequest.",
                    log_id_.val());
    finalize(E::INVALID_PARAM, /*delete_this=*/false);
    return Execution::COMPLETE;
  }

  client_timeout_timer_ =
      std::make_unique<Timer>([this] { this->onClientTimeout(); });
  client_timeout_timer_->activate(client_timeout_);

  auto insert_result = Worker::onThisThread()->runningFindKey().map.insert(
      std::make_pair(id_, std::unique_ptr<FindKeyRequest>(this)));
  ld_check(insert_result.second);

  registerForShardAuthoritativeStatusUpdates();

  initNodeSetFinder();
  return Execution::CONTINUE;
}

void FindKeyRequest::initNodeSetFinder() {
  auto cb = [this](Status status) { this->start(status); };

  // create a nodeset finder with timeout = client_timeout / 2
  // Half of the timeout is for reading the metadata log
  nodeset_finder_ = makeNodeSetFinder(log_id_, client_timeout_ / 2, cb);
  nodeset_finder_->start();
}

std::unique_ptr<NodeSetFinder>
FindKeyRequest::makeNodeSetFinder(logid_t log_id,
                                  std::chrono::milliseconds timeout,
                                  std::function<void(Status)> cb) const {
  return std::make_unique<NodeSetFinder>(log_id, timeout, cb);
}

void FindKeyRequest::start(Status status) {
  if (status != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Unable to find nodeset for log %lu: %s",
                    log_id_.val_,
                    error_description(status));

    if (status != E::INVALID_PARAM && status != E::ACCESS) {
      ld_check_in(status, ({E::TIMEDOUT, E::FAILED}));
      status = E::FAILED;
    }

    finalize(status);
    return;
  }

  ld_check(nodeset_finder_);

  initStorageSetAccessor();
  ld_check(nodeset_accessor_ != nullptr);
  ld_check(result_hi_ > result_lo_ + 1);
}

StorageSetAccessor::SendResult FindKeyRequest::sendTo(ShardID shard) {
  const auto& nodes_configuration = getNodesConfiguration();
  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard.node())) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Cannot find node at index %u in NodesConfiguration",
                    shard.node());
    return {StorageSetAccessor::Result::PERMANENT_ERROR, Status::NOTFOUND};
  }

  FINDKEY_flags_t flags = 0;
  if (accuracy_ == FindKeyAccuracy::APPROXIMATE) {
    flags |= FINDKEY_Header::APPROXIMATE;
  }
  if (key_.hasValue()) {
    flags |= FINDKEY_Header::USER_KEY;
  }
  FINDKEY_Header header = {id_,
                           log_id_,
                           timestamp_.count(),
                           flags,
                           result_lo_,
                           result_hi_,
                           running_timer_timeout_.count(),
                           shard.shard()};
  if (sendOneMessage(header, shard) != 0) {
    if (err == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "FINDKEY is not supported by the server at %s",
                      shard.toString().c_str());
      return {StorageSetAccessor::Result::PERMANENT_ERROR, err};
    } else {
      return {StorageSetAccessor::Result::TRANSIENT_ERROR, err};
    }
  }
  return {StorageSetAccessor::Result::SUCCESS, err};
}

int FindKeyRequest::sendOneMessage(const FINDKEY_Header& header, ShardID to) {
  auto msg = std::make_unique<FINDKEY_Message>(header, key_);
  NodeID node_id(to.node());
  return Worker::onThisThread()->sender().sendMessage(std::move(msg), node_id);
}

void FindKeyRequest::finalize(Status status, bool delete_this) {
  ld_check(!callback_called_);
  ld_check_in(status,
              ({E::INVALID_PARAM,
                E::OK,
                E::PARTIAL,
                E::FAILED,
                E::SHUTDOWN,
                E::ACCESS}));
  callback_called_ = true;
  bool lsn_invalid = (status == E::FAILED || status == E::INVALID_PARAM);

  if (!key_.hasValue()) {
    // If we got at least one successful reply, we can return at least a
    // somewhat accurate result to the application.  Otherwise, return
    // LSN_INVALID.
    lsn_t result_lsn = !lsn_invalid ? result_lo_ + 1 : LSN_INVALID;
    callback_(*this, status, result_lsn);
  } else {
    lsn_t result_lo = lsn_invalid ? LSN_INVALID : result_lo_;
    lsn_t result_hi = lsn_invalid ? LSN_INVALID : result_hi_;
    FindKeyResult result = {status, result_lo, result_hi};
    callback_key_(*this, result);
  }

  if (status != E::OK && nodeset_accessor_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Failed for log %lu. Status: %s. %s",
                   log_id_.val(),
                   error_name(status),
                   nodeset_accessor_->describeState().c_str());
  }

  if (delete_this) {
    deleteThis();
  }
}

FindKeyRequest::~FindKeyRequest() {
  const Worker* worker = Worker::onThisThread(false /* enforce_worker */);
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!callback_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    ld_check(worker->shuttingDown());
    ld_warning("FindKeyRequest destroyed while still processing");
    finalize(E::SHUTDOWN, false);
  }
}

void FindKeyRequest::onMessageSent(ShardID to, Status status) {
  if (status != E::OK) {
    if (status == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "FINDKEY is not supported by the server at %s",
                      to.toString().c_str());
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::PERMANENT_ERROR, status});
    } else {
      nodeset_accessor_->onShardAccessed(
          to, {StorageSetAccessor::Result::TRANSIENT_ERROR, status});
    }
  }
}

void FindKeyRequest::onReply(ShardID from, Status status, lsn_t lo, lsn_t hi) {
  ld_debug(
      "received FINDKEY_REPLY from %s, status=%s, result=(%s, %s] for log %lu",
      from.toString().c_str(),
      error_description(status),
      lsn_to_string(lo).c_str(),
      lsn_to_string(hi).c_str(),
      log_id_.val_);

  auto res = StorageSetAccessor::Result::SUCCESS;

  switch (status) {
    case E::OK:
      if (lo >= hi) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "got FINDKEY_REPLY message from %s with malformed result "
            "range (%s, %s], ignoring",
            from.toString().c_str(),
            lsn_to_string(lo).c_str(),
            lsn_to_string(hi).c_str());
        res = StorageSetAccessor::Result::PERMANENT_ERROR;
        break;
      }

      result_lo_ = std::max(result_lo_, lo);
      result_hi_ = std::min(result_hi_, hi);

      if (result_hi_ < result_lo_ || result_hi_ - result_lo_ <= 1) {
        // Ideally result_hi == result_lo + 1 and that is the correct answer.
        // Empty or malformed ranges can happen when record timestamps are not
        // increasing, in which case we don't guarantee correctness but the
        // result should still be somewhat reasonable.
        finalize(E::OK);
        return;
      }
      break;

    case E::NOTSTORAGE:
    case E::REBUILDING:
    case E::TIMEDOUT:
      res = StorageSetAccessor::Result::TRANSIENT_ERROR;
      break;

    case E::AGAIN:
      res = StorageSetAccessor::Result::TRANSIENT_ERROR;
      break;

    case E::FAILED:
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;

    case E::ACCESS:
      finalize(E::ACCESS);
      return;

    default:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "received FINDKEY_REPLY message from %s with unexpected "
                      "status %s",
                      from.toString().c_str(),
                      error_description(status));
      res = StorageSetAccessor::Result::PERMANENT_ERROR;
      break;
  }

  ld_check(nodeset_accessor_);
  nodeset_accessor_->onShardAccessed(from, {res, status});

  return;
}

void FindKeyRequest::onClientTimeout() {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  10,
                  "timed out (%ld ms)",
                  client_timeout_.count());
  Status st;
  if (hasResponse()) {
    st = E::PARTIAL;
  } else {
    st = E::FAILED;
  }
  finalize(st);
}

void FindKeyRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningFindKey().map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

void FindKeyRequest::onShardStatusChanged() {
  // nodeset accessor may be not constructed yet.
  if (nodeset_accessor_) {
    nodeset_accessor_->onShardStatusChanged();
  }
}

void FindKeyRequest::initStorageSetAccessor() {
  ld_check(nodeset_finder_);
  auto shards = nodeset_finder_->getUnionStorageSet(*getNodesConfiguration());
  ReplicationProperty minRep = nodeset_finder_->getNarrowestReplication();
  ld_debug("Building StorageSetAccessor with %lu shards in nodeset, "
           "replication %s",
           shards.size(),
           minRep.toString().c_str());

  StorageSetAccessor::ShardAccessFunc shard_access =
      [this](ShardID shard, const StorageSetAccessor::WaveInfo&) {
        return this->sendTo(shard);
      };

  StorageSetAccessor::CompletionFunc completion = [this](Status st) {
    // E::ABORTED should be impossible because sendTo() never returns ABORT.
    ld_check_in(st, ({E::OK, E::TIMEDOUT, E::FAILED}));

    if (st != E::OK) {
      st = hasResponse() ? E::PARTIAL : E::FAILED;
    }

    finalize(st);
  };

  nodeset_accessor_ = makeStorageSetAccessor(
      getNodesConfiguration(), shards, minRep, shard_access, completion);
  nodeset_accessor_->noEarlyAbort();

  ld_check(nodeset_accessor_ != nullptr);
  nodeset_accessor_->start();
}

std::unique_ptr<StorageSetAccessor> FindKeyRequest::makeStorageSetAccessor(
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    StorageSet shards,
    ReplicationProperty minRep,
    StorageSetAccessor::ShardAccessFunc shard_access,
    StorageSetAccessor::CompletionFunc completion) {
  return std::make_unique<StorageSetAccessor>(
      log_id_,
      shards,
      nodes_configuration,
      minRep,
      shard_access,
      completion,
      StorageSetAccessor::Property::FMAJORITY,
      client_timeout_);
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
FindKeyRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

}} // namespace facebook::logdevice
