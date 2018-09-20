/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "TrimRequest.h"

#include <folly/Memory.h>
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/TRIM_Message.h"

namespace facebook { namespace logdevice {

Request::Execution TrimRequest::execute() {
  auto it = Worker::onThisThread()->runningTrimRequests().map.insert(
      std::make_pair(id_, std::unique_ptr<TrimRequest>(this)));
  ld_check(it.second);

  // Kick off the state machine
  if (bypass_write_token_check_) {
    // No need to fetch log config in this case
    onWriteTokenCheckDone();
  } else {
    fetchLogConfig();
  }

  return Execution::CONTINUE;
}

TrimRequest::~TrimRequest() {
  const Worker* worker = static_cast<Worker*>(EventLoop::onThisThread());
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (callback_) {
    if (worker->shuttingDown()) {
      // Request was aborted because the worker is shutting down.
      callback_(E::SHUTDOWN);
    } else {
      // Request was aborted because `this` was destroyed before it could
      // finish.
      callback_(E::ABORTED);
    }
    callback_ = nullptr;
  }
}

void TrimRequest::onTrimPastTailCheckDone() {
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    start(E::OK);
  } else {
    // for data logs, find the historical nodesets using NodeSetFinder
    initNodeSetFinder();
  }
}

void TrimRequest::checkForTrimPastTail() {
  auto ticket = callback_helper_.ticket();
  auto cb_wrapper = [ticket](Status st,
                             NodeID /*unused*/,
                             lsn_t next_lsn,
                             std::unique_ptr<LogTailAttributes> /*unused*/,
                             std::shared_ptr<const EpochMetaDataMap> /*unused*/,
                             std::shared_ptr<TailRecord> /*unused*/) {
    ticket.postCallbackRequest([=](TrimRequest* trimReq) {
      if (trimReq) {
        if (st == E::OK && trimReq->trim_point_ < next_lsn) {
          // Continue to the next stage.
          trimReq->onTrimPastTailCheckDone();
        } else {
          // something went wrong
          Status s = st;
          if (s == E::OK) {
            // Trim was past the tail
            s = E::TOOBIG;
          }
          trimReq->finalize(s);
        }
      }
    });
  };

  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      log_id_,
      0, // SyncSequencerRequest flags
      cb_wrapper,
      GetSeqStateRequest::Context::GET_TAIL_LSN,
      client_timeout_);

  Worker::onThisThread()->processor_->postImportant(req);
}

void TrimRequest::initNodeSetFinder() {
  nodeset_finder_ = makeNodeSetFinder();
  nodeset_finder_->start();
}

std::unique_ptr<NodeSetFinder> TrimRequest::makeNodeSetFinder() {
  return std::make_unique<NodeSetFinder>(
      log_id_, client_timeout_, [this](Status status) { this->start(status); });
}

void TrimRequest::start(Status status) {
  if (status != E::OK) {
    ld_error("Unable to get the set of shards to send TrimRequest requests to "
             "for log %lu: %s.",
             log_id_.val_,
             error_description(status));
    finalize(status);
    return;
  }

  initStorageSetAccessor();
}

void TrimRequest::fetchLogConfig() {
  ld_check(client_ != nullptr);
  request_id_t rqid = id_;
  Worker::onThisThread()->getConfig()->getLogGroupByIDAsync(
      log_id_, [rqid](std::shared_ptr<LogsConfig::LogGroupNode> logcfg) {
        // Callback must not bind to `this', need to go through
        // `Worker::runningTrimRequests_' in case the TrimRequest timed out
        // while waiting for the config.
        auto& runningTrimRequests =
            Worker::onThisThread()->runningTrimRequests().map;
        auto it = runningTrimRequests.find(rqid);
        if (it != runningTrimRequests.end()) {
          it->second->onLogConfigAvailable(std::move(logcfg));
        }
      });
}

void TrimRequest::onLogConfigAvailable(
    std::shared_ptr<LogsConfig::LogGroupNode> cfg) {
  if (!cfg) {
    finalize(E::NOTFOUND);
    return;
  }
  auto attrs = cfg->attrs();
  // Check the write token ...
  if (attrs.writeToken().hasValue() && attrs.writeToken().value().hasValue() &&
      !client_->hasWriteToken(attrs.writeToken().value().value()) &&
      (!per_request_token_ ||
       *per_request_token_ != attrs.writeToken().value().value())) {
    ld_error(
        "Attempting to trim log %lu which is configured to require a write "
        "token.  Are you writing into the correct log?  If so, call "
        "Client::addWriteToken() to supply the write token.",
        log_id_.val_);
    finalize(E::ACCESS);
    return;
  }
  // Check passed, proceed with trimming.
  onWriteTokenCheckDone();
}

void TrimRequest::onWriteTokenCheckDone() {
  if (Worker::settings().disable_trim_past_tail_check) {
    // Just continue to the next stage if the feature
    // to check for a trim-past-tail is disabled
    onTrimPastTailCheckDone();
  } else {
    // Otherwise make sure that the trim LSN is not past the tail of the
    // log. If the log is accidentally trimmed past its tail, then even
    // though new records can be appended reads below the trim point
    // will not succeed.
    checkForTrimPastTail();
  }
}

void TrimRequest::onReply(ShardID from, Status status) {
  ld_debug("Received TRIMMED_Message from %s, status=%s",
           from.toString().c_str(),
           error_name(status));

  StorageSetAccessor::AccessResult res =
      StorageSetAccessor::AccessResult::SUCCESS;

  switch (status) {
    case E::NOTSTORAGE:
    case E::OK:
      break;
    case E::NOTSUPPORTED:
    case E::FAILED:
    case E::INVALID_PARAM:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Failure while trimming from %s with status %s.",
                        from.toString().c_str(),
                        error_name(status));
      res = StorageSetAccessor::AccessResult::PERMANENT_ERROR;
      break;
    case E::ACCESS:
      finalize(E::ACCESS);
      return;
    case E::NOTREADY:
    case E::AGAIN:
      res = StorageSetAccessor::AccessResult::TRANSIENT_ERROR;
      break;
    default:
      ld_error("Received TRIMMED_Message message from %s with "
               "unexpected status %s",
               from.toString().c_str(),
               error_name(status));
      res = StorageSetAccessor::AccessResult::PERMANENT_ERROR;
      break;
  }

  storage_set_accessor_->onShardAccessed(from, res);
}

void TrimRequest::onMessageSent(ShardID to, Status status) {
  if (status == E::ACCESS) {
    // If permission is denied, reply to the client to unblock it. There is no
    // need to retry as the result would be the same
    finalize(status);
  } else if (status != E::OK) {
    if (status == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "TRIM is not supported by the server at %s",
                      to.toString().c_str());
      storage_set_accessor_->onShardAccessed(
          to, StorageSetAccessor::AccessResult::PERMANENT_ERROR);
    } else {
      storage_set_accessor_->onShardAccessed(
          to, StorageSetAccessor::AccessResult::TRANSIENT_ERROR);
    }
  }
}

void TrimRequest::onClientTimeout() {
  finalize(E::TIMEDOUT);
}

void TrimRequest::finalize(Status status) {
  callback_(status);
  callback_ = nullptr;
  deleteThis();
}

int TrimRequest::sendOneMessage(ShardID to) {
  NodeID node_id(to.node());
  TRIM_Header header = {id_, log_id_, trim_point_, to.shard()};
  auto msg = std::make_unique<TRIM_Message>(header);
  return Worker::onThisThread()->sender().sendMessage(std::move(msg), node_id);
}

StorageSetAccessor::SendResult TrimRequest::sendTo(ShardID shard) {
  auto config = getConfig();
  auto n = config->getNode(shard.node());
  if (!n) {
    ld_error("Cannot find node at index %u", shard.node());
    return StorageSetAccessor::SendResult::PERMANENT_ERROR;
  }

  if (sendOneMessage(shard) != 0) {
    if (err == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "TRIM is not supported by the server at %s",
                      shard.toString().c_str());
      return StorageSetAccessor::SendResult::PERMANENT_ERROR;
    } else {
      return StorageSetAccessor::SendResult::TRANSIENT_ERROR;
    }
  }
  return StorageSetAccessor::SendResult::SUCCESS;
}

void TrimRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();
  TrimRequestMap& rqmap = worker->runningTrimRequests();
  auto it = rqmap.map.find(id_);
  ld_check(it != rqmap.map.end());

  rqmap.map.erase(it);
}

void TrimRequest::initStorageSetAccessor() {
  StorageSet shards;
  ReplicationProperty minRep;

  auto config = getConfig();
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    shards = EpochMetaData::nodesetToStorageSet(
        config->getMetaDataNodeIndices(), log_id_, *config);
    const std::shared_ptr<LogsConfig::LogGroupNode> meta_log =
        config->getMetaDataLogGroup();
    minRep = ReplicationProperty::fromLogAttributes(meta_log->attrs());
  } else {
    ld_check(nodeset_finder_);
    shards = nodeset_finder_->getUnionStorageSet(config);
    minRep = nodeset_finder_->getNarrowestReplication();
  }

  ld_debug("Building StorageSetAccessor with %lu shards in storage set, "
           "replication %s",
           shards.size(),
           minRep.toString().c_str());

  StorageSetAccessor::ShardAccessFunc shard_access =
      [this](ShardID shard, const StorageSetAccessor::WaveInfo&) {
        return this->sendTo(shard);
      };

  StorageSetAccessor::CompletionFunc completion = [this](Status st) {
    finalize(st);
  };

  storage_set_accessor_ =
      makeStorageSetAccessor(config, shards, minRep, shard_access, completion);
  storage_set_accessor_->successIfAllShardsAccessed();

  ld_check(storage_set_accessor_ != nullptr);
  storage_set_accessor_->start();
}

std::unique_ptr<StorageSetAccessor> TrimRequest::makeStorageSetAccessor(
    const std::shared_ptr<ServerConfig>& /*config*/,
    StorageSet shards,
    ReplicationProperty minRep,
    StorageSetAccessor::ShardAccessFunc shard_access,
    StorageSetAccessor::CompletionFunc completion) {
  return std::make_unique<StorageSetAccessor>(
      log_id_,
      shards,
      getConfig(),
      minRep,
      shard_access,
      completion,
      StorageSetAccessor::Property::FMAJORITY,
      client_timeout_);
}

std::shared_ptr<ServerConfig> TrimRequest::getConfig() const {
  return Worker::onThisThread()->getConfig()->serverConfig();
}

}} // namespace facebook::logdevice
