/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetEpochRecoveryMetadataRequest.h"

#include <functional>

#include <folly/CppAttributes.h>
#include <folly/Memory.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

const std::chrono::milliseconds
    GetEpochRecoveryMetadataRequest::INITIAL_RETRY_DELAY{2000};
const std::chrono::milliseconds
    GetEpochRecoveryMetadataRequest::MAX_RETRY_DELAY{60000};

GetEpochRecoveryMetadataRequest::GetEpochRecoveryMetadataRequest(
    worker_id_t worker_id,
    logid_t log_id,
    shard_index_t shard,
    epoch_t start,
    epoch_t end,
    epoch_t purge_to,
    std::shared_ptr<EpochMetaData> epoch_metadata,
    CompletionCallback cb)
    : sender_(std::make_unique<SenderProxy>()),
      epoch_metadata_(epoch_metadata),
      worker_id_(worker_id),
      log_id_(log_id),
      shard_(shard),
      start_(start),
      end_(end),
      purge_to_(purge_to),
      cb_(cb) {
  ld_check(epoch_metadata->isValid());
}

int GetEpochRecoveryMetadataRequest::getThreadAffinity(int /*nthreads*/) {
  return worker_id_.val_;
}

Request::Execution GetEpochRecoveryMetadataRequest::execute() {
  registerWithWorker();
  registerForShardAuthoritativeStatusUpdates();
  start();
  return Execution::CONTINUE;
}

void GetEpochRecoveryMetadataRequest::registerWithWorker() {
  auto worker = Worker::onThisThread();
  auto& runningERM = worker->runningGetEpochRecoveryMetadata().requests;
  auto insert_it =
      runningERM.insert(std::unique_ptr<GetEpochRecoveryMetadataRequest>(this));
  ld_check(insert_it.second);
}

void GetEpochRecoveryMetadataRequest::start() {
  if (retry_timer_ == nullptr) {
    retry_timer_ = createRetryTimer();
  } else {
    // resets the delay on restart
    retry_timer_->reset();
  }

  // Keeping track of which nodes have sent us any response.
  // Replication property doesn't matter because we're only interested in
  // when _all_ authoritative nodes respond.
  nodes_responded_ = std::make_unique<ResponseNodeSet>(
      epoch_metadata_->shards,
      *getNodesConfiguration(),
      ReplicationProperty({{NodeLocationScope::NODE, 1}}));

  createStorageSetAccessorAndStart();

  // apply initial shard authoritative status for nodes_responded_
  onShardStatusChanged();
}

void GetEpochRecoveryMetadataRequest::createStorageSetAccessorAndStart() {
  storage_set_accessor_ = makeStorageSetAccessor(
      [this](ShardID shard, const StorageSetAccessor::WaveInfo&)
          -> StorageSetAccessor::SendResult {
        return sendGetEpochRecoveryMetadataRequest(shard);
      },
      [this](Status st) { onStorageSetAccessorComplete(st); });
  storage_set_accessor_->start();
}

std::unique_ptr<StorageSetAccessor>
GetEpochRecoveryMetadataRequest::makeStorageSetAccessor(
    StorageSetAccessor::ShardAccessFunc node_access,
    StorageSetAccessor::CompletionFunc completion) {
  return std::make_unique<StorageSetAccessor>(
      log_id_,
      *epoch_metadata_,
      getNodesConfiguration(),
      node_access,
      completion,
      // TODO 10237599, 11866467 Improve availability of purging
      // TODO: T23693338 Change this to ANY after all servers
      // are confirmed to running the latest version
      StorageSetAccessor::Property::REPLICATION
      // no timeout
  );
}

void GetEpochRecoveryMetadataRequest::onShardStatusChanged() {
  if (result_ != E::UNKNOWN) {
    return;
  }

  storage_set_accessor_->onShardStatusChanged();

  // the line above might conclude storage_set_accessor_ and change the state,
  // recheck the state again.
  ld_check(nodes_responded_ != nullptr);
  const auto& shard_status_map = getShardAuthoritativeStatusMap();
  const auto& storage_membership =
      getNodesConfiguration()->getStorageMembership();

  for (const ShardID shard : epoch_metadata_->shards) {
    if (storage_membership->shouldReadFromShard(shard) &&
        nodes_responded_->containsShard(shard)) {
      auto st = shard_status_map.getShardStatus(shard.node(), shard.shard());
      // Purging should not be stalled because some nodes are rebuilding.
      // Considering the nodes that are unavailable as underreplicated will
      // let purging make a decision when it has heard from all nodes that
      // are fully authoritative. This mimics the condition used during
      // log recovery
      if (st == AuthoritativeStatus::UNAVAILABLE) {
        st = AuthoritativeStatus::UNDERREPLICATION;
      }
      nodes_responded_->setShardAuthoritativeStatus(shard, st);
    }
  }

  checkAllFullyAuthoritativeNodesResponded();
}

void GetEpochRecoveryMetadataRequest::
    checkAllFullyAuthoritativeNodesResponded() {
  if (result_ != E::UNKNOWN) {
    // Complete was already called. Nothing to
    // check here
    return;
  }

  ld_check(nodes_responded_ != nullptr);
  if (!nodes_responded_->isCompleteSet(true)) {
    return;
  }

  ld_info("GetEpochRecoveryMetadataRequest for log %lu shard %u epochs [%u,%u] "
          "purge_to %u received reply from all fully authoritative nodes in the"
          "nodeset but still unable to make a decision. It is likely that some "
          "epoch has been recovered by non-authoritative recovery.",
          log_id_.val_,
          shard_,
          start_.val_,
          end_.val_,
          purge_to_.val_);
  complete(E::OK);
}

void GetEpochRecoveryMetadataRequest::buildEpochRecoveryStateMap() {
  // We should build the map only after complete has been called
  ld_check(result_ != E::UNKNOWN);

  EpochRecoveryStateMap epochRecoveryStateMap;
  for (auto epoch = start_.val_; epoch <= end_.val_; epoch++) {
    if (!epochRecoveryMetadataResponses_.count(epoch)) {
      // We never got a valid response for this epoch. This is possible
      // if all FullyAuthoritative nodes responded but none of them had a
      // valid response.
      epochRecoveryStateMap.emplace(
          epoch, std::make_pair(E::UNKNOWN, EpochRecoveryMetadata()));
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          100,
          "GetEpochRecoveryMetadataRequest for log %lu shard %u epochs "
          "[%u,%u] purge_to %u - All nodes responded but there is no "
          "valid response for epoch:%u",
          log_id_.val_,
          shard_,
          start_.val_,
          end_.val_,
          purge_to_.val_,
          epoch);
      continue;
    }

    if (epochRecoveryMetadataResponses_[epoch].first.valid()) {
      // We have a metadata for this epoch
      epochRecoveryStateMap.emplace(
          epoch,
          std::make_pair(E::OK, epochRecoveryMetadataResponses_[epoch].first));
      continue;
    }

    // If the operation was not aborted and we got atleast one reply
    // with E::EMPTY, consider the epoch as empty.
    if (epochRecoveryMetadataResponses_[epoch].second.size() > 0 &&
        result_ != E::ABORTED) {
      WORKER_STAT_INCR(purging_v2_purge_epoch_empty_all_responsed);
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          100,
          "GetEpochRecoveryMetadataRequest for log %lu shard %u epochs "
          "[%u,%u] purge_to %u got at least one reply with E::EMPTY, "
          "for the epoch %u.",
          log_id_.val_,
          shard_,
          start_.val_,
          end_.val_,
          purge_to_.val_,
          epoch);
      epochRecoveryStateMap.emplace(
          epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
      continue;
    }

    epochRecoveryStateMap.emplace(
        epoch, std::make_pair(E::UNKNOWN, EpochRecoveryMetadata()));
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        100,
        "GetEpochRecoveryMetadataRequest for log %lu shard %u epochs "
        "[%u,%u] purge_to %u. All nodes responded but there is no valid "
        "response for epoch:%u.",
        log_id_.val_,
        shard_,
        start_.val_,
        end_.val_,
        purge_to_.val_,
        epoch);
  }

  epochRecoveryStateMap_ =
      std::make_unique<EpochRecoveryStateMap>(epochRecoveryStateMap);
}

StorageSetAccessor::SendResult
GetEpochRecoveryMetadataRequest::sendGetEpochRecoveryMetadataRequest(
    ShardID shard) {
  if (!getNodesConfiguration()->isNodeInServiceDiscoveryConfig(shard.node())) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Try to send to node %u which is no longer in config",
                   shard.node());
    return {StorageSetAccessor::Result::PERMANENT_ERROR, Status::NOTFOUND};
  }

  NodeID send_to(shard.node());

  GET_EPOCH_RECOVERY_METADATA_Header msg_header{log_id_,
                                                purge_to_,
                                                start_,
                                                /*flags=*/0,
                                                /*shard=*/shard.shard(),
                                                /*purging_shard=*/shard_,
                                                end_,
                                                id_};
  auto msg = std::make_unique<GET_EPOCH_RECOVERY_METADATA_Message>(msg_header);
  int rv = sender_->sendMessage(std::move(msg), send_to);

  if (rv == 0) {
    return {StorageSetAccessor::Result::SUCCESS, Status::OK};
  }

  switch (err) {
    // permanent errors
    case E::SHUTDOWN:
    case E::INTERNAL:
      return {StorageSetAccessor::Result::PERMANENT_ERROR, err};

    case E::PROTONOSUPPORT:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "GET_EPOCH_RECOVERY_METADATA_Message for range of epochs"
                      "is not supported by the recipient server at %s",
                      Sender::describeConnection(send_to).c_str());
      // If the messgae was for a range of epochs and the socket does
      // not support this message, abort the operation
      if (isRangeRequest()) {
        return {StorageSetAccessor::Result::ABORT, err};
      }
      break;
    // all other errors are considered transient
    default:
      break;
  }

  return {StorageSetAccessor::Result::TRANSIENT_ERROR, err};
}

/*static*/
void GetEpochRecoveryMetadataRequest::onSent(
    const GET_EPOCH_RECOVERY_METADATA_Message& msg,
    Status status,
    const Address& to) {
  // forward to the state machine
  const GET_EPOCH_RECOVERY_METADATA_Header& header = msg.getHeader();
  Worker* worker = Worker::onThisThread();
  const auto& index =
      worker->runningGetEpochRecoveryMetadata()
          .requests.get<GetEpochRecoveryMetadataRequestMap::RequestIndex>();

  auto it = index.find(header.id);

  if (it != index.end()) {
    ShardID to_shard(to.id_.node_.index(), header.shard);
    (*it)->onSent(to_shard, status);
  }
}

void GetEpochRecoveryMetadataRequest::onSent(ShardID to, Status st) {
  if (result_ != E::UNKNOWN) {
    return;
  }

  ld_check(storage_set_accessor_ != nullptr);
  auto result = StorageSetAccessor::Result::TRANSIENT_ERROR;
  if (st != E::OK) {
    if (isRangeRequest() && st == E::PROTONOSUPPORT) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "GET_EPOCH_RECOVERY_METADATA_Message for range of epochs"
                      "not supported by the recipient server at %s",
                      to.toString().c_str());
      result = StorageSetAccessor::Result::ABORT;
    }

    // all error conditions are considered as transient error
    storage_set_accessor_->onShardAccessed(to, {result, st});
  }
}

void GetEpochRecoveryMetadataRequest::onReply(
    ShardID from,
    Status status,
    std::unique_ptr<EpochRecoveryStateMap> epochRecoveryStateMap) {
  if (result_ != E::UNKNOWN) {
    // We already concluded the request.
    // This is a stale reply
    return;
  }

  ld_debug("Received GET_EPOCH_RECOVERY_METADATA_REPLY for log %lu from %s,"
           "status=%s",
           log_id_.val_,
           from.toString().c_str(),
           error_name(status));

  ld_check(storage_set_accessor_ != nullptr);
  ld_check(nodes_responded_ != nullptr);
  StorageSetAccessor::AccessResult result = {
      StorageSetAccessor::Result::SUCCESS, Status::OK};
  if (status != E::AGAIN) {
    nodes_responded_->setShardAttribute(from, true);
  }

  switch (status) {
    // success
    case E::OK:
      // metadata must be valid, this is guaranteed by
      // GET_EPOCH_RECOVERY_METADATA_REPLY_Message::onReceived()
      ld_check(epochRecoveryStateMap != nullptr);
      result = processResponse(from, status, std::move(epochRecoveryStateMap));
      break;

    case E::EMPTY:
      ld_check(!epochRecoveryStateMap);
      epochRecoveryStateMap = std::make_unique<EpochRecoveryStateMap>();
      for (auto epoch = start_.val_; epoch <= end_.val_; epoch++) {
        epochRecoveryStateMap->emplace(
            epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
      }
      result = processResponse(from, status, std::move(epochRecoveryStateMap));
      break;

    case E::NOTSTORAGE:
      result = {StorageSetAccessor::Result::PERMANENT_ERROR, status};
      break;

    case E::NOTREADY: // epoch not clean
      result = {StorageSetAccessor::Result::TRANSIENT_ERROR, status};
      // The CopySetSelector within the StorageSetAccessor could
      // sometimes select the node trying to do purging itself to
      // send GET_EPOCH_RECOVERY_METADATA message and it will always
      // return NOTREADY. In that case, mark the node as in error
      // so that it does not get picked again
      if (ShardID(getMyNodeID().index(), shard_) == from) {
        result = {StorageSetAccessor::Result::PERMANENT_ERROR, status};
      }
      break;
    case E::REBUILDING: // remote node in rebuilding
    case E::FAILED:     // unable to read metadata
    case E::AGAIN:      // overloaded
      FOLLY_FALLTHROUGH;
    default:
      // all other errors are considered transient error
      RATELIMIT_INFO(std::chrono::seconds(10),
                     1,
                     "Received GET_EPOCH_RECOVERY_METADATA_REPLY from %s "
                     "for purging log %lu shard %u, epoch %u, purge to %u with "
                     "a error status: %s.",
                     from.toString().c_str(),
                     log_id_.val_,
                     shard_,
                     start_.val_,
                     purge_to_.val_,
                     error_name(status));
      result = {StorageSetAccessor::Result::TRANSIENT_ERROR, status};
      break;
  }

  // Check if we have received partial replies from many nodes
  // but have valid responses for all the epochs we care about
  if (checkIfDone()) {
    complete(E::OK);
  } else {
    // This could conclude storage_set_accessor
    storage_set_accessor_->onShardAccessed(from, result);
    // also check to see if all fully authoritative nodes have sent us
    // a response. if so, make a decision based on these reponses.
    checkAllFullyAuthoritativeNodesResponded();
  }
}

StorageSetAccessor::AccessResult
GetEpochRecoveryMetadataRequest::processResponse(
    ShardID from,
    Status status,
    std::unique_ptr<EpochRecoveryStateMap> epochRecoveryStateMap) {
  ld_check(status == E::OK || status == E::EMPTY);
  ld_check(epochRecoveryStateMap);

  bool allSuccess = true;
  auto last_failure = Status::OK;

  for (auto& entry : *epochRecoveryStateMap) {
    auto epoch = entry.first;

    std::pair<EpochRecoveryMetadata, std::set<ShardID>>& state =
        epochRecoveryMetadataResponses_[epoch];

    if (state.first.valid()) {
      // We already have the metadata from some other shard.
      // Ignore the response for this particular epoch.
      continue;
    }

    auto& response = entry.second;
    bool success = false;
    switch (response.first) {
      case E::OK:
        state.first = response.second;
        success = true;
        break;
      case E::EMPTY:
        state.second.insert(from);
        success = true;
        break;
      default:
        last_failure = response.first;
        success = false;
        break;
    }
    allSuccess = allSuccess && success;
  }

  return allSuccess
      ? StorageSetAccessor::AccessResult{StorageSetAccessor::Result::SUCCESS,
                                         Status::OK}
      : StorageSetAccessor::AccessResult{
            StorageSetAccessor::Result::TRANSIENT_ERROR, last_failure};
}

bool GetEpochRecoveryMetadataRequest::checkIfDone() {
  auto isSingleEmptySufficient = getSettings().single_empty_erm;
  // An epoch is considered to have received sufficient response if
  // 1/ We have a valid EpochRecoveryMetadata or
  // 2/ single_empty_erm is true and we have one or more response
  //    indicating epoch is Empty
  //
  // We are done if all the epochs in the range [start_, end_] have
  // received sufficient response.
  auto isEpochResponseSufficient =
      [this, isSingleEmptySufficient](epoch_t epoch) -> bool {
    return epochRecoveryMetadataResponses_.count(epoch.val_) &&
        (epochRecoveryMetadataResponses_[epoch.val_].first.valid() ||
         (isSingleEmptySufficient &&
          epochRecoveryMetadataResponses_[epoch.val_].second.size() >= 1));
  };

  bool allEpochResponsesSufficient = true;

  for (auto epoch = start_.val_; epoch <= end_.val_; epoch++) {
    allEpochResponsesSufficient = allEpochResponsesSufficient &&
        isEpochResponseSufficient(epoch_t(epoch));
  }

  return allEpochResponsesSufficient;
}

void GetEpochRecoveryMetadataRequest::onStorageSetAccessorComplete(
    Status status) {
  ld_check(retry_timer_ != nullptr);

  if (status != E::OK && status != E::ABORTED) {
    // If request failed with anything other than E:ABORTED,
    // schedule a retry
    retry_timer_->setCallback(
        std::bind(&GetEpochRecoveryMetadataRequest::start, this));
    retry_timer_->activate();
    return;
  }

  //  nodeset_accessor is not needed anymore
  storage_set_accessor_.reset();
  ld_check(status == E::OK || (status == E::ABORTED && isRangeRequest()));
  complete(status);
}

void GetEpochRecoveryMetadataRequest::complete(Status status) {
  // currently only allowed failure is if operation was aborted
  // becasue the protocol is not supported
  ld_check(status == E::OK || status == E::ABORTED);

  // the state machine should only complete once
  ld_check(result_ == E::UNKNOWN);
  if (result_ == E::UNKNOWN) {
    result_ = status;
    buildEpochRecoveryStateMap();
    deferredComplete();
  }
}

void GetEpochRecoveryMetadataRequest::deleteThis() {
  auto& rqmap =
      Worker::onThisThread()
          ->runningGetEpochRecoveryMetadata()
          .requests.get<GetEpochRecoveryMetadataRequestMap::RequestIndex>();
  auto it = rqmap.find(id_);
  ld_check(it != rqmap.end());
  rqmap.erase(it);
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GetEpochRecoveryMetadataRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

const Settings& GetEpochRecoveryMetadataRequest::getSettings() const {
  return Worker::settings();
}

std::unique_ptr<BackoffTimer>
GetEpochRecoveryMetadataRequest::createRetryTimer() {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      std::function<void()>(), INITIAL_RETRY_DELAY, MAX_RETRY_DELAY);

  return std::move(timer);
}

const ShardAuthoritativeStatusMap&
GetEpochRecoveryMetadataRequest::getShardAuthoritativeStatusMap() {
  return Worker::onThisThread()
      ->shardStatusManager()
      .getShardAuthoritativeStatusMap();
}

std::unique_ptr<Timer>
GetEpochRecoveryMetadataRequest::createDeferredCompleteTimer(
    std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

void GetEpochRecoveryMetadataRequest::activateDeferredCompleteTimer() {
  ld_check(deferredCompleteTimer_);
  deferredCompleteTimer_->activate(std::chrono::milliseconds(0));
}

void GetEpochRecoveryMetadataRequest::done() {
  ld_check(result_ != E::UNKNOWN);
  cb_(result_, std::move(epochRecoveryStateMap_));
  deleteThis();
}

NodeID GetEpochRecoveryMetadataRequest::getMyNodeID() {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

void GetEpochRecoveryMetadataRequest::deferredComplete() {
  ld_check(!deferredCompleteTimer_);
  ld_check(result_ != E::UNKNOWN);
  ld_check(result_ == E::OK || result_ == E::ABORTED);
  // Start a timer with zero delay.
  deferredCompleteTimer_ = createDeferredCompleteTimer([this] { done(); });
  activateDeferredCompleteTimer();
}

}} // namespace facebook::logdevice
