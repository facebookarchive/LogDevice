/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Mutator.h"

#include <folly/Memory.h>

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/STORE_Message.h"

namespace facebook { namespace logdevice {

const std::chrono::milliseconds Mutator::MUTATION_MAX_DELAY{10000};

Mutator::Mutator(const STORE_Header& header,
                 const STORE_Extra& store_extra,
                 Payload payload,
                 StorageSet mutation_set,
                 ReplicationProperty replication,
                 std::set<ShardID> amend_metadata,
                 std::set<ShardID> conflict_copies,
                 EpochRecovery* epoch_recovery)
    : sender_(std::make_unique<SenderProxy>()),
      header_(header),
      store_extra_(store_extra),
      payload_(payload),
      nodeset_(std::move(mutation_set)),
      replication_(replication),
      amend_metadata_(std::move(amend_metadata)),
      conflict_copies_(std::move(conflict_copies)),
      epoch_recovery_(epoch_recovery) {
  // ensure that all nodes in the three sets belong the nodeset
  if (folly::kIsDebug) {
    auto check_set = [](const std::set<ShardID>& s1, const StorageSet& s2) {
      for (ShardID n : s1) {
        ld_assert(std::find(s2.begin(), s2.end(), n) != s2.end() &&
                  "Node in the given set must belong to the mutation set.");
      }
    };

    check_set(amend_metadata_, nodeset_);
    check_set(conflict_copies_, nodeset_);
  }

  // STORE_Header must be prepared by epoch recovery machine with appropriate
  // flags
  ld_check(header_.flags & STORE_Header::WRITTEN_BY_RECOVERY);
  ld_check(header_.rid.logid != LOGID_INVALID);

  // recovery epoch must be a valid seal epoch set by epoch recovery
  ld_check(store_extra_.recovery_epoch != EPOCH_INVALID);
}

void Mutator::start() {
  ld_debug("Start mutation for record %s flag %s, amend set: %s, "
           "conflict set: %s",
           header_.rid.toString().c_str(),
           STORE_Message::flagsToString(header_.flags).c_str(),
           toString(amend_metadata_).c_str(),
           toString(conflict_copies_).c_str());

  // pre-populate socket callbacks
  for (ShardID shard : nodeset_) {
    auto insert_result =
        node_state_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(shard),
                            std::forward_as_tuple(this, shard));
    ld_check(insert_result.second);
  }

  StorageSet required_nodes;
  std::copy(amend_metadata_.begin(),
            amend_metadata_.end(),
            std::back_inserter(required_nodes));
  std::copy(conflict_copies_.begin(),
            conflict_copies_.end(),
            std::back_inserter(required_nodes));

  if (folly::kIsDebug) {
    // required_nodes should not contain duplicates
    StorageSet w(required_nodes);
    std::sort(w.begin(), w.end());
    ld_assert(std::unique(w.begin(), w.end()) == w.end() &&
              "amend_metadata and conflict_copies should not "
              "intersect");
  }

  // An EpochMetaData object to use with CopySetSelector. Can't use the "real"
  // metadata of this epoch because the copyset has to stay within the mutation
  // set, which is only a subset of the epoch's full nodeset.
  // Put fake weights in the EpochMetaData to prevent copyset selector from
  // using weights from config in case some of the weights were changed to zero.
  EpochMetaData epoch_metadata_with_mutation_set(
      nodeset_,
      replication_,
      EPOCH_MIN,
      EPOCH_MIN,
      std::vector<double>(nodeset_.size(), 1.));

  nodeset_accessor_ = createStorageSetAccessor(
      header_.rid.logid,
      epoch_metadata_with_mutation_set,
      getNodesConfiguration(),
      [this](ShardID shard, const StorageSetAccessor::WaveInfo& wave_info)
          -> StorageSetAccessor::SendResult {
        return sendSTORE(shard, wave_info);
      },
      [this](Status st) { onStorageSetAccessorComplete(st); },
      StorageSetAccessor::Property::REPLICATION);

  nodeset_accessor_->setWaveTimeout(getMutationTimeout());
  // do not set extras to prevent unnecessary overreplication
  nodeset_accessor_->setExtras(0);
  nodeset_accessor_->setRequiredShards(required_nodes);
  nodeset_accessor_->successIfAllShardsAccessed();

  if (print_debug_trace_when_complete_) {
    nodeset_accessor_->enableDebugTrace();
  }

  // this makes Mutator more like Appender: it sends in waves and only conclude
  // when successfully stored nodes in the latest wave can meet the replication
  // requirement.
  nodeset_accessor_->requireStrictWaves();

  // deactivate socket close callback and bw available callback at the beginning
  // of each wave
  nodeset_accessor_->setWavePreflightFunc([this]() {
    for (auto& kv : node_state_) {
      kv.second.socket_callback_.deactivate();
      kv.second.bw_available_callback_.deactivate();
    }
  });

  nodeset_accessor_->start();

  // apply authoritative status only once at the time of start
  if (!done()) {
    applyShardAuthoritativeStatus();
  }

  finalizeIfDone();
}

void Mutator::applyShardAuthoritativeStatus() {
  ld_check(nodeset_accessor_ != nullptr);
  for (ShardID shard : nodeset_) {
    AuthoritativeStatus status = getNodeAuthoritativeStatus(shard);
    if (status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
      nodeset_accessor_->setShardAuthoritativeStatus(shard, status);
    }
  }
}

StorageSetAccessor::SendResult
Mutator::sendSTORE(ShardID shard,
                   const StorageSetAccessor::WaveInfo& wave_info) {
  // must have gotten a valid wave_info
  const auto& wave_shards = wave_info.wave_shards;
  ld_check(!wave_shards.empty());
  ld_check(wave_shards.size() >= wave_info.offset);
  ld_check(wave_shards[wave_info.offset] == shard);

  // keeping track of the latest wave we have sent
  current_wave_ = std::max(current_wave_, wave_info.wave);

  bool include_payload = true;
  STORE_flags_t additional_flags = STORE_Header::CHECKSUM_PARITY;
  if (amend_metadata_.count(shard) > 0) {
    include_payload = false;
    additional_flags |= STORE_Header::AMEND;
  }

  folly::small_vector<StoreChainLink, 6> copyset(wave_shards.size());
  std::transform(
      wave_shards.begin(), wave_shards.end(), copyset.begin(), [](ShardID s) {
        return StoreChainLink{s, ClientID()};
      });

  // update the wave number and copyset in STORE_Header
  header_.wave = wave_info.wave;
  header_.copyset_size = copyset.size();

  auto msg = std::make_unique<STORE_Message>(
      header_,
      &copyset[0],
      wave_info.offset,
      additional_flags,
      store_extra_,
      std::map<KeyType, std::string>(),
      (include_payload
           ? std::make_shared<PayloadHolder>(payload_, PayloadHolder::UNOWNED)
           : nullptr));

  // socket callbacks must be deactivated at the beginning of the wave
  ld_assert(node_state_.count(shard) > 0);
  auto& socket_callback = node_state_.at(shard).socket_callback_;
  auto& bw_callback = node_state_.at(shard).bw_available_callback_;

  ld_check(!socket_callback.active());
  ld_check(!bw_callback.active());

  ShardID send_to = copyset[wave_info.offset].destination;
  ld_check(send_to == shard);
  int rv = sender_->sendMessage(
      std::move(msg), send_to.asNodeID(), &bw_callback, &socket_callback);
  if (rv != 0 && err == E::CBREGISTERED) {
    ld_check(msg != nullptr);
    bw_callback.setMessage(std::move(msg));
  }

  if (rv == 0 || err == E::CBREGISTERED) {
    return {StorageSetAccessor::Result::SUCCESS, Status::OK};
  }

  RATELIMIT_INFO(std::chrono::seconds(1),
                 10,
                 "Sending a STORE message (mutation) for record %s to "
                 "node %s failed: %s",
                 header_.rid.toString().c_str(),
                 shard.toString().c_str(),
                 error_description(err));

  if (err == E::NOTINCONFIG) {
    // Node was replaced or is no longer in the config, we should abort and
    // notify EpochRecovery.
    shard_not_in_config_ = shard;
    mutation_status_ = E::NOTINCONFIG;
    return {StorageSetAccessor::Result::PERMANENT_ERROR, err};
  }

  // consider all other error cases transient error and the node can
  // be retried later if needed
  return {StorageSetAccessor::Result::TRANSIENT_ERROR, err};
}

void Mutator::onStored(ShardID from, const MUTATED_Header& header) {
  SCOPE_EXIT {
    finalizeIfDone();
  };

  if (done()) {
    return;
  }

  Status st = header.status;
  if (!nodeset_accessor_ || !nodeset_accessor_->containsShard(from)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "STORED reply for %s received from an unexpected node %s "
                    "with status %s",
                    header_.rid.toString().c_str(),
                    from.toString().c_str(),
                    error_description(st));
    return;
  }

  if (st == E::OK) {
    // node successfully stored
    nodeset_accessor_->onShardAccessed(
        from, {StorageSetAccessor::Result::SUCCESS, st}, header.wave);
  } else if (st == E::PREEMPTED) {
    mutation_status_ = st;
    if (header.seal.valid()) {
      preempted_seal_ = header.seal;
    }
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Received a MUTATED reply for record %s from node %s "
                   "with status %s",
                   header_.rid.toString().c_str(),
                   from.toString().c_str(),
                   error_description(st));
    // no need to invoke nodeset_accessor as Mutator will finalize immediately
    // on preemption
  } else {
    // consider all other error case as transient error
    nodeset_accessor_->onShardAccessed(
        from, {StorageSetAccessor::Result::TRANSIENT_ERROR, st}, header.wave);
  }
}

void Mutator::onMessageSent(ShardID to, Status st, const STORE_Header& header) {
  SCOPE_EXIT {
    finalizeIfDone();
  };

  // if the node is no longer in config, the socket will be closed and socket
  // closed callback will handle the E::NOTINCONFIG case
  if (st != E::OK && !done()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Failed to send a STORE message (mutation) for record %s "
                    "to node %s: %s",
                    header_.rid.toString().c_str(),
                    to.toString().c_str(),
                    error_description(st));

    ld_check(nodeset_accessor_ != nullptr);
    // We failed to send a message to the destination. consider this as a
    // transient error and retries may get scheduled
    nodeset_accessor_->onShardAccessed(
        to, {StorageSetAccessor::Result::TRANSIENT_ERROR, st}, header.wave);
  }
}

void Mutator::onConnectionClosed(ShardID to, Status status) {
  SCOPE_EXIT {
    finalizeIfDone();
  };

  RATELIMIT_INFO(std::chrono::seconds(10),
                 2,
                 "Connection to shard %s has been closed with status %s "
                 "for record %s.",
                 to.toString().c_str(),
                 error_description(status),
                 header_.rid.toString().c_str());

  if (status == E::NOTINCONFIG) {
    shard_not_in_config_ = to;
    mutation_status_ = E::NOTINCONFIG;

    nodeset_accessor_->onShardAccessed(
        to, {StorageSetAccessor::Result::PERMANENT_ERROR, status});
    return;
  }

  // all other situations are considered as transient error
  nodeset_accessor_->onShardAccessed(
      to, {StorageSetAccessor::Result::TRANSIENT_ERROR, status});
}

void Mutator::onStorageSetAccessorComplete(Status status) {
  if (!done()) {
    mutation_status_ = status;
  }
}

void Mutator::finalizeIfDone() {
  if (done()) {
    finalize(mutation_status_, shard_not_in_config_);
  }
}

void Mutator::finalize(Status status, ShardID node_to_reseal) {
  ld_check(done());
  if (status == E::NOTINCONFIG) {
    ld_check(node_to_reseal.isValid());
  }

  if (print_debug_trace_when_complete_) {
    ld_info("Mutator %s completed (%s). Trace: %s",
            header_.rid.toString().c_str(),
            error_name(status),
            nodeset_accessor_->getDebugTrace().c_str());
  }

  epoch_recovery_->onMutationComplete(header_.rid.esn, status, node_to_reseal);
}

std::string Mutator::describeState() const {
  std::stringstream ss;
  ss << "amend: " << toString(amend_metadata_)
     << ", conflict: " << toString(conflict_copies_) << ", "
     << nodeset_accessor_->describeState();
  return ss.str();
}

void Mutator::printDebugTraceWhenComplete() {
  ld_check(nodeset_accessor_ == nullptr);
  print_debug_trace_when_complete_ = true;
}

std::unique_ptr<StorageSetAccessor> Mutator::createStorageSetAccessor(
    logid_t log_id,
    EpochMetaData epoch_metadata_with_mutation_set,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    StorageSetAccessor::ShardAccessFunc node_access,
    StorageSetAccessor::CompletionFunc completion,
    StorageSetAccessor::Property property) {
  return std::make_unique<StorageSetAccessor>(
      log_id,
      std::move(epoch_metadata_with_mutation_set),
      std::move(nodes_configuration),
      node_access,
      completion,
      property);
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
Mutator::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

chrono_interval_t<std::chrono::milliseconds>
Mutator::getMutationTimeout() const {
  return chrono_interval_t<std::chrono::milliseconds>{
      Worker::settings().mutation_timeout,
      std::max<std::chrono::milliseconds>(
          Worker::settings().mutation_timeout, MUTATION_MAX_DELAY)};
}

AuthoritativeStatus Mutator::getNodeAuthoritativeStatus(ShardID shard) const {
  const auto& status_map = Worker::onThisThread()
                               ->shardStatusManager()
                               .getShardAuthoritativeStatusMap();
  return status_map.getShardStatus(shard.node(), shard.shard());
}

void Mutator::sendDeferredSTORE(ShardID shard,
                                std::unique_ptr<STORE_Message> msg) {
  SCOPE_EXIT {
    finalizeIfDone();
  };

  ld_assert(node_state_.count(shard) > 0);
  auto& socket_callback = node_state_.at(shard).socket_callback_;
  auto& bw_callback = node_state_.at(shard).bw_available_callback_;
  socket_callback.deactivate();
  ld_check(!bw_callback.active());
  int rv = sender_->sendMessage(
      std::move(msg), NodeID(shard.node()), &bw_callback, &socket_callback);
  if (rv == 0) {
    return;
  }

  RATELIMIT_INFO(std::chrono::seconds(5),
                 5,
                 "Sending a defered STORE message (mutation) for record %s to "
                 "node %s failed: %s",
                 header_.rid.toString().c_str(),
                 shard.toString().c_str(),
                 error_description(err));

  if (err == E::NOTINCONFIG) {
    shard_not_in_config_ = shard;
    mutation_status_ = E::NOTINCONFIG;
    nodeset_accessor_->onShardAccessed(
        shard, {StorageSetAccessor::Result::PERMANENT_ERROR, err});
    return;
  }

  if (err == E::CBREGISTERED) {
    // The traffic shaping contract that there is sufficient credit to
    // issue the first message from a callback has been violated.
    ld_check(false);

    // do not attempt to resend deferred store again in this wave,
    // consider it transient error for accessing the node
    RATELIMIT_CRITICAL(std::chrono::seconds(5),
                       5,
                       "not enough bandwidth available when sending deferred "
                       "STORE (%s) to node %s in bw available callback! "
                       "shouldn't happen.",
                       header_.rid.toString().c_str(),
                       shard.toString().c_str());
    bw_callback.deactivate();
  }

  nodeset_accessor_->onShardAccessed(
      shard, {StorageSetAccessor::Result::TRANSIENT_ERROR, err});
}

void Mutator::SocketClosedCallback::operator()(Status st, const Address& addr) {
  ld_check(!addr.isClientAddress());
  mutator_->onConnectionClosed(shard_, st);
}

void Mutator::NodeBWAvailable::operator()(FlowGroup&, std::mutex&) {
  ld_check(msg_);
  mutator_->sendDeferredSTORE(shard_, std::move(msg_));
}

}} // namespace facebook::logdevice
