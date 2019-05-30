/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/StoreStateMachine.h"

#include <folly/Memory.h>
#include <folly/ScopeGuard.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/StoreStorageTask.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

Message::Disposition StoreStateMachine::onReceived(STORE_Message* msg,
                                                   const Address& from) {
  if (!from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got a STORE %s from an outgoing (server) "
                    "connection to %s. STORE messages can only arrive from "
                    "incoming (client) connections",
                    msg->header_.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if ((msg->header_.flags & STORE_Header::DRAINED) != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got STORE %s from %s with DRAINED set, flags: %s."
                    "Closing socket due to protocol violation.",
                    msg->header_.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str(),
                    STORE_Message::flagsToString(msg->header_.flags).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  ld_spew("Got STORE %s from %s, flags: %s",
          msg->header_.rid.toString().c_str(),
          Sender::describeConnection(from).c_str(),
          STORE_Message::flagsToString(msg->header_.flags).c_str());

  ServerWorker* worker = ServerWorker::onThisThread();
  NodeID my_node_id = worker->processor_->getMyNodeID();
  auto start_time = std::chrono::steady_clock::now();

  // Check that we should even be processing this
  if (!worker->processor_->runningOnStorageNode()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got STORE %s from %s but not configured as a storage node",
                    msg->header_.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    // close connection to the sender. However if chain-sending and we are not
    // the first node in the chain, Appender will not be immediately notified
    // but this wave will timeout.
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  ssize_t payload_size = msg->payload_->size();
  if (msg->header_.flags & STORE_Header::CHECKSUM &&
      !(msg->header_.flags & STORE_Header::AMEND)) {
    payload_size -= (msg->header_.flags & STORE_Header::CHECKSUM_64BIT) ? 8 : 4;
  }

  ld_check(msg->header_.copyset_offset < msg->copyset_.size());
  const shard_index_t shard_idx =
      msg->copyset_[msg->header_.copyset_offset].destination.shard();
  const bool rebuilding = msg->header_.flags & STORE_Header::REBUILDING;

  StatsHolder* stats = Worker::stats();
  if (msg->header_.flags & STORE_Header::SYNC) {
    STAT_INCR(stats, store_synced);
  }
  if (msg->header_.flags & STORE_Header::AMEND) {
    STAT_INCR(stats, store_received_amend);
  }
  TRAFFIC_CLASS_STAT_INCR(stats, msg->tc_, store_received);
  TRAFFIC_CLASS_STAT_ADD(stats, msg->tc_, store_payload_bytes, payload_size);
  if (rebuilding) {
    PER_SHARD_STAT_INCR(stats, rebuilding_stores_received, shard_idx);
    PER_SHARD_STAT_ADD(
        stats, rebuilding_stores_received_bytes, shard_idx, payload_size);
  } else {
    PER_SHARD_STAT_INCR(stats, append_stores_received, shard_idx);
    PER_SHARD_STAT_ADD(
        stats, append_stores_received_bytes, shard_idx, payload_size);
  }

  const ShardedStorageThreadPool* sharded_pool =
      worker->processor_->sharded_storage_thread_pool_;

  ShardID rebuilding_node;
  if (validateCopyset(
          *msg, from, my_node_id, &msg->my_pos_in_copyset_, &rebuilding_node) !=
      0) {
    if (err == E::PROTO) {
      // invalid copyset, error logged
      return Message::Disposition::ERROR;
    }

    Status status = err;
    if (err == E::REBUILDING) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "STORE %s that we got from %s has a copyset intersecting "
                      " rebuilding set; %s is in the intersection",
                      msg->header_.rid.toString().c_str(),
                      Sender::describeConnection(from).c_str(),
                      rebuilding_node.toString().c_str());
    } else {
      ld_check(err == E::NOTFOUND);
      status = E::NOTINCONFIG;
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "STORE %s that we got from %s does not have our node id "
                      "%s in its copyset",
                      msg->header_.rid.toString().c_str(),
                      Sender::describeConnection(from).c_str(),
                      my_node_id.toString().c_str());
    }
    if (!(msg->header_.flags & STORE_Header::CHAIN) ||
        from.id_.client_ == msg->copyset_[0].origin) {
      // We have a connection to the Appender. Send it the error.
      // If we are a 2nd or subsequent link in a delivery chain,
      // the Appender will just have to retry after a timeout.
      msg->reply_to_ = from.id_.client_;
      msg->sendReply(status, Seal(), rebuilding_node);
    }
    return Message::Disposition::NORMAL;
  }

  if (msg->header_.flags & STORE_Header::CHAIN) {
    if (msg->header_.nsync > msg->header_.copyset_size) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: STORE %s that we got from %s requests "
                      "syncing on more nodes (%uhh) than are in "
                      "its copyset (%uhh)",
                      msg->header_.rid.toString().c_str(),
                      Sender::describeConnection(from).c_str(),
                      msg->header_.nsync,
                      msg->header_.copyset_size);
      err = E::PROTO;
      return Message::Disposition::ERROR;
    }

    if (msg->header_.nsync > 0) {
      const char* errstr = nullptr;
      if (msg->my_pos_in_copyset_ <= msg->header_.nsync) {
        if (!(msg->header_.flags & STORE_Header::SYNC)) {
          errstr = "is NOT set";
          msg->header_.flags |= STORE_Header::SYNC;
        }
      } else if (msg->header_.flags & STORE_Header::SYNC) {
        errstr = "IS still set";
      }
      if (errstr) {
        RATELIMIT_ERROR(
            std::chrono::seconds(1),
            1,
            "CONFIG ERROR: we are #%d in the chain for "
            "STORE %s that we got from %s. nsync for the STORE "
            "is %hhu, but the SYNC flag %s in msg->header_. Cluster "
            "configs on sequencer and this node have "
            "likely diverged. Record may be under-replicated.",
            msg->my_pos_in_copyset_,
            msg->header_.rid.toString().c_str(),
            Sender::describeConnection(from).c_str(),
            msg->header_.nsync,
            errstr);
        // TODO: it's not clear what the best way to correct for this is
        // for now just log the message and set the SYNC flag.
      }
    }
  }

  // Calculate the ClientID to reply to based on the valid copyset
  if (msg->header_.flags & STORE_Header::CHAIN) {
    msg->reply_to_ = msg->copyset_[msg->my_pos_in_copyset_].origin;
    ld_debug("Will send a reply to chained STORE %s directly to %s.",
             msg->header_.rid.toString().c_str(),
             Sender::describeConnection(Address(msg->reply_to_)).c_str());
  } else {
    msg->reply_to_ = from.id_.client_;
  }

  const auto& log_map = Worker::settings().dont_serve_stores_logs;
  if (log_map.find(msg->header_.rid.logid) != log_map.end()) {
    Status status = Worker::settings().dont_serve_stores_status;
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "Denying stores for log %lu based on settings with status %s",
        msg->header_.rid.logid.val(),
        error_description(status));
    msg->sendReply(status);
    return Message::Disposition::NORMAL;
  }

  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring STORE message: not accepting more work");
    msg->sendReply(E::SHUTDOWN);
    return Message::Disposition::NORMAL;
  }

  if (worker->processor_->isDataMissingFromShard(shard_idx)) {
    ld_debug("Got STORE %s from %s but shard %u is waiting for rebuilding",
             msg->header_.rid.toString().c_str(),
             Sender::describeConnection(from).c_str(),
             shard_idx);
    msg->sendReply(E::DISABLED);
    return Message::Disposition::NORMAL;
  }

  // If LocalLogStore is not accepting writes, fail early.
  Status accepting =
      sharded_pool->getByIndex(shard_idx).getLocalLogStore().acceptingWrites();
  if (accepting == E::DISABLED) {
    msg->sendReply(accepting);
    return Message::Disposition::NORMAL;
  }

  // Recover parts of log state that may still be missing.
  worker->processor_->getLogStorageStateMap().recoverLogState(
      msg->header_.rid.logid,
      shard_idx,
      LogStorageState::RecoverContext::STORE_MESSAGE);

  bool requested_sync = msg->header_.flags & STORE_Header::SYNC;
  if (MetaDataLog::isMetaDataLog(msg->header_.rid.logid) &&
      Worker::settings().sync_metadata_log_writes) {
    // if sync_metadata_log_writes setting is enabled, always sync metadata log
    // writes
    requested_sync = true;
  }

  Durability default_durability = rebuilding
      ? Worker::settings().rebuild_store_durability
      : Worker::settings().append_store_durability;

  if (rebuilding && default_durability <= Durability::MEMORY) {
    if (msg->extra_.rebuilding_id == LOG_REBUILDING_ID_INVALID) {
      // rebuilding_id is used as proxy to determine if the node
      // where this STORE originated from can tolerate a record with
      // low durability.
      default_durability = Durability::ASYNC_WRITE;
    }
  }

  auto sm = std::make_unique<StoreStateMachine>(
      std::unique_ptr<STORE_Message>(msg),
      from,
      start_time,
      requested_sync ? Durability::SYNC_WRITE : default_durability);
  sm->execute();

  // ownership was transferred
  sm.release();
  return Message::Disposition::KEEP;
}

// Check if a node index is being rebuilt in RELOCATE mode. If that's the
// case, we will deny the STORE with E::REBUILDING.
static bool destIsRebuilding(ShardID dest,
                             logid_t /*logid*/,
                             const EventLogRebuildingSet& set) {
  return set.isRebuildingFullShard(dest.node(), dest.shard()) ==
      RebuildingMode::RELOCATE;
}

int StoreStateMachine::validateCopyset(const STORE_Message& msg,
                                       const Address& from,
                                       NodeID my_node_id,
                                       copyset_off_t* out_my_offset,
                                       ShardID* out_rebuilding_node) {
  *out_my_offset = -1;
  *out_rebuilding_node = ShardID(); // invalid.
  auto rebuilding_set =
      Worker::onThisThread()->processor_->rebuilding_set_.get();

  for (copyset_off_t i = 0; i < msg.header_.copyset_size; i++) {
    ShardID dest = msg.copyset_[i].destination;
    if (msg.header_.flags & STORE_Header::CHAIN) {
      ClientID origin = msg.copyset_[i].origin;
      if (!origin.valid()) {
        RATELIMIT_ERROR(
            std::chrono::seconds(1),
            10,
            "PROTOCOL ERROR: got an invalid STORE message for record %s "
            "from %s. Origin value %u at position %d in copyset is not "
            "a valid client id.",
            msg.header_.rid.toString().c_str(),
            Sender::describeConnection(from).c_str(),
            (unsigned)origin,
            i);
        err = E::PROTO;
        return -1;
      }
    }
    if (i == msg.header_.copyset_offset) {
      if (dest.node() != my_node_id.index()) {
        // if the recipient at offset `copyset_offset` is not us, this is likely
        // a result of inconsistent configuration across the cluster.  Consider
        // the copyset invalid.d
        err = E::NOTFOUND;
        return -1;
      }
      *out_my_offset = i;
    }
    if (rebuilding_set && Worker::settings().reject_stores_based_on_copyset &&
        destIsRebuilding(dest, msg.header_.rid.logid, *rebuilding_set)) {
      *out_rebuilding_node = dest;
    }
  }

  // If we did not find our node ID in the copyset, this is likely a result of
  // inconsistent configuration across the cluster.  Consider the copyset
  // invalid.
  if (*out_my_offset < 0) {
    err = E::NOTFOUND;
    return -1;
  }
  ld_check(*out_my_offset == msg.header_.copyset_offset);

  // Copyset intersects rebuilding set.
  if (out_rebuilding_node->isValid()) {
    err = E::REBUILDING;
    return -1;
  }

  return 0;
}

class StoreStateMachine::SoftSealStorageTask : public StorageTask {
 public:
  explicit SoftSealStorageTask(StoreStateMachine* owner)
      : StorageTask(StorageTask::Type::SOFT_SEAL), owner_(owner) {}

  // Unlike SealStorageTask, doesn't sync.

  ThreadType getThreadType() const override {
    return ThreadType::FAST_TIME_SENSITIVE;
  }

  void execute() override {
    auto& store = storageThreadPool_->getLocalLogStore();
    auto& state_map =
        storageThreadPool_->getProcessor().getLogStorageStateMap();
    RecordID rid = owner_->message_->header_.rid;
    NodeID seq_node = owner_->message_->header_.sequencer_node_id;
    ld_check(rid.epoch.val_ > 0);

    LogStorageState* log_state =
        state_map.insertOrGet(rid.logid, storageThreadPool_->getShardIdx());
    if (log_state == nullptr) {
      RATELIMIT_ERROR(
          std::chrono::seconds(5),
          1,
          "No state for log %lu; this should never happen; error: %s",
          rid.logid.val_,
          error_description(err));
      ld_check(false);
      return;
    }

    SoftSealMetadata softseal_metadata{
        Seal(epoch_t(rid.epoch.val_ - 1), seq_node)};
    ld_check(softseal_metadata.seal_.validOrEmpty());

    LocalLogStore::WriteOptions write_options;
    int rv =
        store.updateLogMetadata(rid.logid, softseal_metadata, write_options);
    if (rv != 0 && err != E::UPTODATE) {
      // Don't fail the store. If updateLogMetadata failed, the subsequent
      // StoreStorageTask will most likely fail too and report the correct error
      // (considering LocalLogStore::acceptingWrites()).
      return;
    }

    log_state->updateSeal(
        softseal_metadata.seal_, LogStorageState::SealType::SOFT);

    RecordCache* cache = log_state->record_cache_.get();
    if (cache != nullptr) {
      cache->updateLastNonAuthoritativeEpoch(rid.logid);
    }

    ld_debug("Log %lu: updating soft seal to e%u",
             rid.logid.val_,
             softseal_metadata.seal_.epoch.val_);
  }
  void onDone() override {
    owner_->storeAndForward();
  }
  void onDropped() override {
    owner_->message_->sendReply(E::DROPPED);
    delete owner_;
  }

 private:
  StoreStateMachine* owner_;
};

void StoreStateMachine::execute() {
  auto deleter = folly::makeGuard([this] { delete this; });

  auto& map = ServerWorker::onThisThread()->processor_->getLogStorageStateMap();
  LogStorageState* log_state =
      map.insertOrGet(message_->header_.rid.logid, shard_);
  if (log_state == nullptr) {
    // unlikely
    message_->sendReply(E::DISABLED);
    return;
  }

  if (message_->header_.flags & STORE_Header::REBUILDING) {
    // Accept this rebuilding store only if
    //  the lsn of the store is at or below last_released OR
    //  the epoch of the store is at or below LCE.
    // Otherwise, treat this as a release so that
    // it triggers purging which consequently updates the
    // LCE and last_released_lsn
    auto last_clean_epoch = log_state->getLastCleanEpoch();
    auto last_released_lsn = log_state->getLastReleasedLSN();
    if ((last_released_lsn.hasValue() &&
         message_->header_.rid.lsn() <= last_released_lsn.value()) ||
        (last_clean_epoch.hasValue() &&
         message_->header_.rid.epoch <= last_clean_epoch.value())) {
      deleter.dismiss();
      storeAndForward();
    } else {
      NodeID seq = message_->header_.sequencer_node_id;

      // Special case: if it's a metadata log that's not in config, don't do
      // purging because purging would get stuck in some cases.
      if (seq.isNodeID() &&
          MetaDataLog::isMetaDataLog(message_->header_.rid.logid)) {
        auto logs_config = Worker::onThisThread()->getLogsConfig();
        if (logs_config->isFullyLoaded() &&
            !logs_config->logExists(
                MetaDataLog::dataLogID(message_->header_.rid.logid))) {
          seq = NodeID();
        }
      }

      if (seq.isNodeID()) {
        // Trigger purging by treating this store as a proxy
        // for release message.
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            1,
            "Treating rebuilding STORE as proxy for release for log:%lu,"
            "store lsn: %s, current last_clean_epoch: %u, "
            "current last_released_lsn: %s, sequencer: %s",
            message_->header_.rid.logid.val(),
            lsn_to_string(message_->header_.rid.lsn()).c_str(),
            last_clean_epoch.hasValue() ? last_clean_epoch.value().val_ : 0,
            lsn_to_string(last_released_lsn.hasValue()
                              ? last_released_lsn.value()
                              : LSN_INVALID)
                .c_str(),
            seq.toString().c_str());

        log_state->purge_coordinator_->onReleaseMessage(
            message_->header_.rid.lsn(), seq, ReleaseType::GLOBAL, true);
        message_->sendReply(E::DISABLED);
      } else if (MetaDataLog::isMetaDataLog(message_->header_.rid.logid)) {
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            1,
            "Got rebuilding STORE %lu%s with invalid sequencer_node_id. "
            "This is probably a metadata log deleted from config. Storing "
            "without purging.",
            message_->header_.rid.logid.val(),
            lsn_to_string(message_->header_.rid.lsn()).c_str());
        deleter.dismiss();
        storeAndForward();
      } else {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Got rebuilding STORE %lu%s with invalid "
                        "sequencer_node_id (%d). Rejecting.",
                        message_->header_.rid.logid.val(),
                        lsn_to_string(message_->header_.rid.lsn()).c_str(),
                        (unsigned)seq);
        message_->sendReply(E::DISABLED);
      }
    }
    return;
  }

  deleter.dismiss();
  log_state->recoverSeal(std::bind(&StoreStateMachine::onSealRecovered,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
}

StoreStateMachine::StoreStateMachine(
    std::unique_ptr<STORE_Message> message,
    Address from,
    std::chrono::steady_clock::time_point start_time,
    Durability durability)
    : message_(std::move(message)),
      from_(from),
      start_time_(start_time),
      durability_(durability) {
  ld_check(message_->header_.copyset_offset < message_->copyset_.size());
  shard_ =
      message_->copyset_[message_->header_.copyset_offset].destination.shard();
}

StoreStateMachine::~StoreStateMachine() = default;

void StoreStateMachine::onSealRecovered(Status status,
                                        LogStorageState::Seals seals) {
  auto deleter = folly::makeGuard([this] { delete this; });

  if (status != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Failed to recover the seal record for log %lu: %s",
                    message_->header_.rid.logid.val_,
                    error_description(status));

    // TODO (#9289267): handle E::LOCAL_LOG_STORE_READ.
    ld_check(status == E::DROPPED || status == E::LOCAL_LOG_STORE_READ ||
             status == E::FAILED);

    // If there's an error in recovering seal, chances are the error will
    // persist, so the log store is as good as disabled.
    message_->sendReply(status == E::DROPPED ? E::DROPPED : E::DISABLED);
    return;
  }

  // both normal and soft seals must have recovered its value
  const Seal normal_seal = seals.getSeal(LogStorageState::SealType::NORMAL);
  const Seal soft_seal = seals.getSeal(LogStorageState::SealType::SOFT);
  const bool drain = message_->header_.flags & STORE_Header::DRAINING;
  auto result = STORE_Message::checkIfPreempted(message_->header_.rid,
                                                message_->extra_.recovery_epoch,
                                                normal_seal,
                                                soft_seal,
                                                drain);
  switch (result.first) {
    case STORE_Message::PreemptResult::PREEMPTED_SOFT_ONLY:
      message_->soft_preempted_only_ = true;
    case STORE_Message::PreemptResult::PREEMPTED_NORMAL:
      ld_check(result.second.valid());
      // Reject the STORE if this record's epoch has been sealed
      message_->sendReply(E::PREEMPTED, result.second);
      /* `this` is deleted */
      return;
    case STORE_Message::PreemptResult::NOT_PREEMPTED:
      break;
  };

  // If we got a record from sequencer for epoch e, corresponding recovery
  // should seal the log up to epoch e-1. If we don't have such seal yet, let's
  // help recovery and create it. It helps in the case when a new sequencer
  // does some appends and crashes before sealing. Without this, older
  // sequencer could run forever. If we seal now, old sequencer will know it's
  // preempted as soon as it tries to store something on this node.
  // Note: only updates soft seals but leaves normal seal intact.
  if (message_->header_.sequencer_node_id.isNodeID() &&
      message_->header_.rid.epoch > epoch_t(soft_seal.epoch.val_ + 1)) {
    ServerWorker* worker = ServerWorker::onThisThread();
    logid_t log_id = message_->header_.rid.logid;
    ld_debug("Log %lu: got record %s that is more than one epoch ahead of "
             "soft seal in e%u; updating seal",
             log_id.val_,
             lsn_to_string(message_->header_.rid.lsn()).c_str(),
             soft_seal.epoch.val_);
    auto task = std::make_unique<SoftSealStorageTask>(this);
    worker->getStorageTaskQueueForShard(shard_)->putTask(std::move(task));
    deleter.dismiss();
    return;
  }

  deleter.dismiss();
  storeAndForward();
}

void StoreStateMachine::storeAndForward() {
  SCOPE_EXIT {
    delete this;
  };

  ServerWorker* worker = ServerWorker::onThisThread();
  auto& header = message_->header_;
  logid_t log_id = header.rid.logid;

  // Determine whether to write (merge) mutable per-epoch metadata along with
  // the data record.  Only do this if the feature is enabled in the settings,
  // and the lng in the new data record is greater than the lng stored in the
  // RecordCache, if any.  (If the lng in the RecordCache is greater or equal,
  // we know that a metadata record for this greater lng has already been
  // written out.  This can happen if records are received or released out of
  // order.)
  std::shared_ptr<Configuration> cfg = worker->getConfiguration();
  const auto log_config = cfg->getLogGroupByIDShared(log_id);
  bool merge_mutable_per_epoch_log_metadata = log_config &&
      log_config->attrs().mutablePerEpochLogMetadataEnabled().value();
  if (merge_mutable_per_epoch_log_metadata) {
    if (const LogStorageState* log_state =
            worker->processor_->getLogStorageStateMap().find(log_id, shard_)) {
      if (const auto& record_cache = log_state->record_cache_) {
        epoch_t epoch = header.rid.epoch;
        const auto& result = record_cache->getEpochRecordCache(epoch);
        if (result.first == RecordCache::Result::HIT &&
            result.second->getLNG() >= header.last_known_good) {
          merge_mutable_per_epoch_log_metadata = false;
        }
      }
    }
  }

  const auto& worker_settings = Worker::settings();

  // Decide if we need to write a CSI entry.
  // For internal logs, write CSI even if it's disabled in settings.
  // This way if we want to enable CSI later we don't have to do any migration
  // for internal logs.
  folly::Optional<lsn_t> block_starting_lsn;
  if ((worker_settings.write_copyset_index &&
       worker_settings.write_sticky_copysets_deprecated) ||
      MetaDataLog::isMetaDataLog(header.rid.logid) ||
      configuration::InternalLogs::isInternal(header.rid.logid)) {
    block_starting_lsn.assign(message_->block_starting_lsn_);
  }

  // First create a storage task for the local log store.  The constructor
  // will copy any needed data from the parameters (as well as attach to the
  // std::shared_ptr<PayloadHolder>), making the task self-sufficient.  We'll
  // send the task to a storage thread later (at the end of this method), to
  // avoid delaying forwarding.
  auto task = std::make_unique<StoreStorageTask>(
      header,
      message_->copyset_.begin(),
      block_starting_lsn,
      message_->optional_keys_,
      message_->payload_,
      message_->extra_,
      message_->reply_to_,
      start_time_,
      durability_,
      worker_settings.write_find_time_index,
      merge_mutable_per_epoch_log_metadata,
      worker_settings.write_shard_id_in_copyset);

  // Forward to next node in chain
  if (header.flags & STORE_Header::CHAIN) {
    if (message_->my_pos_in_copyset_ + 1 < header.copyset_size) {
      ShardID next_dest =
          message_->copyset_[message_->my_pos_in_copyset_ + 1].destination;

      ld_debug("Forwarding a STORE %s that we got from %s to %s "
               "(link #%d).",
               header.rid.toString().c_str(),
               Sender::describeConnection(from_).c_str(),
               next_dest.toString().c_str(),
               message_->my_pos_in_copyset_ + 1);

      if (message_->my_pos_in_copyset_ == header.nsync) {
        header.flags &= ~STORE_Header::SYNC;
      }
      header.copyset_offset = message_->my_pos_in_copyset_ + 1;
      if (header.copyset_offset >= message_->extra_.first_amendable_offset) {
        header.flags |= STORE_Header::AMEND;
      }

      int rv = worker->sender().sendMessage(
          std::move(message_), next_dest.asNodeID());
      if (rv != 0) {
        // sendMessage() failed, we still own message_
        message_->onForwardingFailure(err);
      }
    }
  }

  // At this point message_ may already be destroyed if we forwarded to the next
  // node in the chain.  Must not use it or any members.

  // Send the task to the storage node
  worker->getStorageTaskQueueForShard(shard_)->putTask(std::move(task));
}

}} // namespace facebook::logdevice
