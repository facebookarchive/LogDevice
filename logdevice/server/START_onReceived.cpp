/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "START_onReceived.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerRecordFilterFactory.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/ServerReadStream.h"
#include "logdevice/server/storage/AllCachedDigests.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

static Message::Disposition
onReceivedContinuation(START_Message* msg,
                       const Address& from,
                       PermissionCheckStatus permission_status);

static Message::Disposition
send_error_reply(const START_Message* msg,
                 const Address& to,
                 Status status,
                 lsn_t last_released = LSN_INVALID) {
  STARTED_Header reply_header = {msg->header_.log_id,
                                 msg->header_.read_stream_id,
                                 status,
                                 msg->header_.filter_version,
                                 last_released,
                                 msg->header_.shard};

  // For errors, we try once using a high priority traffic class. If the
  // send fails, typically due to E::NOBUFS, return ERROR so the socket
  // is closed. This allows the client to detect that a message with
  // reliable delivery semantics was lost without having to fall back on
  // timeouts.
  auto reply =
      std::make_unique<STARTED_Message>(reply_header, TrafficClass::HANDSHAKE);
  if (Worker::onThisThread()->sender().sendMessage(std::move(reply), to) != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "error sending STARTED message to client %s "
                    "for log %lu and read stream %" PRIu64 ": %s. "
                    "Closing socket",
                    Sender::describeConnection(to).c_str(),
                    msg->header_.log_id.val_,
                    msg->header_.read_stream_id.val_,
                    error_name(err));
    err = E::INTERNAL;
    return Message::Disposition::ERROR;
  }
  return Message::Disposition::NORMAL;
}

static bool isRSMLog(ServerWorker* w, logid_t log_id) {
  std::shared_ptr<LogsConfig> logs_config = w->getLogsConfig();
  return logs_config->isInternalLogID(log_id) ||
      (MetaDataLog::isMetaDataLog(log_id) &&
       logs_config->isInternalLogID(MetaDataLog::dataLogID(log_id)));
}

Message::Disposition START_onReceived(START_Message* msg, const Address& from) {
  const START_Header& header = msg->header_;
  ld_debug("START message from %s: log_id %" PRIu64 ", "
           "read_stream_id %" PRIu64 ", start_lsn %s, "
           "until_lsn %s, window_high %s, flags = 0x%x.",
           Sender::describeConnection(from).c_str(),
           header.log_id.val_,
           header.read_stream_id.val_,
           lsn_to_string(header.start_lsn).c_str(),
           lsn_to_string(header.until_lsn).c_str(),
           lsn_to_string(header.window_high).c_str(),
           header.flags);

  if (!from.isClientAddress()) {
    ld_error("got START message from non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  ServerWorker* w = ServerWorker::onThisThread();
  if (!w->isAcceptingWork()) {
    ld_debug("Ignoring START message: not accepting more work");
    return send_error_reply(msg, from, E::SHUTDOWN);
  }

  if (!w->processor_->runningOnStorageNode()) {
    // This may happen if the weight of this storage node is changed from -1 to
    // 1 but this storage node was not yet restarted to take the change into
    // account.
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "got START message from client %s but not a storage node",
                      Sender::describeConnection(from).c_str());
    return send_error_reply(msg, from, E::NOTSTORAGE);
  }

  // We cannot proceed reading this log unless it's an internal log!
  if (!w->getLogsConfig()->isFullyLoaded() &&
      !w->getLogsConfig()->isInternalLogID(header.log_id) &&
      !MetaDataLog::isMetaDataLog(header.log_id)) {
    // The client will retry until the configuration is fully loaded on the
    // server.
    return send_error_reply(msg, from, E::AGAIN);
  }

  auto scfg = w->getServerConfig();
  auto* node = scfg->getNode(scfg->getMyNodeID().index());
  ld_check(node);
  ld_check(node->storage_attributes);
  const shard_size_t n_shards = node->getNumShards();
  ld_check(n_shards > 0); // We already checked we are a storage node.

  shard_index_t shard_idx = header.shard;
  if (shard_idx < 0 || shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got START message from client %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return send_error_reply(msg, from, E::BADMSG);
  }

  // If rebuilding is in progress and this is digest read stream, send
  // E::REBUILDING.
  // For non digest read streams, a STARTED(E::REBUILDING) will be sent later
  // but the stream will be created anyway so it can later be woken up when the
  // shard is available again (@see AllServerReadStreams::onShardRebuilt).
  if ((header.flags & START_Header::DIGEST) &&
      (w->processor_->isDataMissingFromShard(shard_idx))) {
    // Recoveries only send START_Message to nodes that successfully sealed.
    // SEAL_Message's are rejected when isDataMissingFromShard() is true.
    // So, this should be unreachable, hence the ld_error().
    ld_error("Got START message with DIGEST flag from %s but shard %u is "
             "waiting for rebuilding",
             Sender::describeConnection(from).c_str(),
             shard_idx);
    return send_error_reply(msg, from, E::REBUILDING);
  }

  // TODO validate log ID and send back error

  // Validate LSN ranges.  Note that it *is* valid for window_high to be less
  // than start_lsn - the client would be initiating a read stream where the
  // server won't send anything until it receives a window update.
  if (header.start_lsn > header.until_lsn ||
      header.until_lsn < header.window_high) {
    ld_error("invalid START message from %s: log_id %lu, "
             "read_stream_id %lu, start_lsn %s, "
             "until_lsn %s, window_high %s.",
             Sender::describeConnection(from).c_str(),
             header.log_id.val_,
             header.read_stream_id.val_,
             lsn_to_string(header.start_lsn).c_str(),
             lsn_to_string(header.until_lsn).c_str(),
             lsn_to_string(header.window_high).c_str());
    err = E::BADMSG;
    return Message::Disposition::ERROR; // client misbehaving, close connection
  }

  if (header.scd_copyset_reordering >= SCDCopysetReordering::MAX) {
    ld_error("START message from %s contains invalid `scd_copyset_reordering' "
             "member %d",
             Sender::describeConnection(from).c_str(),
             static_cast<int>(header.scd_copyset_reordering));
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  std::shared_ptr<PermissionChecker> permission_checker =
      Worker::onThisThread()
          ->processor_->security_info_->getPermissionChecker();

  if (permission_checker) {
    const PrincipalIdentity* principal = w->sender().getPrincipal(from);
    if (principal == nullptr) {
      ld_critical("START_Message from %s for log %lu failed because "
                  "there is no Principal Associated with from %s",
                  Sender::describeConnection(from).c_str(),
                  header.log_id.val_,
                  Sender::describeConnection(from).c_str());
      // This should never happen. We are invoking onReceived from the socket
      // that has already performed the hello/ack handshake.
      ld_check(false);
      err = E::ACCESS;
      return Message::Disposition::ERROR;
    }

    permission_checker->isAllowed(
        ACTION::READ,
        *principal,
        header.log_id,

        [msg, from](PermissionCheckStatus permission_status) mutable {
          onReceivedContinuation(std::unique_ptr<START_Message>(msg).get(),
                                 from,
                                 permission_status);
        });
    return Message::Disposition::KEEP;
  } else {
    return onReceivedContinuation(msg, from, PermissionCheckStatus::NONE);
  }
}

static Message::Disposition
onReceivedContinuation(START_Message* msg,
                       const Address& from,
                       PermissionCheckStatus permission_status) {
  ServerWorker* w = ServerWorker::onThisThread();
  if (!w->isAcceptingWork()) {
    ld_debug("Ignoring START message: not accepting more work");
    return send_error_reply(msg, from, E::SHUTDOWN);
  }
  auto scfg = w->getServerConfig();
  const START_Header& header = msg->header_;
  shard_index_t shard_idx = header.shard;

  auto* node = scfg->getNode(scfg->getMyNodeID().index());
  ld_check(node);
  ld_check(node->storage_attributes);
  const shard_size_t n_shards = node->getNumShards();
  ld_check(n_shards > 0); // We already checked we are a storage node.

  if (permission_status == PermissionCheckStatus::DENIED) {
    // The client is not allowed to read from this log_id.
    // Do not send client any information related to the log_id
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    3,
                    "START_Message from %s for log %lu failed with "
                    "E::ACCESS because client does not have the required "
                    "permissions to complete the request",
                    Sender::describeConnection(from).c_str(),
                    header.log_id.val_);
    return send_error_reply(msg, from, E::ACCESS);
  } else if (permission_status == PermissionCheckStatus::NOTREADY) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   3,
                   "Got START_Message from %s for log %lu but "
                   "permission checker is not yet loaded. "
                   "Responding with E::NOTREADY",
                   Sender::describeConnection(from).c_str(),
                   header.log_id.val_);
    return send_error_reply(msg, from, E::AGAIN);
  } else if (permission_status == PermissionCheckStatus::SYSLIMIT) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   3,
                   "Got START_Message from %s for log %lu but "
                   "there is not enough background threads. "
                   "Responding with E::SYSLIMIT",
                   Sender::describeConnection(from).c_str(),
                   header.log_id.val_);
    return send_error_reply(msg, from, E::SYSLIMIT);
  } else if (permission_status == PermissionCheckStatus::NOTFOUND) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   3,
                   "Got START_Message from %s for log %lu but "
                   "log is not present in config. "
                   "Responding with E::NOTFOUND",
                   Sender::describeConnection(from).c_str(),
                   header.log_id.val_);
    return send_error_reply(msg, from, E::NOTFOUND);
  }

  // If we're going to do any reading, we need to have a LogStorageState
  // instance.
  LogStorageState* log_state =
      w->processor_->getLogStorageStateMap().insertOrGet(
          header.log_id, shard_idx);
  if (log_state == nullptr || log_state->hasPermanentError()) {
    // One of:
    //  - LogStorageStateMap is at capacity,
    //  - LogStorageState is in permanent error. In that case we give up
    //    reading.
    return send_error_reply(msg, from, E::FAILED);
  }

  if (header.flags & START_Header::DIGEST) {
    // If LocalLogStore is not accepting writes, refuse to send digest.
    // This node won't be able to participate in recovery.

    const ShardedStorageThreadPool* sharded_pool =
        w->processor_->sharded_storage_thread_pool_;
    Status accepting = sharded_pool->getByIndex(shard_idx)
                           .getLocalLogStore()
                           .acceptingWrites();
    if (accepting == E::DISABLED || log_state->hasPermanentError()) {
      // Send a persistent error if either:
      // 1) local log store is not accepting writes (e.g., fail-safe), OR
      // 2) the log has permanent error indicated in the log state.
      // Recovery won't try to get a digest from this node until it closes the
      // socket.
      return send_error_reply(msg, from, E::FAILED);
    }

    if (!(header.flags & START_Header::INCLUDE_EXTRA_METADATA)) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        2,
                        "Received a START message from %s for log %lu with "
                        "DIGEST flag set, but not requesting extra_metadata. "
                        "The sequencer node might be running with an old "
                        "logdevice version.",
                        Sender::describeConnection(from).c_str(),
                        header.log_id.val_);
      // TODO 11866467: enable the protocol check once every deployment has been
      // upgraded.
      // err = E::PROTO;
      // return Message::Disposition::ERROR;
    }

    // for recovery reads for digest, attempt a cache lookup
    WORKER_STAT_INCR(epoch_recovery_digest_received);
    RecordCache* cache = log_state->record_cache_.get();
    if (cache) {
      auto result = cache->getEpochRecordCache(lsn_to_epoch(header.start_lsn));
      switch (result.first) {
        case RecordCache::Result::NO_RECORD:
          ld_check(result.second == nullptr);
          FOLLY_FALLTHROUGH;
        case RecordCache::Result::HIT: {
          std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot;
          if (result.second != nullptr) {
            // if true, the epoch cache does not have all records requested
            // by the digest stream. It is possible that the LNG has been
            // advanced after the SEAL phase.
            bool out_of_range = false;
            if (result.second->getAuthoritativeRangeBegin() >
                lsn_to_esn(header.start_lsn)) {
              // Note that ESN_INVALID can be returned by
              // getAuthoritativeRangeBegin()here, in this case it is safe to
              // use the cache (given that it is consistent) since it means that
              // nothing is ever per-epoch released or evicted from the cache so
              // the cache should have stored all records in this epoch stored
              // on the node so far.
              out_of_range = true;
            } else {
              // TODO (T35832374) : Remove enable_offset_map when OffsetMap is
              // supported by all servers
              bool enable_offset_map = w->settings().enable_offset_map;
              // create an immutable snapshot of the epoch cache
              epoch_snapshot =
                  result.second->createSerializableSnapshot(enable_offset_map);
              ld_check(epoch_snapshot != nullptr);
              ld_check(epoch_snapshot->getHeader() != nullptr);

              if (epoch_snapshot->getHeader()->disabled ||
                  epoch_snapshot->getAuthoritativeRangeBegin() >
                      lsn_to_esn(header.start_lsn)) {
                // it is possible that the epoch cache has changed since our
                // previous check, recheck the same condition on the immutatble
                // snapshot
                out_of_range = true;
              }
            }
            if (out_of_range) {
              // consider this as a cache miss, and fallback to reading
              // from local log store
              WORKER_STAT_INCR(record_cache_digest_miss);
              if (!MetaDataLog::isMetaDataLog(header.log_id)) {
                WORKER_STAT_INCR(record_cache_digest_miss_datalog);
              }
              break;
            }
          }

          // cache hit, proceed to serving the digest from the epoch
          // record cache snapshot
          WORKER_STAT_INCR(record_cache_digest_hit);
          if (!MetaDataLog::isMetaDataLog(header.log_id)) {
            WORKER_STAT_INCR(record_cache_digest_hit_datalog);
          }

          auto status =
              w->cachedDigests().startDigest(header.log_id,
                                             shard_idx,
                                             header.read_stream_id,
                                             from.id_.client_,
                                             header.start_lsn,
                                             std::move(epoch_snapshot));
          if (status != E::OK) {
            return send_error_reply(msg, from, status);
          }
          return Message::Disposition::NORMAL;
        }
        case RecordCache::Result::MISS:
          WORKER_STAT_INCR(record_cache_digest_miss);
          if (!MetaDataLog::isMetaDataLog(header.log_id)) {
            WORKER_STAT_INCR(record_cache_digest_miss_datalog);
          }
          // cache miss, fall back to create a ServerReadStream instance
          break;
      }
    }
  }

  auto insert_result = w->serverReadStreams().insertOrGet(
      from.id_.client_, header.log_id, shard_idx, header.read_stream_id);
  ServerReadStream* stream = insert_result.first;
  if (stream == nullptr) {
    // Failing to create a ServerReadStream means the server's data structures
    // are at capacity.  The issue may be temporary or persistent.
    Status status;
    switch (err) {
      case E::PERMLIMIT:
        status = E::FAILED;
        break;
      case E::TEMPLIMIT:
        status = E::AGAIN;
        break;
      default:
        ld_error("AllServerReadStreams::insertOrGet() returned unexpected "
                 "error %s",
                 error_description(err));
        ld_check(false);
        status = E::AGAIN;
    }
    return send_error_reply(msg, from, status);
  }

  stream->replication_ = header.replication;

  if (insert_result.second) {
    // This is a new stream. Keep track of start_lsn for debug purposes.
    stream->start_lsn_ = header.start_lsn;
  } else if (header.filter_version < stream->filter_version_) {
    // this is old message, ignore it. This could be due to
    // onReceivedContinuation reordering. See T24126024 for details
    return Message::Disposition::NORMAL;
  }

  if (insert_result.second || header.filter_version > stream->filter_version_) {
    // This is a new stream, or the filter version increased in which case we
    // allow it to rewind. Initialize the stream.

    if (insert_result.second) {
      ld_check_eq(stream->filter_version_, filter_version_t{0});
      ld_check_ge(header.filter_version, filter_version_t{1});
    } else {
      // Rewind of existing stream.
      stream->last_rewind_time_ = SteadyTimestamp::now();
    }

    stream->setReadPtr(header.start_lsn);
    stream->until_lsn_ = header.until_lsn;
    stream->last_delivered_lsn_ = std::max(header.start_lsn, lsn_t(1)) - 1;
    stream->need_to_deliver_lsn_zero_ = header.start_lsn == LSN_INVALID;
    // We don't expect clients to ever change fill_cache_ for an existing read
    // stream but it likely wouldn't have effect anyway because of iterator
    // caching
    stream->fill_cache_ = !(header.flags & START_Header::DIRECT);
    stream->digest_ = header.flags & START_Header::DIGEST;

    stream->filter_version_ = header.filter_version;
    if (header.flags & START_Header::SINGLE_COPY_DELIVERY) {
      stream->enableSingleCopyDelivery(
          msg->filtered_out_, scfg->getMyNodeID().index());
      if (header.flags & START_Header::LOCAL_SCD_ENABLED) {
        auto client_location = w->sender().getClientLocation(from.id_.client_);
        if (client_location.empty()) {
          RATELIMIT_WARNING(std::chrono::seconds(1),
                            1,
                            "Got LOCAL_SCD_ENABLED flag from client %s, but no "
                            "client location was found",
                            Sender::describeConnection(from).c_str());
          stream->disableLocalScd();
        } else {
          ld_debug("Got LOCAL_SCD_ENABLED flag from client %s, with client "
                   "location: %s",
                   Sender::describeConnection(from).c_str(),
                   client_location.c_str());
          stream->enableLocalScd(client_location);
        }
      } else {
        stream->disableLocalScd();
      }
    } else {
      stream->disableSingleCopyDelivery();
    }
    stream->needs_started_message_ = true;
  }

  stream->setWindowHigh(header.window_high);

  stream->include_extra_metadata_ =
      header.flags & START_Header::INCLUDE_EXTRA_METADATA;
  stream->ignore_released_status_ =
      header.flags & START_Header::IGNORE_RELEASED_STATUS;
  stream->no_payload_ = header.flags & START_Header::NO_PAYLOAD;
  stream->csi_data_only_ = header.flags & START_Header::CSI_DATA_ONLY;
  stream->payload_hash_only_ = header.flags & START_Header::PAYLOAD_HASH_ONLY;
  stream->include_byte_offset_ =
      (header.flags & START_Header::INCLUDE_BYTE_OFFSET) &&
      Worker::settings().byte_offsets;

  if (stream->digest_ || stream->no_payload_) {
    stream->setTrafficClass(TrafficClass::RECOVERY);
  } else if (isRSMLog(w, header.log_id)) {
    stream->setTrafficClass(TrafficClass::RSM);
  } else if (stream->include_extra_metadata_) {
    stream->setTrafficClass(TrafficClass::REBUILD);
  } else {
    auto& tsc = scfg->getTrafficShapingConfig();
    stream->setTrafficClass(tsc.default_read_traffic_class);

    const PrincipalIdentity* principalIdentity = w->sender().getPrincipal(from);

    // principalIdentity identity could be nullptr if connection was closed
    if (principalIdentity) {
      // we look only on first identity which hast traffic class set
      for (auto identity : principalIdentity->identities) {
        auto principal = scfg->getPrincipalByName(&identity.second);
        if (principal != nullptr &&
            principal->max_read_traffic_class != TrafficClass::INVALID) {
          stream->setTrafficClass(principal->max_read_traffic_class);
          ld_debug("Assigning traffic class to principal: %s, "
                   "max_read_traffic_class: %s ",
                   principal->name.c_str(),
                   trafficClasses()[principal->max_read_traffic_class].c_str());
          break;
        }
      }
    }
  }

  stream->setSCDCopysetReordering(
      header.scd_copyset_reordering, msg->csid_hash_pt1, msg->csid_hash_pt2);

  stream->proto_ = msg->proto_;

  if (w->processor_->isDataMissingFromShard(shard_idx)) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got START message from client %s but shard %u is "
                   "waiting for rebuilding",
                   Sender::describeConnection(from).c_str(),
                   shard_idx);
    // When set, this flag indicates that this storage node should not serve
    // records/gaps for this read stream because the shard for this log is being
    // rebuilt. When rebuilding of the shard completes, the read stream will be
    // woken up.
    stream->rebuilding_ = true;
  }

  stream->filter_pred_ = ServerRecordFilterFactory::create(msg->attrs_);
  if (stream->filter_pred_ != nullptr) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Server-side filtering is enabled. %s",
                   toString(*stream->filter_pred_).c_str());
  }

  w->processor_->getLogStorageStateMap().recoverLogState(
      header.log_id, shard_idx, LogStorageState::RecoverContext::START_MESSAGE);
  w->serverReadStreams().notifyNeedsCatchup(*stream, /* allow_delay */ false);

  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
