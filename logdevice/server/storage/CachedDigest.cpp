/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/CachedDigest.h"

#include <folly/CppAttributes.h>
#include <folly/Memory.h>

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/server/storage/AllCachedDigests.h"

namespace facebook { namespace logdevice {

static const std::chrono::milliseconds INITIAL_PUSH_DELAY(50);
static const std::chrono::milliseconds MAX_PUSH_DELAY(1000);

using Snapshot = EpochRecordCache::Snapshot;

CachedDigest::CachedDigest(logid_t log_id,
                           shard_index_t shard,
                           read_stream_id_t stream_id,
                           ClientID client_id,
                           lsn_t start_lsn,
                           std::unique_ptr<const Snapshot> snapshot,
                           ClientDigests* client_digests,
                           AllCachedDigests* all_digests)
    : sender_(std::make_unique<SenderProxy>()),
      resume_cb_(this),
      log_id_(log_id),
      shard_(shard),
      stream_id_(stream_id),
      client_id_(client_id),
      start_lsn_(start_lsn),
      epoch_empty_(snapshot == nullptr),
      epoch_snapshot_(std::move(snapshot)),
      end_lsn_(compose_lsn(
          getEpoch(),
          (epoch_empty_ ? ESN_INVALID
                        : esn_t(epoch_snapshot_->getHeader()->max_seen_esn)))),
      client_digests_(client_digests),
      all_digests_(all_digests) {
  ld_check(log_id_ != LOGID_INVALID);
  ld_check(lsn_to_epoch(start_lsn_) != EPOCH_INVALID);
  // must be a full (serializeable) snapshot
  if (epoch_snapshot_ != nullptr) {
    ld_check(epoch_snapshot_->isSerializable());
    ld_check(epoch_snapshot_->getHeader() != nullptr);
    ld_check(!epoch_snapshot_->getHeader()->disabled);

    // caller must ensure that the snapshot has authoritative range
    // that covers start_lsn_
    ld_check(lsn_to_esn(start_lsn_) >=
             epoch_snapshot_->getAuthoritativeRangeBegin());
  }
}

void CachedDigest::start() {
  ld_check(!started());
  state_ = State::SEND_STARTED;
  next_lsn_to_deliver_ = start_lsn_;
  if (epoch_snapshot_ != nullptr) {
    snapshot_iterator_ = epoch_snapshot_->createIterator();
  }

  push_timer_ = createPushTimer([this] { pushRecords(); });
  pushRecords();
}

CachedDigest::~CachedDigest() {
  if (all_digests_) {
    all_digests_->onDigestDestroyed(started());
  }
}

bool CachedDigest::allRecordsShipped() const {
  // do not use last_esn_delivered_ since there might be a race condition
  // (although quite unlikely) that the cache no longer has record in end_lsn_
  // since esn_lsn_ is obtained before the last snapshot is created and the
  // cache can still be written by mutations of other epoch recovery procedure
  return epoch_empty_ || next_lsn_to_deliver_ > end_lsn_ ||
      lsn_to_epoch(next_lsn_to_deliver_) > getEpoch();
}

void CachedDigest::pushRecords() {
  // track total bytes pushed in this run
  size_t bytes_pushed = 0;
  int rv = 0;

  switch (state_) {
    case State::SEND_STARTED:
      rv = shipStarted();
      if (rv < 0) {
        break;
      }
      state_ = State::SEND_RECORDS;
      FOLLY_FALLTHROUGH;
    case State::SEND_RECORDS:
      rv = shipRecords(&bytes_pushed);
      if (rv < 0 || !allRecordsShipped()) {
        break;
      }
      state_ = State::CONCLUDE_DIGEST;
      FOLLY_FALLTHROUGH;
    case State::CONCLUDE_DIGEST:
      rv = concludeDigest();
      if (rv == 0) {
        onDigestComplete();
        return;
      }
      break;
    default:
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Invalid state %d encounterd "
                         "in CachedDigest state machine",
                         (int)state_);
      ld_check(false);
      return;
  }

  if (rv != 0) {
    if (err == E::SHUTDOWN) {
      // Worker is shutting down, stop pushing records.
      return;
    }
    if (err == E::CBREGISTERED) {
      if (delay_timer_ != nullptr) {
        delay_timer_->reset();
      }
      cancelPushTimer();
      return;
    }
    // For all other errors (from Sender::sendMessage() e.g. NOBUFS),
    // rely on the timer based retry below.
  }

  if (bytes_pushed > 0) {
    // In the error case, the evbuffer for the socket or sender is likely
    // full. In the non-error case, canPushRecords() has indicated that
    // we've hit the maximum outstanding bytes limit for this digest.
    // Retry on next event loop iteration.
    if (delay_timer_ != nullptr) {
      delay_timer_->reset();
    }
    activatePushTimer();
  } else {
    // We did not make any progress on this push. The outgoing evbuffer
    // for socket may be really backed-up. Retry push with a backoff
    // delay.
    if (delay_timer_ == nullptr) {
      // create delay timer on demand
      delay_timer_ = createDelayTimer([this] { pushRecords(); });
    }
    delay_timer_->activate();
    cancelPushTimer();
  }
}

int CachedDigest::shipStarted() {
  lsn_t epoch_lng = LSN_INVALID;
  if (epoch_snapshot_ != nullptr) {
    esn_t::raw_type head = epoch_snapshot_->getHeader()->head;
    epoch_lng = compose_lsn(
        epoch_snapshot_->getHeader()->epoch, EpochRecordCache::getLNG(head));
  }

  STARTED_Header header = {
      log_id_,
      stream_id_,
      E::OK,
      filter_version_t(1),
      // see the doc block of STARTED_Header in STARTED_Message.h
      epoch_lng,
      shard_};

  auto reply =
      std::make_unique<STARTED_Message>(header,
                                        TrafficClass::RECOVERY,
                                        STARTED_Message::Source::CACHED_DIGEST,
                                        start_lsn_);
  if (sender_->sendMessage(std::move(reply), client_id_, &resume_cb_) != 0) {
    if (err != E::CBREGISTERED) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "error sending STARTED message for DIGEST to client %s "
                      "for log %lu and read stream %" PRIu64 ": %s",
                      Sender::describeConnection(client_id_).c_str(),
                      log_id_.val_,
                      stream_id_.val_,
                      error_name(err));
    }
    return -1;
  }
  return 0;
}

int CachedDigest::shipRecords(size_t* bytes_pushed) {
  while (!allRecordsShipped() && canPushRecords()) {
    // overwise all records are shipped
    ld_check(epoch_snapshot_ != nullptr);
    ld_check(snapshot_iterator_ != nullptr);
    if (epoch_snapshot_->getTailRecord().isValid() &&
        next_lsn_to_deliver_ == epoch_snapshot_->getTailRecord().header.lsn) {
      // special case: we need to deliver the tail record
      const esn_t esn = nextEsnToDeliver();
      const TailRecord& tail = epoch_snapshot_->getTailRecord();
      STORE_flags_t tail_record_flags = tail.header.flags &
          (TailRecordHeader::CHECKSUM | TailRecordHeader::CHECKSUM_64BIT |
           TailRecordHeader::CHECKSUM_PARITY);
      if (tail.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH) {
        tail_record_flags |= STORE_Header::OFFSET_WITHIN_EPOCH;
      }

      // build a Snapshot::Record based on the tail record and send it out.
      // Note that some information are missing (e.g., copyset, wave) for the
      // tail record and here we just use dummy ones. This is fine because the
      // tail record is always per-epoch released and fuly replicated so that
      // epoch recovery will never use its metadata.
      Snapshot::Record tail_record{
          tail_record_flags,
          tail.header.timestamp,           // timestamp
          lsn_to_esn(tail.header.lsn - 1), // lng
          1,                               // wave
          copyset_t({ShardID(0, 0)}),      // copyset (dummy)
          tail.offsets_map_,
          tail.getPayloadSlice()};
      ssize_t pushed = shipRecord(esn, tail_record);
      if (pushed < 0) {
        return -1;
      }
      ld_check(esn > last_esn_delivered_);
      last_esn_delivered_ = esn;
      *bytes_pushed += pushed;
      ++next_lsn_to_deliver_;
      // already shipped the tail record, recheck
      // condition before shipping the next record
      continue;
    }

    ld_check(lsn_to_epoch(next_lsn_to_deliver_) == getEpoch());
    // forward iterator to next_lsn_to_deliver_
    while (!snapshot_iterator_->atEnd() &&
           snapshot_iterator_->getESN() < nextEsnToDeliver()) {
      snapshot_iterator_->next();
    }

    if (snapshot_iterator_->atEnd()) {
      // shipped everything we have
      next_lsn_to_deliver_ = end_lsn_ + 1;
      ld_check(allRecordsShipped());
      break;
    }

    // deliver the record
    esn_t esn = snapshot_iterator_->getESN();
    ld_check(esn >= nextEsnToDeliver());
    Snapshot::Record record = snapshot_iterator_->getRecord();

    ssize_t pushed = shipRecord(esn, record);
    if (pushed < 0) {
      return -1;
    }

    ld_check(esn > last_esn_delivered_);
    last_esn_delivered_ = esn;
    *bytes_pushed += pushed;
    next_lsn_to_deliver_ = compose_lsn(getEpoch(), esn) + 1;
    snapshot_iterator_->next();
  }

  return 0;
}

ssize_t
CachedDigest::shipRecord(esn_t esn,
                         const EpochRecordCache::Snapshot::Record& record) {
  RECORD_Header header = {log_id_,
                          stream_id_,
                          compose_lsn(getEpoch(), esn),
                          record.timestamp,
                          // record.flags is actually of STORE_flags_t
                          StoreFlagsToRecordFlags(record.flags),
                          shard_};

  // size == 0 may happen when we get a STORE message for a hole, make sure we
  // are sending an empty payload
  Payload payload_raw{
      record.payload_raw.size > 0 ? record.payload_raw.data : nullptr,
      record.payload_raw.size};

  std::unique_ptr<ExtraMetadata> extra_metadata = nullptr;
  if (includeExtraMetadata()) {
    header.flags |= RECORD_Header::INCLUDES_EXTRA_METADATA;
    extra_metadata = std::make_unique<ExtraMetadata>();
    extra_metadata->header.last_known_good = record.last_known_good;
    extra_metadata->header.wave = record.wave_or_recovery_epoch;
    extra_metadata->header.copyset_size = record.copyset.size();
    extra_metadata->copyset = record.copyset;
    extra_metadata->offsets_within_epoch = record.offsets_within_epoch;
  }

  if (extra_metadata && extra_metadata->offsets_within_epoch.isValid()) {
    header.flags |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
  }

  // We are doing zero-copies all the way here since the original STORE message
  // is constructed from the evbuffer. However, we have to make a copy of the
  // payload, since currently RECORD_Message owns the payload.
  auto msg =
      std::make_unique<RECORD_Message>(header,
                                       TrafficClass::RECOVERY,
                                       payload_raw.dup(),
                                       std::move(extra_metadata),
                                       RECORD_Message::Source::CACHED_DIGEST);

  const auto msg_size = msg->size();
  int rv = sender_->sendMessage(std::move(msg), client_id_, &resume_cb_);
  if (rv != 0) {
    return -1;
  }

  onBytesEnqueued(msg_size);
  return msg_size;
}

int CachedDigest::concludeDigest() {
  int rv = 0;

  ld_check(allRecordsShipped());
  ld_check(state_ == State::CONCLUDE_DIGEST);

  // For partially filled epochs, send a gap of
  // [last_esn_delivered_ + 1, ESN_MAX] to conclude
  // the digest.
  if (last_esn_delivered_.val_ < ESN_MAX.val_) {
    GAP_flags_t wire_flags = 0;
    wire_flags |= GAP_Header::DIGEST;

    auto message = std::make_unique<GAP_Message>(
        GAP_Header{
            log_id_,
            stream_id_,
            std::max(
                start_lsn_,
                compose_lsn(getEpoch(), esn_t(last_esn_delivered_.val_ + 1))),
            compose_lsn(getEpoch(), ESN_MAX),
            GapReason::NO_RECORDS,
            wire_flags,
            shard_,
        },
        TrafficClass::RECOVERY,
        GAP_Message::Source::CACHED_DIGEST);

    rv = sender_->sendMessage(std::move(message), client_id_, &resume_cb_);
  }
  return rv;
}

RECORD_flags_t
CachedDigest::StoreFlagsToRecordFlags(STORE_flags_t store_flags) {
  LocalLogStoreRecordFormat::flags_t disk_flags =
      store_flags & LocalLogStoreRecordFormat::FLAG_MASK;

  RECORD_flags_t record_flags = disk_flags;
  record_flags |= RECORD_Header::DIGEST;
  return record_flags;
}

bool CachedDigest::canPushRecords() const {
  return client_digests_->canPushRecords();
}

void CachedDigest::onBytesEnqueued(size_t msg_size) {
  client_digests_->onBytesEnqueued(msg_size);
}

void CachedDigest::onDigestComplete() {
  all_digests_->eraseDigest(client_id_, stream_id_);
}

std::unique_ptr<Timer>
CachedDigest::createPushTimer(std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

std::unique_ptr<BackoffTimer>
CachedDigest::createDelayTimer(std::function<void()> callback) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      callback, INITIAL_PUSH_DELAY, MAX_PUSH_DELAY);

  return std::move(timer);
}

void CachedDigest::activatePushTimer() {
  ld_check(push_timer_ != nullptr);
  push_timer_->activate(std::chrono::microseconds(0));
}

void CachedDigest::cancelPushTimer() {
  ld_check(push_timer_ != nullptr);
  push_timer_->cancel();
}

void CachedDigest::ResumeDigestCallback::operator()(FlowGroup&, std::mutex&) {
  cached_digest_->pushRecords();
}

}} // namespace facebook::logdevice
