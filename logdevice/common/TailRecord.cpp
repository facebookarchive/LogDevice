/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "TailRecord.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

// TODO(T33977412): Remove once all call sites are converted to using
// the other overload
TailRecord::TailRecord(const TailRecordHeader& header_in,
                       std::shared_ptr<PayloadHolder> payload)
    : header(header_in), payload_(hasPayload() ? std::move(payload) : nullptr) {
  // should be a flat payload for this constructor
  ld_check(payload == nullptr || !payload->isEvbuffer());
  offsets_map_.setCounter(CounterType::BYTE_OFFSET, header_in.u.byte_offset);
}

TailRecord::TailRecord(const TailRecordHeader& header_in,
                       OffsetMap offset_map,
                       std::shared_ptr<PayloadHolder> payload)
    : header(header_in),
      offsets_map_(std::move(offset_map)),
      payload_(hasPayload() ? std::move(payload) : nullptr) {
  // should be a flat payload for this constructor
  ld_check(payload == nullptr || !payload->isEvbuffer());
}

// TODO(T33977412)
TailRecord::TailRecord(const TailRecordHeader& header_in,
                       std::shared_ptr<ZeroCopiedRecord> record)
    : header(header_in),
      zero_copied_record_(hasPayload() ? std::move(record) : nullptr) {
  offsets_map_.setCounter(CounterType::BYTE_OFFSET, header_in.u.byte_offset);
}

TailRecord::TailRecord(TailRecord&& rhs) noexcept
    : header(rhs.header),
      offsets_map_(std::move(rhs.offsets_map_)),
      payload_(std::move(rhs.payload_)),
      zero_copied_record_(std::move(rhs.zero_copied_record_)) {
  rhs.reset();
}

TailRecord::TailRecord(const TailRecordHeader& header_in,
                       OffsetMap offset_map,
                       std::shared_ptr<ZeroCopiedRecord> record)
    : header(header_in),
      offsets_map_(std::move(offset_map)),
      zero_copied_record_(hasPayload() ? std::move(record) : nullptr) {}

TailRecord& TailRecord::operator=(TailRecord&& rhs) noexcept {
  if (this != &rhs) {
    header = rhs.header;
    payload_ = std::move(rhs.payload_);
    zero_copied_record_ = std::move(rhs.zero_copied_record_);
    offsets_map_ = std::move(rhs.offsets_map_);
    rhs.reset();
  }
  return *this;
}

Slice TailRecord::getPayloadSlice() const {
  ld_check(isValid());
  if (!hasPayload()) {
    return Slice();
  }

  if (zero_copied_record_ != nullptr) {
    return zero_copied_record_->payload_raw;
  }

  ld_check(payload_ != nullptr);
  return Slice(payload_->getFlatPayload());
}

TailRecordHeader::blob_size_t TailRecord::calculateBlobSize() const {
  ld_check(isValid());
  TailRecordHeader::blob_size_t offset_map_size = 0;

  if (containsOffsetMap()) {
    offset_map_size = static_cast<TailRecordHeader::blob_size_t>(
        offsets_map_.sizeInLinearBuffer());
  }

  if (!hasPayload()) {
    // currently the blob only contains payload
    return offset_map_size;
  }

  const size_t payload_size = getPayloadSlice().size;
  ld_check(payload_size < Message::MAX_LEN);
  return static_cast<TailRecordHeader::blob_size_t>(payload_size) +
      sizeof(TailRecordHeader::payload_size_t) + offset_map_size;
}

void TailRecord::serialize(ProtocolWriter& writer) const {
  if (!isValid()) {
    writer.setError(E::INVALID_PARAM);
    return;
  }

  TailRecordHeader write_header = header;
  ld_check(offsets_map_.getCounter(CounterType::BYTE_OFFSET) ==
           write_header.u.offset_within_epoch);

  const TailRecordHeader::blob_size_t blob_size = calculateBlobSize();

  if (blob_size > 0) {
    write_header.flags |= TailRecordHeader::INCLUDE_BLOB;
  }

  writer.write(write_header);
  if (blob_size > 0) {
    writer.write(blob_size);
    if (hasPayload()) {
      auto payload_slice = getPayloadSlice();
      TailRecordHeader::payload_size_t payload_size =
          static_cast<TailRecordHeader::payload_size_t>(payload_slice.size);
      writer.write(payload_size);
      // if possible, zero-copy write the actual payload
      writer.writeWithoutCopy(payload_slice.data, payload_slice.size);
    }
  }

  if (containsOffsetMap()) {
    offsets_map_.serialize(writer);
  }
}

void TailRecord::deserialize(ProtocolReader& reader,
                             bool evbuffer_zero_copy,
                             folly::Optional<size_t> /*not used*/) {
#define CHECK_READER()  \
  if (reader.error()) { \
    err = E::BADMSG;    \
    return;             \
  }
  reset();
  const size_t bytes_read_before_deserialize = reader.bytesRead();
  reader.read(&header);
  CHECK_READER();

  TailRecordHeader::blob_size_t blob_size = 0;
  if (header.flags & TailRecordHeader::INCLUDE_BLOB) {
    reader.read(&blob_size);

    if (hasPayload()) {
      TailRecordHeader::payload_size_t payload_size;
      reader.read(&payload_size);
      if (payload_size == 0) {
        // do not use zero-copy if payload size is 0
        evbuffer_zero_copy = false;
      }
      payload_ = std::make_shared<PayloadHolder>(
          PayloadHolder::deserialize(reader, payload_size, evbuffer_zero_copy));
      if (payload_ && evbuffer_zero_copy) {
        // linearize the payload
        auto ph_raw = payload_->getPayload();
        // must be on worker thread
        auto w = Worker::onThisThread();
        // further wraps the payload into ZeroCopiedRecord
        zero_copied_record_ = ZeroCopiedRecord::create<ZeroCopiedRecord>(
            ZeroCopiedRecord::Disposer(
                &w->processor_->zeroCopiedRecordDisposal()),
            header.lsn,
            /*unused flags*/ 0,
            header.timestamp,
            /*unused lng*/ ESN_INVALID,
            /*unused wave*/ 0,
            /*unused copyset*/ copyset_t{},
            header.u.offset_within_epoch,
            /*unused keys*/ std::map<KeyType, std::string>{},
            Slice{ph_raw},
            std::move(payload_));
        ld_check(payload_ == nullptr);
      }
    }
  }

  if (containsOffsetMap()) {
    offsets_map_.deserialize(reader, false /* unused */);
  } else {
    // TODO(T33977412)
    offsets_map_.setCounter(CounterType::BYTE_OFFSET, header.u.byte_offset);
  }

  // clear the TailRecordHeader::INCLUDE_BLOB flag as it is only used
  // in serialization format
  header.flags &= ~TailRecordHeader::INCLUDE_BLOB;

  // draining the remaining bytes for forward compatibility
  CHECK_READER();
  ld_check(reader.bytesRead() >= bytes_read_before_deserialize);
  const size_t bytes_consumed =
      reader.bytesRead() - bytes_read_before_deserialize;
  const size_t bytes_expected = expectedRecordSizeInBuffer(blob_size);
  if (bytes_consumed > bytes_expected) {
    // we already read more than we should, the record must be malformed
    reader.setError(E::BADMSG);
    return;
  }

  bool has_unknown_flags = header.flags & ~TailRecordHeader::ALL_KNOWN_FLAGS;
  if (has_unknown_flags) {
    reader.allowTrailingBytes();
  } else {
    reader.disallowTrailingBytes();
  }
  reader.handleTrailingBytes(bytes_expected - bytes_consumed);
}

bool TailRecord::sameContent(const TailRecord& rhs) const {
  if (!isValid() != !rhs.isValid()) {
    return false;
  }

  if (!isValid()) {
    // records are both invalid, consider then as the same
    return true;
  }

  if (memcmp(&header, &rhs.header, sizeof(header)) != 0) {
    return false;
  }

  Slice s = getPayloadSlice();
  Slice s_r = rhs.getPayloadSlice();
  return s.size == s_r.size &&
      (s.size == 0 || memcmp(s.data, s_r.data, s.size) == 0);
}

/*static*/
bool TailRecord::sameContent(const TailRecord& lhs, const TailRecord& rhs) {
  return lhs.sameContent(rhs);
}

std::string TailRecord::toString() const {
  std::string out = "[L:" + std::to_string(header.log_id.val_) +
      " N:" + lsn_to_string(header.lsn) +
      " T:" + std::to_string(header.timestamp) +
      ((containOffsetWithinEpoch() ? " O:" : " B:") +
       std::to_string(header.u.byte_offset)) +
      " F:" + std::to_string(header.flags) + "]";
  if (!isValid()) {
    out += "(Invalid)";
  }
  return out;
}

}} // namespace facebook::logdevice
