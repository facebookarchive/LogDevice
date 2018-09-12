/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "Metadata.h"

namespace facebook { namespace logdevice {

Status EpochRecoveryMetadata::update(PerEpochLogMetadata& new_metadata) {
  ld_assert(dynamic_cast<EpochRecoveryMetadata*>(&new_metadata) != nullptr);
  EpochRecoveryMetadata* new_meta =
      static_cast<EpochRecoveryMetadata*>(&new_metadata);

  // TODO: we probably still need to merge the recovery window
  // (last_known_good, last_digest_esn]
  if (new_meta->valid() &&
      (!valid() ||
       header_.sequencer_epoch < new_meta->header_.sequencer_epoch)) {
    deserialize(new_meta->serialize());
    return E::OK;
  }

  new_meta->deserialize(serialize());
  return E::UPTODATE;
}

std::string EpochRecoveryMetadata::toStringShort() const {
  std::string out = "[";
  if (!valid()) {
    out += "(invalid) ";
  }

  out += ("S:" + std::to_string(header_.sequencer_epoch.val_) +
          " L:" + std::to_string(header_.last_known_good.val_) +
          " H:" + std::to_string(header_.last_digest_esn.val_) +
          " F:" + std::to_string(header_.flags) +
          " O:" + std::to_string(header_.epoch_end_offset) +
          " Z:" + std::to_string(header_.epoch_size));
  out += "]";
  return out;
}

std::string MutablePerEpochLogMetadata::toString() const {
  std::string out("[");
  if (!valid()) {
    out.append("(invalid) ");
  }
  out.append("L:");
  out.append(std::to_string(data_.last_known_good.val_));
  out.append(" Z:");
  out.append(std::to_string(data_.epoch_size));
  out.append(1, ']');
  return out;
}

void RebuildingRangesMetadata::addTimeInterval(DataClass dc,
                                               RecordTimeInterval time_range) {
  // ld_check() not dd_assert() since callers are expected to always
  // call with a valid time range.
  ld_check(time_range.lower() < time_range.upper());
  if (time_range.lower() < time_range.upper()) {
    auto& time_ranges = per_dc_dirty_ranges_[dc];
    time_ranges.insert(time_range);
  }
}

Slice RebuildingRangesMetadata::serialize() const {
  Header h;

  h.len = sizeof(h);
  h.data_classes_offset = 0;
  h.data_classes_len = per_dc_dirty_ranges_.size() * sizeof(DataClassHeader);
  uint32_t record_size = h.len;
  if (h.data_classes_len != 0) {
    h.data_classes_offset = h.len;
    record_size += h.data_classes_len;
    for (auto& trs_kv : per_dc_dirty_ranges_) {
      ld_assert(trs_kv.second.iterative_size() != 0);
      record_size += trs_kv.second.iterative_size() * sizeof(TimeRange);
    }
  }

  serialize_buffer_.resize(record_size);

  memcpy(serialize_buffer_.data(), &h, sizeof(h));

  uint32_t dc_offset = h.data_classes_offset;
  uint32_t data_offset =
      std::max((uint32_t)h.len, h.data_classes_offset + h.data_classes_len);
  for (auto& trs_kv : per_dc_dirty_ranges_) {
    DataClassHeader dc;
    dc.len = sizeof(dc);
    dc.data_class = (DataClass)trs_kv.first;
    dc.time_ranges_offset = data_offset;
    dc.time_ranges_len = trs_kv.second.iterative_size() * sizeof(TimeRange);
    memcpy(serialize_buffer_.data() + dc_offset, &dc, sizeof(dc));
    dc_offset += dc.len;
    data_offset += dc.time_ranges_len;

    uint32_t tr_offset = dc.time_ranges_offset;
    for (auto& tr_entry : trs_kv.second) {
      TimeRange tr;
      tr.len = sizeof(tr);
      tr.start_ms = tr_entry.lower().toMilliseconds().count();
      tr.end_ms = tr_entry.upper().toMilliseconds().count();
      memcpy(serialize_buffer_.data() + tr_offset, &tr, sizeof(tr));
      tr_offset += tr.len;
    }
    ld_check(tr_offset == data_offset);
  }
  ld_check(data_offset == record_size);
  return Slice(serialize_buffer_.data(), record_size);
}

int RebuildingRangesMetadata::deserialize(Slice blob) {
  // Reset to empty state
  per_dc_dirty_ranges_.clear();

  Header h;
  const uint8_t* data = static_cast<const uint8_t*>(blob.data);
  if (blob.size < sizeof(h)) {
    ld_check(false);
    err = E::MALFORMED_RECORD;
    return -1;
  }

  memcpy(&h, data, sizeof(h));
  if (h.len < sizeof(h) || h.len > blob.size ||
      (h.data_classes_len != 0 &&
       (h.data_classes_offset < h.len || h.data_classes_offset > blob.size))) {
    ld_check(false);
    err = E::MALFORMED_RECORD;
    return -1;
  }

  uint32_t dc_offset = h.data_classes_offset;
  uint32_t data_offset = dc_offset + h.data_classes_len;
  while (dc_offset < blob.size &&
         (dc_offset - h.data_classes_offset) < h.data_classes_len) {
    DataClassHeader dc;

    if (!dd_assert((blob.size - dc_offset) >= sizeof(dc),
                   "RebuildingRangesRecord data_class offset (%s) exceeds "
                   "record size (%s)",
                   logdevice::toString(dc_offset).c_str(),
                   logdevice::toString(blob.size).c_str())) {
      err = E::MALFORMED_RECORD;
      return -1;
    }

    memcpy(&dc, data + dc_offset, sizeof(dc));
    dc_offset += dc.len;

    if (!dd_assert(
            dc.time_ranges_offset >= data_offset &&
                dc.time_ranges_offset <= blob.size &&
                (blob.size - dc.time_ranges_offset) >= dc.time_ranges_len,
            "RebuildingRangesRecord time ranges "
            "(class %s, offset %s, len %s) exceed the record size (%s)",
            logdevice::toString(dc.data_class).c_str(),
            logdevice::toString(dc.time_ranges_offset).c_str(),
            logdevice::toString(dc.time_ranges_len).c_str(),
            logdevice::toString(blob.size).c_str())) {
      err = E::MALFORMED_RECORD;
      return -1;
    }
    data_offset = dc.time_ranges_offset + dc.time_ranges_len;

    if (dc.time_ranges_len == 0) {
      ld_warning("RebuildingRangesMetadata for %s contains an empty range list",
                 logdevice::toString(dc.data_class).c_str());
      continue;
    }

    auto& data_class = per_dc_dirty_ranges_[dc.data_class];
    uint32_t tr_offset = dc.time_ranges_offset;
    while (tr_offset < blob.size &&
           (tr_offset - dc.time_ranges_offset) < dc.time_ranges_len) {
      TimeRange tr;

      if ((blob.size - tr_offset) < sizeof(tr)) {
        ld_check(false);
        err = E::MALFORMED_RECORD;
        return -1;
      }

      memcpy(&tr, data + tr_offset, sizeof(tr));
      tr_offset += tr.len;

      data_class.insert(RecordTimeInterval(
          RecordTimestamp::from(std::chrono::milliseconds(tr.start_ms)),
          RecordTimestamp::from(std::chrono::milliseconds(tr.end_ms))));
    }
  }

  return 0;
}

std::string RebuildingRangesMetadata::toString() const {
  return logdevice::toString(per_dc_dirty_ranges_);
}

// EnumMap boilerplate.

template <>
const std::string&
EnumMap<LogMetadataType, std::string, LogMetadataType::MAX>::invalidValue() {
  static const std::string invalidName("INVALID");
  return invalidName;
}
template <>
const std::string& EnumMap<PerEpochLogMetadataType,
                           std::string,
                           PerEpochLogMetadataType::MAX>::invalidValue() {
  static const std::string invalidName("INVALID");
  return invalidName;
}
template <>
const std::string& EnumMap<StoreMetadataType,
                           std::string,
                           StoreMetadataType::MAX>::invalidValue() {
  static const std::string invalidName("INVALID");
  return invalidName;
}

EnumMap<LogMetadataType, std::string, LogMetadataType::MAX>&
logMetadataTypeNames() {
  // Leak it to avoid static destruction order fiasco.
  static auto map =
      new EnumMap<LogMetadataType, std::string, LogMetadataType::MAX>();
  return *map;
}
EnumMap<PerEpochLogMetadataType, std::string, PerEpochLogMetadataType::MAX>&
perEpochLogMetadataTypeNames() {
  // Leak it to avoid static destruction order fiasco.
  static auto map = new EnumMap<PerEpochLogMetadataType,
                                std::string,
                                PerEpochLogMetadataType::MAX>();
  return *map;
}
EnumMap<StoreMetadataType, std::string, StoreMetadataType::MAX>&
storeMetadataTypeNames() {
  // Leak it to avoid static destruction order fiasco.
  static auto map =
      new EnumMap<StoreMetadataType, std::string, StoreMetadataType::MAX>();
  return *map;
}

// Boilerplate listing all the types.

std::unique_ptr<LogMetadata> LogMetadataFactory::create(LogMetadataType type) {
  switch (type) {
    case LogMetadataType::DEPRECATED_1:
    case LogMetadataType::DEPRECATED_2:
    case LogMetadataType::DEPRECATED_3:
      assert(false);
      std::abort();
      return nullptr;
    case LogMetadataType::LAST_RELEASED:
      return std::make_unique<LastReleasedMetadata>();
    case LogMetadataType::TRIM_POINT:
      return std::make_unique<TrimMetadata>();
    case LogMetadataType::SEAL:
      return std::make_unique<SealMetadata>();
    case LogMetadataType::LAST_CLEAN:
      return std::make_unique<LastCleanMetadata>();
    case LogMetadataType::REBUILDING_CHECKPOINT:
      return std::make_unique<RebuildingCheckpointMetadata>();
    case LogMetadataType::SOFT_SEAL:
      return std::make_unique<SoftSealMetadata>();
    case LogMetadataType::LOG_REMOVAL_TIME:
      return std::make_unique<LogRemovalTimeMetadata>();
    case LogMetadataType::MAX:
      break;
  }
  ld_check(false);
  return nullptr;
}

std::unique_ptr<PerEpochLogMetadata>
PerEpochLogMetadataFactory::create(PerEpochLogMetadataType type) {
  switch (type) {
    case PerEpochLogMetadataType::RECOVERY:
      return std::make_unique<EpochRecoveryMetadata>();
    case PerEpochLogMetadataType::MUTABLE:
      return std::make_unique<MutablePerEpochLogMetadata>();
    case PerEpochLogMetadataType::MAX:
      break;
  }
  ld_check(false);
  return nullptr;
}

std::unique_ptr<StoreMetadata>
StoreMetadataFactory::create(StoreMetadataType type) {
  switch (type) {
    case StoreMetadataType::CLUSTER_MARKER:
      return std::make_unique<ClusterMarkerMetadata>();
    case StoreMetadataType::REBUILDING_COMPLETE:
      return std::make_unique<RebuildingCompleteMetadata>();
    case StoreMetadataType::REBUILDING_RANGES:
      return std::make_unique<RebuildingRangesMetadata>();
    case StoreMetadataType::UNUSED:
    case StoreMetadataType::MAX:
      break;
  }
  ld_check(false);
  return nullptr;
}

template <>
void EnumMap<LogMetadataType, std::string, LogMetadataType::MAX>::setValues() {
  set(LogMetadataType::LAST_RELEASED, "LAST_RELEASED");
  set(LogMetadataType::TRIM_POINT, "TRIM_POINT");
  set(LogMetadataType::SEAL, "SEAL");
  set(LogMetadataType::LAST_CLEAN, "LAST_CLEAN");
  set(LogMetadataType::REBUILDING_CHECKPOINT, "REBUILDING_CHECKPOINT");
  set(LogMetadataType::REBUILDING_CHECKPOINT, "REBUILDING_CHECKPOINT");
  set(LogMetadataType::SOFT_SEAL, "SOFT_SEAL");
  set(LogMetadataType::LOG_REMOVAL_TIME, "LOG_REMOVAL_TIME");

  static_assert((int)LogMetadataType::MAX == 10,
                "Added a LogMetadataType? Add it here too.");
}

template <>
void EnumMap<PerEpochLogMetadataType,
             std::string,
             PerEpochLogMetadataType::MAX>::setValues() {
  set(PerEpochLogMetadataType::RECOVERY, "RECOVERY");
  set(PerEpochLogMetadataType::MUTABLE, "MUTABLE");

  static_assert((int)PerEpochLogMetadataType::MAX == 2,
                "Added a PerEpochLogMetadataType? Add it here too.");
}

template <>
void EnumMap<StoreMetadataType, std::string, StoreMetadataType::MAX>::
    setValues() {
  set(StoreMetadataType::CLUSTER_MARKER, "CLUSTER_MARKER");
  set(StoreMetadataType::REBUILDING_COMPLETE, "REBUILDING_COMPLETE");
  set(StoreMetadataType::REBUILDING_RANGES, "REBUILDING_RANGES");

  static_assert(
      (int)StoreMetadataType::MAX == 4 && (int)StoreMetadataType::UNUSED == 2,
      "Added a StoreMetadataType? Add it here too.");
}

}} // namespace facebook::logdevice
