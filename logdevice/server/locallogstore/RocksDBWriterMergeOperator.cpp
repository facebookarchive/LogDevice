/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"

#include <chrono>
#include <vector>

#include <folly/Likely.h>
#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/small_vector.h>
#include <rocksdb/env.h>

#include "logdevice/common/Digest.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"

namespace facebook { namespace logdevice {

/* static */ constexpr char RocksDBWriterMergeOperator::DATA_MERGE_HEADER;

using RocksDBKeyFormat::CopySetIndexKey;
using RocksDBKeyFormat::DataKey;
using RocksDBKeyFormat::PerEpochLogMetaKey;

bool RocksDBWriterMergeOperator::PartialMergeMulti(
    const rocksdb::Slice& key,
    const std::deque<rocksdb::Slice>& operand_list,
    std::string* new_value,
    rocksdb::Logger*) const {
  ld_check(new_value != nullptr);
  return AnyWayMerge(false, key, nullptr, operand_list, *new_value, nullptr);
}

bool RocksDBWriterMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  ld_check(merge_out != nullptr);
  return AnyWayMerge(true,
                     merge_in.key,
                     merge_in.existing_value,
                     merge_in.operand_list,
                     merge_out->new_value,
                     &merge_out->existing_operand);
}

namespace {

// Flags that should be ORed together from all merge operands.
const LocalLogStoreRecordFormat::flags_t CUMULATIVE_RECORD_FLAGS =
    LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_REBUILDING |
    LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;

// Convert rocksdb::Slice to logdevice::Slice.
static inline Slice toLdSlice(const rocksdb::Slice& rocksDbSlice) {
  return Slice(rocksDbSlice.data(), rocksDbSlice.size());
}

// Convert logdevice::Slice to rocksdb::Slice.
static inline rocksdb::Slice toRocksDbSlice(const Slice& ldSlice) {
  return rocksdb::Slice(static_cast<const char*>(ldSlice.data), ldSlice.size);
}

// Just like rocksdb::MergeOperator::MergeOperationOutput, but existing_operand
// is optional.
// Helper struct for the output of a data record merge.  This is produced by
// handleDataMerge and may be used both for a FullMergeV2 (in which case the
// output is the value of a row) or a PartialMergeMulti (in which case the
// output is another merge operand).  In practice these only differ in that
// the merge operand has a 'd' byte prefixed (to allow different merge operands
// in the future if needed).
struct DataMergeOutput {
  DataMergeOutput(std::string& new_value, rocksdb::Slice* existing_operand)
      : new_value(new_value), existing_operand(existing_operand) {}

  // Output value to be returned to RocksDB (unless existing_operand is used).
  std::string& new_value;

  // If not nullptr, and the merge result is one of the existing operands (or
  // existing_value), we assign to this slice the operand (or existing_value).
  // Will be a valid pointer if the call came through FullMergeV2(), and nullptr
  // if the call came through PartialMergeMulti().
  rocksdb::Slice* const existing_operand;

  // To be called if a merge...() function produces just a single output slice,
  // and that output slice already points into memory owned by RocksDB
  // (an existing operand). Will use existing_operand if possible, performs deep
  // copy into new_value otherwise.
  void setExistingOperand(const Slice& slice);
};

inline void DataMergeOutput::setExistingOperand(const Slice& slice) {
  ld_check(existing_operand == nullptr || existing_operand->data() == nullptr);
  if (existing_operand != nullptr && new_value.empty()) {
    *existing_operand = toRocksDbSlice(slice);
  } else {
    new_value.append(static_cast<const char*>(slice.data), slice.size);
  }
}

template <typename OperandList>
bool handleDataRecord(const rocksdb::Slice& key,
                      const rocksdb::Slice* existing_value,
                      const OperandList& operand_list,
                      shard_index_t this_shard,
                      DataMergeOutput& out);

template <typename OperandList>
bool handleCopySetIndexEntry(const rocksdb::Slice& key,
                             const rocksdb::Slice* existing_value,
                             const OperandList& operand_list,
                             shard_index_t this_shard,
                             DataMergeOutput& out);

template <typename OperandList>
bool handleMutablePerEpochLogMetadata(const rocksdb::Slice& key,
                                      const rocksdb::Slice* existing_value,
                                      const OperandList& operand_list,
                                      DataMergeOutput& out);
} // anonymous namespace

template <typename OperandList>
bool RocksDBWriterMergeOperator::AnyWayMerge(
    bool full_merge,
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const OperandList& operands,
    std::string& new_value,
    rocksdb::Slice* existing_operand) const {
  if (key.empty()) {
    ld_error("invoked on empty key");
    return false;
  }

  new_value.clear();
  ld_check(existing_operand == nullptr || existing_operand->data() == nullptr);
  DataMergeOutput out(new_value, existing_operand);

  bool rv = false;
  switch (key[0]) {
    case DataKey::HEADER:
      if (!full_merge) {
        // Prepend the 'd' header byte that identifies this class of merge
        // operands
        new_value.assign(1, DATA_MERGE_HEADER);
      }
      rv = handleDataRecord(key, existing_value, operands, thisShard_, out);
      break;
    case CopySetIndexKey::HEADER:
      rv = handleCopySetIndexEntry(
          key, existing_value, operands, thisShard_, out);
      break;
    case PerEpochLogMetaKey::HEADER_MUTABLE:
      rv = handleMutablePerEpochLogMetadata(key, existing_value, operands, out);
      break;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "invoked on unsupported key %s",
                      key.ToString(true).c_str());
      return false;
  }
  if (rv) {
    if (existing_operand && existing_operand->data() != nullptr) {
      ld_spew("key = %s, existing_operand = %s",
              key.ToString(true).c_str(),
              existing_operand->ToString(true).c_str());
    } else {
      ld_spew("key = %s, new_value = %s",
              key.ToString(true).c_str(),
              rocksdb::Slice(new_value).ToString(true).c_str());
    }
  }
  return rv;
}

namespace {
// Descriptor which is compared to decide which copyset to keep.
struct Precedence {
  LocalLogStoreRecordFormat::flags_t flags = 0;
  uint32_t wave = 1;
  int32_t index = -1;

  Precedence() {}
  Precedence(LocalLogStoreRecordFormat::flags_t flags,
             uint32_t wave,
             int32_t index)
      : flags(flags), wave(wave), index(index) {}

  bool empty() {
    ld_check(index >= -1);
    return index <= -1;
  }

  bool operator<(const Precedence& rhs) const {
    // use the same precedence rules by Digest in epoch recovery.
    // In addtion, for two records that have the same FLAG_WRITTEN_BY_RECOVERY
    // flag and the same seal_epoch/wave number, the one with higher index
    // (more recently written) has the higher precedence

    auto get = [](const Precedence& p) {
      return std::make_tuple(
          Digest::RecordMetadata{
              p.flags & LocalLogStoreRecordFormat::FLAG_MASK, p.wave},
          p.index);
    };

    return get(*this) < get(rhs);
  }
};

void combine(const Slice&,
             const Slice&,
             LocalLogStoreRecordFormat::flags_t,
             uint32_t,
             shard_index_t this_shard,
             copyset_size_t,
             DataMergeOutput&);

// The input to this function is an (optional) existing RocksDB value and any
// number of merge operands.  Most common situations are:
// - No existing value, one merge operand.  The storage node received a single
//   STORE; the merge operand will have the payload.
// - No existing value, multiple merge operands some of which have the payload.
//   The node received multiple STOREs, different waves for the same record.
//   The first few of these will have the payload, subsequent ones might just
//   amend the copyset.
// - No existing value, multiple merge operands none with the payload.  We are
//   probably performing a partial merge of later waves that don't have the
//   payload.
//
// Despite these being the common situations, the function is written to
// handle more complex scenarios that may occur only in theory.  The
// implementation looks for the payload in any of the waves, and picks up the
// copyset in the latest wave.
template <typename OperandList>
bool handleDataRecord(const rocksdb::Slice& key,
                      const rocksdb::Slice* existing_value,
                      const OperandList& operand_list,
                      shard_index_t this_shard,
                      DataMergeOutput& out) {
  struct Pick {
    // This is the entire slice (as can be passed to
    // LocalLogStoreRecordFormat::parse)
    Slice slice{nullptr, 0};
    Precedence precedence;
    copyset_size_t copyset_size;
  };
  // These will contain the latest slices found with and without the payload,
  // respectively.
  Pick with_payload;
  Pick amend;
  int32_t next_index = 0;
  // Flags that shoud get bitwise ORed across all applied amends.
  LocalLogStoreRecordFormat::flags_t cumulative_flags = 0;

  // Helper function used both for *existing_value and merge operands.
  // Updates `with_payload' and `amend'.
  auto process = [&](const Slice& slice) {
    LocalLogStoreRecordFormat::flags_t flags;
    uint32_t wave;
    copyset_size_t copyset_size;
    int rv = LocalLogStoreRecordFormat::parse(slice,
                                              nullptr,
                                              nullptr,
                                              &flags,
                                              &wave,
                                              &copyset_size,
                                              nullptr,
                                              0,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              -1 /* unused */);
    if (rv == -1) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "LocalLogStoreRecordFormat::parse failed; key: %s",
                      hexdump_buf(key.data(), key.size()).c_str());
      return false;
    }

    // We're modifying `with_payload' or `amend' depending on FLAG_AMEND
    Pick& pick =
        (flags & LocalLogStoreRecordFormat::FLAG_AMEND) ? amend : with_payload;
    Precedence p(flags, wave, next_index++);
    if (pick.precedence < p) {
      pick.slice = slice;
      pick.precedence = p;
      pick.copyset_size = copyset_size;
    }

    // Collect flags.
    cumulative_flags |= flags & CUMULATIVE_RECORD_FLAGS;

    return true;
  };

  if (existing_value != nullptr) {
    if (!process(toLdSlice(*existing_value))) {
      return false;
    }
  }
  for (const rocksdb::Slice& op : operand_list) {
    if (op.size() == 0) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "encountered empty merge operand.  Very suspicious! key: %s",
          hexdump_buf(key.data(), key.size()).c_str());
      return false;
    }
    if (op.data()[0] != RocksDBWriterMergeOperator::DATA_MERGE_HEADER) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "encountered merge operand with unsupported header byte "
                      "0x%02hhx; key: "
                      "%s.",
                      static_cast<uint8_t>(op.data()[0]),
                      hexdump_buf(key.data(), key.size()).c_str());
      return false;
    }
    if (!process(Slice(op.data() + 1, op.size() - 1))) {
      return false;
    }
  }

  // Set the result to one of the existing operands, but make sure that its
  // cumulative flags match cumulative_flags.
  auto use_existing_but_maybe_modify_flags = [&](const Pick& p) {
    if ((p.precedence.flags & CUMULATIVE_RECORD_FLAGS) == cumulative_flags) {
      // Common case: the existing operand has the right flags.
      out.setExistingOperand(p.slice);
      return;
    }

    // Rare case: we need to make a copy just to change flags.
    // This is an annoying thing we have to do to keep the behavior exactly the
    // same as in CSI merging (which doesn't know which values have payloads).
    // Just "combine" the value with itself to reuse combine()'s
    // parsing, formatting and flag-replacing code.
    combine(p.slice,
            p.slice,
            p.precedence.flags | cumulative_flags,
            p.precedence.wave,
            p.copyset_size,
            this_shard,
            out);
  };

  if (amend.precedence < with_payload.precedence) {
    // Common case: latest entry had the payload, often just one
    ld_check(!with_payload.precedence.empty());
    use_existing_but_maybe_modify_flags(with_payload);
    return true;
  }

  if (with_payload.precedence.empty()) {
    // Much less common: we did not find the payload, probably just doing a
    // partial merge of amends
    ld_check(!amend.precedence.empty());
    use_existing_but_maybe_modify_flags(amend);
    return true;
  }

  // The most complicated case.  We found an entry with the payload but also
  // some amends.
  ld_check(!with_payload.precedence.empty());
  ld_check(!amend.precedence.empty());
  ld_check(with_payload.precedence < amend.precedence);

  combine(with_payload.slice,
          amend.slice,
          amend.precedence.flags | cumulative_flags,
          amend.precedence.wave,
          amend.copyset_size,
          this_shard,
          out);
  return true;
}

template <typename OperandList>
bool handleCopySetIndexEntry(const rocksdb::Slice& key,
                             const rocksdb::Slice* existing_value,
                             const OperandList& operand_list,
                             shard_index_t this_shard,
                             DataMergeOutput& out) {
  if (!CopySetIndexKey::valid(key.data(), key.size())) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "invalid copyset index key %s",
                    key.ToString(true).c_str());
  }
  // TODO (t9002309): block entry support
  if (CopySetIndexKey::isBlockEntry(key.data())) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Encountered block entry key %s. Block entries are not "
                    "supported by this version of logdeviced.",
                    key.ToString(true).c_str());
  }

  int32_t next_index = 0;
  Slice result_value{nullptr, 0};
  Precedence max_precedence;
  LocalLogStoreRecordFormat::flags_t cumulative_record_flags = 0;

  // Helper function used both for *existing_value and merge operands.
  auto process = [&](const rocksdb::Slice& rocks_value) {
    const auto value = toLdSlice(rocks_value);
    if (!value.data || !value.size) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "encountered empty copyset index entry at key %s",
                      key.ToString(true).c_str());
      return false;
    }
    std::vector<ShardID> copyset;
    uint32_t wave;
    LocalLogStoreRecordFormat::csi_flags_t csi_flags = 0;

    if (!LocalLogStoreRecordFormat::parseCopySetIndexSingleEntry(
            value, &copyset, &wave, &csi_flags, this_shard)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Unparseable copyset index entry at key %s.",
                      key.ToString(true).c_str());
      return false;
    }
    if (copyset.empty()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Copyset index entry with no copyset at key %s.",
                      key.ToString(true).c_str());
      return false;
    }

    LocalLogStoreRecordFormat::flags_t record_flags =
        LocalLogStoreRecordFormat::copySetIndexFlagsToRecordFlags(csi_flags);
    cumulative_record_flags |= record_flags & CUMULATIVE_RECORD_FLAGS;
    Precedence p(record_flags, wave, next_index++);
    if (max_precedence < p) {
      result_value = value;
      max_precedence = p;
    }
    return true;
  };

  if (existing_value && !process(*existing_value)) {
    return false;
  }

  for (const rocksdb::Slice& op : operand_list) {
    if (!process(op)) {
      return false;
    }
  }

  // Some flags need to be bitwise ORed from all operands, while other flags
  // are just taken from the highest-precedence operand.
  if ((max_precedence.flags & CUMULATIVE_RECORD_FLAGS) ==
      cumulative_record_flags) {
    // Common case: the highest-precedence operand already has all needed flags.
    out.setExistingOperand(result_value);
    return true;
  }

  // Annoying rare case: we need to copy the operand and change some flags.
  // Deserialize, bitwise OR the flags, serialize.

  std::vector<ShardID> copyset;
  uint32_t wave;
  LocalLogStoreRecordFormat::csi_flags_t csi_flags = 0;
  bool rv = LocalLogStoreRecordFormat::parseCopySetIndexSingleEntry(
      result_value, &copyset, &wave, &csi_flags, this_shard);
  ld_check(rv); // parsing succeeded before
  ld_check(!copyset.empty());
  // Assume that cumulative record flags 1:1 correspond to CSI flags.
  csi_flags |=
      LocalLogStoreRecordFormat::formCopySetIndexFlags(cumulative_record_flags);
  LocalLogStoreRecordFormat::formCopySetIndexEntry(wave,
                                                   &copyset[0],
                                                   copyset.size(),
                                                   LSN_INVALID,
                                                   csi_flags,
                                                   &out.new_value);
  ld_check(!out.new_value.empty());
  return true;
}

void combine(const Slice& with_payload_slice,
             const Slice& amend_slice,
             LocalLogStoreRecordFormat::flags_t amend_flags,
             uint32_t amend_wave,
             copyset_size_t copyset_size,
             shard_index_t this_shard,
             DataMergeOutput& out) {
  // Taking most of the header from `with_payload_slice' ...
  std::chrono::milliseconds timestamp;
  esn_t last_known_good;
  LocalLogStoreRecordFormat::flags_t flags;
  Payload payload;
  OffsetMap offsets_within_epoch;
  OffsetMap amend_offsets_within_epoch;
  std::map<KeyType, std::string> optional_keys;
  {
    int rv = LocalLogStoreRecordFormat::parse(with_payload_slice,
                                              &timestamp,
                                              &last_known_good,
                                              &flags,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              0,
                                              &offsets_within_epoch,
                                              &optional_keys,
                                              &payload,
                                              -1 /* unused */);
    ld_check(rv == 0);
  }

  // copyset from `amend_slice' and offset_with_epoch are possibly things
  // to amend
  copyset_custsz_t<8> copyset(copyset_size);
  {
    int rv = LocalLogStoreRecordFormat::parse(amend_slice,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              copyset.data(),
                                              copyset.size(),
                                              &amend_offsets_within_epoch,
                                              nullptr,
                                              nullptr,
                                              this_shard);
    ld_check(rv == 0);
  }

  // Some flags, once set, are sticky. An amend can set them, but not clear
  // them.
  flags |= amend_flags & CUMULATIVE_RECORD_FLAGS;

  // Drained records may be replaced during a subsequent rebuilding event
  // with a copy that should be visible. So the amend's flags override any
  // previous value.
  flags &= ~LocalLogStoreRecordFormat::FLAG_DRAINED;
  flags |= amend_flags & LocalLogStoreRecordFormat::FLAG_DRAINED;

  // For bridge records, the amend's value overrides any previous value.
  flags &= ~LocalLogStoreRecordFormat::FLAG_BRIDGE;
  flags |= amend_flags & LocalLogStoreRecordFormat::FLAG_BRIDGE;

  if (amend_flags & LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH) {
    flags |= LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH;
    if (amend_flags & LocalLogStoreRecordFormat::FLAG_OFFSET_MAP) {
      flags |= LocalLogStoreRecordFormat::FLAG_OFFSET_MAP;
    }
    if (amend_offsets_within_epoch.isValid()) {
      offsets_within_epoch = std::move(amend_offsets_within_epoch);
    }
  }

  // Reserve appropriate amount of memory for new_value.
  folly::Range<const ShardID*> copyset_range(copyset.begin(), copyset.end());
  Slice optional_keys_slice = Slice();
  std::string optional_keys_string;
  if (!optional_keys.empty()) {
    LocalLogStoreRecordFormat::serializeOptionalKeys(
        &optional_keys_string, optional_keys);
    optional_keys_slice = Slice::fromString(optional_keys_string);
  }
  size_t header_size_est = LocalLogStoreRecordFormat::recordHeaderSizeEstimate(
      flags, copyset_range.size(), optional_keys_slice, offsets_within_epoch);
  size_t new_value_reserved =
      out.new_value.size() + header_size_est + payload.size();

  out.new_value.reserve(new_value_reserved);
  // Rebuild header with new copyset
  LocalLogStoreRecordFormat::formRecordHeaderBufAppend(
      timestamp.count(),
      last_known_good,
      flags,
      amend_wave,
      copyset_range,
      std::move(offsets_within_epoch),
      optional_keys_slice,
      &out.new_value);

  // NOTE: Here we assume the value is the concatenation of the header and
  // data blobs.  This is how RocksDBWriter writes it; make sure to keep in
  // sync.
  out.new_value.append(payload.toString());

  // Make sure we had reserved enough memory.
  ld_check(out.new_value.size() <= new_value_reserved);
}

template <typename OperandList>
bool handleMutablePerEpochLogMetadata(const rocksdb::Slice& /*key*/,
                                      const rocksdb::Slice* existing_value,
                                      const OperandList& operand_list,
                                      DataMergeOutput& out) {
  // NOTE: This type of metadata is normally merged by
  //  PartitionedRocksDBStore::MetadataMergeOperator, but we will land here if
  //  using a RocksDBLocalLogStore. No problem. This code is actually faster
  //  than MetadataMergeOperator, because it is a vectorized/batching
  //  multi-merge that calls only inlined functions. The compiler should
  //  optimize this nicely.

  // Deserialize first metadata record, either existing_value or the first
  // element in operator_list.
  MutablePerEpochLogMetadata acc;
  auto operand_it(operand_list.begin());
  if (existing_value != nullptr) {
    if (UNLIKELY(!!acc.deserialize(toLdSlice(*existing_value)))) {
      return false;
    }
  } else {
    ld_check(operand_it != operand_list.end());
    if (UNLIKELY(!!acc.deserialize(toLdSlice(*operand_it)))) {
      return false;
    }
    ++operand_it;
  }
  ld_check(acc.valid());

  // Merge remaining elements in operator_list.
  while (operand_it != operand_list.end()) {
    if (UNLIKELY(!MutablePerEpochLogMetadata::valid(toLdSlice(*operand_it)))) {
      return false;
    }
    acc.merge(toLdSlice(*operand_it));
    ld_check(acc.valid());
    ++operand_it;
  }

  // Write merged metadata record to new_value.
  const Slice acc_slice(acc.serialize());
  out.new_value.assign(
      static_cast<const char*>(acc_slice.data), acc_slice.size);
  return true;
}

} // anonymous namespace

}} // namespace facebook::logdevice
