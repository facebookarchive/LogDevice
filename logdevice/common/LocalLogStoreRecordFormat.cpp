/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRIu64 etc
#include "logdevice/common/LocalLogStoreRecordFormat.h"

#include <folly/Range.h>
#include <folly/Varint.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/Checksum.h"
#include "logdevice/common/CopySet.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice { namespace LocalLogStoreRecordFormat {

namespace {
/**
 * Returns minimum possible size for log store blob.
 */
constexpr size_t minLogStoreBlobSize() {
  return sizeof(uint64_t) + sizeof(esn_t) +
      sizeof(uint8_t) + // flags are at least 1 byte
      sizeof(copyset_size_t);
}

uint32_t getRecordWaveOrRecoveryEpoch(const STORE_Header& header,
                                      const STORE_Extra& extra) {
  return (((header.flags & STORE_Header::RECOVERY ||
            header.flags & STORE_Header::WRITTEN_BY_RECOVERY)) &&
          (extra.recovery_epoch != EPOCH_INVALID))
      ? extra.recovery_epoch.val_
      : header.wave;
}

} // namespace

#define APPEND_TO_STRING(pstr, thing)                                     \
  do {                                                                    \
    pstr->append(reinterpret_cast<const char*>(&(thing)), sizeof(thing)); \
  } while (0)

size_t recordHeaderSizeEstimate(flags_t flags,
                                copyset_size_t copyset_size,
                                const Slice& optional_keys,
                                const OffsetMap& offsets_within_epoch) {
  size_t ret = sizeof(int64_t) + sizeof(esn_t) + folly::kMaxVarintLength32 +
      sizeof(uint32_t) +
      ((flags & FLAG_OFFSET_WITHIN_EPOCH && !(flags & FLAG_OFFSET_MAP))
           ? sizeof(uint64_t)
           : 0) +
      ((flags & FLAG_OFFSET_MAP && flags & FLAG_OFFSET_WITHIN_EPOCH)
           ? offsets_within_epoch.sizeInLinearBuffer()
           : 0);

  if ((flags & FLAG_CUSTOM_KEY) || (flags & FLAG_OPTIONAL_KEYS)) {
    ret += sizeof(uint16_t) + optional_keys.size;
  }

  ret += sizeof(copyset_size_t);
  if (flags & FLAG_SHARD_ID) {
    ret += sizeof(ShardID) * copyset_size;
  } else {
    ret += sizeof(node_index_t) * copyset_size;
  }

  return ret;
}

Slice formRecordHeaderBufAppend(int64_t timestamp,
                                esn_t last_known_good,
                                flags_t flags,
                                uint32_t wave_or_recovery_epoch,
                                const folly::Range<const ShardID*>& copyset,
                                const OffsetMap& offsets_within_epoch,
                                const Slice& optional_keys,
                                std::string* buf) {
  ld_check(buf != nullptr);
  size_t buf_size_prev = buf->size();

  APPEND_TO_STRING(buf, timestamp);
  APPEND_TO_STRING(buf, last_known_good);

  uint8_t varint_buf[folly::kMaxVarintLength32];
  size_t n = folly::encodeVarint(flags, varint_buf);
  buf->append((const char*)varint_buf, n);

  APPEND_TO_STRING(buf, wave_or_recovery_epoch);
  copyset_size_t copyset_size = copyset.size();
  APPEND_TO_STRING(buf, copyset_size);
  if (flags & FLAG_SHARD_ID) {
    for (ShardID shard : copyset) {
      APPEND_TO_STRING(buf, shard);
    }
  } else {
    for (ShardID shard : copyset) {
      const node_index_t nid = shard.node();
      APPEND_TO_STRING(buf, nid);
    }
  }

  if (flags & FLAG_OFFSET_WITHIN_EPOCH && !(flags & FLAG_OFFSET_MAP)) {
    uint64_t offset_within_epoch = offsets_within_epoch.getCounter(BYTE_OFFSET);
    APPEND_TO_STRING(buf, offset_within_epoch);
  }

  if ((flags & FLAG_CUSTOM_KEY) || (flags & FLAG_OPTIONAL_KEYS)) {
    ld_check(optional_keys.size <= UINT16_MAX);
    uint16_t length = optional_keys.size;
    APPEND_TO_STRING(buf, length);
    buf->append((const char*)optional_keys.data, optional_keys.size);
  }

  if (flags & FLAG_OFFSET_WITHIN_EPOCH && flags & FLAG_OFFSET_MAP) {
    ssize_t offsets_map_size = offsets_within_epoch.sizeInLinearBuffer();
    size_t buf_size_before_offset_map = buf->size();
    buf->resize(buf->size() + offsets_map_size, '0');
    int rv = offsets_within_epoch.serialize(
        (char*)buf->data() + buf_size_before_offset_map, offsets_map_size);
    ld_check(rv != -1);
  }

  return Slice(buf->data() + buf_size_prev, buf->size() - buf_size_prev);
}

Slice formRecordHeader(int64_t timestamp,
                       esn_t last_known_good,
                       flags_t flags,
                       uint32_t wave_or_recovery_epoch,
                       const folly::Range<const ShardID*>& copyset,
                       const OffsetMap& offsets_within_epoch,
                       const std::map<KeyType, std::string>& optional_keys,
                       std::string* buf) {
  std::string optional_keys_string;
  serializeOptionalKeys(&optional_keys_string, optional_keys);
  Slice optional_keys_slice = Slice::fromString(optional_keys_string);

  ld_check(buf != nullptr);
  buf->clear();
  buf->reserve(recordHeaderSizeEstimate(
      flags, copyset.size(), optional_keys_slice, offsets_within_epoch));
  return formRecordHeaderBufAppend(timestamp,
                                   last_known_good,
                                   flags,
                                   wave_or_recovery_epoch,
                                   copyset,
                                   offsets_within_epoch,
                                   optional_keys_slice,
                                   buf);
}

Slice formRecordHeader(const STORE_Header& store_header,
                       const StoreChainLink* copyset,
                       std::string* buf,
                       const bool shard_id_in_copyset,
                       const std::map<KeyType, std::string>& optional_keys,
                       const STORE_Extra& store_extra) {
  flags_t flags = store_header.flags & FLAG_MASK;
  uint32_t wave_or_recovery_epoch_to_store =
      getRecordWaveOrRecoveryEpoch(store_header, store_extra);
  OffsetMap offsets_within_epoch;

  if (shard_id_in_copyset) {
    flags |= FLAG_SHARD_ID;
  }
  if (store_header.flags & STORE_Header::AMEND) {
    flags |= FLAG_AMEND;
  }
  if (store_header.flags & STORE_Header::OFFSET_WITHIN_EPOCH) {
    flags |= FLAG_OFFSET_WITHIN_EPOCH;
    offsets_within_epoch = store_extra.offsets_within_epoch;
  }

  if (store_header.flags & STORE_Header::OFFSET_MAP) {
    flags |= FLAG_OFFSET_MAP;
  }

  // TODO 11866467: deprecate STORE_Header::RECOVERY
  if (store_header.flags & STORE_Header::RECOVERY ||
      store_header.flags & STORE_Header::WRITTEN_BY_RECOVERY) {
    flags |= FLAG_WRITTEN_BY_RECOVERY;
  }

  if ((store_header.flags & (STORE_Header::REBUILDING | STORE_Header::AMEND)) ==
      STORE_Header::REBUILDING) {
    // A full record is written by rebuilding and thus can be excluded from
    // append only mini-rebuilding.
    flags |= FLAG_WRITTEN_BY_REBUILDING;
  }

  if (!optional_keys.empty()) {
    flags |= FLAG_CUSTOM_KEY;
    flags |= FLAG_OPTIONAL_KEYS;
  }

  copyset_custsz_t<8> copyset_indexes;
  for (copyset_size_t i = 0; i < store_header.copyset_size; ++i) {
    copyset_indexes.push_back(copyset[i].destination);
  }
  return formRecordHeader(store_header.timestamp,
                          store_header.last_known_good,
                          flags,
                          wave_or_recovery_epoch_to_store,
                          folly::Range<const ShardID*>(
                              copyset_indexes.begin(), copyset_indexes.end()),
                          offsets_within_epoch,
                          optional_keys,
                          buf);
}

csi_flags_t formCopySetIndexFlags(const STORE_Header& store_header,
                                  bool shard_id_in_copyset) {
  dd_assert((store_header.flags & STORE_Header::DRAINED) == 0,
            "A STORE with DRAINED flag should never be issued.");
  csi_flags_t ret = 0;
  ret |= (store_header.flags & STORE_Header::HOLE) ? CSI_FLAG_HOLE : 0;
  ret |= shard_id_in_copyset ? CSI_FLAG_SHARD_ID : 0;
  ret |= ((store_header.flags &
           (STORE_Header::REBUILDING | STORE_Header::AMEND)) ==
          STORE_Header::REBUILDING)
      ? CSI_FLAG_WRITTEN_BY_REBUILDING
      : 0;
  ret |= (store_header.flags &
          (STORE_Header::WRITTEN_BY_RECOVERY | STORE_Header::RECOVERY))
      ? CSI_FLAG_WRITTEN_BY_RECOVERY
      : 0;
  return ret;
}

csi_flags_t formCopySetIndexFlags(const flags_t flags) {
#define CONV(x) ((flags & FLAG_##x) ? CSI_FLAG_##x : 0)
  return CONV(HOLE) | CONV(DRAINED) | CONV(SHARD_ID) |
      CONV(WRITTEN_BY_REBUILDING) | CONV(WRITTEN_BY_RECOVERY);
#undef CONV
}

flags_t copySetIndexFlagsToRecordFlags(csi_flags_t flags) {
#define CONV(x) ((flags & CSI_FLAG_##x) ? FLAG_##x : 0)
  return CONV(HOLE) | CONV(DRAINED) | CONV(SHARD_ID) |
      CONV(WRITTEN_BY_REBUILDING) | CONV(WRITTEN_BY_RECOVERY);
#undef CONV
}

Slice formCopySetIndexEntry(uint32_t wave,
                            const ShardID* copyset,
                            const copyset_size_t copyset_size,
                            const folly::Optional<lsn_t>& block_starting_lsn,
                            const csi_flags_t flags,
                            std::string* buf) {
  buf->clear();

  if (!block_starting_lsn.hasValue()) {
    // not writing copyset index entry
    return Slice(nullptr, 0);
  }

  // Writing single entry
  ld_check(block_starting_lsn.hasValue());
  ld_check_eq(block_starting_lsn.value(), LSN_INVALID);
  // TODO: block entry support (t9002309), will use the block starting lsn then
  size_t nbytes = sizeof(wave) + sizeof(flags) + sizeof(copyset_size) +
      copyset_size *
          (flags & CSI_FLAG_SHARD_ID ? sizeof(ShardID) : sizeof(node_index_t));
  buf->reserve(nbytes);

  APPEND_TO_STRING(buf, wave);
  APPEND_TO_STRING(buf, flags);
  APPEND_TO_STRING(buf, copyset_size);
  if (flags & CSI_FLAG_SHARD_ID) {
    for (size_t i = 0; i < copyset_size; ++i) {
      APPEND_TO_STRING(buf, copyset[i]);
    }
  } else {
    for (size_t i = 0; i < copyset_size; ++i) {
      const node_index_t nid = copyset[i].node();
      APPEND_TO_STRING(buf, nid);
    }
  }

  // Check that we'd reserved the right number of bytes
  ld_check(buf->size() == nbytes);

  return Slice(buf->data(), buf->size());
}

#undef APPEND_TO_STRING

Slice formCopySetIndexEntry(const STORE_Header& store_header,
                            const STORE_Extra& store_extra,
                            const StoreChainLink* chainlink,
                            const folly::Optional<lsn_t>& block_starting_lsn,
                            bool shard_id_in_copyset,
                            std::string* buf) {
  uint32_t wave = getRecordWaveOrRecoveryEpoch(store_header, store_extra);
  csi_flags_t flags = formCopySetIndexFlags(store_header, shard_id_in_copyset);

  copyset_custsz_t<8> copyset;
  for (int i = 0; i < store_header.copyset_size; ++i) {
    copyset.push_back(chainlink[i].destination);
  }
  return formCopySetIndexEntry(wave,
                               copyset.data(),
                               store_header.copyset_size,
                               block_starting_lsn,
                               flags,
                               buf);
}

#define READ_FROM_SLICE(slice, offset, thing)                           \
  do {                                                                  \
    if (!dd_assert(offset + sizeof(thing) <= slice.size,                \
                   "Truncated copyset index entry: trying to read %zu " \
                   "bytes, %zu bytes available. Full slice: %s",        \
                   offset + sizeof(thing),                              \
                   slice.size,                                          \
                   hexdump_buf(slice, 200).c_str())) {                  \
      err = E::MALFORMED_RECORD;                                        \
      return false;                                                     \
    }                                                                   \
    memcpy(&(thing),                                                    \
           static_cast<const char*>(slice.data) + offset,               \
           sizeof(thing));                                              \
    offset += sizeof(thing);                                            \
  } while (0)

namespace {
int parseOffsetWithinEpochValue(uint64_t* offset_within_epoch_out,
                                const uint8_t** ptr,
                                const uint8_t* end) {
  if (*ptr + sizeof(*offset_within_epoch_out) > end) {
    ld_error(
        "Invalid record: past end while parsing offset within epoch value");
    err = E::MALFORMED_RECORD;
    return -1;
  }

  memcpy(offset_within_epoch_out, *ptr, sizeof(*offset_within_epoch_out));

  *ptr += sizeof(*offset_within_epoch_out);
  return 0;
}

int parseFlagsValue(flags_t& flags, const uint8_t** ptr, const uint8_t* end) {
  try {
    folly::ByteRange range(*ptr, end);
    flags = folly::decodeVarint(range);
    *ptr = range.begin();
  } catch (...) {
    ld_error("Invalid record: failed to decode flag varint");
    err = E::MALFORMED_RECORD;
    return -1;
  }
  return 0;
}
} // namespace

// TODO (t9002309): block entry support
bool parseCopySetIndexSingleEntry(const Slice& cs_dir_slice,
                                  std::vector<ShardID>* copyset,
                                  uint32_t* wave,
                                  csi_flags_t* flags,
                                  shard_index_t this_shard) {
  if (copyset) {
    copyset->clear();
  }
  if (wave) {
    *wave = 0;
  }
  if (flags) {
    *flags = 0;
  }

  if (!dd_assert(cs_dir_slice.size != 0, "Unexpected zero length CSI Entry")) {
    return false;
  }

  size_t offset = 0;
  csi_flags_t _flags;

  if (wave) {
    READ_FROM_SLICE(cs_dir_slice, offset, *wave);
  } else {
    offset += sizeof(*wave);
  }
  READ_FROM_SLICE(cs_dir_slice, offset, _flags);
  if (flags) {
    *flags = _flags;
  }
  copyset_size_t copyset_size;
  READ_FROM_SLICE(cs_dir_slice, offset, copyset_size);
  // reading the copyset into the target vector
  if (copyset) {
    copyset->resize(copyset_size);
    if (_flags & CSI_FLAG_SHARD_ID) {
      for (size_t i = 0; i < copyset_size; ++i) {
        READ_FROM_SLICE(cs_dir_slice, offset, (*copyset)[i]);
      }
    } else {
      ld_check(this_shard >= 0);
      for (size_t i = 0; i < copyset_size; ++i) {
        node_index_t nid;
        READ_FROM_SLICE(cs_dir_slice, offset, nid);
        (*copyset)[i] = ShardID(nid, this_shard);
      }
    }
  } else {
    // not reading copysets into the vector - skipping forward
    offset +=
        (_flags & CSI_FLAG_SHARD_ID ? sizeof(ShardID) : sizeof(node_index_t)) *
        copyset_size;
  }

  // Check that we'd calculated the right number of bytes
  return dd_assert(offset <= cs_dir_slice.size,
                   "Unexpected CSI Entry size. Expected at least %s bytes. "
                   "Record is %s bytes: %s",
                   toString(offset).c_str(),
                   toString(cs_dir_slice.size).c_str(),
                   hexdump_buf(cs_dir_slice, 200).c_str());
}

#undef READ_FROM_SLICE

int parse(const Slice& log_store_blob,
          std::chrono::milliseconds* timestamp_out,
          esn_t* last_known_good_out,
          flags_t* flags_out,
          uint32_t* wave_or_recovery_epoch_out,
          copyset_size_t* copyset_size_out,
          ShardID* copyset_arr_out,
          size_t copyset_arr_out_size,
          OffsetMap* offsets_within_epoch_out,
          std::map<KeyType, std::string>* optional_keys,
          Payload* payload_out,
          shard_index_t this_shard) {
  const uint8_t *const start = reinterpret_cast<const uint8_t*>(
                           log_store_blob.data),
                       *const end = start + log_store_blob.size, *ptr = start;

  // Throughout this function, remember that we need to fully verify record
  // format regardless of which options are nullptr.

  const size_t MINIMUM_SIZE = minLogStoreBlobSize();
  if (log_store_blob.size < MINIMUM_SIZE) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Invalid record: too small (%zu bytes); blob: %s",
                    log_store_blob.size,
                    hexdump_buf(log_store_blob, 700).c_str());
    err = E::MALFORMED_RECORD;
    return -1;
  }

  //
  // Timestamp
  //
  if (timestamp_out != nullptr) {
    uint64_t raw;
    memcpy(&raw, ptr, sizeof(raw));
    *timestamp_out = std::chrono::milliseconds(raw);
  }
  ptr += sizeof(uint64_t);

  //
  // Last known good
  //
  if (last_known_good_out != nullptr) {
    memcpy(last_known_good_out, ptr, sizeof(*last_known_good_out));
  }
  ptr += sizeof(esn_t);

  //
  // Flags
  //
  flags_t flags;
  int rv = parseFlagsValue(flags, &ptr, end);
  if (rv != 0) {
    return rv;
  }

  if (flags_out != nullptr) {
    *flags_out = flags;
  }

  //
  // Wave number
  //
  if (ptr + sizeof(*wave_or_recovery_epoch_out) > end) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Invalid record: past end while parsing wave; blob: %s",
                    hexdump_buf(log_store_blob, 700).c_str());
    err = E::MALFORMED_RECORD;
    return -1;
  }
  if (wave_or_recovery_epoch_out != nullptr) {
    memcpy(
        wave_or_recovery_epoch_out, ptr, sizeof(*wave_or_recovery_epoch_out));
  }
  ptr += sizeof(*wave_or_recovery_epoch_out);

  //
  // Copyset size
  //
  copyset_size_t copyset_size;
  if (ptr + sizeof(copyset_size) > end) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Invalid record: past end while parsing copyset size; blob: %s",
        hexdump_buf(log_store_blob, 700).c_str());
    err = E::MALFORMED_RECORD;
    return -1;
  }
  memcpy(&copyset_size, ptr, sizeof(copyset_size));
  if (copyset_size < 1 || copyset_size > COPYSET_SIZE_MAX) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Invalid record: copyset size is %" PRIu8 "; blob: %s",
        copyset_size,
        // only print the header part of the blob, for security reasons
        hexdump_buf(log_store_blob, (ptr - start) * 2).c_str());
    err = E::MALFORMED_RECORD;
    return -1;
  }

  if (copyset_size_out != nullptr) {
    *copyset_size_out = copyset_size;
  }
  ptr += sizeof(copyset_size_t);

  //
  // Copyset
  //
  Status status = E::OK;
  if (flags & FLAG_SHARD_ID) {
    if (ptr + copyset_size * sizeof(ShardID) > end) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "Invalid record: past end while parsing copyset; blob: %s",
          hexdump_buf(log_store_blob, 700).c_str());
      err = E::MALFORMED_RECORD;
      return -1;
    }

    if (copyset_arr_out != nullptr) {
      if (copyset_size <= copyset_arr_out_size) {
        memcpy(copyset_arr_out, ptr, copyset_size * sizeof(ShardID));
      } else {
        status = E::NOBUFS;
      }
    }
    ptr += copyset_size * sizeof(ShardID);
  } else {
    if (ptr + copyset_size * sizeof(node_index_t) > end) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "Invalid record: past end while parsing copyset; blob: %s",
          hexdump_buf(log_store_blob, 700).c_str());
      err = E::MALFORMED_RECORD;
      return -1;
    }

    if (copyset_arr_out != nullptr) {
      ld_check(this_shard >= 0);
      if (copyset_size <= copyset_arr_out_size) {
        folly::small_vector<node_index_t, 6> copyset;
        copyset.resize(copyset_size);
        memcpy(&copyset[0], ptr, copyset_size * sizeof(node_index_t));
        for (size_t i = 0; i < copyset_size; ++i) {
          copyset_arr_out[i] = ShardID(copyset[i], this_shard);
        }
      } else {
        status = E::NOBUFS;
      }
    }
    ptr += copyset_size * sizeof(node_index_t);
  }

  //
  // Offset within epoch (if FLAG_OFFSET_WITHIN_EPOCH was set)
  //
  if (flags & FLAG_OFFSET_WITHIN_EPOCH && !(flags & FLAG_OFFSET_MAP)) {
    uint64_t out;
    rv = parseOffsetWithinEpochValue(&out, &ptr, end);
    if (rv != 0) {
      return rv;
    }
    if (offsets_within_epoch_out) {
      OffsetMap o;
      o.setCounter(BYTE_OFFSET, out);
      *offsets_within_epoch_out = std::move(o);
    }
  }

  //
  // User optional keys
  //

  if ((flags & FLAG_CUSTOM_KEY) || (flags & FLAG_OPTIONAL_KEYS)) {
    uint16_t blob_size = 0;
    if (ptr + sizeof(uint16_t) > end) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "Invalid record: past end while parsing optional blob size; blob: %s",
          hexdump_buf(log_store_blob, 700).c_str());
      err = E::MALFORMED_RECORD;
      return -1;
    }
    memcpy(&blob_size, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    const uint8_t* map_head_pos = ptr;
    if (ptr + blob_size > end) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "Invalid record: past end while parsing optional keys blob size: %s",
          hexdump_buf(log_store_blob, 700).c_str());
      err = E::MALFORMED_RECORD;
      return -1;
    }

    // We are deserializing differently for different versions.
    // If FLAG_OPTIONAL_KEYS is set, we deserialize it as a map.
    // Otherwise we use legacy code to deserialize it as a string.
    if (flags & FLAG_OPTIONAL_KEYS) {
      uint16_t optional_keys_size;
      if (ptr + sizeof(uint16_t) > end) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "Invalid record: past end while parsing optional_keys_size: %s",
            hexdump_buf(log_store_blob, 700).c_str());
        err = E::MALFORMED_RECORD;
        return -1;
      }
      memcpy(&optional_keys_size, ptr, sizeof(uint16_t));
      ptr += sizeof(uint16_t);
      for (uint16_t i = 0; i < optional_keys_size; i++) {
        uint8_t key_type;
        uint16_t key_length;
        if (ptr + sizeof(uint8_t) + sizeof(uint16_t) > end) {
          RATELIMIT_ERROR(std::chrono::seconds(10),
                          10,
                          "Invalid record: past end while parsing key type: %s",
                          hexdump_buf(log_store_blob, 700).c_str());
          err = E::MALFORMED_RECORD;
          return -1;
        }
        memcpy(&key_type, ptr, sizeof(uint8_t));
        ptr += sizeof(uint8_t);
        memcpy(&key_length, ptr, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        if (ptr + key_length > end) {
          RATELIMIT_ERROR(
              std::chrono::seconds(10),
              10,
              "Invalid record: past end while parsing key: %s",
              hexdump_buf(log_store_blob, (ptr - start) * 2).c_str());
          err = E::MALFORMED_RECORD;
          return -1;
        }
        if (optional_keys != nullptr) {
          std::string key = std::string(
              reinterpret_cast<const char*>(ptr), (size_t)key_length);
          optional_keys->insert(
              std::make_pair(static_cast<KeyType>(key_type), std::move(key)));
        }
        ptr += key_length;
      }
      if (ptr - map_head_pos != blob_size) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Optional keys map size is not equal to blob size: %s",
                        hexdump_buf(log_store_blob, (ptr - start) * 2).c_str());
        err = E::MALFORMED_RECORD;
        return -1;
      }
    } else {
      ld_check(flags & FLAG_CUSTOM_KEY);
      // We parse it as a string and put it into optional keys map
      if (optional_keys != nullptr) {
        optional_keys->insert(
            std::make_pair(KeyType::FINDKEY,
                           std::string(reinterpret_cast<const char*>(ptr),
                                       (size_t)blob_size)));
      }
      ptr += blob_size;
    }
  }

  //
  // OffsetMap
  //
  if (flags & FLAG_OFFSET_WITHIN_EPOCH && flags & FLAG_OFFSET_MAP) {
    OffsetMap out;
    rv = out.deserialize(Slice(ptr, end - ptr));
    if (rv == -1) {
      return rv;
    }

    if (offsets_within_epoch_out) {
      *offsets_within_epoch_out = std::move(out);
    }
    ptr += rv;
  }

  //
  // User payload
  //

  if (payload_out != nullptr) {
    if ((end - ptr) != 0) {
      *payload_out = Payload(ptr, end - ptr);
    } else {
      *payload_out = Payload();
    }
  }
  if (status == E::OK) {
    return 0;
  } else {
    err = status;
    return -1;
  }
}

int checkWellFormed(Slice blob, Slice payload) {
  Payload parsed_payload_p;
  flags_t flags;
  uint32_t wave;
  int rv = parse(blob,
                 nullptr,
                 nullptr,
                 &flags,
                 &wave,
                 nullptr,
                 nullptr,
                 0,
                 nullptr,
                 nullptr,
                 &parsed_payload_p,
                 -1 /* unused */);
  Slice parsed_payload(parsed_payload_p);
  if (rv != 0) {
    ld_check(err == E::MALFORMED_RECORD);
    return rv;
  }
  if (payload.size == 0) {
    // If payload is in `blob`, move it into `payload`.
    payload = parsed_payload;
    parsed_payload = Slice();
  }
  if (flags & FLAG_WRITTEN_BY_RECOVERY) {
    // TODO 11866467: uncomment this when we actually set the wave as sequencer
    // epoch
    // if (wave == uint32_t(EPOCH_INVALID.val_)) {
    //   ld_error("Invalid record: FLAG_WRITTEN_BY_RECOVERY set but its "
    //             "sequencer epoch is EPOCH_INVALID!");
    //   err = E::MALFORMED_RECORD;
    //   return -1;
    // }
  }

  // TODO 11866467: uncomment this once we finish migration for all use cases
  // if (flags & FLAG_HOLE) {
  //   if ((flags & FLAG_WRITTEN_BY_RECOVERY) == 0) {
  //     ld_error("Invalid record: FLAG_HOLE set but FLAG_WRITTEN_BY_RECOVERY "
  //               "is not!");
  //     err = E::MALFORMED_RECORD;
  //     return -1;
  //   }
  // }

  if (flags & FLAG_AMEND) {
    if (payload.size != 0) {
      ld_error("Invalid record: AMEND flag is set but payload "
               "is not empty (%lu bytes).",
               payload.size);
      err = E::MALFORMED_RECORD;
      return -1;
    }
    return 0;
  }
  if ((bool(flags & FLAG_CHECKSUM) ^ bool(flags & FLAG_CHECKSUM_64BIT) ^ 1) !=
      bool(flags & FLAG_CHECKSUM_PARITY)) {
    ld_error("Invalid record header: checksum parity mismatch.");
    err = E::MALFORMED_RECORD;
    return -1;
  }
  if (flags & FLAG_CHECKSUM) {
    const size_t checksum_size = (flags & FLAG_CHECKSUM_64BIT) ? 8 : 4;

    // Checksum can be either in parsed_payload or at the beginning of payload.
    Slice checksum_slice = parsed_payload.size == 0 ? payload : parsed_payload;
    if (checksum_slice.size < checksum_size) {
      ld_error("Invalid record: expected %lu checksum bytes, got %lu.",
               checksum_size,
               checksum_slice.size);
      err = E::MALFORMED_RECORD;
      return -1;
    }
    if (parsed_payload.size == 0) {
      // If `payload` is prefixed with checksum, unprefix.
      payload.data = (const char*)payload.data + checksum_size;
      payload.size -= checksum_size;
    }
    char buf[8];
    checksum_bytes(
        Slice((const char*)payload.data, payload.size), checksum_size * 8, buf);
    if (memcmp(buf, checksum_slice.data, checksum_size) != 0) {
      uint64_t payload_checksum = 0;
      uint64_t expected_checksum = 0;
      memcpy(&payload_checksum, checksum_slice.data, checksum_size);
      memcpy(&expected_checksum, buf, checksum_size);
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      100,
                      "checksum mismatch: Invalid record. Payload: %s, "
                      "expected checksum: %lu, checksum in payload: %lu",
                      hexdump_buf(payload, 500).c_str(),
                      expected_checksum,
                      payload_checksum);
      err = E::CHECKSUM_MISMATCH;
      return -1;
    }
  }
  return 0;
}

int parseTimestamp(const Slice& log_store_blob,
                   std::chrono::milliseconds* timestamp_out) {
  ld_check(timestamp_out != nullptr);

  if (log_store_blob.size < minLogStoreBlobSize()) {
    ld_error("Invalid record: too small (%zu bytes)", log_store_blob.size);
    err = E::MALFORMED_RECORD;
    return -1;
  }

  uint64_t raw;
  memcpy(&raw, log_store_blob.data, sizeof(raw));
  *timestamp_out = std::chrono::milliseconds(raw);
  return 0;
}

// For a bitmask `mask` with exactly one one-bit, tests that `varint` has that
// bit set to one.
static inline bool testVarInt(const uint8_t* varint, size_t mask) {
  ld_assert(mask != 0 && (mask & (mask - 1)) == 0); // mask is a power of two
  constexpr uint8_t ADDITIONAL_OCTETS_MASK = 1ul << 7;
  const uint8_t* cur_octet = varint;
  for (; mask > 127; mask >>= 7, ++cur_octet) {
    if ((*cur_octet & ADDITIONAL_OCTETS_MASK) == 0) {
      return false;
    }
  }
  return (*cur_octet & mask) != 0;
}

int isWrittenByRecovery(const Slice& log_store_blob,
                        bool* is_written_by_recovery_out) {
  ld_check(is_written_by_recovery_out);
  const size_t offset = sizeof(uint64_t) + sizeof(esn_t);

  // Check that blob is long enough for at least 2 bytes of varint flags.
  static_assert(minLogStoreBlobSize() > offset + 1, "");
  if (log_store_blob.size < minLogStoreBlobSize()) {
    ld_error("Invalid record: too small (%zu bytes)", log_store_blob.size);
    err = E::MALFORMED_RECORD;
    return -1;
  }

  // Check that the flag fits in a 2-byte varint.
  static_assert(FLAG_WRITTEN_BY_RECOVERY < (1 << 14), "");

  auto p = reinterpret_cast<const uint8_t*>(log_store_blob.data) + offset;
  *is_written_by_recovery_out = testVarInt(p, FLAG_WRITTEN_BY_RECOVERY);

  return 0;
}

int parseFlags(const Slice& log_store_blob, flags_t* flags_out) {
  ld_check(flags_out != nullptr);
  return parse(log_store_blob,
               nullptr,
               nullptr,
               flags_out,
               nullptr,
               nullptr,
               nullptr,
               0,
               nullptr,
               nullptr,
               nullptr,
               -1 /* unused */);
}

int getCopysetHash(const Slice& log_store_blob, size_t* hash_out) {
  static thread_local std::array<ShardID, COPYSET_SIZE_MAX> copyset;
  ld_check(hash_out != nullptr);
  copyset_size_t copyset_size;

  // If FLAG_SHARD_ID is not set, retrieve ShardIDs that have the shard_index_t
  // component set to zero. We don't need this value to be accurate since this
  // is just used for hashing.
  shard_index_t this_shard = 0;

  int rv = parse(log_store_blob,
                 nullptr,
                 nullptr,
                 nullptr,
                 nullptr,
                 &copyset_size,
                 &copyset[0],
                 copyset.size(),
                 nullptr,
                 nullptr,
                 nullptr,
                 this_shard);
  if (rv != 0) {
    ld_check(err != E::NOBUFS);
    return rv;
  }
  *hash_out = folly::hash::SpookyHashV2::Hash64(
      &copyset[0], copyset_size * sizeof(ShardID), 0);
  return 0;
}

void serializeOptionalKeys(
    std::string* optional_keys_string,
    const std::map<KeyType, std::string>& optional_keys) {
  uint16_t optional_keys_size = static_cast<uint16_t>(optional_keys.size());
  optional_keys_string->append(
      reinterpret_cast<const char*>(&optional_keys_size), sizeof(uint16_t));
  for (const auto& key_pair : optional_keys) {
    uint8_t key_type = static_cast<uint8_t>(key_pair.first);
    optional_keys_string->append(
        reinterpret_cast<const char*>(&key_type), sizeof(uint8_t));
    uint16_t key_length = key_pair.second.size();
    optional_keys_string->append(
        reinterpret_cast<const char*>(&key_length), sizeof(uint16_t));
    optional_keys_string->append(key_pair.second);
  }
}

std::string flagsToString(flags_t flags) {
  std::string s;

#define FLAG(f)           \
  if (flags & FLAG_##f) { \
    if (!s.empty()) {     \
      s += "|";           \
    }                     \
    s += #f;              \
    flags ^= FLAG_##f;    \
  }

  FLAG(CHECKSUM)
  FLAG(CHECKSUM_64BIT)
  FLAG(CHECKSUM_PARITY)
  FLAG(AMEND)
  FLAG(HOLE)
  FLAG(BUFFERED_WRITER_BLOB)
  FLAG(OFFSET_WITHIN_EPOCH)
  FLAG(CSI_DATA_ONLY)
  FLAG(WRITTEN_BY_RECOVERY)
  FLAG(CUSTOM_KEY)
  FLAG(BRIDGE)
  FLAG(EPOCH_BEGIN)
  FLAG(WRITTEN_BY_REBUILDING)
  FLAG(OPTIONAL_KEYS)
  FLAG(DRAINED)
  FLAG(SHARD_ID)
  FLAG(OFFSET_MAP)

#undef FLAG

  if (flags) {
    if (!s.empty()) {
      s += "|";
    }
    s += std::to_string(flags);
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "LocalLogStoreRecordFormat::flagsToString() was called with some "
        "unexpected flags: %u. Please check the code of flagsToString() to see "
        "if it's missing some newly added flags.",
        flags);
  }

  return s;
}

std::string csiFlagsToString(csi_flags_t flags) {
  std::string s;

#define FLAG(f)               \
  if (flags & CSI_FLAG_##f) { \
    if (!s.empty()) {         \
      s += "|";               \
    }                         \
    s += #f;                  \
    flags ^= CSI_FLAG_##f;    \
  }

  FLAG(WRITTEN_BY_REBUILDING)
  FLAG(WRITTEN_BY_RECOVERY)
  FLAG(SHARD_ID)
  FLAG(HOLE)
  FLAG(DRAINED)

#undef FLAG

  if (flags) {
    if (!s.empty()) {
      s += "|";
    }
    s += std::to_string(flags);
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "LocalLogStoreRecordFormat::csiFlagsToString() was called with some "
        "unexpected flags: %u. Please check the code of csiFlagsToString() to "
        "see if it's missing some newly added flags.",
        flags);
  }

  return s;
}

}}} // namespace facebook::logdevice::LocalLogStoreRecordFormat
