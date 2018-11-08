/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include <folly/Range.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

/**
 * @file
 * Collection of utilities for formatting records and copyset index entries
 * when writing to the local log store and parsing it when reading.
 *
 * The format of records in the local log store is:
 *   [8 bytes]       timestamp in milliseconds
 *   [4 bytes]       last_known_good in epoch (see STORE_Header)
 *   [1+ bytes]      bitset of flags defined below (might be varint!)
 *   [4 bytes]       one of:
 *                   1) wave number if FLAG_WRITTEN_BY_RECOVERY is not set; OR
 *                   2) seal epoch of the log recovery that wrote this
 *                      record if FLAG_WRITTEN_BY_RECOVERY is set.
 *   [1 byte]        copyset size N
 *   [N * 2/4 bytes] If FLAG_SHARD_ID is set, ShardID for each of the N copyset
 *                   entries. If this flag is not set, the copyset was
 *                   constructed using a legacy format (node_index_t). A ShardID
 *                   is then constructed with it's shard_index_t component
 *                   equal to hash(log_id) % num_shards.
 *   [0 or 8 bytes]  offset within epoch if FLAG_OFFSET_WITHIN_EPOCH is set
 *   [0 to  64kb]    user defined key.
 *   [?? bytes]      the rest of the blob is the user-provided data
 *
 * The format of single copyset index entries is:
 *   [4 bytes] wave number
 *   [1 byte]  flags. is_hole and written by rebuilding
 *   [1 byte]  copyset size
 *   [N * 2 bytes]  node_index_t for each of the N nodes in copyset
 *
 * All integers are in host byte order, assumed to be little endian.
 */

namespace facebook { namespace logdevice {

struct STORE_Header;
struct StoreChainLink;

namespace LocalLogStoreRecordFormat {

using flags_t = uint32_t;

// If set in flags, the payload is prefixed with a checksum.  The length of
// the checksum depends on the CHECKSUM_64BIT flag; if it is set, the checksum
// is 64 bits, otherwise 32 bits.
const flags_t FLAG_CHECKSUM = 1u << 2;       //=4
const flags_t FLAG_CHECKSUM_64BIT = 1u << 3; //=8
// XNOR of the other two checksum bits.  Ensures that single-bit corruptions
// in the checksum bits (or flags getting zeroed out) get detected.
const flags_t FLAG_CHECKSUM_PARITY = 1u << 4; //=16
// Write does not contain payload, just the record metadata
// (e.g., copyset, flags).  Sequencer uses this to amend the copyset when
// sending multiple waves without resending the payload. It also uses this
// during recovery to amend existing records with its seal epoch number.
const flags_t FLAG_AMEND = 1u << 5; //=32
// record represents a plug for a hole in the numbering sequence
const flags_t FLAG_HOLE = 1u << 6; //=64
// Record was written through BufferedWriter
const flags_t FLAG_BUFFERED_WRITER_BLOB = 1u << 7; //=128
// Include offset within epoch info with current record
const flags_t FLAG_OFFSET_WITHIN_EPOCH = 1u << 8; //=256
// Record contains CSI data only. (copyset and HOLE flag) should not be written
// to disk.
const flags_t FLAG_CSI_DATA_ONLY = 1u << 9; //=512
// the record is written / overwritten by epoch recovery. In such case,
// the wave number field is repurposed to store the `seal epoch' number
// of the log recovery that stores this record.
// note: must be set if FLAG_HOLE is set
const flags_t FLAG_WRITTEN_BY_RECOVERY = 1u << 13; //=8192
// the user-provided key is stored in the record header
const flags_t FLAG_CUSTOM_KEY = 1u << 14; //=16384

// the record is a special hole record plugged by epoch recovery indicating
// the end of an epoch
const flags_t FLAG_BRIDGE = 1u << 15; //=32768

// the record marks the beginning of the an epoch. It is guaranteed that no
// data is stored before the esn of the record in this epoch
const flags_t FLAG_EPOCH_BEGIN = 1u << 16; //=65536

// Written by the record rebuilding process.
const flags_t FLAG_WRITTEN_BY_REBUILDING = 1u << 17; //=131072

// user provided optional keys in a map
const flags_t FLAG_OPTIONAL_KEYS = 1u << 18; //=262144

// the record was explicitly drained from this node and amended to have
// a new copyset. The new copyset should not include "my node id". If
// this flag is clear, the copyset should include "my node id".
const flags_t FLAG_DRAINED = 1u << 19; //=524288

// copyset is made of ShardIDs instead of node_index_t.
const flags_t FLAG_SHARD_ID = 1u << 20; //=1048576

// Indicates if record contains OffsetMap.
const flags_t FLAG_OFFSET_MAP = 1u << 21; //=2097152

// Please update flagsToString() when adding new flags.

// Flags that indicate that the record in question is a pseudorecord, and can
// be ignored e.g. when considering whether a log is empty or not.
const flags_t PSEUDORECORD_MASK =
    FLAG_AMEND | FLAG_HOLE | FLAG_CSI_DATA_ONLY | FLAG_BRIDGE;

// Flags that have the same value and meaning
// here, in STORE_Message and in RECORD_Message.
const flags_t FLAG_MASK = FLAG_CHECKSUM | FLAG_CHECKSUM_64BIT |
    FLAG_CHECKSUM_PARITY | FLAG_HOLE | FLAG_BUFFERED_WRITER_BLOB |
    FLAG_WRITTEN_BY_RECOVERY | FLAG_BRIDGE | FLAG_EPOCH_BEGIN | FLAG_DRAINED;

static_assert(FLAG_CHECKSUM == RECORD_Header::CHECKSUM &&
                  FLAG_CHECKSUM_64BIT == RECORD_Header::CHECKSUM_64BIT &&
                  FLAG_CHECKSUM_PARITY == RECORD_Header::CHECKSUM_PARITY &&
                  FLAG_HOLE == RECORD_Header::HOLE &&
                  FLAG_BUFFERED_WRITER_BLOB ==
                      RECORD_Header::BUFFERED_WRITER_BLOB &&
                  FLAG_WRITTEN_BY_RECOVERY ==
                      RECORD_Header::WRITTEN_BY_RECOVERY &&
                  FLAG_BRIDGE == RECORD_Header::BRIDGE &&
                  FLAG_EPOCH_BEGIN == RECORD_Header::EPOCH_BEGIN &&
                  FLAG_DRAINED == RECORD_Header::DRAINED,
              "Flag constants don't match");

static_assert(FLAG_CHECKSUM == STORE_Header::CHECKSUM &&
                  FLAG_CHECKSUM_64BIT == STORE_Header::CHECKSUM_64BIT &&
                  FLAG_CHECKSUM_PARITY == STORE_Header::CHECKSUM_PARITY &&
                  FLAG_HOLE == STORE_Header::HOLE &&
                  FLAG_BUFFERED_WRITER_BLOB ==
                      STORE_Header::BUFFERED_WRITER_BLOB &&
                  FLAG_BRIDGE == STORE_Header::BRIDGE &&
                  FLAG_EPOCH_BEGIN == STORE_Header::EPOCH_BEGIN &&
                  FLAG_DRAINED == STORE_Header::DRAINED,
              "Flag constants don't match");

using csi_flags_t = uint8_t;

// record was created through rebuilding
const csi_flags_t CSI_FLAG_WRITTEN_BY_REBUILDING = (unsigned)1 << 0;

// record was explicitly drained from this node and amended to have
// a new copyset. The new copyset should not include "my node id". If
// this flag is clear, the copyset should include "my node id".
const csi_flags_t CSI_FLAG_DRAINED = (unsigned)1 << 1;

// copyset is made of ShardIDs instead of node_index_t.
const csi_flags_t CSI_FLAG_SHARD_ID = (unsigned)1 << 2;

// written by recovery
const csi_flags_t CSI_FLAG_WRITTEN_BY_RECOVERY = (unsigned)1 << 3;

// record represents a plug for a hole in the numbering sequence
const csi_flags_t CSI_FLAG_HOLE = (unsigned)1 << 6;

static_assert(CSI_FLAG_HOLE == RECORD_Header::HOLE,
              "Flag constants don't match");
static_assert(CSI_FLAG_HOLE == STORE_Header::HOLE,
              "Flag constants don't match");

/**
 * Estimate size of string produced by formRecordHeader(). Can be used to
 * reserve an appropriate amount of memory before calling
 * formRecordHeaderBufAppend().
 */
size_t recordHeaderSizeEstimate(flags_t flags,
                                copyset_size_t copyset_size,
                                const Slice& optional_keys,
                                const OffsetMap& offsets_within_epoch);

/**
 * Behaves like formRecordHeader(), but does not clear, reserve, and overwrite
 * the buf string, but simply appends to it.
 */
Slice formRecordHeaderBufAppend(int64_t timestamp,
                                esn_t last_known_good,
                                flags_t flags,
                                uint32_t wave_or_recovery_epoch,
                                const folly::Range<const ShardID*>& copyset,
                                const OffsetMap& offsets_within_epoch,
                                const Slice& optional_keys,
                                std::string* buf);

/**
 * Forms the header part of the blob to be written into the local log store.
 * The record header is everything except the user payload, as described at
 * the top of the file.
 *
 * @param wave_or_recovery_epoch   wave number if FLAG_WRITTEN_BY_RECOVERY is
 *                                 not set; OR seal epoch of the log recovery
 *                                 that wrote this record if
 *                                 flags & FLAG_WRITTEN_BY_RECOVERY.
 *
 * @param offset_within_epoch   effective only if
 *                              flags & FLAG_OFFSET_WITHIN_EPOCH. How much bytes
 *                              were written in epoch (to which this record
 *                              belongs) before current record. This info is
 *                              going to be included in Slice only if
 *                              OFFSET_WITHIN_EPOCH flag is on.
 *
 * @return Slice pointing into supplied std::string
 */
Slice formRecordHeader(int64_t timestamp,
                       esn_t last_known_good,
                       flags_t flags,
                       uint32_t wave_or_recovery_epoch,
                       const folly::Range<const ShardID*>& copyset,
                       const OffsetMap& offsets_within_epoch,
                       const std::map<KeyType, std::string>& optional_keys,
                       std::string* buf);

/**
 * Helper overload that takes a STORE_Header and STORE_Extra.
 *
 * @param store_header          Header of STORE_Message being processed
 * @param copyset               Copy set, also in STORE_Message
 * @param buf                   std::string to use as storage
 * @param store_extra           optional parameter for extra information along
 *                              with the STORE_Message
 * @param optional_keys         optional strings provided by client:
 *                              KeyType::FINDKEY is for original key (with
 *                              monotonically increasing order);
 *                              KeyType::FILTERABLE is used by server-side
 *                              filtering.[Experimental feature]
 */
Slice formRecordHeader(const STORE_Header& store_header,
                       const StoreChainLink* copyset,
                       std::string* buf,
                       bool shard_id_in_copyset,
                       const std::map<KeyType, std::string>& optional_keys,
                       const STORE_Extra& store_extra = STORE_Extra());

/**
 * Form copyset index entry flags from the content of a STORE_Header.
 */
csi_flags_t formCopySetIndexFlags(const STORE_Header&,
                                  bool shard_id_in_copyset);

/**
 * Convert relevant content in record flags into copyset index entry flags.
 */
csi_flags_t formCopySetIndexFlags(const flags_t flags);

/**
 * Convert copyset index entry flags into record flags
 */
flags_t copySetIndexFlagsToRecordFlags(csi_flags_t flags);

/**
 * Forms the copyset index entry for the record. This entry will contain
 * the size of the copyset, and the nodes present in the copyset.
 *
 * @param wave               wave number
 * @param copyset            pointer to indices of nodes in the copyset
 * @param copyset_size       number of nodes in the copyset
 * @param block_starting_lsn LSN where the block starts, also in
 *                           STORE_Message
 * @param buf                std::string to use as storage
 *
 * @return Slice pointing into supplied std::string
 */
Slice formCopySetIndexEntry(uint32_t wave,
                            const ShardID* copyset,
                            const copyset_size_t copyset_size,
                            const folly::Optional<lsn_t>& block_starting_lsn,
                            const csi_flags_t csi_flags,
                            std::string* buf);

/**
 * Forms the copyset index entry for the record. This entry will contain
 * the size of the copyset, and the nodes present in the copyset.
 *
 * @param store_header        header of STORE_Message being processed
 * @param store_extra         extra attributes of STORE_Message being processed
 * @param copyset             copy set, also in STORE_Message
 * @param block_starting_lsn  LSN where the block starts, also in STORE_Message
 * @param buf                 std::string to use as storage
 *
 * @return Slice pointing into supplied std::string
 */
Slice formCopySetIndexEntry(const STORE_Header& store_header,
                            const STORE_Extra& store_extra,
                            const StoreChainLink* copyset,
                            const folly::Optional<lsn_t>& block_starting_lsn,
                            bool shard_id_in_copyset,
                            std::string* buf);

/**
 * Parses the single copyset index entry blob as read from the local log store.
 * The blob is validated to be in the format described at the top of the file.
 *
 * All of the pointer parameters are optional, pass in nullptr if not
 * interested.
 *
 * `this_shard` is used for copysets that are serialized in the old format
 *              (node_index_t instead of ShardID). For these, we assume the
 *              shard offset of all copies is the offset of the shard we are
 *              reading from. Unused if `copyset` is nullptr.
 *              TODO(T15517759): remove once all production clusters have used
 *              --write-shard-id-in-copyset for more than the total retention
 *              period and after all internal logs have been snapshotted.
 *
 * @return On success, returns true.  On failure, returns false and sets err to:
 *           MALFORMED_RECORD  parse error
 */
bool parseCopySetIndexSingleEntry(const Slice& cs_dir_slice,
                                  std::vector<ShardID>* copyset,
                                  uint32_t* wave,
                                  csi_flags_t* csi_flags,
                                  shard_index_t this_shard);

/**
 * Parses the record blob as read from the local log store. The blob is
 * validated to be in the format described at the top of the file. Note that
 * the validation doesn't depend on the arguments passed to parse(); e.g.
 * if the blob has malformed description of optional keys, parse() will report
 * MALFORMED_RECORD even if optional_keys = nullptr.
 *
 * All of the pointer parameters are optional, pass in nullptr if not
 * interested.
 *
 * Since the copyset is of variable length, you can call this twice: first to
 * get the copyset size, then with a copyset_arr_out of the right size.  Or
 * call with a small copyset_arr_out first, then retry if it was too small
 * (NOBUFS).
 *
 * `offset_within_epoch_out` is populated only if FLAG_OFFSET_WITHIN_EPOCH was
 *                           set in flags.
 *
 * `this_shard` is used for copysets that are serialized in the old format
 *              (node_index_t instead of ShardID). For these, we assume the
 *              shard offset of all copies is the offset of the shard we are
 *              reading from. Unused if `copyset` is nullptr.
 *              TODO(T15517759): remove once all production clusters have used
 *              --write-shard-id-in-copyset for more than the total retention
 *              period and after all internal logs have been snapshotted.
 *
 * @return On success, returns 0.  On failure, returns -1 and sets err to:
 *           MALFORMED_RECORD  parse error
 *           NOBUFS            copyset_arr_out_size was too small
 *                             (sets *copyset_size_out if not null)
 */
int parse(const Slice& log_store_blob,
          std::chrono::milliseconds* timestamp_out,
          esn_t* last_known_good_out,
          flags_t* flags_out,
          uint32_t* wave_or_recovery_epoch_out,
          copyset_size_t* copyset_size_out,
          ShardID* copyset_arr_out,
          size_t copyset_arr_out_size,
          OffsetMap* offsets_within_epoch,
          std::map<KeyType, std::string>* optional_keys,
          Payload* payload_out,
          shard_index_t this_shard);

/**
 * Same as parse() but faster and only parses timestamp.
 */
int parseTimestamp(const Slice& log_store_blob,
                   std::chrono::milliseconds* timestamp_out);

/**
 * Same as parse() but only parses flags.
 */
int parseFlags(const Slice& log_store_blob, flags_t* flags_out);

/**
 * Same as (parseFlags() & FLAG_WRITTEN_BY_RECOVERY) but faster.
 */
int isWrittenByRecovery(const Slice& log_store_blob,
                        bool* is_written_by_recovery_out);

/**
 * Computes a hash of record's copyset.
 */
int getCopysetHash(const Slice& log_store_blob, size_t* hash_out);

/**
 * Does some basic checks of record format, including payload checksum check.
 *
 * @param payload  Can be one of:
 *   - empty; the payload is in `blob`,
 *   - payload without checksum; checksum (if any) is at the end of `blob`,
 *   - payload with checksum.
 */
int checkWellFormed(Slice blob, Slice payload = Slice());

/**
 * Helper method to forms Slice from optional_keys
 * @ param  optional_keys_string  a pointer to string that will hold serialized
 *                                optional_keys
 *          optional_keys         map for all optional keys
 */
void serializeOptionalKeys(std::string* optional_keys_string,
                           const std::map<KeyType, std::string>& optional_keys);

/**
 * Formats flags bitmask into a human readable string.
 */
std::string flagsToString(flags_t flags);
std::string csiFlagsToString(csi_flags_t flags);

} // namespace LocalLogStoreRecordFormat

}} // namespace facebook::logdevice
