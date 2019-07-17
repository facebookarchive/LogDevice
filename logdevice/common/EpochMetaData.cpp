/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochMetaData.h"

#include <folly/Memory.h>

#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

void EpochMetaData::setShards(StorageSet storage_set) {
  shards = std::move(storage_set);
  h.nodeset_size = shards.size();
}

bool EpochMetaData::isValid() const {
  bool rv = MetaDataLogRecordHeader::isValid(h) &&
      shards.size() == h.nodeset_size &&
      isStrictlyAscending(shards.begin(), shards.end()) &&
      (weights.empty() || weights.size() == h.nodeset_size) &&
      replication.isValid();
  if (!rv) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Invalid EpochMetaData: %s",
                   toString().c_str());
  }
  return rv;
}

void EpochMetaData::setEpochIncrementAt() {
  epoch_incremented_at = RecordTimestamp::now();
}

bool EpochMetaData::validWithConfig(
    logid_t log_id,
    const std::shared_ptr<Configuration>& cfg,
    const std::shared_ptr<const NodesConfiguration>& nodes_cfg) const {
  if (!isValid() || log_id == LOGID_INVALID) {
    ld_error("Invalid epoch metadata for log %lu", log_id.val_);
    return false;
  }
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      cfg->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    ld_error("No log config for log %lu", log_id.val_);
    return false;
  }
  // check nodeset and replication factor with config
  if (!configuration::nodes::validStorageSet(*nodes_cfg, shards, replication)) {
    ld_error("Invalid nodeset for log %lu", log_id.val_);
    return false;
  }

  return true;
}

void EpochMetaData::reset() {
  h.version = 0;
  h.epoch = h.effective_since = EPOCH_INVALID;
  h.nodeset_size = 0;
  h.replication_factor_DO_NOT_USE = 0;
  h.sync_replication_scope_DO_NOT_USE = NodeLocationScope::NODE;
  h.flags = 0;

  shards.clear();
  replication_conf_changed_at = RecordTimestamp();
  epoch_incremented_at = RecordTimestamp();
}

bool EpochMetaData::isEmpty() const {
  return *this == EpochMetaData();
}

bool EpochMetaData::identicalInMetaDataLog(const EpochMetaData& rhs) const {
  static_assert(sizeof(h) == 20, "MetaDataLogRecordHeader size changed");

  // flags to ignore:
  // - wire flags only used for serialization: HAS_*
  // - only used to indicate written status: WRITTEN_IN_METADATALOG
  const epoch_metadata_flags_t flags_mask =
      MetaDataLogRecordHeader::ALL_KNOWN_FLAGS &
      ~MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY &
      ~MetaDataLogRecordHeader::HAS_WEIGHTS &
      ~MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG &
      ~MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE &
      ~MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED &
      ~MetaDataLogRecordHeader::HAS_TIMESTAMPS;

  // Ignore epoch, nodeset_params, epoch_incremented_at.
  // replication_conf_changed_at has same purpose as effective_since so ignore
  // that as well.
  // NOTE: it would be good to have a warning if for some reason the timestamp
  // in epoch store doesn't match the timestamp in metadata log (from
  // Sequencer::updateMetaDataMap()'s identicalInMetaDataLog() check).
  // To make it backward compatible we can downgrade, activate the sequencer,
  // then upgrade, the timestamp in epoch store will be cleared and timestamps
  // won't match.
  return h.version == rhs.h.version &&
      h.effective_since == rhs.h.effective_since &&
      h.nodeset_size == rhs.h.nodeset_size &&
      (h.flags & flags_mask) == (rhs.h.flags & flags_mask) &&
      shards == rhs.shards && weights == rhs.weights &&
      replication == rhs.replication;
}

bool EpochMetaData::operator!=(const EpochMetaData& rhs) const {
  return !(*this == rhs);
}

bool EpochMetaData::operator==(const EpochMetaData& rhs) const {
  return h.epoch == rhs.h.epoch &&
      epoch_incremented_at == rhs.epoch_incremented_at &&
      identicalInMetaDataLog(rhs) &&
      replication_conf_changed_at == rhs.replication_conf_changed_at &&
      writtenInMetaDataLog() == rhs.writtenInMetaDataLog() &&
      nodeset_params == rhs.nodeset_params;
}

bool EpochMetaData::disabled() const {
  return h.flags & MetaDataLogRecordHeader::DISABLED;
}

bool EpochMetaData::writtenInMetaDataLog() const {
  return h.flags & MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
}

int EpochMetaData::sizeInPayload() const {
  return toPayload(nullptr, 0);
}

void EpochMetaData::deserialize(ProtocolReader& reader,
                                bool /*unused*/,
                                folly::Optional<size_t> expected_size,
                                logid_t logid,
                                const NodesConfiguration& cfg) {
  const size_t bytes_read_before_deserialize = reader.bytesRead();
  reader.read(&h.version);

// ProtocolReader prints error message
#define CHECK_READER()  \
  if (reader.error()) { \
    return;             \
  }

  CHECK_READER();

  // version not supported
  if (!epoch_metadata_version::validToRead(h.version)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Epoch metadata version %u not supported. Minimum "
                    "supported version: %u. "
                    "Full payload: %s",
                    h.version,
                    epoch_metadata_version::MIN_SUPPORTED,
                    reader.getSource().hexDump().c_str());
    reader.setError(E::BADMSG);
    return;
  }

  // provide defaults for fields that are not in all versions
  h.sync_replication_scope_DO_NOT_USE = NodeLocationScope::NODE;
  h.flags = 0;
  const size_t header_size = MetaDataLogRecordHeader::headerSize(h.version);
  ld_check(header_size >= sizeof(h.version));
  // read the rest of header
  reader.read((char*)&h + sizeof(h.version), header_size - sizeof(h.version));
  ld_check(header_size == sizeof(h) ||
           header_size ==
               sizeof(h) - sizeof(h.sync_replication_scope_DO_NOT_USE) -
                   sizeof(h.flags));
  bool has_unknown_flags = h.flags & ~MetaDataLogRecordHeader::ALL_KNOWN_FLAGS;
  if (has_unknown_flags) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Epoch metadata has unsupported flags. Proceeding with "
        "deserialization, assuming that this is metadata from the future, and "
        "that the people of the future have designed the new format to be "
        "forward compatible. All flags: %u, supported flags: %u. "
        "Full payload: %s",
        h.flags,
        MetaDataLogRecordHeader::ALL_KNOWN_FLAGS,
        reader.getSource().hexDump().c_str());
    // Let's not keep the flags around if we don't know what they mean.
    // In particular, often an unknown flag means that an unknown field is set
    // and serialized. We better not set such flag if we're not going to
    // serialize the field.
    h.flags &= MetaDataLogRecordHeader::ALL_KNOWN_FLAGS;
  }

  // check if we read the header successfully before moving on
  CHECK_READER();

  // copy the nodeset index array
  if (h.flags & MetaDataLogRecordHeader::HAS_STORAGE_SET) {
    StorageSet storage_set;
    reader.readVector(&storage_set, h.nodeset_size);
    setShards(std::move(storage_set));
  } else {
    NodeSetIndices nodeset;
    reader.readVector(&nodeset, h.nodeset_size);
    // TODO: use NodesConfiguration natively instead
    setShards(nodesetToStorageSet(nodeset, logid, cfg));
  }

  if (h.flags & MetaDataLogRecordHeader::HAS_WEIGHTS) {
    reader.readVector(&weights, h.nodeset_size);
  } else {
    weights.resize(0);
  }

  if (h.flags & MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY) {
    uint32_t replication_property_length;
    reader.read(&replication_property_length);
    std::vector<ReplicationProperty::ScopeReplication> r;
    reader.readVector(&r, replication_property_length);
    replication.clear();
    int rv = replication.assign(r);
    if (rv != 0) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          10,
          "Invalid replication property in EpochMetaData. Full payload: %s",
          reader.getSource().hexDump().c_str());
      reader.setError(E::BADMSG);
      return;
    }
    if (h.replication_factor_DO_NOT_USE != replication.getReplicationFactor()) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "Replication factor doesn't match replication property. Factor: %d, "
          "property: %s. Full payload: %s",
          (int)h.replication_factor_DO_NOT_USE,
          replication.toString().c_str(),
          reader.getSource().hexDump().c_str());
    }
  } else {
    // ReplicationProperty constructor asserts on input
    CHECK_READER();
    replication = ReplicationProperty(
        h.replication_factor_DO_NOT_USE, h.sync_replication_scope_DO_NOT_USE);
  }

  if (h.flags & MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE) {
    reader.read(&nodeset_params.signature);
  } else {
    nodeset_params.signature = 0;
  }

  if (h.flags & MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED) {
    CHECK_READER();
    if (reader.bytesRemaining() == 0 ||
        (expected_size.hasValue() &&
         reader.bytesRead() - bytes_read_before_deserialize ==
             expected_size.value())) {
      // This is a hack to work around what D13463100 fixes.
      // Remove this around summer 2019 or later (when D13463100 has been in
      // production for some time).
      //
      // An older version of the code (without D13463100) could deserialize an
      // EpochMetaData with HAS_TARGET_NODESET_SIZE_AND_SEED flag without
      // knowing what the flag means, then serialize that EpochMetaData with
      // the flag set but without the actual target_nodeset_size and seed.
      nodeset_params.target_nodeset_size = 0;
      nodeset_params.seed = 0;
    } else {
      reader.read(&nodeset_params.target_nodeset_size);
      reader.read(&nodeset_params.seed);
    }
  } else {
    nodeset_params.target_nodeset_size = 0;
    nodeset_params.seed = 0;
  }

  if (h.flags & MetaDataLogRecordHeader::HAS_TIMESTAMPS) {
    std::chrono::milliseconds replication_conf_changed_at_millis;
    reader.read(&replication_conf_changed_at_millis);
    replication_conf_changed_at =
        RecordTimestamp::from(replication_conf_changed_at_millis);

    std::chrono::milliseconds epoch_incremented_at_millis;
    reader.read(&epoch_incremented_at_millis);
    epoch_incremented_at = RecordTimestamp::from(epoch_incremented_at_millis);
  }

  CHECK_READER();
  // Check trailing bytes if expected_size is set.
  // Note that if reader.error(), reader.bytesRemaining() will be zero.
  if (expected_size.hasValue()) {
    const size_t bytes_consumed =
        reader.bytesRead() - bytes_read_before_deserialize;

    if (bytes_consumed > expected_size.value()) {
      // we consumed more than we should
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "deserializing EpochMetaData consumed more bytes than "
                      "expected. expected size: %lu, consumed: %lu. "
                      "Full buffer %s.",
                      expected_size.value(),
                      bytes_consumed,
                      reader.getSource().hexDump().c_str());
      reader.setError(E::BADMSG);
      return;
    }

    if (bytes_consumed < expected_size.value()) {
      // there are trailing bytes
      if (has_unknown_flags) {
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            10,
            "Unexpectedly long serialized EpochMetaData: consumed %lu, actual "
            "%lu. "
            "Ignoring the tail because there are unknown flags. Full payload: "
            "%s",
            bytes_consumed,
            expected_size.value(),
            reader.getSource().hexDump().c_str());

        reader.allowTrailingBytes();
      } else {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Unexpectedly long serialized EpochMetaData: consumed "
                        "%lu, actual %lu. "
                        "Full payload: %s",
                        bytes_consumed,
                        expected_size.value(),
                        reader.getSource().hexDump().c_str());
        reader.setError(E::BADMSG);
        return;
      }

      // drain the trailing bytes so that it consumes exactly _expected_size_
      reader.handleTrailingBytes(expected_size.value() - bytes_consumed);
    } else {
      // no trailing bytes
      ld_check(bytes_consumed == expected_size.value());
    }
  }

  CHECK_READER();
  if (!isValid()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "EpochMetaData::isValid() failed. Full payload: %s",
                    reader.getSource().hexDump().c_str());
    reader.setError(E::BADMSG);
    return;
  }

  ld_check(reader.ok());
  return;

#undef CHECK_READER
}

int EpochMetaData::fromPayload(const Payload& payload,
                               logid_t logid,
                               const NodesConfiguration& cfg) {
  ProtocolReader reader({payload.data(), payload.size()},
                        "EpochMetaData",
                        /*unused proto*/ 0);
  // do not use zero copy for EpochMetaData
  deserialize(reader,
              /*evbuffer_zero_copy=*/false,
              /*expected_size=*/payload.size(),
              logid,
              cfg);
  if (reader.error()) {
    err = E::BADMSG;
    return -1;
  }
  return 0;
}

void EpochMetaData::serialize(ProtocolWriter& writer) const {
  if (!isValid()) {
    writer.setError(E::INVALID_PARAM);
    return;
  }

  MetaDataLogRecordHeader new_h = h;
  new_h.replication_factor_DO_NOT_USE = replication.getReplicationFactor();

  auto legacy_replication = replication.toOldRepresentation();
  if (legacy_replication.hasValue()) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY;
    new_h.sync_replication_scope_DO_NOT_USE =
        legacy_replication->sync_replication_scope;
  } else {
    // TODO: Since version 1 is not used in production any more, we can clean
    // up that version check.
    if (h.version >= 2) {
      new_h.flags |= MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY;
      new_h.sync_replication_scope_DO_NOT_USE =
          replication.getBiggestReplicationScope();
    } else {
      new_h.flags &= ~MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY;
    }
  }

  if (h.version < 2 ||
      (replication_conf_changed_at == RecordTimestamp() &&
       epoch_incremented_at == RecordTimestamp())) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_TIMESTAMPS;
  } else {
    // TODO: Since version 1 is not used in production any more, we can clean
    // up that version check.
    new_h.flags |= MetaDataLogRecordHeader::HAS_TIMESTAMPS;
  }

  if (h.version < 2 || weights.empty()) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_WEIGHTS;
  } else {
    // TODO: Since version 1 is not used in production any more, we can clean
    // up that version check.
    new_h.flags |= MetaDataLogRecordHeader::HAS_WEIGHTS;
  }

  if (h.version < 2 || nodeset_params.signature == 0) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE;
  } else {
    // TODO: Since version 1 is not used in production any more, we can clean
    // up that version check.
    new_h.flags |= MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE;
  }

  if (nodeset_params.target_nodeset_size == 0 && nodeset_params.seed == 0) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED;
  } else {
    new_h.flags |= MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED;
  }

  // preserve the format with the header version by _only_ copying the portion
  // of the header that matches the header version
  writer.write(&new_h, MetaDataLogRecordHeader::headerSize(new_h.version));
  ld_check(new_h.nodeset_size == shards.size());
  if (new_h.flags & MetaDataLogRecordHeader::HAS_STORAGE_SET) {
    writer.writeVector(shards);
  } else {
    writer.writeVector(storageSetToNodeset(shards));
  }

  if (new_h.flags & MetaDataLogRecordHeader::HAS_WEIGHTS) {
    ld_check(weights.size() == new_h.nodeset_size);
    writer.writeVector(weights);
  }

  if (new_h.flags & MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY) {
    std::vector<ReplicationProperty::ScopeReplication> v =
        replication.getDistinctReplicationFactors();
    uint32_t len = v.size();
    writer.write(len);
    for (auto r : v) {
      // ScopeReplication has some padding bytes. Fill them with zeros to make
      // serialization deterministic (some tests assume that).
      ReplicationProperty::ScopeReplication sanitized_r;
      memset(&sanitized_r, 0, sizeof(sanitized_r));
      sanitized_r.first = r.first;
      sanitized_r.second = r.second;
      writer.write(sanitized_r);
    }
  }

  if (new_h.flags & MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE) {
    writer.write(nodeset_params.signature);
  }

  if (new_h.flags & MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED) {
    writer.write(nodeset_params.target_nodeset_size);
    writer.write(nodeset_params.seed);
  }

  if (new_h.flags & MetaDataLogRecordHeader::HAS_TIMESTAMPS) {
    writer.write(replication_conf_changed_at.toMilliseconds());
    writer.write(epoch_incremented_at.toMilliseconds());
  }
}

int EpochMetaData::toPayload(void* payload, size_t size) const {
  // proto is not used here
  ProtocolWriter writer({payload, size}, "EpochMetaData", /*proto*/ 0);
  serialize(writer);
  if (writer.error()) {
    err = writer.status();
    return -1;
  }
  // bytes written
  return writer.result();
}

std::string EpochMetaData::toStringPayload() const {
  std::string ret;
  if (!isValid()) {
    err = E::INVALID_PARAM;
    return ret;
  }
  ret.resize(sizeInPayload());
  int size = toPayload(&ret[0], ret.size());
  // ensured that the string payload has enough size
  ld_check(size == ret.size());
  ld_assert(ret.size() == sizeInPayload());
  return ret;
}

std::string EpochMetaData::toString() const {
  std::string out = "[E:" + std::to_string(h.epoch.val_) + " (at " +
      format_time(epoch_incremented_at) + ")" +
      " since:" + std::to_string(h.effective_since.val_) + " (at " +
      format_time(replication_conf_changed_at) + ")" +
      " R:" + replication.toString() + " V:" + std::to_string(h.version) +
      " flags:" + flagsToString(h.flags) +
      " params:" + nodeset_params.toString() +
      " N:" + facebook::logdevice::toString(shards) +
      " W:" + facebook::logdevice::toString(weights) + "]";
  if (!isValid()) {
    out += "(Invalid)";
  }
  return out;
}

std::string EpochMetaData::flagsToString(epoch_metadata_flags_t flags) {
  std::string s;

#define FLAG(f)                             \
  if (flags & MetaDataLogRecordHeader::f) { \
    if (!s.empty()) {                       \
      s += "|";                             \
    }                                       \
    s += #f;                                \
    flags ^= MetaDataLogRecordHeader::f;    \
  }

  FLAG(WRITTEN_IN_METADATALOG)
  FLAG(DISABLED)
  FLAG(HAS_WEIGHTS)
  FLAG(HAS_REPLICATION_PROPERTY)
  FLAG(HAS_NODESET_SIGNATURE)
  FLAG(HAS_STORAGE_SET)
  FLAG(HAS_TARGET_NODESET_SIZE_AND_SEED)
  FLAG(HAS_TIMESTAMPS)

#undef FLAG

  if (flags) {
    if (!s.empty()) {
      s += "|";
    }
    s += std::to_string(flags);
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "EpochMetaData::flagsToString() was called with some "
        "unexpected flags: %u. Please check the code of flagsToString() to see "
        "if it's missing some newly added flags.",
        flags);
  }

  return s;
}

/*static*/
StorageSet EpochMetaData::nodesetToStorageSet(const NodeSetIndices& indices,
                                              shard_index_t shard_id) {
  StorageSet set;
  set.reserve(indices.size());
  for (node_index_t nid : indices) {
    set.push_back(ShardID(nid, shard_id));
  }
  return set;
}

/*static*/
StorageSet EpochMetaData::nodesetToStorageSet(
    const NodeSetIndices& indices,
    logid_t logid,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const shard_size_t n_shards = nodes_configuration.getNumShards();
  ld_check(n_shards > 0);
  shard_index_t index = getLegacyShardIndexForLog(logid, n_shards);

  StorageSet set;
  set.reserve(indices.size());
  for (node_index_t nid : indices) {
    set.push_back(ShardID(nid, index));
  }
  return set;
}

/*static*/
NodeSetIndices
EpochMetaData::storageSetToNodeset(const StorageSet& storage_set) {
  NodeSetIndices nodeset;
  nodeset.reserve(storage_set.size());
  for (ShardID shard : storage_set) {
    nodeset.push_back(shard.node());
  }
  return nodeset;
}

/*static*/
EpochMetaData EpochMetaData::genEpochMetaDataForMetaDataLog(
    logid_t logid,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    epoch_t epoch,
    epoch_t effective_since) {
  auto meta_storage_set = EpochMetaData::nodesetToStorageSet(
      nodes_configuration.getStorageMembership()->getMetaDataNodeIndices(),
      MetaDataLog::metaDataLogID(logid),
      nodes_configuration);

  return EpochMetaData(std::move(meta_storage_set),
                       nodes_configuration.getMetaDataLogsReplication()
                           ->getReplicationProperty(),
                       epoch,
                       effective_since);
}

bool EpochMetaData::NodeSetParams::operator==(const NodeSetParams& rhs) const {
  return std::tie(signature, target_nodeset_size, seed) ==
      std::tie(rhs.signature, rhs.target_nodeset_size, rhs.seed);
}
bool EpochMetaData::NodeSetParams::operator!=(const NodeSetParams& rhs) const {
  return !(*this == rhs);
}

std::string EpochMetaData::NodeSetParams::toString() const {
  std::stringstream ss;
  ss << "{target:" << target_nodeset_size << " seed:" << seed
     << " signature:" << std::hex << signature << std::dec << "}";
  return ss.str();
}

}} // namespace facebook::logdevice
