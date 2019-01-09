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
#include "logdevice/common/NodeSetSelectorUtils.h"
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

bool EpochMetaData::validWithConfig(
    logid_t log_id,
    const std::shared_ptr<Configuration>& cfg) const {
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
  const auto& all_nodes = cfg->serverConfig()->getNodes();
  // check nodeset and replication factor with config
  if (!ServerConfig::validStorageSet(all_nodes, shards, replication)) {
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
}

bool EpochMetaData::isEmpty() const {
  return *this == EpochMetaData();
}

bool EpochMetaData::isSubstantiallyIdentical(const EpochMetaData& rhs) const {
  static_assert(sizeof(h) == 20, "MetaDataLogRecordHeader size changed");

  // flags to ignore:
  // - wire flags only used for serialization: HAS_REPLICATION_PROPERTY and
  //   HAS_WEIGHTS
  // - only used to indicate written status: WRITTEN_IN_METADATALOG
  const epoch_metadata_flags_t flags_mask =
      MetaDataLogRecordHeader::ALL_KNOWN_FLAGS &
      ~MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY &
      ~MetaDataLogRecordHeader::HAS_WEIGHTS &
      ~MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;

  return h.version == rhs.h.version &&
      h.effective_since == rhs.h.effective_since &&
      h.nodeset_size == rhs.h.nodeset_size &&
      (h.flags & flags_mask) == (rhs.h.flags & flags_mask) &&
      shards == rhs.shards && weights == rhs.weights &&
      replication == rhs.replication &&
      nodesconfig_hash == rhs.nodesconfig_hash;
}

bool EpochMetaData::operator!=(const EpochMetaData& rhs) const {
  return !(*this == rhs);
}

bool EpochMetaData::operator==(const EpochMetaData& rhs) const {
  return h.epoch == rhs.h.epoch && isSubstantiallyIdentical(rhs) &&
      writtenInMetaDataLog() == rhs.writtenInMetaDataLog();
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

namespace {
bool nodesetContainsInvalidNodes(logid_t log_id,
                                 const StorageSet& shards,
                                 ServerConfig& cfg) {
  for (auto shard : shards) {
    auto node = cfg.getNode(shard.node());
    if (!node || !node->isReadableStorageNode() ||
        node->storage_attributes->exclude_from_nodesets) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Node %d is in the nodeset for log %lu, but is not a "
                     "storage node or a node that should be included in the "
                     "nodesets according to the config",
                     shard.node(),
                     log_id.val());
      return true;
    }
  }
  return false;
}
} // namespace

bool EpochMetaData::matchesConfig(logid_t log_id,
                                  const std::shared_ptr<Configuration>& cfg,
                                  bool output_errors) const {
  dbg::Level debug_level = output_errors ? dbg::Level::ERROR : dbg::Level::SPEW;
  if (!validWithConfig(log_id, cfg)) {
    return false;
  }
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      cfg->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    ld_log(debug_level, "No log config for log %lu", log_id.val_);
    return false;
  }
  ReplicationProperty replication_in_cfg =
      ReplicationProperty::fromLogAttributes(logcfg->attrs());

  // TODO: this doesn't verify the nodeset size if there is none set in the
  // config. Should something be done about this?
  nodeset_size_t expected_nodeset_size = get_real_nodeset_size(
      cfg->serverConfig()->getMetaDataLogsConfig().nodeset_selector_type,
      log_id,
      cfg,
      logcfg->attrs().nodeSetSize().value(),
      replication_in_cfg);
  if (logcfg->attrs().nodeSetSize().value().hasValue() &&
      (h.nodeset_size != expected_nodeset_size)) {
    ld_log(debug_level,
           "Nodeset size mismatch for log %lu, epoch metadata: %d, expected "
           "nodeset size from configuration: %d",
           log_id.val(),
           h.nodeset_size,
           expected_nodeset_size);
    return false;
  }

  if (replication != replication_in_cfg) {
    ld_log(debug_level,
           "Replication property mismatch for log %lu, epoch metadata: %s, "
           "configured: %s",
           log_id.val(),
           replication.toString().c_str(),
           replication_in_cfg.toString().c_str());
    return false;
  }

  if (disabled()) {
    // this log is disabled, but is in the config. Should be reprovisioned
    ld_log(debug_level,
           "log %lu disabled in epoch metadata, but present in config",
           log_id.val());
    return false;
  }

  if (!nodesconfig_hash.hasValue()) {
    // config hash is expected, but not written
    ld_log(debug_level, "Config hash not written for log %lu", log_id.val());
    return false;
  }
  if (nodesetContainsInvalidNodes(log_id, shards, *cfg->serverConfig())) {
    // we want to change a nodeset if it contains nodes that aren't in the
    // config anymore
    ld_log(debug_level,
           "Nodeset contains invalid nodes for log %lu",
           log_id.val());
    return false;
  }
  if (nodesconfig_hash.hasValue() &&
      cfg->serverConfig()->getStorageNodesConfigHash() !=
          nodesconfig_hash.value()) {
    // metadata has been generated from a different config
    ld_log(
        debug_level,
        "Metadata generated from a different config than the one supplied for "
        "log %lu. Metadata generated from nodes config with this hash: %lu. "
        "Current nodes config hash: %lu",
        log_id.val(),
        nodesconfig_hash.value(),
        cfg->serverConfig()->getStorageNodesConfigHash());
    return false;
  }

  return true;
}

void EpochMetaData::deserialize(ProtocolReader& reader,
                                bool /*unused*/,
                                folly::Optional<size_t> expected_size,
                                logid_t logid,
                                const ServerConfig& cfg) {
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
        "deserialization, "
        "assuming that this is metadata from the future, and that the people "
        "of "
        "the future have designed the new format to be forward compatible. "
        "All flags: %u, supported flags: %u. Full payload: %s",
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

  if (h.flags & MetaDataLogRecordHeader::HAS_NODESCONFIG_HASH) {
    uint64_t hash;
    reader.read(&hash);
    nodesconfig_hash.assign(hash);
  } else {
    nodesconfig_hash.clear();
  }

  CHECK_READER();
  // note that if reader.error(), reader.bytesRemaining() will be zero
  // check trailing bytes if expected_size is set
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
                               const ServerConfig& cfg) {
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
    ld_check(h.version >= 2);
    new_h.flags |= MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY;
    new_h.sync_replication_scope_DO_NOT_USE =
        replication.getBiggestReplicationScope();
  }

  if (weights.empty()) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_WEIGHTS;
  } else {
    ld_check(h.version >= 2);
    new_h.flags |= MetaDataLogRecordHeader::HAS_WEIGHTS;
  }

  if (!nodesconfig_hash.hasValue()) {
    new_h.flags &= ~MetaDataLogRecordHeader::HAS_NODESCONFIG_HASH;
  } else {
    ld_check(h.version >= 2);
    new_h.flags |= MetaDataLogRecordHeader::HAS_NODESCONFIG_HASH;
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

  if (new_h.flags & MetaDataLogRecordHeader::HAS_NODESCONFIG_HASH) {
    ld_check(nodesconfig_hash.hasValue());
    writer.write(&nodesconfig_hash.value(), sizeof(uint64_t));
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
  std::string out = "[E:" + std::to_string(h.epoch.val_) +
      " S:" + std::to_string(h.effective_since.val_) +
      " R:" + replication.toString() + " V:" + std::to_string(h.version) +
      " FL:" + flagsToString(h.flags) +
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
  FLAG(HAS_NODESCONFIG_HASH)
  FLAG(HAS_STORAGE_SET)

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

StorageSet EpochMetaData::nodesetToStorageSet(const NodeSetIndices& indices) {
  StorageSet set;
  set.reserve(indices.size());
  for (node_index_t nid : indices) {
    set.push_back(ShardID(nid, 0));
  }
  return set;
}

StorageSet EpochMetaData::nodesetToStorageSet(const NodeSetIndices& indices,
                                              logid_t logid,
                                              const ServerConfig& cfg) {
  const shard_size_t n_shards = cfg.getNumShards();
  ld_check(n_shards > 0);
  shard_index_t index = getLegacyShardIndexForLog(logid, n_shards);

  StorageSet set;
  set.reserve(indices.size());
  for (node_index_t nid : indices) {
    set.push_back(ShardID(nid, index));
  }
  return set;
}

NodeSetIndices
EpochMetaData::storageSetToNodeset(const StorageSet& storage_set) {
  NodeSetIndices nodeset;
  nodeset.reserve(storage_set.size());
  for (ShardID shard : storage_set) {
    nodeset.push_back(shard.node());
  }
  return nodeset;
}

}} // namespace facebook::logdevice
