/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochMetaDataMap.h"

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

EpochMetaDataMap::EpochMetaDataMap(std::shared_ptr<const Map> epoch_map,
                                   epoch_t effective_until)
    : epoch_map_(std::move(epoch_map)), effective_until_(effective_until) {
  ld_check(epoch_map_ != nullptr);
  ld_check(isValid());
}

/*static*/
std::shared_ptr<const EpochMetaDataMap>
EpochMetaDataMap::create(std::shared_ptr<const Map> epoch_map,
                         epoch_t effective_until) {
  if (!epoch_map || !isValid(*epoch_map, effective_until)) {
    err = E::INVALID_PARAM;
    return nullptr;
  }

  // std::make_shared does not work with private constructors...
  return std::shared_ptr<const EpochMetaDataMap>(
      new EpochMetaDataMap(std::move(epoch_map), effective_until));
}

/*static*/
bool EpochMetaDataMap::isValid(const Map& epoch_map, epoch_t effective_until) {
  if (epoch_map.empty() ||
      (epoch_map.size() >
       std::numeric_limits<EpochMetaDataMapHeader::entry_size_t>::max()) ||
      effective_until == EPOCH_INVALID ||
      effective_until < epoch_map.crbegin()->second.h.effective_since) {
    return false;
  }

  for (const auto& kv : epoch_map) {
    const auto& m = kv.second;
    if (!m.isValid() || m.h.effective_since != kv.first) {
      return false;
    }
  }

  return true;
}

std::shared_ptr<const EpochMetaDataMap>
EpochMetaDataMap::withNewEffectiveUntil(epoch_t new_effective_until) const {
  return create(getMetaDataMap(), new_effective_until);
}

std::shared_ptr<const EpochMetaDataMap>
EpochMetaDataMap::withNewEntry(const EpochMetaData& new_metadata,
                               epoch_t new_effective_until) const {
  ld_check(isValid());
  if (effective_until_ >= new_metadata.h.effective_since ||
      new_effective_until < new_metadata.h.effective_since) {
    err = E::INVALID_PARAM;
    return nullptr;
  }
  Map map = *getMetaDataMap();
  auto result =
      map.insert(std::make_pair(new_metadata.h.effective_since, new_metadata));
  ld_check(result.second);
  return create(
      std::make_shared<const Map>(std::move(map)), new_effective_until);
}

EpochMetaDataMap::Map::const_iterator
EpochMetaDataMap::getIteratorForEpoch(epoch_t epoch) const {
  ld_check(epoch <= effective_until_);
  ld_check(!epoch_map_->empty());
  auto it = epoch_map_->upper_bound(epoch);
  // assume the first entry covers the range from EPOCH_MIN to its
  // effective_since as well
  return it == epoch_map_->cbegin() ? it : --it;
}

std::unique_ptr<EpochMetaData>
EpochMetaDataMap::getEpochMetaData(epoch_t epoch) const {
  if (epoch > effective_until_) {
    // query metadata for an epoch that is out of range
    err = E::INVALID_PARAM;
    return nullptr;
  }

  const auto it = getIteratorForEpoch(epoch);
  ld_check(it != epoch_map_->end());
  return std::make_unique<EpochMetaData>(it->second);
}

std::unique_ptr<EpochMetaData> EpochMetaDataMap::getLastEpochMetaData() const {
  if (epoch_map_ == nullptr || epoch_map_->empty()) {
    return nullptr;
  }

  return std::make_unique<EpochMetaData>(epoch_map_->crbegin()->second);
}

const EpochMetaData&
EpochMetaDataMap::getValidEpochMetaData(epoch_t epoch) const {
  if (epoch > effective_until_) {
    ld_critical("INTERNAL ERROR: called with epoch %u larger than "
                "effective until %u.",
                epoch.val_,
                effective_until_.val_);
    ld_check(false);
  }
  const auto it = getIteratorForEpoch(epoch);
  ld_check(it != epoch_map_->end());
  return it->second;
}

std::unique_ptr<StorageSet>
EpochMetaDataMap::getUnionStorageSet(epoch_t min_epoch,
                                     epoch_t max_epoch) const {
  ld_check(min_epoch <= max_epoch);
  if (max_epoch > effective_until_) {
    err = E::INVALID_PARAM;
    return nullptr;
  }

  std::set<ShardID> shards;
  const auto begin = getIteratorForEpoch(min_epoch);
  const auto end = ++getIteratorForEpoch(max_epoch);
  for (auto it = begin; it != end; ++it) {
    for (ShardID shard : it->second.shards) {
      shards.insert(shard);
    }
  }

  return std::make_unique<StorageSet>(shards.begin(), shards.end());
}

std::unique_ptr<StorageSet> EpochMetaDataMap::getUnionStorageSet(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    epoch_t min_epoch,
    epoch_t max_epoch) const {
  auto result = getUnionStorageSet(min_epoch, max_epoch);
  if (result == nullptr) {
    return nullptr;
  }
  // filter out non-storge nodes
  result->erase(std::remove_if(result->begin(),
                               result->end(),
                               [&nodes_configuration](ShardID shard) {
                                 return !nodes_configuration
                                             .getStorageMembership()
                                             ->shouldReadFromShard(shard);
                               }),
                result->end());
  return result;
}

std::unique_ptr<StorageSet> EpochMetaDataMap::getUnionStorageSet() const {
  return getUnionStorageSet(EPOCH_MIN, effective_until_);
}

std::unique_ptr<StorageSet> EpochMetaDataMap::getUnionStorageSet(
    const configuration::nodes::NodesConfiguration& nodes_configuration) const {
  return getUnionStorageSet(nodes_configuration, EPOCH_MIN, effective_until_);
}

std::unique_ptr<ReplicationProperty>
EpochMetaDataMap::getNarrowestReplication(epoch_t min_epoch,
                                          epoch_t max_epoch) const {
  ld_check(min_epoch <= max_epoch);
  if (max_epoch > effective_until_) {
    err = E::INVALID_PARAM;
    return nullptr;
  }

  const auto begin = getIteratorForEpoch(min_epoch);
  const auto end = ++getIteratorForEpoch(max_epoch);
  ReplicationProperty rep;
  for (auto it = begin; it != end; ++it) {
    const ReplicationProperty& r = it->second.replication;
    rep = it == begin ? r : rep.narrowest(r);
  }

  return std::make_unique<ReplicationProperty>(rep);
}

void EpochMetaDataMap::serialize(ProtocolWriter& writer) const {
  if (!isValid()) {
    writer.setError(E::INVALID_PARAM);
    // shouldn't happen as the object must be created valid
    ld_check(false);
    return;
  }

  EpochMetaDataMapHeader header{
      0,
      effective_until_.val_,
      static_cast<EpochMetaDataMapHeader::entry_size_t>(epoch_map_->size())};
  writer.write(header);
  for (const auto& kv : *epoch_map_) {
    EpochMetaDataMapHeader::entry_size_t entry_size = kv.second.sizeInPayload();
    writer.write(entry_size);
    kv.second.serialize(writer);
  }
}

int EpochMetaDataMap::serialize(void* buffer, size_t size) const {
  ProtocolWriter w({buffer, size}, name(), 0);
  serialize(w);
  if (w.error()) {
    err = w.status();
    return -1;
  }
  return w.result();
}

ssize_t EpochMetaDataMap::sizeInLinearBuffer() const {
  return serialize(nullptr, 0);
}

void EpochMetaDataMap::deserialize(ProtocolReader& reader,
                                   bool /*unused*/,
                                   folly::Optional<size_t> /*unused*/,
                                   logid_t /*unused*/,
                                   const NodesConfiguration& /*unused*/) {
  // not supported for immutable object, see docblock in .h file
  reader.setError(E::INTERNAL);
  ld_check(false);
  return;
}

/*static*/
std::shared_ptr<const EpochMetaDataMap>
EpochMetaDataMap::deserialize(ProtocolReader& reader,
                              bool /*unused*/,
                              logid_t logid,
                              const NodesConfiguration& cfg) {
#define CHECK_READER()  \
  if (reader.error()) { \
    err = E::BADMSG;    \
    return nullptr;     \
  }
  EpochMetaDataMapHeader header;
  reader.read(&header);

  CHECK_READER();
  Map epoch_map;
  for (size_t i = 0; i < header.num_entries; ++i) {
    EpochMetaDataMapHeader::entry_size_t entry_size;
    reader.read(&entry_size);
    CHECK_READER();
    EpochMetaData metadata;
    metadata.deserialize(reader,
                         /*evbuffer_zero_copy=*/false,
                         /*expected_size=*/{entry_size},
                         logid,
                         cfg);
    CHECK_READER();
    const epoch_t since = metadata.h.effective_since;
    auto result = epoch_map.insert(std::make_pair(since, std::move(metadata)));
    if (!result.second) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Deserialization failed: Duplicate entries with "
                      "effective_since %u found.",
                      metadata.h.effective_since.val_);
      reader.setError(E::BADMSG);
      return nullptr;
    }
  }

#undef CHECK_READER
  ld_check(epoch_map.size() == header.num_entries);
  return create(std::make_shared<const Map>(std::move(epoch_map)),
                epoch_t(header.effective_until));
}

/*static*/
std::shared_ptr<const EpochMetaDataMap>
EpochMetaDataMap::deserialize(Slice buffer,
                              size_t* bytes_read,
                              logid_t logid,
                              const NodesConfiguration& cfg) {
  ProtocolReader reader(buffer, "EpochMetaDataMap", 0);
  auto map = deserialize(reader, false, logid, cfg);
  if (map == nullptr) {
    return nullptr;
  }

  ld_check(reader.ok());
  if (bytes_read != nullptr) {
    *bytes_read = reader.bytesRead();
  }
  return map;
}

std::string EpochMetaDataMap::toString() const {
  std::string result = "{";
  for (const auto& kv : *epoch_map_) {
    result += kv.second.toString();
    result += ",";
  }
  result += (" Until:" + std::to_string(effective_until_.val_) + "}");
  return result;
}

}} // namespace facebook::logdevice
