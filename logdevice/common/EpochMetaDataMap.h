/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <memory>

#include "logdevice/common/EpochMetaData.h"

namespace facebook { namespace logdevice {

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

/**
 * @file: EpochMetaDataMap is the collection of EpochMetaData for a log upto
 *        a certain `effective until' epoch. It contains the historical and/or
 *        current metadata information that was stored in metadata logs and/or
 *        epoch store. It provides method for query EpochMetaData for a given
 *        epoch in the effective range.
 */

struct EpochMetaDataMapHeader {
  using flags_t = uint32_t;
  using entry_size_t = uint32_t;

  flags_t flags;
  uint32_t effective_until;
  uint32_t num_entries;
};

/**
 * serialization format:
 * fixed size header + num_entries * (entry_size + serialized EpochMetaData)
 */
static_assert(sizeof(EpochMetaDataMapHeader) == 12,
              "EpochMetaDataMapHeader is not packed.");

// TODO(TT15517759): inherit from SerializableData when EpochMetaData does so
class EpochMetaDataMap {
 public:
  using Map = std::map<epoch_t, EpochMetaData>;

  static std::shared_ptr<const EpochMetaDataMap>
  create(std::shared_ptr<const Map> epoch_map, epoch_t effective_until);

  /**
   * Create a new epoch metadata map with a different effective_until.
   */
  std::shared_ptr<const EpochMetaDataMap>
  withNewEffectiveUntil(epoch_t new_effective_until) const;

  /**
   * Creating a new epoch metadata map by adding a new entry the end. Rules:
   * 1) effective since of the new metadata must be larger than the existing
   * effective until, and 2) new_effective_until must not be smaller than
   * effective since of the new entry. Otherwise nullptr will be returned.
   */
  std::shared_ptr<const EpochMetaDataMap>
  withNewEntry(const EpochMetaData& new_metadata,
               epoch_t new_effective_until) const;

  /**
   * Get EpochMetaData for a given epoch.
   *
   * @return        a unique_ptr containing the results, or
   *                nullptr with err set to E::INVALID_PARAM if the requested
   *                epoch is larger than effective_until
   */
  std::unique_ptr<EpochMetaData> getEpochMetaData(epoch_t epoch) const;

  /**
   * Get the last known epoch metadata from the map.
   *
   * @return        a unique_ptr constaining the results, or nullptr if the
   *                map is empty
   */
  std::unique_ptr<EpochMetaData> getLastEpochMetaData() const;

  /**
   * Get EpochMetaData for a given epoch which must not be larger than
   * effective_until.
   *
   * @return        a const reference of the result metadata. Undefined
   *                behavior if the requested epoch is larger than
   *                effective_until
   */
  const EpochMetaData& getValidEpochMetaData(epoch_t epoch) const;

  /**
   * Computes the union of StorageSet_s for epochs in [min_epoch, max_epoch].
   *
   *  @return       a unique_ptr containing the results, or
   *                nullptr with err set to E::INVALID_PARAM if the requested
   *                epoch is larger than effective_until
   */
  std::unique_ptr<StorageSet> getUnionStorageSet(epoch_t min_epoch,
                                                 epoch_t max_epoch) const;

  /**
   * Same as the one above, but filters out non-storage shard or shards that no
   * longer exist in nodes configuration
   */
  std::unique_ptr<StorageSet> getUnionStorageSet(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      epoch_t min_epoch,
      epoch_t max_epoch) const;

  // convenience overrides for getting union of storage sets in
  // [EPOCH_MIN, effective_until_]. should not return nullptr.
  std::unique_ptr<StorageSet> getUnionStorageSet() const;
  std::unique_ptr<StorageSet> getUnionStorageSet(
      const configuration::nodes::NodesConfiguration& nodes_configuration)
      const;

  /**
   * Computes the "minimum" replication property for epochs between min and max
   * (inclusive). Use this when you need to read from the union nodeset.
   *
   *  @return       a unique_ptr containing the results, or
   *                nullptr with err set to E::INVALID_PARAM if the requested
   *                epoch is larger than effective_until
   */
  std::unique_ptr<ReplicationProperty>
  getNarrowestReplication(epoch_t min_epoch,
                          epoch_t max_epoch = EPOCH_MAX) const;

  std::shared_ptr<const Map> getMetaDataMap() const {
    return epoch_map_;
  }

  epoch_t getEffectiveUntil() const {
    return effective_until_;
  }

  /**
   * See SerializableData::serialize().
   */
  void serialize(ProtocolWriter& writer) const;

  /**
   * See SerializableData::deserialize().
   * Note: EpochMetaDataMap is an immutable object. As a result, this
   * method is disabled. use the static deserialize() method instead.
   */
  void deserialize(ProtocolReader& reader,
                   bool evbuffer_zero_copy,
                   folly::Optional<size_t> expected_size,
                   logid_t logid,
                   const NodesConfiguration& cfg);

  const char* name() const {
    return "EpochMetaDataMap";
  }

  /**
   * See SerializableData::serialize().
   */
  int serialize(void* buffer, size_t size) const;

  /**
   * See SerializableData::sizeInLinearBuffer().
   */
  ssize_t sizeInLinearBuffer() const;

  /**
   * @return   a human readable string for logging and debug
   */
  std::string toString() const;

  bool operator==(const EpochMetaDataMap& rhs) const {
    return *epoch_map_ == *rhs.epoch_map_ &&
        effective_until_ == rhs.effective_until_;
  }

  /**
   * static methods to create EpochMetaDataMap object from various buffer.
   */
  static std::shared_ptr<const EpochMetaDataMap>
  deserialize(ProtocolReader& reader,
              bool evbuffer_zero_copy,
              logid_t logid,
              const NodesConfiguration& cfg);

  static std::shared_ptr<const EpochMetaDataMap>
  deserialize(Slice buffer,
              size_t* bytes_read,
              logid_t logid,
              const NodesConfiguration& cfg);

  using EpochInterval = std::pair<epoch_t, epoch_t>;
  using EpochMapping = std::pair<EpochInterval, const EpochMetaData&>;
  class const_iterator
      : public std::iterator<std::input_iterator_tag, EpochMapping> {
   public:
    using InnerIt = decltype(Map().cbegin());
    const_iterator(const EpochMetaDataMap& owner, const Map& map, InnerIt&& it)
        : owner_(owner), map_(map), it_(std::move(it)) {
      ld_check(map.size() >= 1);
    }
    const_iterator& operator++() {
      it_++;
      return *this;
    }
    bool operator==(const const_iterator& rhs) const {
      return it_ == rhs.it_;
    }
    bool operator!=(const const_iterator& rhs) const {
      return !(*this == rhs);
    }
    EpochMapping operator*() {
      auto nxt = std::next(it_);
      auto epoch_first = (it_ == map_.cbegin() ? EPOCH_MIN : it_->first);
      auto epoch_last = (nxt == map_.cend() ? owner_.getEffectiveUntil()
                                            : previous_epoch(nxt->first));
      ld_check(epoch_first <= epoch_last);
      return EpochMapping(EpochInterval(epoch_first, epoch_last), it_->second);
    }

   private:
    const EpochMetaDataMap& owner_;
    const Map& map_;
    InnerIt it_;
  };

  const_iterator begin() const {
    return const_iterator(*this, *epoch_map_.get(), epoch_map_->cbegin());
  }

  const_iterator end() const {
    return const_iterator(*this, *epoch_map_.get(), epoch_map_->cend());
  }

  const_iterator find(epoch_t epoch) const {
    return const_iterator(*this, *epoch_map_.get(), getIteratorForEpoch(epoch));
  }

 private:
  // private constructor,
  EpochMetaDataMap(std::shared_ptr<const Map> epoch_map,
                   epoch_t effective_until);

  const std::shared_ptr<const Map> epoch_map_;
  const epoch_t effective_until_;

  // return a const iterator which points to the correct epoch metadata
  // for the given epoch. _epoch_ must be <= effective_until_
  Map::const_iterator getIteratorForEpoch(epoch_t epoch) const;

  bool isValid() const {
    return isValid(*epoch_map_, effective_until_);
  }

  static bool isValid(const Map& epoch_map, epoch_t effective_until);
};

}} // namespace facebook::logdevice
