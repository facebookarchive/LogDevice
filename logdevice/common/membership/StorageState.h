/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace facebook { namespace logdevice { namespace membership {

/**
 * Enumerates possible storage states for a single storage shard.
 */
enum class StorageState : uint8_t {

  // For a more formal and accurate description, checkout the cluster
  // membership design spec document.
  // TODO T33035439: link to doc/storage_config.md when the doc is ready

  // The storage shard is empty and considered not part of the cluster.
  // can write to (defined below): no
  // should read from (defined below): no
  NONE = 0,

  // The storage shard is about to join the cluster as a read-only shard.
  // can write to: no
  // should read from: yes
  NONE_TO_RO,

  // The storage shard is read-only. It can happen when 1) the shard has just
  // joined the the cluster or 3) the shard needs to be disabled and it is
  // going to get its data migrated out before removal.
  // can write to: no
  // should read from: yes
  READ_ONLY,

  // The storage shard is fully functional for both read and writes. This is the
  // common "healthy" state of a storage shard.
  // can write to: yes
  // should read from: yes
  READ_WRITE,

  // The storage shard is about to become read-only but there can be writers
  // with old version of the config which can still successful perform a write
  // with shard being part of the copyset.
  // can write to: no
  // should read from: yes
  RW_TO_RO,

  // The storage shard is READ_ONLY wrt readers and writers. In addition, a data
  // migration process is going on (e.g., rebuilding) that relocates copies of
  // data originally stored on this shard to other part of the cluster.
  // can write to: no
  // should read from: yes
  DATA_MIGRATION,

  INVALID,

  // This indicates that the shard is newly added and it didn't yet ack
  // the health of its underlying storage. Newly added shards for the first time
  // start in this state and are treated exactly the same as a NONE state.
  // Once the shard transitions out of this state, it can't go back to it.
  //
  // The provisioning that the shard needs to do before marking itself as
  // provisioned is to check that:
  //   1. It's underlying local storate is readable.
  //   2. It persistently stored the metadata indicating the shard is not
  //      missing any data (i.e., RebuildingCompleteMetadata).
  //   3. The local store must be empty.
  // can write to: no
  // should read: no
  PROVISIONING
};

/**
 * If true, writers are allowed to pick the storage shard to store copies of
 * record for a write operation.
 */
constexpr bool canWriteTo(StorageState state) {
  return state == StorageState::READ_WRITE;
}

/**
 * If true, readers must read from the storage shard before reporting dataloss
 * for any record.
 */
constexpr bool shouldReadFrom(StorageState state) {
  return state == StorageState::READ_WRITE ||
      state == StorageState::READ_ONLY || state == StorageState::NONE_TO_RO ||
      state == StorageState::RW_TO_RO || state == StorageState::DATA_MIGRATION;
}

/**
 * Returns whether the state is considered intermediary to the membership
 * protocol, i.e., whether it indicates the first phase of the 2PC.
 */
constexpr bool isIntermediaryState(StorageState state) {
  return state == StorageState::NONE_TO_RO || state == StorageState::RW_TO_RO;
}

constexpr StorageState toNonIntermediaryState(StorageState state) {
  switch (state) {
    case StorageState::NONE_TO_RO:
      return StorageState::READ_ONLY;
    case StorageState::RW_TO_RO:
      return StorageState::READ_ONLY;
    default:
      return state;
  }
}

/**
 * Metadata storage state w.r.t. if the storage shard is also storing metadata
 * for LogDevice.
 *
 * Besides storage shards that stores regular data, storage membership supports
 * maintaining a separate membership for metadata storage and provides ways to
 * change the membership. Storage shards in the metadata membership are called
 * "metadata storage shards". Metadata storage shards can store _both_ regular
 * data and metadata.
 */
enum class MetaDataStorageState : uint8_t {
  // the shard does not store metadata.
  NONE = 0,

  // the shard is a metadata storage shard.
  METADATA,

  // A regular storage shard of READ_WRITE state is being promoted to become
  // a metadata shard.
  // Note: PROMOTING always means that corresponding storage state is READ_WRITE
  PROMOTING,

  INVALID
};

/**
 * If true, writers of metadata are allowed to pick the storage shard to store
 * copies of record for a metadata write operation.
 */
constexpr bool canWriteMetaDataTo(StorageState state,
                                  MetaDataStorageState metadata_state) {
  return metadata_state == MetaDataStorageState::METADATA && canWriteTo(state);
}

/**
 * If true, metadata readers must read from the storage shard before reporting
 * dataloss for any metadata record.
 */
constexpr bool shouldReadMetaDataFrom(StorageState state,
                                      MetaDataStorageState metadata_state) {
  return ((metadata_state == MetaDataStorageState::METADATA ||
           metadata_state == MetaDataStorageState::PROMOTING) &&
          shouldReadFrom(state));
}

/**
 * Returns whether the state is considered intermediary to the membership
 * protocol, i.e., whether it indicates the first phase of the 2PC.
 */
constexpr bool isIntermediaryState(MetaDataStorageState metadata_state) {
  return metadata_state == MetaDataStorageState::PROMOTING;
}

constexpr MetaDataStorageState
toNonIntermediaryState(MetaDataStorageState metadata_state) {
  switch (metadata_state) {
    case MetaDataStorageState::PROMOTING:
      return MetaDataStorageState::METADATA;
    default:
      return metadata_state;
  }
}

namespace StorageStateFlags {

using Type = uint32_t;

static constexpr Type NONE = 0;

// This special flag indicates a combination of three conditions:
// 1) the storage shard has permanently lost some or all (most likely) of its
//    data copies; and
// 2) the storage shard must be aware of that it is missing data if it is
//    available, and it should never send NO_RECORD response to readers; and
// 3) the storage shard must reject all incoming write copies from writers.
//
// The flags is usually applied by the cluster admin in situations that the
// cluster lost too many nodes/shards in rw or rw2ro state so that it is
// impossible (i.e., no f-majority subset available) to make the transition
// rw2ro -> ro and start data migration. It's also used by readers to skip
// forward and report dataloss when too many shards are UNRECOVERABLE.
//
// After data migration is completed and the storage shard became empty
// (NONE state), the UNRECOVERABLE flag becomes irrelevant and it gets cleared.
// However, to re-enable the shard maintenance must ensures that condition 2)
// and 3) will no longer hold before making the NONE->NONE_TO_RO transition.
static constexpr Type UNRECOVERABLE = 1u << 0;

std::string toString(Type flags);
//// Note: change toString() implementation in utils.cpp accordingly
//// when adding new flags

} // namespace StorageStateFlags

}}} // namespace facebook::logdevice::membership
