/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstring>
#include <vector>

#include "logdevice/common/SerializableData.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/EpochMetaDataVersion.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file: EpochMetaData contains metadata information for a particular epoch of
 *        a data log. It is stored in both the epoch store and the metadata
 *        log. The class also specifies the storage format of the metadata
 *        information in record payload of metadata logs, and provides utilities
 *        to construct, read and update the metadata.
 */

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class MetaDataTracer;
struct EpochStoreMetaProperties;
struct Payload;

typedef uint32_t epoch_metadata_flags_t;

struct MetaDataLogRecordHeader {
  epoch_metadata_version::type version; // version number of the header format
                                        // should be in the first field of the
                                        // header

  epoch_t epoch;           // current epoch of the data log
  epoch_t effective_since; // epoch since which this metadata is
                           // in effect

  nodeset_size_t nodeset_size; // size of the nodeset

  // These two fields are deprecated. Use EpochMetaData::replication instead.
  // They're only used by EpochMetaData serialization/deserialization.
  // Unfortunately, they can't be made private while keeping this struct
  // a "standard layout type".

  copyset_size_t replication_factor_DO_NOT_USE;

  // Since version 2

  NodeLocationScope sync_replication_scope_DO_NOT_USE;

  // Additional flags. In order to add a new field to epoch metadata, reserve
  // one more bit in _flags_, and append the actual data for the field at the
  // end of the binary data section after the header and nodeset array.
  epoch_metadata_flags_t flags;

  // the epoch metadata has been successfully written and fully replicated
  // (with confirmation from the sequencer) in the metadata log
  static constexpr epoch_metadata_flags_t WRITTEN_IN_METADATALOG = 1u << 1; // 2

  // the metadata indicates that the log is disabled and its metadata should
  // no longer be used
  static constexpr epoch_metadata_flags_t DISABLED = 1u << 2; // 4

  // if set, the epoch metadata contains weights for nodeset entries;
  // don't use this flag directly, it's only used by EpochMetaData
  // serialization/deserialization methods.
  static const epoch_metadata_flags_t HAS_WEIGHTS = 1u << 3; // 8

  // if set, the serialized epoch metadata contains ReplicationProperty;
  // this only affects deserialization.
  static const epoch_metadata_flags_t HAS_REPLICATION_PROPERTY = 1u << 4; // 16

  // If set, the serialized epoch metadata contains NodeSetParams::signature.
  // Don't use this flag directly, it's only used by EpochMetaData
  // serialization/deserialization methods.
  static const epoch_metadata_flags_t HAS_NODESET_SIGNATURE = 1u << 5; // 32

  // if set, the serialized epoch metadata contains a StorageSet instead of
  // NodeSetIndices.
  static const epoch_metadata_flags_t HAS_STORAGE_SET = 1u << 6; // 64

  static const epoch_metadata_flags_t HAS_TARGET_NODESET_SIZE_AND_SEED = 1u
      << 7; // 128

  static const epoch_metadata_flags_t HAS_TIMESTAMPS = 1u << 8; // 256

  static const epoch_metadata_flags_t ALL_KNOWN_FLAGS = WRITTEN_IN_METADATALOG |
      DISABLED | HAS_WEIGHTS | HAS_REPLICATION_PROPERTY |
      HAS_NODESET_SIGNATURE | HAS_STORAGE_SET |
      HAS_TARGET_NODESET_SIZE_AND_SEED | HAS_TIMESTAMPS;

  // return the actual header size in payload for different versions
  static size_t headerSize(epoch_metadata_version::type version) {
    static_assert(sizeof(MetaDataLogRecordHeader) == 20,
                  "MetaDataLogRecordHeader size changed");

    if (version < 2) { // this is the version that introduces the _flag_ field
      return offsetof(
          MetaDataLogRecordHeader, sync_replication_scope_DO_NOT_USE);
    }

    // the size of the Header should not ever change since version 2
    return sizeof(MetaDataLogRecordHeader);
  }

  static bool isValid(const MetaDataLogRecordHeader& h) {
    return epoch_metadata_version::validToRead(h.version) &&
        h.effective_since <= h.epoch && h.epoch <= EPOCH_MAX;
  }

  // Sticky copysets flag in metadata is deprecated
  static const epoch_metadata_flags_t STICKY_COPYSETS_DEPRECATED = 1u << 0; // 1

 private:
  friend class EpochMetaData;
} __attribute__((__packed__));

// TODO(TT15517759): inherit from SerializableData when StorageSet are used
// everywhere and fromPayload() does not need to take logid and config as
// input parameters.
class EpochMetaData {
 public:
  // Information about how the current nodeset was generated. Used for deciding
  // when nodeset needs updating and for generating new nodeset.
  // Can be changed without bumping epoch or reactivating sequencer, as
  // long as other parts of EpochMetaData remain the same.
  struct NodeSetParams {
    // A value produced by nodeset selector to identify the nodeset.
    // Can be used for determining whether nodeset needs to be changed.
    // Calculating and using (or not using) the signature is up to
    // NodeSetSelector implementations.
    // Usually it's either zero (if nodeset selector doesn't use signatures)
    // or equal to a hash of nodeset selector's inputs (such as nodes config
    // and target nodeset size).
    // See comments in NodeSetSelector.h for details.
    //
    // Note: Signature is only really needed for nondeterministic or slow
    // nodeset selectors. It may be worth considering making all nodeset
    // selectors deterministic and getting rid of this field. Note that any
    // nodeset selector can be easily made deterministic by seeding its RNG with
    // the log ID.
    uint64_t signature = 0;

    // What was the target nodeset size when selecting the nodeset in this
    // EpochMetaData. It can be different from the actual `shards.size()`,
    // e.g. if some nodes were unwritable and we picked more writable nodes to
    // compensate, or if we needed more nodes to reach minimum required
    // number of nodes per rack, or if there were fewer nodes in config than
    // requested nodeset size.
    // Used for avoiding nodeset resizing if the change in target size is small.
    // Zero means that the EpochMetaData was produced by an older version of
    // the code that didn't support this feature.
    nodeset_size_t target_nodeset_size = 0;

    // Random seed used for generating nodeset. Used for forcing deterministic
    // nodeset selectors to randomize nodeset.
    uint64_t seed = 0;

    bool operator==(const NodeSetParams& rhs) const;
    bool operator!=(const NodeSetParams& rhs) const;

    std::string toString() const;
  };

  // init epoch metadata to the empty state
  explicit EpochMetaData()
      : h{0, EPOCH_INVALID, EPOCH_INVALID, 0, 0, NodeLocationScope::NODE, 0} {}

  EpochMetaData(StorageSet storage_set,
                ReplicationProperty r,
                epoch_t epoch = EPOCH_MIN,
                epoch_t effective_since = EPOCH_MIN,
                std::vector<double> _weights = {})
      : shards(std::move(storage_set)),
        replication(std::move(r)),
        weights(std::move(_weights)) {
    h.version = epoch_metadata_version::CURRENT;
    h.epoch = epoch;
    h.effective_since = effective_since;
    h.nodeset_size = static_cast<nodeset_size_t>(shards.size());
    h.flags = 0;

    h.replication_factor_DO_NOT_USE =
        std::numeric_limits<copyset_size_t>::max();
    h.sync_replication_scope_DO_NOT_USE = NodeLocationScope::INVALID;
  }

  // Set epoch increment timestamp to current.
  void setEpochIncrementAt();

  // Change the storage set.
  void setShards(StorageSet shards);

  // check if the object represents valid epoch metadata
  bool isValid() const;

  // check if the metadata is valid with the given cluster configuration @cfg
  // for a particular @logid. This just validates that the log exists in the
  // logs config and that the nodeset is valid with a given nodes config.
  bool validWithConfig(
      logid_t logid,
      const std::shared_ptr<Configuration>& cfg,
      const std::shared_ptr<const NodesConfiguration>& nodes_cfg) const;

  // reset to empty state
  void reset();

  // check if the metdata is in the empty state
  bool isEmpty() const;

  //
  // TODO(TT15517759): inherit from SerializableData
  void serialize(ProtocolWriter& writer) const;
  void deserialize(ProtocolReader& reader,
                   bool evbuffer_zero_copy,
                   folly::Optional<size_t> expected_size,
                   logid_t logid,
                   const NodesConfiguration& cfg);

  /**
   * Fill the content by reading epoch metadata from a record payload
   *
   * @return  0 on success, -1 on failure and set err to:
   *          BADMSG  content of payload data does not contain valid
   *                  epoch metadata
   *
   * TODO(TT15517759): do not need logid or cfg once all state machines are
   * converted to use StorageSet.
   */
  int fromPayload(const Payload& payload,
                  logid_t logid,
                  const NodesConfiguration& cfg);

  /**
   * Copy the serialized data into a pre-allocated payload
   * payload must be contiguous and have valid length of @size.
   * @size must be at least sizeInPayload().
   *
   * @return  actual bytes copied on success,
   *          -1 on failure, and set err to:
   *           INVALID_PARAM  object does not have valid epoch metadata
   *                          or payload is invalid
   *           NOBUFS         @size is not enough to hold the data
   */
  int toPayload(void* payload, size_t size) const;

  /**
   * Returns serialized size, or -1 if the epoch metadata is invalid.
   */
  int sizeInPayload() const;

  // @return: a std::string containing serialized data in binary form;
  //          empty string if object is invaid
  std::string toStringPayload() const;

  // @return: if the EpochMetadata indicates that its content has been
  //          successfully written to the metadata log
  bool writtenInMetaDataLog() const;

  // @return: if the EpochMetadata indicates that the log has been
  //          disabled
  bool disabled() const;

  // check if epoch metadata is _exactly_ the same as @param rhs
  bool operator==(const EpochMetaData& rhs) const;

  // Checks if epoch metadata is the same as @param rhs, except for current
  // epoch, WRITTEN_IN_METADATA_LOG flag and nodeset_params.
  // These fields are not needed in metadata logs. They're only written there
  // to keep the serialization format the same between epoch store and metadata
  // logs.
  bool identicalInMetaDataLog(const EpochMetaData& rhs) const;

  bool operator!=(const EpochMetaData& rhs) const;

  // return a human readable string to show the content of metadata
  std::string toString() const;

  static std::string flagsToString(epoch_metadata_flags_t flags);

 public:
  MetaDataLogRecordHeader h;
  StorageSet shards; // size must be equal h.nodeset_size

  ReplicationProperty replication;
  // Weights of elements of `nodes`. If empty, weights from config should be
  // used (with some preprocessing to account for weight-unaware nodeset
  // selection). Otherwise size must be equal to h.nodeset_size.
  std::vector<double> weights;

  NodeSetParams nodeset_params;

  RecordTimestamp replication_conf_changed_at;
  RecordTimestamp epoch_incremented_at;

 public:
  /*
   * The enum defines states that describe the change between the previous epoch
   * metadata and the newly generated epoch metadata. This exact information is
   * then used to determine whether and how to perform sequencer reactivation:
   *
   * UNCHANGED:
   * Neither the nodeset changed nor the logs config or the nodeset params.
   *
   * ONLY_NODESET_PARAMS_CHANGED:
   * Just the nodeset params changed, not the log config or the nodeset itself.
   * Only need to update the epoch store. We can perform this action now
   * since it is cheap to do.
   *
   * NONSUBSTANTIAL_RECONFIGURATION:
   * The nodeset selector selected a new nodeset but none of the logs config
   * (replication factor or target nodeset size). We need reactivation but it
   * can be delayed.
   *
   * Note that this state is returned even if the signature in the NodesetParams
   * changed as long as just the signature and nothing else changed. The
   * Signature is a hash over a number of input parameters and as long as none
   * of the other parameters changed we can safely assume that the signature
   * changed because the Nodes config hash changed. Thich implies that one or
   * more nodes changed their state or that there was an expansion or a shrink.
   *
   * SUBSTANTIAL_RECONFIGURATION: Either nodeset, or repl factor, or one of the
   * nodeset params (target nodeset size or seed) changed. Update the metadata
   * log and perform reactivation now.
   *
   * CREATED:
   * A new metadata was generated.
   *
   * FAILED: Nothing to do. Return error.
   */
  enum class UpdateResult : uint8_t {
    UNCHANGED = 0,
    ONLY_NODESET_PARAMS_CHANGED,
    NONSUBSTANTIAL_RECONFIGURATION,
    SUBSTANTIAL_RECONFIGURATION,
    CREATED,
    FAILED,
  };

  /**
   * Updater is a type of callable that performs an update on an existing
   * EpochMetaData object. It takes logid and a pointer to an EpochMetaData
   * object as input, performs update on metadata and returns an UpdateResult.
   * If incoming metadata is a nullptr, that indicates that the metadata has
   * not been provisioned. This is different from empty metadata that might be
   * observed after provisioning. In the empty metadata case the pointer would
   * not be nullptr, but ptr->isEmpty() would return `true`.
   *
   * Example use of Updaters could be to provision the initial
   * metadata, update metadata in epochstore, etc.
   *
   * @return     could be one of
   *             UpdateResult::UNCHANGED  EpochMetaData stays the same
   *             UpdateResult::SUBSTANTIAL_RECONFIGURATION    EpochMetaData is
   *                                                          updated
   *             UpdateResult::CREATED    new EpochMetaData has been created
   *             UpdateResult::FAILED     failed to perform the update
   *
   *             The Updater must guarantee that if result is not FAILED,
   *             the EpochMetaData object is valid after the call.
   */
  class Updater {
   public:
    virtual UpdateResult operator()(logid_t,
                                    std::unique_ptr<EpochMetaData>&,
                                    MetaDataTracer* tracer) = 0;

    using CompletionFunction =
        std::function<void(Status,
                           logid_t,
                           std::unique_ptr<EpochMetaData>,
                           std::unique_ptr<EpochStoreMetaProperties>)>;

    virtual ~Updater() {}
  };

  /**
   * Convert a NodeSetIndices to a StorageSet.
   *
   * Converts node_index_t values to ShardIDs where the shard_index_t component
   * is 0 by default.
   *
   * TODO(TT15517759): this function will be removed once all state machines are
   * converted to use StorageSet.
   */
  static StorageSet nodesetToStorageSet(const NodeSetIndices& indices,
                                        shard_index_t shard_id = 0);

  /**
   * Convert a NodeSetIndices to a StorageSet.
   *
   * Computes the shard_index_t component of ShardID from the number of shards
   * configured for storage nodes on the cluster and the logid using
   * getLegacyShardIndexForLog().
   *
   * TODO(TT15517759): this function will be removed once all state machines are
   * converted to use StorageSet.
   */
  static StorageSet nodesetToStorageSet(
      const NodeSetIndices& indices,
      logid_t logid,
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  /**
   * Convert a NodeSetIndices to a StorageSet.
   * TODO(TT15517759): this function will be removed once EpochMetaData defaults
   * to serializing a storage_set instead of a nodeset.
   */
  static NodeSetIndices storageSetToNodeset(const StorageSet& storage_set);

  /**
   * get the EpochMetaData for the metadata log for @param logid
   */
  static EpochMetaData genEpochMetaDataForMetaDataLog(
      logid_t logid,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      epoch_t epoch = EPOCH_MIN,
      epoch_t effective_since = EPOCH_MIN);
};

}} // namespace facebook::logdevice
