/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/SequencersConfig.h"
#include "logdevice/common/configuration/nodes/MetaDataLogsReplication.h"
#include "logdevice/common/configuration/nodes/SequencerConfig.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"
#include "logdevice/common/configuration/nodes/StorageConfig.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

/**
 * @file NodesConfiguration  NodesConfiguration consists of the following three
 * different components:
 *
 * 1) ServiceDiscoveryConfig: node properties that stay immutable all the time
 * after the initial provisioning. This includes ip/port, location, and
 * provisioned roles, etc.
 *
 * 2) (per-role) Membership: These mutable node attributes need agreement among
 * node members and usually require a synchronization protocol and have to be
 * versioned. Example include: storage membership (contains storage state),
 * sequencer membership(contains sequencer weight that affects sequencer
 * placement).
 *
 * 3) (per-role) NodeAttributesConfig: mutable node attributes that does not
 * require versioning or synchronization between nodes. These are the attributes
 * can be changed freely without worrying about correctness and does not need a
 * config synchronization protocol to achieve agreement. Example includes:
 * storage_weight, compaction schedule, etc.
 *
 * 4) MetaDatqLogsReplication: defines replication properties of
 * metadata logs.  Although this doesn't directly describe node
 * properties, it strongly corelates with metadata storage membership and
 * changing the replication property usually requries.
 */

class NodesConfiguration {
 public:
  struct Update {
    std::unique_ptr<ServiceDiscoveryConfig::Update> service_discovery_update{
        nullptr};
    std::unique_ptr<SequencerConfig::Update> sequencer_config_update{nullptr};
    std::unique_ptr<StorageConfig::Update> storage_config_update{nullptr};
    std::unique_ptr<MetaDataLogsReplication::Update> metadata_logs_rep_update{
        nullptr};

    membership::MaintenanceID::Type maintenance{
        membership::MaintenanceID::MAINTENANCE_NONE};
    std::string context{};

    bool isValid() const;
    bool hasAllUpdates() const;
    bool empty() const;
    std::string toString() const;
  };

  // create an empty nodes config
  explicit NodesConfiguration();

  //////////////////////// Accessors ///////////////////////////
  const std::shared_ptr<const ServiceDiscoveryConfig>&
  getServiceDiscovery() const {
    return service_discovery_;
  }

  // @return  cluster size as the number of nodes that have service discovery
  // info. Note that some of the nodes may not be in the membership.
  size_t clusterSize() const {
    return service_discovery_->numNodes();
  }

  virtual const NodeServiceDiscovery*
  getNodeServiceDiscovery(node_index_t node) const;

  // note: return nullptr if generation number mismatches
  virtual const NodeServiceDiscovery*
  getNodeServiceDiscovery(NodeID node) const;

  bool isNodeInServiceDiscoveryConfig(node_index_t node) const {
    return service_discovery_->hasNode(node);
  }

  // Checks whether this node has sequencer role
  bool isSequencerNode(node_index_t node) const {
    return sequencer_config_->getMembership()->hasNode(node);
  }

  // Checks whether this node has storage role
  bool isStorageNode(node_index_t node) const {
    return storage_config_->getMembership()->hasNode(node);
  }

  const std::shared_ptr<const SequencerConfig>& getSequencerConfig() const {
    return sequencer_config_;
  }

  const std::shared_ptr<const StorageConfig>& getStorageConfig() const {
    return storage_config_;
  }

  const std::shared_ptr<const StorageAttributeConfig>&
  getStorageAttributes() const {
    return storage_config_->getAttributes();
  }

  // return nullptr if node does not exist or does not have storage role
  const StorageNodeAttribute* getNodeStorageAttribute(node_index_t node) const;

  // helper function to get the storage capcity of a shard's node if the shard
  // is writable in the storage membership. return 0 if the shard is not
  // writable.
  double getWritableStorageCapacity(ShardID shard) const;

  // note: return default generation 1 for nodes not having storage role or
  // node not existed
  node_gen_t getNodeGeneration(node_index_t node) const;

  // note: does not check if node exists in the config. If not exists,
  // NodeID(node, 1) will be returned
  NodeID getNodeID(node_index_t node) const {
    return NodeID(node, getNodeGeneration(node));
  }

  // return  0 if node is not a storage node
  shard_size_t getNumShards(node_index_t node) const;

  const std::shared_ptr<const MetaDataLogsReplication>&
  getMetaDataLogsReplication() const {
    return metadata_logs_rep_;
  }

  const std::shared_ptr<const membership::SequencerMembership>&
  getSequencerMembership() const {
    return sequencer_config_->getMembership();
  }

  const std::shared_ptr<const membership::StorageMembership>&
  getStorageMembership() const {
    return storage_config_->getMembership();
  }

  // @return  the list of storage nodes in the current storage membership
  std::vector<node_index_t> getStorageNodes() const {
    return getStorageMembership()->getMembershipNodes();
  }

  // @return  the list of nodes in the current sequencer membership
  std::vector<node_index_t> getSequencerNodes() const {
    return getSequencerMembership()->getMembershipNodes();
  }

  std::shared_ptr<const NodesConfiguration>
  applyUpdate(const Update& update) const;

  // validations
  bool serviceDiscoveryConsistentWithMembership() const;
  bool membershipVersionsConsistentWithVersion() const;
  bool validate(bool validate_metadata = true) const;
  bool validateConfigMetadata() const;

  uint64_t getStorageNodesHash() const {
    return storage_hash_;
  }

  // TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
  shard_size_t getNumShards() const {
    return num_shards_;
  }

  membership::MembershipVersion::Type getVersion() const {
    return version_;
  }

  SystemTimestamp getLastChangeTimestamp() const {
    using namespace std::chrono;
    auto dur = std::chrono::duration_cast<system_clock::duration>(
        std::chrono::milliseconds{last_change_timestamp_});
    return system_clock::time_point{dur};
  }

  node_index_t getMaxNodeIndex() const {
    return max_node_index_;
  }

  const SequencersConfig& getSequencersConfig() const {
    return sequencer_locator_config_;
  }

  // TODO(T33035439): this should only be used in migration or emergency. Config
  // version bump should be automatically handled through Update.
  void setVersion(membership::MembershipVersion::Type version) {
    version_ = version;
  }

  // For testing purposes:
  //
  // returns a new config with an incremented version and touch the
  // last_change_timestamp_.
  //
  // @param new_nc_version should either be folly::none, in which case the new
  // version will be the current version + 1, or be strictly greater than the
  // current version.
  //
  // @param new_sequencer_membership_version and new_storage_membership_version
  // work similarly. Note that one can bump the NC version without bumping the
  // underlying membership versions; however, bumping the membership versions
  // will also bump the NC version.
  //
  // @return the new config or nullptr if (any one of the supplied)
  // new_version(s) is <= (the corresponding) current version
  std::shared_ptr<const NodesConfiguration> withIncrementedVersionAndTimestamp(
      folly::Optional<membership::MembershipVersion::Type> new_nc_version =
          folly::none,
      folly::Optional<membership::MembershipVersion::Type>
          new_sequencer_membership_version = folly::none,
      folly::Optional<membership::MembershipVersion::Type>
          new_storage_membership_version = folly::none,
      std::string context = "manual touch") const;

  std::shared_ptr<const NodesConfiguration>
  withVersion(membership::MembershipVersion::Type version) const;

  bool operator==(const NodesConfiguration& rhs) const;

  // same as == operator but with config timestamp ignored
  bool equalWithTimestampIgnored(const NodesConfiguration& rhs) const;

  // same as == operator but with config timestamp and version ignored
  bool equalWithTimestampAndVersionIgnored(const NodesConfiguration& rhs) const;

  virtual ~NodesConfiguration() = default;

 private:
  std::shared_ptr<const ServiceDiscoveryConfig> service_discovery_{nullptr};
  std::shared_ptr<const SequencerConfig> sequencer_config_{nullptr};
  std::shared_ptr<const StorageConfig> storage_config_{nullptr};
  std::shared_ptr<const MetaDataLogsReplication> metadata_logs_rep_{nullptr};

  ///// configuration metadata

  // provide a total order of config updates. However, we do not use this
  // version for synchronoization or any correctness purpose. Instead we use the
  // version in Membership for each role
  membership::MembershipVersion::Type version_{
      membership::MembershipVersion::EMPTY_VERSION};
  uint64_t storage_hash_{};

  // TODO(T15517759): NodesConfigParser currently verifies that all nodes in the
  // config have the same amount of shards, which is also stored here. This
  // member will be removed when the Flexible Log Sharding project is fully
  // implemented. In the mean time, this member is used by state machines that
  // need to convert node_index_t values to ShardID values.
  shard_size_t num_shards_{};
  node_index_t max_node_index_{};

  // SeqeuncersConfig is the current data structure used by SequencerLocator and
  // sequencer routing logic. It is derived from sequencer membership and
  // refreshed when the config is updated.
  // TODO T41571347: use SequencerMembership instead of SequencersConfig in
  // sequencer locator / routing / placement logic and get rid of this field
  SequencersConfig sequencer_locator_config_;

  // Unix timestamp in milliseconds.
  uint64_t last_change_timestamp_{0};

  membership::MaintenanceID::Type last_maintenance_{
      membership::MaintenanceID::MAINTENANCE_NONE};
  std::string last_change_context_{};

  uint64_t computeStorageNodesHash() const;
  shard_size_t computeNumShards() const;
  node_index_t computeMaxNodeIndex() const;
  SequencersConfig computeSequencersConfig() const;

  // recompute configuration metadata (e.g., storage_hash_ and num_shards_)
  // from each sub-configuration, note that version, timestamp,
  // etc are not reset in this function
  void recomputeConfigMetadata();

  // Increments config version, sets last_change_timestamp_ and context
  void touch(std::string context);

  friend class NodesConfigLegacyConverter;
  friend class NodesConfigurationThriftConverter;
};

}}}} // namespace facebook::logdevice::configuration::nodes
