/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

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
    std::unique_ptr<ServiceDiscoveryConfig::Update> service_discovery_update;
    std::unique_ptr<SequencerConfig::Update> sequencer_config_update;
    std::unique_ptr<StorageConfig::Update> storage_config_update;
    std::unique_ptr<MetaDataLogsReplication::Update> metadata_logs_rep_update;

    membership::MaintenanceID::Type maintenance;
    std::string context;

    bool isValid() const;
    bool hasAllUpdates() const;
  };

  // create an empty nodes config
  explicit NodesConfiguration();

  const std::shared_ptr<const ServiceDiscoveryConfig>&
  getServiceDiscovery() const {
    return service_discovery_;
  }

  const std::shared_ptr<const SequencerConfig>& getSequencerConfig() const {
    return sequencer_config_;
  }

  const std::shared_ptr<const StorageConfig>& getStorageConfig() const {
    return storage_config_;
  }

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

  std::shared_ptr<const NodesConfiguration> applyUpdate(Update update) const;

  // validations
  bool serviceDiscoveryConsistentWithMembership() const;
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

 private:
  std::shared_ptr<const ServiceDiscoveryConfig> service_discovery_;
  std::shared_ptr<const SequencerConfig> sequencer_config_;
  std::shared_ptr<const StorageConfig> storage_config_;
  std::shared_ptr<const MetaDataLogsReplication> metadata_logs_rep_;

  ///// configuration metadata

  // provide a total order of config updates. However, we do not use this
  // version for synchronoization or any correctness purpose. Instead we use the
  // version in Membership for each role
  membership::MembershipVersion::Type version_;
  uint64_t storage_hash_;

  // TODO(T15517759): NodesConfigParser currently verifies that all nodes in the
  // config have the same amount of shards, which is also stored here. This
  // member will be removed when the Flexible Log Sharding project is fully
  // implemented. In the mean time, this member is used by state machines that
  // need to convert node_index_t values to ShardID values.
  shard_size_t num_shards_;

  uint64_t last_change_timestamp_;
  membership::MaintenanceID::Type last_maintenance_;
  std::string last_change_context_;

  uint64_t computeStorageNodesHash() const;
  shard_size_t computeNumShards() const;
};

}}}} // namespace facebook::logdevice::configuration::nodes
