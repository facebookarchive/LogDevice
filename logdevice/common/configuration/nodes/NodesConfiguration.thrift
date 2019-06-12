/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/common/membership/Membership.thrift"

namespace cpp2 facebook.logdevice.configuration.nodes.thrift

typedef byte (cpp2.type = "std::uint8_t") u8
typedef i16 (cpp2.type = "std::uint16_t") u16
typedef i32 (cpp2.type = "std::uint32_t") u32
typedef i64 (cpp2.type = "std::uint64_t") u64

typedef u16 node_idx

 struct NodeServiceDiscovery {
  1: string address;
  2: optional string gossip_address;
  3: optional string ssl_address;
  4: optional string location;
  5: u64 roles;
  // 6: string hostname;
  7: string name;
}

struct ServiceDiscoveryConfig {
   // A map from a
   1: map<node_idx, NodeServiceDiscovery> node_states;
}

struct SequencerNodeAttribute {
}

struct SequencerAttributeConfig {
   1: map<node_idx, SequencerNodeAttribute> node_states;
}

struct StorageNodeAttribute {
  1: double capacity;
  2: i16 num_shards;
  3: u16 generation;
  4: bool exclude_from_nodesets;
}

struct StorageAttributeConfig {
  1: map<node_idx, StorageNodeAttribute> node_states;
}

struct SequencerConfig {
  1: SequencerAttributeConfig attr_conf;
  2: Membership.SequencerMembership membership;
}

struct StorageConfig {
  1: StorageAttributeConfig attr_conf;
  2: Membership.StorageMembership membership;
}

struct ScopeReplication {
  1: u8 scope;
  2: u8 replication_factor;
}

struct ReplicationProperty {
  1: list<ScopeReplication> scopes;
}

struct MetaDataLogsReplication {
  1: u64 version;
  2: ReplicationProperty replication;
}

struct NodesConfiguration {
  1: u32 proto_version;
  2: u64 version;
  3: ServiceDiscoveryConfig service_discovery;
  4: SequencerConfig sequencer_config;
  5: StorageConfig storage_config;
  6: MetaDataLogsReplication metadata_logs_rep;
  7: u64 last_timestamp;
  8: u64 last_maintenance;
  9: string last_context;
}

struct NodesConfigurationHeader {
  1: u32 proto_version;
  2: u64 config_version;
  3: bool is_compressed;
}

struct NodesConfigurationWrapper {
  1: NodesConfigurationHeader header;
  2: binary serialized_config;
}
