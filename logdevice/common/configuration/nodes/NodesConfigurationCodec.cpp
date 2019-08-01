/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"

#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/MembershipThriftConverter.h"
#include "logdevice/include/Err.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

//////////////////////// NodeServiceDiscovery //////////////////////////////

/*static*/
thrift::NodeServiceDiscovery NodesConfigurationThriftConverter::toThrift(
    const NodeServiceDiscovery& discovery) {
  thrift::NodeServiceDiscovery disc;
  disc.set_name(discovery.name);
  disc.set_address(discovery.address.toString());
  if (discovery.gossip_address.hasValue()) {
    disc.set_gossip_address(discovery.gossip_address->toString());
  }
  if (discovery.ssl_address.hasValue()) {
    disc.set_ssl_address(discovery.ssl_address.value().toString());
  }
  if (discovery.location.hasValue()) {
    disc.set_location(discovery.location.value().toString());
  }
  disc.set_roles(discovery.roles.to_ullong());
  return disc;
}

/*static*/
int NodesConfigurationThriftConverter::fromThrift(
    const thrift::NodeServiceDiscovery& obj,
    NodeServiceDiscovery* out) {
  NodeServiceDiscovery result;

  result.name = obj.name;

  if (obj.address.empty()) {
    ld_error("Missing required field address.");
    return -1;
  } else {
    auto sock = Sockaddr::fromString(obj.address);
    if (!sock.hasValue()) {
      ld_error("malformed socket addr field address.");
      return -1;
    }
    result.address = sock.value();
  }

  if (obj.gossip_address_ref().has_value()) {
    auto sock = Sockaddr::fromString(obj.gossip_address_ref().value());
    if (!sock.hasValue()) {
      ld_error("malformed socket addr field gossip_address.");
      return -1;
    }
    result.gossip_address = sock.value();
  }

  if (obj.ssl_address_ref().has_value()) {
    auto sock = Sockaddr::fromString(obj.ssl_address_ref().value());
    if (!sock.hasValue()) {
      ld_error("malformed socket addr field ssl_address.");
      return -1;
    }
    result.ssl_address = sock.value();
  }

  if (obj.location_ref().has_value()) {
    NodeLocation location;
    int rv = location.fromDomainString(obj.location_ref().value());
    if (rv != 0) {
      ld_error("Invalid \"location\" string %s",
               obj.location_ref().value_unchecked().c_str());
      return -1;
    }
    result.location = location;
  }

  result.roles = NodeServiceDiscovery::RoleSet(obj.roles);

  if (out != nullptr) {
    *out = result;
  }
  return 0;
}

//////////////////////// SequencerNodeAttribute //////////////////////////////

/*static*/
thrift::SequencerNodeAttribute NodesConfigurationThriftConverter::toThrift(
    const SequencerNodeAttribute& /*unused*/) {
  return thrift::SequencerNodeAttribute{};
}

/*static*/
int NodesConfigurationThriftConverter::fromThrift(
    const thrift::SequencerNodeAttribute& /* unused */,
    SequencerNodeAttribute* out) {
  if (out != nullptr) {
    *out = SequencerNodeAttribute();
  }
  return 0;
}

//////////////////////// StorageNodeAttribute //////////////////////////////

/*static*/
thrift::StorageNodeAttribute NodesConfigurationThriftConverter::toThrift(
    const StorageNodeAttribute& storage_attr) {
  thrift::StorageNodeAttribute attr;
  attr.set_capacity(storage_attr.capacity);
  attr.set_num_shards(storage_attr.num_shards);
  attr.set_generation(storage_attr.generation);
  attr.set_exclude_from_nodesets(storage_attr.exclude_from_nodesets);
  return attr;
}

/*static*/
int NodesConfigurationThriftConverter::fromThrift(
    const thrift::StorageNodeAttribute& obj,
    StorageNodeAttribute* out) {
  StorageNodeAttribute result{
      obj.capacity, obj.num_shards, obj.generation, obj.exclude_from_nodesets};

  if (out != nullptr) {
    *out = result;
  }
  return 0;
}

//////////////////////// NodeAttributesConfig //////////////////////////////

#define GEN_SERIALIZATION_NODE_ATTRS_CONFIG(_Config, _Attribute)          \
  /*static*/                                                              \
  thrift::_Config NodesConfigurationThriftConverter::toThrift(            \
      const _Config& _config) {                                           \
    std::map<thrift::node_idx, thrift::_Attribute> node_states;           \
    for (const auto& node_kv : _config.node_states_) {                    \
      node_states.emplace(node_kv.first, toThrift(node_kv.second));       \
    }                                                                     \
    thrift::_Config config;                                               \
    config.set_node_states(std::move(node_states));                       \
    return config;                                                        \
  }                                                                       \
                                                                          \
  /*static*/                                                              \
  std::shared_ptr<_Config> NodesConfigurationThriftConverter::fromThrift( \
      const thrift::_Config& _thrift_config) {                            \
    auto result = std::make_shared<_Config>();                            \
    for (const auto& state : _thrift_config.node_states) {                \
      node_index_t node = state.first;                                    \
      auto node_attribute = state.second;                                 \
      _Attribute attr;                                                    \
      int rv = fromThrift(node_attribute, &attr);                         \
      if (rv != 0) {                                                      \
        err = E::INVALID_CONFIG;                                          \
        return nullptr;                                                   \
      }                                                                   \
      result->setNodeAttributes(node, std::move(attr));                   \
    }                                                                     \
    /* note: we don't do validation here since it will be done */         \
    /* at NodesConfiguration deserialization */                           \
    return result;                                                        \
  }

GEN_SERIALIZATION_NODE_ATTRS_CONFIG(ServiceDiscoveryConfig,
                                    NodeServiceDiscovery)
GEN_SERIALIZATION_NODE_ATTRS_CONFIG(SequencerAttributeConfig,
                                    SequencerNodeAttribute)
GEN_SERIALIZATION_NODE_ATTRS_CONFIG(StorageAttributeConfig,
                                    StorageNodeAttribute)
#undef GEN_SERIALIZATION_NODE_ATTRS_CONFIG

//////////////////////// PerRoleConfig
////////////////////////////////////

#define GEN_SERIALIZATION_PER_ROLE_CONFIG(_Config, _AttrConfig, _Membership) \
  /*static*/                                                                 \
  thrift::_Config NodesConfigurationThriftConverter::toThrift(               \
      const _Config& _config) {                                              \
    /* must serialize a valid config */                                      \
    ld_check(_config.membership_ != nullptr);                                \
    ld_check(_config.attributes_ != nullptr);                                \
    thrift::_Config conf;                                                    \
    conf.set_attr_conf(toThrift(*_config.attributes_));                      \
    conf.set_membership(membership::MembershipThriftConverter::toThrift(     \
        *_config.membership_));                                              \
    return conf;                                                             \
  }                                                                          \
                                                                             \
  /*static*/                                                                 \
  std::shared_ptr<_Config> NodesConfigurationThriftConverter::fromThrift(    \
      const thrift::_Config& _thrift_config) {                               \
    auto attr_config = fromThrift(_thrift_config.attr_conf);                 \
    if (attr_config == nullptr) {                                            \
      err = E::INVALID_CONFIG;                                               \
      return nullptr;                                                        \
    }                                                                        \
    auto membership = membership::MembershipThriftConverter::fromThrift(     \
        _thrift_config.membership);                                          \
    if (membership == nullptr) {                                             \
      err = E::INVALID_CONFIG;                                               \
      return nullptr;                                                        \
    }                                                                        \
    return std::make_shared<_Config>(                                        \
        std::move(membership), std::move(attr_config));                      \
  }

GEN_SERIALIZATION_PER_ROLE_CONFIG(SequencerConfig,
                                  SequencerAttributeConfig,
                                  membership::SequencerMembership)
GEN_SERIALIZATION_PER_ROLE_CONFIG(StorageConfig,
                                  StorageAttributeConfig,
                                  membership::StorageMembership)

#undef GEN_SERIALIZATION_PER_ROLE_CONFIG

//////////////////////// MetaDataLogsReplication //////////////////////////////

/* static */
thrift::MetaDataLogsReplication NodesConfigurationThriftConverter::toThrift(
    const MetaDataLogsReplication& config) {
  // must serialize a valid config
  ld_check(config.validate());
  std::vector<thrift::ScopeReplication> scopes;
  for (const auto& reps : config.replication_.getDistinctReplicationFactors()) {
    thrift::ScopeReplication rep;
    rep.set_scope(static_cast<uint8_t>(reps.first));
    rep.set_replication_factor(reps.second);
    scopes.push_back(std::move(rep));
  }

  thrift::ReplicationProperty replication;
  replication.set_scopes(std::move(scopes));

  thrift::MetaDataLogsReplication metadata;
  metadata.set_version(config.version_.val());
  metadata.set_replication(std::move(replication));

  return metadata;
}

/* static */
std::shared_ptr<MetaDataLogsReplication>
NodesConfigurationThriftConverter::fromThrift(
    const thrift::MetaDataLogsReplication& thrift_config) {
  auto result = std::make_shared<MetaDataLogsReplication>();
  std::vector<ReplicationProperty::ScopeReplication> scopes;
  for (const auto& scope : thrift_config.replication.scopes) {
    scopes.emplace_back(static_cast<NodeLocationScope>(scope.scope),
                        static_cast<int>(scope.replication_factor));
  }

  result->version_ = membership::MembershipVersion::Type(thrift_config.version);

  // allow empty scopes here (which is
  // prohibited in
  // ReplicationProperty::assign())
  if (!scopes.empty()) {
    int rv = result->replication_.assign(std::move(scopes));
    if (rv != 0) {
      ld_error("Invalid replication property "
               "for metadata logs replication.");
      return nullptr;
    }
  }

  return result;
}

//////////////////////// NodesConfiguration
/////////////////////////////////

/* static */
thrift::NodesConfiguration
NodesConfigurationThriftConverter::toThrift(const NodesConfiguration& config) {
  // config must have valid components
  ld_check(config.service_discovery_ != nullptr);
  ld_check(config.sequencer_config_ != nullptr);
  ld_check(config.storage_config_ != nullptr);
  ld_check(config.metadata_logs_rep_ != nullptr);

  thrift::NodesConfiguration conf;
  conf.set_proto_version(NodesConfigurationCodec::CURRENT_PROTO_VERSION);
  conf.set_version(config.getVersion().val());
  conf.set_service_discovery(toThrift(*config.service_discovery_));
  conf.set_sequencer_config(toThrift(*config.sequencer_config_));
  conf.set_storage_config(toThrift(*config.storage_config_));
  conf.set_metadata_logs_rep(toThrift(*config.metadata_logs_rep_));
  conf.set_last_timestamp(config.last_change_timestamp_);
  conf.set_last_maintenance(config.last_maintenance_.val());
  conf.set_last_context(config.last_change_context_);
  return conf;
}

/* static */
std::shared_ptr<NodesConfiguration>
NodesConfigurationThriftConverter::fromThrift(
    const thrift::NodesConfiguration& thrift_config) {
  NodesConfigurationCodec::ProtocolVersion pv = thrift_config.proto_version;
  if (pv > NodesConfigurationCodec::CURRENT_PROTO_VERSION) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Received codec protocol version "
                    "%u is larger than current "
                    "codec protocol version %u. There "
                    "might be incompatible data, "
                    "aborting deserialization",
                    pv,
                    NodesConfigurationCodec::CURRENT_PROTO_VERSION);
    err = E::NOTSUPPORTED;
    return nullptr;
  }

  auto result = std::make_shared<NodesConfiguration>();

#define PARSE_SUB_CONF(_name)                             \
  do {                                                    \
    result->_name##_ = fromThrift(thrift_config._name);   \
    if (result->_name##_ == nullptr) {                    \
      ld_error("failure to parse subconfig %s.", #_name); \
      err = E::INVALID_CONFIG;                            \
      return nullptr;                                     \
    }                                                     \
  } while (0)

  PARSE_SUB_CONF(service_discovery);
  PARSE_SUB_CONF(sequencer_config);
  PARSE_SUB_CONF(storage_config);
  PARSE_SUB_CONF(metadata_logs_rep);
#undef PARSE_SUB_CONF

  result->version_ = membership::MembershipVersion::Type(thrift_config.version);
  result->last_change_timestamp_ = thrift_config.last_timestamp;
  result->last_maintenance_ =
      membership::MaintenanceID::Type(thrift_config.last_maintenance);
  if (!thrift_config.last_context.empty()) {
    result->last_change_context_ = thrift_config.last_context;
  }

  // recompute all config metadata
  result->recomputeConfigMetadata();

  // perform the final validation
  if (!result->validate()) {
    ld_error("Invalid NodesConfiguration "
             "after deserialization.");
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  return result;
}

}}}} // namespace facebook::logdevice::configuration::nodes
