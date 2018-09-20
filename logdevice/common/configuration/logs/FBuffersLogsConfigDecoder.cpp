/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"

namespace facebook { namespace logdevice { namespace logsconfig {

#define CONVERT_FROM_BUFFERS_TYPE(from, to)                             \
  template <>                                                           \
  Attribute<to>                                                         \
  FBuffersLogsConfigCodec::fbuffers_deserialize<Attribute<to>,          \
                                                fbuffers::LogAttr>(     \
      const fbuffers::LogAttr* attr, const Attribute<to>& parent) {     \
    Attribute<to> out;                                                  \
    if (attr) {                                                         \
      if (attr->value_type() == fbuffers::Any::from) {                  \
        out = Attribute<to>(                                            \
            static_cast<const fbuffers::from*>(attr->value())->value(), \
            attr->inherited());                                         \
      } else {                                                          \
        ld_spew("Type mismatch while trying to decode attribute %s",    \
                attr->name()->str().c_str());                           \
      }                                                                 \
    } else if (parent.hasValue()) {                                     \
      out = Attribute<to>(parent.value(), true);                        \
    }                                                                   \
    return out;                                                         \
  }

CONVERT_FROM_BUFFERS_TYPE(UInt8, uint8_t);
CONVERT_FROM_BUFFERS_TYPE(Int, int32_t);
CONVERT_FROM_BUFFERS_TYPE(Long, int64_t);
CONVERT_FROM_BUFFERS_TYPE(ULong, uint64_t);
CONVERT_FROM_BUFFERS_TYPE(Bool, bool);
#undef CONVERT_FROM_BUFFERS_TYPE

// fbuffers::LogAttr => std::chrono::seconds
template <>
Attribute<std::chrono::seconds>
FBuffersLogsConfigCodec::fbuffers_deserialize<Attribute<std::chrono::seconds>,
                                              fbuffers::LogAttr>(
    const fbuffers::LogAttr* attr,
    const Attribute<std::chrono::seconds>& parent) {
  Attribute<std::chrono::seconds> out;
  // attr is defined if the attribute it holds is set. otherwise we return an
  // Attribute object that has no value (unset)
  if (attr) {
    // we expect std::chrono::seconds to be serialized as fbuffers::Any::Long
    if (attr->value_type() == fbuffers::Any::Long) {
      out = Attribute<std::chrono::seconds>(
          std::chrono::seconds(
              static_cast<const fbuffers::Long*>(attr->value())->value()),
          attr->inherited());
    } else {
      ld_spew("Type mismatch while trying to decode attribute %s",
              attr->name()->str().c_str());
    }
  } else if (parent.hasValue()) {
    out = Attribute<std::chrono::seconds>(parent.value(), true);
  }
  return out;
}

// fbuffers::LogAttr => Compression
template <>
Attribute<Compression>
FBuffersLogsConfigCodec::fbuffers_deserialize<Attribute<Compression>,
                                              fbuffers::LogAttr>(
    const fbuffers::LogAttr* attr,
    const Attribute<Compression>& parent) {
  Attribute<Compression> out;
  // attr is defined if the attribute it holds is set. otherwise we return an
  // Attribute object that has no value (unset)
  if (attr) {
    // we expect Compression to be serialized as fbuffers::Any::UInt8
    if (attr->value_type() == fbuffers::Any::UInt8) {
      uint8_t v = static_cast<const fbuffers::UInt8*>(attr->value())->value();
      out = Attribute<Compression>(
          static_cast<Compression>(v), attr->inherited());
    } else {
      ld_spew("Type mismatch while trying to decode attribute %s",
              attr->name()->str().c_str());
    }
  } else if (parent.hasValue()) {
    out = Attribute<Compression>(parent.value(), true);
  }
  return out;
}

// fbuffers::LogAttr => std::chrono::milliseconds
template <>
Attribute<std::chrono::milliseconds>
FBuffersLogsConfigCodec::fbuffers_deserialize<
    Attribute<std::chrono::milliseconds>,
    fbuffers::LogAttr>(const fbuffers::LogAttr* attr,
                       const Attribute<std::chrono::milliseconds>& parent) {
  Attribute<std::chrono::milliseconds> out;
  if (attr) {
    if (attr->value_type() == fbuffers::Any::Long) {
      out = Attribute<std::chrono::milliseconds>(
          std::chrono::milliseconds(
              static_cast<const fbuffers::Long*>(attr->value())->value()),
          attr->inherited());
    } else {
      ld_spew("Type mismatch while trying to decode attribute %s",
              attr->name()->str().c_str());
    }
  } else if (parent.hasValue()) {
    out = Attribute<std::chrono::milliseconds>(parent.value(), true);
  }
  return out;
}

// fbuffers::LogAttr => std::string
template <>
Attribute<std::string>
FBuffersLogsConfigCodec::fbuffers_deserialize<Attribute<std::string>,
                                              fbuffers::LogAttr>(
    const fbuffers::LogAttr* attr,
    const Attribute<std::string>& parent) {
  Attribute<std::string> out;
  if (attr) {
    if (attr->value_type() == fbuffers::Any::String) {
      out = Attribute<std::string>(
          static_cast<const fbuffers::String*>(attr->value())->value()->str(),
          attr->inherited());
    } else {
      ld_spew("Type mismatch while trying to decode attribute %s",
              attr->name()->str().c_str());
    }
  } else if (parent.hasValue()) {
    out = Attribute<std::string>(parent.value(), true);
  }
  return out;
}

// fbuffers::LogAttr => Attribute<folly::Optional<Type>>
template <typename Type>
Attribute<folly::Optional<Type>>
FBuffersLogsConfigCodec::fbuffers_deserializeOptionalAttr(
    const fbuffers::LogAttr* attr,
    const Attribute<folly::Optional<Type>>& parent) {
  Attribute<folly::Optional<Type>> out;
  if (attr) {
    // This means that the value was explicitly set to be null
    // this results of setting the inner Optional to None. but the outer
    // attribute will be defined as if it has a value so inheritance will be
    // ignored.
    if (attr->value_type() == fbuffers::Any::NoValueSet) {
      out = Attribute<folly::Optional<Type>>(
          folly::Optional<Type>(), attr->inherited());
    } else {
      // the value is set to a real value.
      Attribute<Type> deserialized =
          fbuffers_deserialize<Attribute<Type>>(attr, Attribute<Type>());
      out = Attribute<folly::Optional<Type>>(
          folly::Optional<Type>(deserialized.value()), attr->inherited());
    }
  } else if (parent.hasValue()) {
    out = Attribute<folly::Optional<Type>>(parent.value(), true);
  }
  return out;
}
// fbuffers::LogAttr => NodeLocationScope
template <>
Attribute<NodeLocationScope>
FBuffersLogsConfigCodec::fbuffers_deserialize<Attribute<NodeLocationScope>,
                                              fbuffers::LogAttr>(
    const fbuffers::LogAttr* attr,
    const Attribute<NodeLocationScope>& parent) {
  Attribute<NodeLocationScope> out;
  if (attr) {
    if (attr->value_type() == fbuffers::Any::UInt8) {
      uint8_t v = static_cast<const fbuffers::UInt8*>(attr->value())->value();
      if (v >= static_cast<uint8_t>(NodeLocationScope::INVALID)) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            2,
            "Malformed NodeLocationScope in fbuffers::LogAttributes: %d. "
            "Please investigate. Using NODE scope instead.",
            (int)v);
        v = static_cast<uint8_t>(NodeLocationScope::NODE);
      }
      out = Attribute<NodeLocationScope>(
          static_cast<NodeLocationScope>(v), attr->inherited());
    } else {
      ld_spew("Type mismatch while trying to decode attribute %s",
              attr->name()->str().c_str());
    }
  } else if (parent.hasValue()) {
    out = Attribute<NodeLocationScope>(parent.value(), true);
  }
  return out;
}

// fbuffers::LogAttrs => LogAttributes
template <>
LogAttributes
FBuffersLogsConfigCodec::fbuffers_deserialize<logsconfig::LogAttributes,
                                              fbuffers::LogAttrs>(
    const fbuffers::LogAttrs* attrs,
    const LogAttributes& parent) {
  if (!attrs) {
    return LogAttributes(LogAttributes(), parent);
  }
// This looks up all the attributes in (attrs -- a flatbuffers container) and
// tries to find the attribute matching the name, then tries to deserialize it
// to the destination type that we expect (type)
#define DESERIALIZE_ATTR(name, key, type)                       \
  Attribute<type> name = fbuffers_deserialize<Attribute<type>>( \
      attrs->attributes()->LookupByKey(key), parent.name())

#define DESERIALIZE_ATTR_OPT(name, key, type) \
  Attribute<folly::Optional<type>> name =     \
      fbuffers_deserializeOptionalAttr<type>( \
          attrs->attributes()->LookupByKey(key), parent.name())

  DESERIALIZE_ATTR(replicationFactor, REPLICATION_FACTOR, int32_t);
  DESERIALIZE_ATTR(extraCopies, EXTRA_COPIES, int32_t);
  DESERIALIZE_ATTR(syncedCopies, SYNCED_COPIES, int32_t);
  DESERIALIZE_ATTR(maxWritesInFlight, MAX_WRITES_IN_FLIGHT, int32_t);
  DESERIALIZE_ATTR(singleWriter, SINGLE_WRITER, bool);
  DESERIALIZE_ATTR(
      syncReplicationScope, SYNC_REPLICATION_SCOPE, NodeLocationScope);
  DESERIALIZE_ATTR_OPT(backlogDuration, BACKLOG, std::chrono::seconds);
  DESERIALIZE_ATTR_OPT(nodeSetSize, NODESET_SIZE, int32_t);
  DESERIALIZE_ATTR_OPT(
      deliveryLatency, DELIVERY_LATENCY, std::chrono::milliseconds);
  DESERIALIZE_ATTR(scdEnabled, SCD_ENABLED, bool);
  DESERIALIZE_ATTR(localScdEnabled, LOCAL_SCD_ENABLED, bool);
  DESERIALIZE_ATTR_OPT(writeToken, WRITE_TOKEN, std::string);
  DESERIALIZE_ATTR(stickyCopySets, STICKY_COPYSETS, bool);
  DESERIALIZE_ATTR(mutablePerEpochLogMetadataEnabled,
                   MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED,
                   bool);
  DESERIALIZE_ATTR_OPT(sequencerAffinity, SEQUENCER_AFFINITY, std::string);
  DESERIALIZE_ATTR(sequencerBatching, SEQUENCER_BATCHING, bool);
  DESERIALIZE_ATTR(sequencerBatchingTimeTrigger,
                   SEQUENCER_BATCHING_TIME_TRIGGER,
                   std::chrono::milliseconds);
  DESERIALIZE_ATTR(
      sequencerBatchingSizeTrigger, SEQUENCER_BATCHING_SIZE_TRIGGER, ssize_t);
  DESERIALIZE_ATTR(sequencerBatchingCompression,
                   SEQUENCER_BATCHING_COMPRESSION,
                   Compression);
  DESERIALIZE_ATTR(sequencerBatchingPassthruThreshold,
                   SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
                   ssize_t);
  DESERIALIZE_ATTR(tailOptimized, TAIL_OPTIMIZED, bool);

#undef DESERIALIZE_ATTR_OPT
#undef DESERIALIZE_ATTR

  Attribute<LogAttributes::PermissionsMap> permissions;
  // permissions
  if (attrs->permissions()) {
    LogAttributes::PermissionsMap perm_map;
    for (const auto* permission : *attrs->permissions()) {
      // this entry has a principal and a list of actions (uint8_t)
      if (permission->principal()) {
        std::string principal = permission->principal()->str();
        std::array<bool, static_cast<int>(ACTION::MAX)> actions;
        actions.fill(false);
        if (permission->actions()) {
          for (const uint8_t& action : *permission->actions()) {
            actions[action] = true;
          }
        }
        perm_map[principal] = actions;
      }
    }
    permissions = std::move(perm_map);
  } else if (parent.permissions().hasValue()) {
    // attribute is not set. Inherit from parent if parent is set.
    permissions = Attribute<LogAttributes::PermissionsMap>(
        parent.permissions().value(), true);
  }

  Attribute<LogAttributes::ACLList> acls;
  // acls
  if (attrs->acls()) {
    LogAttributes::ACLList acl_list;
    for (const auto* acl : *attrs->acls()) {
      acl_list.emplace_back(acl->str());
    }
    acls = std::move(acl_list);
  } else if (parent.acls().hasValue()) {
    // attribute is not set. Inherit from parent if parent is set.
    acls = Attribute<LogAttributes::ACLList>(parent.acls().value(), true);
  }

  Attribute<LogAttributes::ACLList> aclsShadow;
  // shadow acls
  if (attrs->acls_shadow()) {
    LogAttributes::ACLList acl_shadow_list;
    for (const auto* acl : *attrs->acls_shadow()) {
      acl_shadow_list.emplace_back(acl->str());
    }
    aclsShadow = std::move(acl_shadow_list);
  } else if (parent.aclsShadow().hasValue()) {
    // attribute is not set. Inherit from parent if parent is set.
    aclsShadow =
        Attribute<LogAttributes::ACLList>(parent.aclsShadow().value(), true);
  }

  Attribute<LogAttributes::ExtrasMap> extras;
  // extras
  if (attrs->extras()) {
    LogAttributes::ExtrasMap extras_map;
    for (const auto* extra : *attrs->extras()) {
      if (extra->key()) {
        std::string key = extra->key()->str();
        std::string value;
        // it's odd if the value is nullptr for the key that we have found but
        // this a protection against calling ->str() on nullptr
        // It's a best practice for flatbuffers to always validate that the
        // object is not nullptr (means that it's unset, since all objects are
        // nullable)
        if (extra->value()) {
          value = extra->value()->str();
        }
        extras_map[key] = value;
      }
    }
    extras = std::move(extras_map);
  } else if (parent.extras().hasValue()) {
    // attribute is not set. Inherit from parent if parent is set.
    extras = Attribute<LogAttributes::ExtrasMap>(parent.extras().value(), true);
  }

  Attribute<LogAttributes::ScopeReplicationFactors> replicateAcross;
  // replicateAcross
  if (attrs->replicate_across()) {
    LogAttributes::ScopeReplicationFactors r;

    for (const auto* rf : *attrs->replicate_across()) {
      if (rf->replication_factor() <= 0) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        2,
                        "Malformed ScopeReplication in fbuffers::LogAttrs: "
                        "replication_factor <= 0. "
                        "Please investigate. Skipping.");
        continue;
      }
      uint8_t v = rf->scope();
      if (v >= static_cast<uint8_t>(NodeLocationScope::INVALID)) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            2,
            "Malformed NodeLocationScope in fbuffers::LogAttributes: %d. "
            "Please investigate. Skipping.",
            (int)v);
        continue;
      }
      r.emplace_back(
          static_cast<NodeLocationScope>(v), rf->replication_factor());
    }
    replicateAcross = std::move(r);
  } else if (parent.replicateAcross().hasValue()) {
    replicateAcross = Attribute<LogAttributes::ScopeReplicationFactors>(
        parent.replicateAcross().value(), true);
  }

  Attribute<LogAttributes::Shadow> shadow;
  // shadow
  if (attrs->shadow()) {
    auto& shadow_item = *attrs->shadow();
    shadow = LogAttributes::Shadow{
        shadow_item.destination()->str(), shadow_item.ratio()};
  } else if (parent.shadow().hasValue()) {
    shadow = Attribute<LogAttributes::Shadow>().withParent(parent.shadow());
  }

  return LogAttributes{std::move(replicationFactor),
                       std::move(extraCopies),
                       std::move(syncedCopies),
                       std::move(maxWritesInFlight),
                       std::move(singleWriter),
                       std::move(syncReplicationScope),
                       std::move(replicateAcross),
                       std::move(backlogDuration),
                       std::move(nodeSetSize),
                       std::move(deliveryLatency),
                       std::move(scdEnabled),
                       std::move(localScdEnabled),
                       std::move(writeToken),
                       std::move(stickyCopySets),
                       std::move(mutablePerEpochLogMetadataEnabled),
                       std::move(permissions),
                       std::move(acls),
                       std::move(aclsShadow),
                       std::move(sequencerAffinity),
                       std::move(sequencerBatching),
                       std::move(sequencerBatchingTimeTrigger),
                       std::move(sequencerBatchingSizeTrigger),
                       std::move(sequencerBatchingCompression),
                       std::move(sequencerBatchingPassthruThreshold),
                       std::move(shadow),
                       std::move(tailOptimized),
                       std::move(extras)};
}

// fbuffers::LogAttrs => LogAttributes -- without applying parent attrs
template <>
LogAttributes
FBuffersLogsConfigCodec::fbuffers_deserialize<logsconfig::LogAttributes,
                                              fbuffers::LogAttrs>(
    const fbuffers::LogAttrs* attrs) {
  return fbuffers_deserialize<LogAttributes>(attrs, LogAttributes());
}

// fbuffers::LogAttrs => unique_ptr<LogAttributes>
template <>
std::unique_ptr<LogAttributes>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes,
                                              fbuffers::LogAttrs>(
    const fbuffers::LogAttrs* attrs,
    const std::string& /*delimiter*/) {
  return std::make_unique<LogAttributes>(
      fbuffers_deserialize<LogAttributes>(attrs));
}

// fbuffers::LogRange => logid_range_t
template <>
logid_range_t FBuffersLogsConfigCodec::fbuffers_deserialize<logid_range_t,
                                                            fbuffers::LogRange>(
    const fbuffers::LogRange* range) {
  logid_range_t out;
  if (range) {
    out = std::make_pair<logid_t, logid_t>(
        logid_t(range->from()), logid_t(range->to()));
  }
  return out;
}

// fbuffers::LogGroup => LogGroupNode
template <>
std::unique_ptr<LogGroupNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogGroupNode, fbuffers::LogGroup>(
    const fbuffers::LogGroup* group,
    const std::string& /* unused */,
    const LogAttributes& parent_attributes) {
  if (group != nullptr) {
    LogAttributes attrs;
    if (group->attrs()) {
      // recursively deserialize the attributes to get us a LogAttributes object
      attrs = fbuffers_deserialize<LogAttributes>(
          group->attrs(), parent_attributes);
    }

    logid_range_t range = fbuffers_deserialize<logid_range_t>(group->range());

    return std::make_unique<LogGroupNode>(group->name()->str(), attrs, range);
  } else {
    return nullptr;
  }
}

// fbuffers::Directory => DirectoryNode
template <>
std::unique_ptr<DirectoryNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<DirectoryNode,
                                              fbuffers::Directory>(
    const fbuffers::Directory* dir,
    DirectoryNode* parent,
    const std::string& delimiter,
    bool apply_defaults) {
  if (dir) {
    LogAttributes attrs;
    if (dir->attrs()) {
      if (parent != nullptr) {
        attrs =
            fbuffers_deserialize<LogAttributes>(dir->attrs(), parent->attrs());
      } else {
        attrs = fbuffers_deserialize<LogAttributes>(
            dir->attrs(),
            apply_defaults ? DefaultLogAttributes() : LogAttributes());
      }
    }
    auto out = std::make_unique<DirectoryNode>(
        dir->name()->str(), parent, attrs, delimiter);

    DirectoryMap dirs;
    if (dir->children()) {
      for (const auto* directory : *dir->children()) {
        auto child = fbuffers_deserialize<DirectoryNode>(
            directory, out.get(), delimiter, false /* do not apply_defaults */);
        auto& dirsEntry = dirs[child->name()];
        dirsEntry = std::move(child);
      }
      out->setChildren(std::move(dirs));
    }

    LogGroupMap logs;
    if (dir->log_groups()) {
      for (const auto* log_group : *dir->log_groups()) {
        auto one_log =
            fbuffers_deserialize<LogGroupNode>(log_group, delimiter, attrs);
        auto& logsEntry = logs[one_log->name()];
        logsEntry = std::move(one_log);
      }
      out->setLogGroups(logs);
    }

    return out;
  } else {
    return nullptr;
  }
}

// fbuffers::LogGroupInDirectory => LogGroupWithParentPath
template <>
std::unique_ptr<LogGroupWithParentPath>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogGroupWithParentPath,
                                              fbuffers::LogGroupInDirectory>(
    const fbuffers::LogGroupInDirectory* group,
    const std::string& /* unused */) {
  auto lid = std::make_unique<LogGroupWithParentPath>();
  lid->log_group =
      fbuffers_deserialize<LogGroupNode>(group->log_group(), "" /* unused */);
  lid->parent_path = group->parent_path()->str();
  return lid;
}

// fbuffers::Directory => DirectoryNode
template <>
std::unique_ptr<DirectoryNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<DirectoryNode,
                                              fbuffers::Directory>(
    const fbuffers::Directory* dir,
    const std::string& delimiter) {
  return fbuffers_deserialize<DirectoryNode>(dir, nullptr, delimiter, false);
}

// fbuffers::LogGroup => LogGroupNode
template <>
std::unique_ptr<LogGroupNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogGroupNode, fbuffers::LogGroup>(
    const fbuffers::LogGroup* group,
    const std::string& delimiter) {
  return fbuffers_deserialize<LogGroupNode>(group, delimiter, LogAttributes());
}

// The parent deserializer for the entire tree. This is where a full
// deserialization of a fbuffers::LogsConfig (flatbuffer) => LogsConfigTree
// (logdevice) starts and recursively deserializes its components.
//
// fbuffers::LogsConfig => LogsConfigTree
template <>
std::unique_ptr<LogsConfigTree>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogsConfigTree,
                                              fbuffers::LogsConfig>(
    const fbuffers::LogsConfig* logs_config,
    const std::string& delimiter) {
  if (logs_config && logs_config->root_dir()) {
    std::unique_ptr<DirectoryNode> root = fbuffers_deserialize<DirectoryNode>(
        logs_config->root_dir(), nullptr, delimiter, true /* apply defaults */);
    auto version = logs_config->version();

    std::unique_ptr<LogsConfigTree> config_tree =
        std::make_unique<LogsConfigTree>(std::move(root), delimiter, version);
    // Build the lookup Index of the tree
    config_tree->rebuildIndex();

    return config_tree;
  } else {
    err = E::BADMSG;
    ld_critical("LogsConfig Tree deserialization failed, this is most likely "
                "a bad payload in the snapshot log");
    return nullptr;
  }
}

// Header => DeltaHeader
template <>
DeltaHeader
FBuffersLogsConfigCodec::fbuffers_deserialize<DeltaHeader, fbuffers::Header>(
    const fbuffers::Header* in) {
  DeltaHeader out;
  if (in) {
    uint64_t base_version = in->base_lsn();
    ConflictResolutionMode resolution_mode =
        static_cast<ConflictResolutionMode>(in->resolution_mode());
    out = DeltaHeader(config_version_t(base_version), resolution_mode);
  }
  return out;
}

// MkDir_Msg => MkDirectoryDelta
template <>
std::unique_ptr<MkDirectoryDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<MkDirectoryDelta,
                                              fbuffers::MkDir_Msg>(
    const fbuffers::MkDir_Msg* in,
    const DeltaHeader& header,
    const std::string& /*delimiter*/) {
  if (in) {
    std::string path = in->name()->str();
    bool mk_intermediates = in->recursive();
    LogAttributes attrs = fbuffers_deserialize<LogAttributes>(in->attrs());
    return std::make_unique<MkDirectoryDelta>(
        header, path, mk_intermediates, std::move(attrs));
  }
  return nullptr;
}

// Rm_Msg => RemoveDelta
template <>
std::unique_ptr<RemoveDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<RemoveDelta, fbuffers::Rm_Msg>(
    const fbuffers::Rm_Msg* in,
    const DeltaHeader& header,
    const std::string& /*delimiter*/) {
  if (in) {
    bool is_directory = in->validate_is_dir();
    bool recursive = in->recursive();
    std::string path = in->name()->str();
    return std::make_unique<RemoveDelta>(header, path, is_directory, recursive);
  }
  return nullptr;
}

// Rename_Msg => RenameDelta
template <>
std::unique_ptr<RenameDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<RenameDelta,
                                              fbuffers::Rename_Msg>(
    const fbuffers::Rename_Msg* in,
    const DeltaHeader& header,
    const std::string& /*delimiter*/) {
  if (in) {
    std::string from_path = in->from_path()->str();
    std::string to_path = in->to_path()->str();
    return std::make_unique<RenameDelta>(header, from_path, to_path);
  }
  return nullptr;
}

// SetAttrs_Msg => SetAttributesDelta
template <>
std::unique_ptr<SetAttributesDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<SetAttributesDelta,
                                              fbuffers::SetAttrs_Msg>(
    const fbuffers::SetAttrs_Msg* in,
    const DeltaHeader& header,
    const std::string& /*delimiter*/) {
  if (in) {
    std::string path = in->name()->str();
    LogAttributes attrs = fbuffers_deserialize<LogAttributes>(in->attrs());
    return std::make_unique<SetAttributesDelta>(header, path, std::move(attrs));
  }
  return nullptr;
}

// SetLogRange_Msg => SetLogRangeDelta
template <>
std::unique_ptr<SetLogRangeDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<SetLogRangeDelta,
                                              fbuffers::SetLogRange_Msg>(
    const fbuffers::SetLogRange_Msg* in,
    const DeltaHeader& header,
    const std::string& /*delimiter*/) {
  if (in) {
    std::string path = in->name()->str();
    logid_range_t range = fbuffers_deserialize<logid_range_t>(in->range());
    return std::make_unique<SetLogRangeDelta>(header, path, range);
  }
  return nullptr;
}

// MkLog_Msg => MkLogGroupDelta
template <>
std::unique_ptr<MkLogGroupDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<MkLogGroupDelta,
                                              fbuffers::MkLog_Msg>(
    const fbuffers::MkLog_Msg* in,
    const DeltaHeader& header,
    const std::string& /*delimiter*/) {
  if (in) {
    std::string path = in->name()->str();
    bool mk_intermediate_paths = in->mk_intermediate_paths();
    logid_range_t range = fbuffers_deserialize<logid_range_t>(in->range());
    LogAttributes attrs = fbuffers_deserialize<LogAttributes>(in->attrs());
    return std::make_unique<MkLogGroupDelta>(
        header, path, range, mk_intermediate_paths, std::move(attrs));
  }
  return nullptr;
}

template <>
std::unique_ptr<Delta>
FBuffersLogsConfigCodec::fbuffers_deserialize<Delta, fbuffers::Delta>(
    const fbuffers::Delta* in,
    const std::string& delimiter);

// BatchDelta => BatchDelta
template <>
std::unique_ptr<BatchDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<BatchDelta, fbuffers::BatchDelta>(
    const fbuffers::BatchDelta* in,
    const DeltaHeader& header,
    const std::string& delimiter) {
  if (in) {
    std::vector<std::unique_ptr<Delta>> ops;
    for (const auto* delta : *in->operations()) {
      ops.push_back(
          fbuffers_deserialize<Delta, fbuffers::Delta>(delta, delimiter));
    }
    return std::make_unique<BatchDelta>(header, std::move(ops));
  }
  return nullptr;
}

// SetTree_Msg => SetTreeDelta
template <>
std::unique_ptr<SetTreeDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<SetTreeDelta,
                                              fbuffers::SetTree_Msg>(
    const fbuffers::SetTree_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter) {
  if (in) {
    std::unique_ptr<LogsConfigTree> tree =
        fbuffers_deserialize<LogsConfigTree>(in->tree(), delimiter);
    return std::make_unique<SetTreeDelta>(header, std::move(tree));
  }
  return nullptr;
}

// fbuffers::Delta => Delta
template <>
std::unique_ptr<Delta>
FBuffersLogsConfigCodec::fbuffers_deserialize<Delta, fbuffers::Delta>(
    const fbuffers::Delta* in,
    const std::string& delimiter) {
  if (in) {
    DeltaHeader header = fbuffers_deserialize<DeltaHeader>(in->header());
    switch (in->payload_type()) {
      case fbuffers::DeltaPayload::MkDir_Msg: {
        const fbuffers::MkDir_Msg* payload =
            static_cast<const fbuffers::MkDir_Msg*>(in->payload());
        return fbuffers_deserialize<MkDirectoryDelta, fbuffers::MkDir_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::Rm_Msg: {
        const fbuffers::Rm_Msg* payload =
            static_cast<const fbuffers::Rm_Msg*>(in->payload());
        return fbuffers_deserialize<RemoveDelta, fbuffers::Rm_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::Rename_Msg: {
        const fbuffers::Rename_Msg* payload =
            static_cast<const fbuffers::Rename_Msg*>(in->payload());
        return fbuffers_deserialize<RenameDelta, fbuffers::Rename_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::SetAttrs_Msg: {
        const fbuffers::SetAttrs_Msg* payload =
            static_cast<const fbuffers::SetAttrs_Msg*>(in->payload());
        return fbuffers_deserialize<SetAttributesDelta, fbuffers::SetAttrs_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::SetLogRange_Msg: {
        const fbuffers::SetLogRange_Msg* payload =
            static_cast<const fbuffers::SetLogRange_Msg*>(in->payload());
        return fbuffers_deserialize<SetLogRangeDelta,
                                    fbuffers::SetLogRange_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::MkLog_Msg: {
        const fbuffers::MkLog_Msg* payload =
            static_cast<const fbuffers::MkLog_Msg*>(in->payload());
        return fbuffers_deserialize<MkLogGroupDelta, fbuffers::MkLog_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::BatchDelta: {
        const fbuffers::BatchDelta* payload =
            static_cast<const fbuffers::BatchDelta*>(in->payload());
        return fbuffers_deserialize<BatchDelta, fbuffers::BatchDelta>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::SetTree_Msg: {
        const fbuffers::SetTree_Msg* payload =
            static_cast<const fbuffers::SetTree_Msg*>(in->payload());
        return fbuffers_deserialize<SetTreeDelta, fbuffers::SetTree_Msg>(
            payload, header, delimiter);
      }
      case fbuffers::DeltaPayload::NONE:
        // This should never happen, this means that we we have a payload
        // without a delta type, we cannot just ignore that.
        ld_check(false);
        break;
    }
  }
  return nullptr;
}

}}} // namespace facebook::logdevice::logsconfig
