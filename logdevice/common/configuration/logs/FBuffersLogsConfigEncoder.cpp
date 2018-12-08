/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"

namespace facebook { namespace logdevice { namespace logsconfig {

// a template to generate a 1:1 mapping between buffers custom types and c++
// types
#define CONVERT_TO_BUFFERS_TYPE(from, to)                                 \
  template <>                                                             \
  flatbuffers::Offset<fbuffers::to>                                       \
  FBuffersLogsConfigCodec::fbuffers_serialize<const from&, fbuffers::to>( \
      flatbuffers::FlatBufferBuilder & builder, const from& in, bool) {   \
    return fbuffers::Create##to(builder, in);                             \
  }

// realization of the converters
CONVERT_TO_BUFFERS_TYPE(uint8_t, UInt8)
CONVERT_TO_BUFFERS_TYPE(int32_t, Int)
CONVERT_TO_BUFFERS_TYPE(int64_t, Long)
CONVERT_TO_BUFFERS_TYPE(uint64_t, ULong)
CONVERT_TO_BUFFERS_TYPE(bool, Bool)
#undef CONVERT_TO_BUFFERS_TYPE

// Converts for non-POD types that our template won't cut.
// Converts std::string to fbuffers::String
template <>
flatbuffers::Offset<fbuffers::String>
FBuffersLogsConfigCodec::fbuffers_serialize<const std::string&,
                                            fbuffers::String>(
    flatbuffers::FlatBufferBuilder& builder,
    const std::string& in,
    bool /*flatten*/) {
  return fbuffers::CreateString(builder, builder.CreateString(in));
}

// Converts std::chrono::seconds to fbuffers::Long
template <>
flatbuffers::Offset<fbuffers::Long>
FBuffersLogsConfigCodec::fbuffers_serialize<const std::chrono::seconds&,
                                            fbuffers::Long>(
    flatbuffers::FlatBufferBuilder& builder,
    const std::chrono::seconds& in,
    bool flatten) {
  return fbuffers_serialize<const int64_t&, fbuffers::Long>(
      builder, in.count(), flatten);
}

// Converts std::chrono::milliseconds to fbuffers::Long
template <>
flatbuffers::Offset<fbuffers::Long>
FBuffersLogsConfigCodec::fbuffers_serialize<const std::chrono::milliseconds&,
                                            fbuffers::Long>(
    flatbuffers::FlatBufferBuilder& builder,
    const std::chrono::milliseconds& in,
    bool flatten) {
  return fbuffers_serialize<const int64_t&, fbuffers::Long>(
      builder, in.count(), flatten);
}

// Converts NodeLocationScope to fbuffers::UInt8
template <>
flatbuffers::Offset<fbuffers::UInt8>
FBuffersLogsConfigCodec::fbuffers_serialize<const NodeLocationScope&,
                                            fbuffers::UInt8>(
    flatbuffers::FlatBufferBuilder& builder,
    const NodeLocationScope& in,
    bool flatten) {
  return fbuffers_serialize<const uint8_t&, fbuffers::UInt8>(
      builder, static_cast<const uint8_t>(in), flatten);
}

// Converts Compression to fbuffers::UInt8
template <>
flatbuffers::Offset<fbuffers::UInt8>
FBuffersLogsConfigCodec::fbuffers_serialize<const Compression&,
                                            fbuffers::UInt8>(
    flatbuffers::FlatBufferBuilder& builder,
    const Compression& in,
    bool flatten) {
  return fbuffers_serialize<const uint8_t&, fbuffers::UInt8>(
      builder, static_cast<const uint8_t>(in), flatten);
}

// We serialize only the non-inherited values to save space
#define SERIALIZE_ATTRIBUTE(key, type, from)                   \
  if (from() && (flatten || !from().isInherited())) {          \
    attrs.push_back(fbuffers::CreateLogAttr(                   \
        builder,                                               \
        builder.CreateString(key),                             \
        from().isInherited(),                                  \
        fbuffers::Any::type,                                   \
        fbuffers_serialize<decltype(*from()), fbuffers::type>( \
            builder, *from(), flatten)                         \
            .Union()));                                        \
  }

// We serialize only the non-inherited values to save space
#define SERIALIZE_ATTRIBUTE_OPT(key, type, from)                           \
  if (from() && (flatten || !from().isInherited())) {                      \
    if (!from().value().hasValue()) {                                      \
      attrs.push_back(fbuffers::CreateLogAttr(builder,                     \
                                              builder.CreateString(key),   \
                                              from().isInherited(),        \
                                              fbuffers::Any::NoValueSet)); \
    } else {                                                               \
      attrs.push_back(fbuffers::CreateLogAttr(                             \
          builder,                                                         \
          builder.CreateString(key),                                       \
          from().isInherited(),                                            \
          fbuffers::Any::type,                                             \
          fbuffers_serialize<decltype(*from().value()), fbuffers::type>(   \
              builder, from().value().value(), flatten)                    \
              .Union()));                                                  \
    }                                                                      \
  }

// LogAttributes => fbuffers::LogAttrs
template <>
flatbuffers::Offset<fbuffers::LogAttrs>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogAttributes& attributes,
    bool flatten) {
  std::vector<flatbuffers::Offset<fbuffers::LogAttr>> attrs;

  SERIALIZE_ATTRIBUTE(REPLICATION_FACTOR, Int, attributes.replicationFactor);
  SERIALIZE_ATTRIBUTE(EXTRA_COPIES, Int, attributes.extraCopies);
  SERIALIZE_ATTRIBUTE(SYNCED_COPIES, Int, attributes.syncedCopies);
  SERIALIZE_ATTRIBUTE(MAX_WRITES_IN_FLIGHT, Int, attributes.maxWritesInFlight);
  SERIALIZE_ATTRIBUTE(SINGLE_WRITER, Bool, attributes.singleWriter);
  SERIALIZE_ATTRIBUTE_OPT(NODESET_SIZE, Int, attributes.nodeSetSize);
  SERIALIZE_ATTRIBUTE_OPT(BACKLOG, Long, attributes.backlogDuration);
  SERIALIZE_ATTRIBUTE_OPT(DELIVERY_LATENCY, Long, attributes.deliveryLatency);
  SERIALIZE_ATTRIBUTE(SCD_ENABLED, Bool, attributes.scdEnabled);
  SERIALIZE_ATTRIBUTE(LOCAL_SCD_ENABLED, Bool, attributes.localScdEnabled);
  SERIALIZE_ATTRIBUTE_OPT(WRITE_TOKEN, String, attributes.writeToken);
  SERIALIZE_ATTRIBUTE(STICKY_COPYSETS, Bool, attributes.stickyCopySets);
  SERIALIZE_ATTRIBUTE(
      SYNC_REPLICATION_SCOPE, UInt8, attributes.syncReplicationScope);
  SERIALIZE_ATTRIBUTE_OPT(
      SEQUENCER_AFFINITY, String, attributes.sequencerAffinity);
  SERIALIZE_ATTRIBUTE(SEQUENCER_BATCHING, Bool, attributes.sequencerBatching);
  SERIALIZE_ATTRIBUTE(SEQUENCER_BATCHING_TIME_TRIGGER,
                      Long,
                      attributes.sequencerBatchingTimeTrigger);
  SERIALIZE_ATTRIBUTE(SEQUENCER_BATCHING_SIZE_TRIGGER,
                      Long,
                      attributes.sequencerBatchingSizeTrigger);
  SERIALIZE_ATTRIBUTE(SEQUENCER_BATCHING_COMPRESSION,
                      UInt8,
                      attributes.sequencerBatchingCompression);
  SERIALIZE_ATTRIBUTE(SEQUENCER_BATCHING_PASSTHRU_THRESHOLD,
                      Long,
                      attributes.sequencerBatchingPassthruThreshold);
  SERIALIZE_ATTRIBUTE(TAIL_OPTIMIZED, Bool, attributes.tailOptimized);

  // permissions
  std::vector<flatbuffers::Offset<fbuffers::Permission>> perms;
  if (attributes.permissions() &&
      (flatten || !attributes.permissions().isInherited())) {
    for (const auto& elem : attributes.permissions().value()) {
      std::vector<uint8_t> actions;
      for (uint8_t i = 0; i < static_cast<uint8_t>(ACTION::MAX); i++) {
        if (elem.second[i] == true) {
          actions.push_back(i);
        }
      }
      auto permission =
          fbuffers::CreatePermission(builder,
                                     builder.CreateString(elem.first),
                                     builder.CreateVector(actions));
      perms.push_back(std::move(permission));
    }
  }
  // acls
  std::vector<flatbuffers::Offset<flatbuffers::String>> acls;
  if (attributes.acls() && (flatten || !attributes.acls().isInherited())) {
    for (const auto& elem : attributes.acls().value()) {
      auto acl = builder.CreateString(elem);
      acls.push_back(std::move(acl));
    }
  }
  // shadow acls
  std::vector<flatbuffers::Offset<flatbuffers::String>> aclsShadow;
  if (attributes.aclsShadow() &&
      (flatten || !attributes.aclsShadow().isInherited())) {
    for (const auto& elem : attributes.aclsShadow().value()) {
      aclsShadow.push_back(builder.CreateString(elem));
    }
  }
  // extras
  std::vector<flatbuffers::Offset<fbuffers::ExtraAttr>> extras;
  if (attributes.extras() && (flatten || !attributes.extras().isInherited())) {
    for (const auto& extra : attributes.extras().value()) {
      auto extra_item =
          fbuffers::CreateExtraAttr(builder,
                                    builder.CreateString(extra.first),
                                    builder.CreateString(extra.second));
      extras.push_back(std::move(extra_item));
    }
  }
  // replicateAcross
  std::vector<flatbuffers::Offset<fbuffers::ScopeReplication>> replicateAcross;
  if (attributes.replicateAcross() &&
      (flatten || !attributes.extras().isInherited())) {
    for (const auto& rf : attributes.replicateAcross().value()) {
      auto scope_replication =
          fbuffers::CreateScopeReplication(builder,
                                           static_cast<uint8_t>(rf.first),
                                           static_cast<uint8_t>(rf.second));
      replicateAcross.push_back(std::move(scope_replication));
    }
  }
  // shadow
  flatbuffers::Offset<fbuffers::Shadow> shadow_item;
  if (attributes.shadow() && (flatten || !attributes.shadow().isInherited())) {
    auto& shadow = attributes.shadow().value();
    shadow_item = fbuffers::CreateShadow(
        builder, builder.CreateString(shadow.destination()), shadow.ratio());
  }

  // Set the permissions map, acls and extras to 0 meaning that it's unset
  // (flatbuffers convention)
  return fbuffers::CreateLogAttrs(
      builder,
      builder.CreateVectorOfSortedTables(&attrs),
      perms.size() ? builder.CreateVectorOfSortedTables(&perms) : 0,
      extras.size() ? builder.CreateVectorOfSortedTables(&extras) : 0,
      replicateAcross.size() ? builder.CreateVector(replicateAcross) : 0,
      acls.size() ? builder.CreateVector(acls) : 0,
      aclsShadow.size() ? builder.CreateVector(aclsShadow) : 0,
      attributes.shadow() ? shadow_item : 0);
}

// logid_range_t => fbuffers::LogRange
template <>
flatbuffers::Offset<fbuffers::LogRange>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const logid_range_t& range,
    bool /*flatten*/) {
  return fbuffers::CreateLogRange(builder,
                                  static_cast<uint64_t>(range.first),
                                  static_cast<uint64_t>(range.second));
}

// LogGroupNode => fbuffers::LogGroup
template <>
flatbuffers::Offset<fbuffers::LogGroup>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogGroupNode& log_group,
    bool flatten) {
  flatbuffers::Offset<fbuffers::LogAttrs> attrs =
      fbuffers_serialize<const LogAttributes&, fbuffers::LogAttrs>(
          builder, log_group.attrs(), flatten);
  auto range = fbuffers_serialize<const logid_range_t&, fbuffers::LogRange>(
      builder, log_group.range(), flatten);
  return fbuffers::CreateLogGroup(
      builder, builder.CreateString(log_group.name()), range, attrs);
}

// DirectoryNode => fbuffers::Directory
template <>
flatbuffers::Offset<fbuffers::Directory>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const DirectoryNode& dir,
    bool flatten) {
  flatbuffers::Offset<fbuffers::LogAttrs> attrs =
      fbuffers_serialize<const LogAttributes&, fbuffers::LogAttrs>(
          builder, dir.attrs(), flatten);
  std::vector<flatbuffers::Offset<fbuffers::Directory>> dirs_vec;
  for (const auto& item : dir.children()) {
    // We don't flatten the sub-directories because we can always apply the
    // current dir's attributes to them and get the same result.
    dirs_vec.push_back(
        fbuffers_serialize<const DirectoryNode&, fbuffers::Directory>(
            builder, *item.second, false /* flatten */));
  }
  auto children = builder.CreateVector(dirs_vec);

  std::vector<flatbuffers::Offset<fbuffers::LogGroup>> logs_vec;
  for (const auto& item : dir.logs()) {
    // We don't flatten the log groups because we can always apply the
    // current dir's attributes to them and get the same result.
    logs_vec.push_back(
        fbuffers_serialize<const LogGroupNode&, fbuffers::LogGroup>(
            builder, *item.second, false /* flatten */));
  }
  auto log_groups = builder.CreateVector(logs_vec);
  return fbuffers::CreateDirectory(
      builder, builder.CreateString(dir.name()), attrs, log_groups, children);
}

// LogGroupWithParentPath => fbuffers::LogGroupInDirectory
template <>
flatbuffers::Offset<fbuffers::LogGroupInDirectory>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogGroupWithParentPath& log_group_in_directory,
    bool flatten) {
  auto log_group = fbuffers_serialize<const LogGroupNode&, fbuffers::LogGroup>(
      builder, *log_group_in_directory.log_group, flatten);
  return fbuffers::CreateLogGroupInDirectory(
      builder,
      log_group,
      builder.CreateString(log_group_in_directory.parent_path));
}

// LogsConfigTree => fbuffers::LogsConfig (buffers)
template <>
flatbuffers::Offset<fbuffers::LogsConfig>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogsConfigTree& tree,
    bool flatten) {
  flatbuffers::Offset<fbuffers::Directory> root = 0;
  if (tree.root()) {
    root = fbuffers_serialize<const DirectoryNode&, fbuffers::Directory>(
        builder, *tree.root(), flatten);
  }

  return fbuffers::CreateLogsConfig(builder, tree.version(), root);
}

// fbuffers::Delta => Delta
flatbuffers::Offset<fbuffers::Delta>
fbuffers_serialize_polymorphic(flatbuffers::FlatBufferBuilder& builder,
                               const Delta* in) {
  fbuffers::Header header{in->header().base_version().val(),
                          static_cast<fbuffers::ConflictResolutionMode>(
                              in->header().resolution_mode())};

  flatbuffers::Offset<void> payload;
  fbuffers::DeltaPayload payload_type = fbuffers::DeltaPayload::NONE;

  switch (in->type()) {
    case DeltaOpType::INVALID:
      // should never happen!
      ld_check(false);
      break;
    case DeltaOpType::MK_DIRECTORY: {
      payload_type = fbuffers::DeltaPayload::MkDir_Msg;
      const MkDirectoryDelta* delta = static_cast<const MkDirectoryDelta*>(in);
      payload =
          fbuffers::CreateMkDir_Msg(
              builder,
              builder.CreateString(delta->path),
              delta->should_make_intermediates,
              FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                          fbuffers::LogAttrs>(
                  builder, delta->attrs, false))
              .Union();
    } break;
    case DeltaOpType::MK_LOG_GROUP: {
      payload_type = fbuffers::DeltaPayload::MkLog_Msg;
      const MkLogGroupDelta* delta = static_cast<const MkLogGroupDelta*>(in);
      payload =
          fbuffers::CreateMkLog_Msg(
              builder,
              builder.CreateString(delta->path),
              FBuffersLogsConfigCodec::fbuffers_serialize<const logid_range_t&,
                                                          fbuffers::LogRange>(
                  builder, delta->range, false),
              delta->should_make_intermediates,
              FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                          fbuffers::LogAttrs>(
                  builder, delta->attrs, false))
              .Union();
    } break;
    case DeltaOpType::REMOVE: {
      payload_type = fbuffers::DeltaPayload::Rm_Msg;
      const RemoveDelta* delta = static_cast<const RemoveDelta*>(in);
      payload = fbuffers::CreateRm_Msg(builder,
                                       builder.CreateString(delta->path),
                                       delta->is_directory,
                                       delta->recursive)
                    .Union();
      break;
    }
    case DeltaOpType::RENAME: {
      payload_type = fbuffers::DeltaPayload::Rename_Msg;
      const RenameDelta* delta = static_cast<const RenameDelta*>(in);
      payload =
          fbuffers::CreateRename_Msg(builder,
                                     builder.CreateString(delta->from_path),
                                     builder.CreateString(delta->to_path))
              .Union();
    } break;
    case DeltaOpType::SET_ATTRS: {
      payload_type = fbuffers::DeltaPayload::SetAttrs_Msg;
      const SetAttributesDelta* delta =
          static_cast<const SetAttributesDelta*>(in);
      payload =
          fbuffers::CreateSetAttrs_Msg(
              builder,
              builder.CreateString(delta->path),
              FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                          fbuffers::LogAttrs>(
                  builder, delta->attrs, false))
              .Union();
    } break;
    case DeltaOpType::SET_LOG_RANGE: {
      payload_type = fbuffers::DeltaPayload::SetLogRange_Msg;
      const SetLogRangeDelta* delta = static_cast<const SetLogRangeDelta*>(in);
      payload =
          fbuffers::CreateSetLogRange_Msg(
              builder,
              builder.CreateString(delta->path),
              FBuffersLogsConfigCodec::fbuffers_serialize<const logid_range_t&,
                                                          fbuffers::LogRange>(
                  builder, delta->range, false))
              .Union();
    } break;
    case DeltaOpType::BATCH: {
      payload_type = fbuffers::DeltaPayload::BatchDelta;
      const BatchDelta* delta = static_cast<const BatchDelta*>(in);
      std::vector<flatbuffers::Offset<fbuffers::Delta>> ops;
      for (const std::unique_ptr<Delta>& operation : delta->deltas) {
        ops.push_back(fbuffers_serialize_polymorphic(builder, operation.get()));
      }
      payload = fbuffers::CreateBatchDelta(builder, builder.CreateVector(ops))
                    .Union();
    } break;
    case DeltaOpType::SET_TREE: {
      payload_type = fbuffers::DeltaPayload::SetTree_Msg;
      const SetTreeDelta* delta = static_cast<const SetTreeDelta*>(in);
      payload =
          fbuffers::CreateSetTree_Msg(
              builder,
              FBuffersLogsConfigCodec::fbuffers_serialize<const LogsConfigTree&,
                                                          fbuffers::LogsConfig>(
                  builder, delta->getTree(), false))
              .Union();
    } break;
  }
  return fbuffers::CreateDelta(builder, &header, payload_type, payload);
}

template <>
// MkDirectoryDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const MkDirectoryDelta& in,
    bool /*flatten*/) {
  // we don't send flatten argument down the chain because we never
  // flatten
  // deltas
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// MkLogGroupDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const MkLogGroupDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// RemoveDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const RemoveDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// RenameDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const RenameDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// SetLogRangeDelta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const SetLogRangeDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// SetAttributesDelta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const SetAttributesDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// BatchDelta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const BatchDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

// SetTreeDelta => fbuffers::Delta
template <>
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const SetTreeDelta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

template <>
// Delta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const Delta& in,
    bool /*flatten*/) {
  return fbuffers_serialize_polymorphic(builder, &in);
}

}}} // namespace facebook::logdevice::logsconfig
