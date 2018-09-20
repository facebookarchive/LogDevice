/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#define ATTR_TO_ATTRIBUTE(to)                                       \
  template <>                                                       \
  Attribute<to>                                                     \
  FBuffersLogsConfigCodec::fbuffers_deserialize<Attribute<to>,      \
                                                fbuffers::LogAttr>( \
      const fbuffers::LogAttr* attr, const Attribute<to>& parent);

ATTR_TO_ATTRIBUTE(uint8_t);
ATTR_TO_ATTRIBUTE(int32_t);
ATTR_TO_ATTRIBUTE(int64_t);
ATTR_TO_ATTRIBUTE(uint64_t);
ATTR_TO_ATTRIBUTE(bool);
ATTR_TO_ATTRIBUTE(std::chrono::seconds);
ATTR_TO_ATTRIBUTE(std::chrono::milliseconds);
ATTR_TO_ATTRIBUTE(std::string);
ATTR_TO_ATTRIBUTE(NodeLocationScope);
#undef ATTR_TO_ATTRIBUTE

// fbuffers::LogAttrs => LogAttributes
template <>
LogAttributes
FBuffersLogsConfigCodec::fbuffers_deserialize<logsconfig::LogAttributes,
                                              fbuffers::LogAttrs>(
    const fbuffers::LogAttrs* attrs,
    const LogAttributes& parent_attributes);

// fbuffers::LogAttrs => LogAttributes -- without applying parent attrs
template <>
LogAttributes
FBuffersLogsConfigCodec::fbuffers_deserialize<logsconfig::LogAttributes,
                                              fbuffers::LogAttrs>(
    const fbuffers::LogAttrs* attrs);

// fbuffers::LogAttrs => LogAttributes
template <>
std::unique_ptr<LogAttributes>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes,
                                              fbuffers::LogAttrs>(
    const fbuffers::LogAttrs* attrs,
    const std::string& delimiter);

// fbuffers::LogRange => logid_range_t
template <>
logid_range_t FBuffersLogsConfigCodec::fbuffers_deserialize<logid_range_t,
                                                            fbuffers::LogRange>(
    const fbuffers::LogRange* range);

// fbuffers::LogGroup => LogGroupNode
template <>
std::unique_ptr<LogGroupNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogGroupNode, fbuffers::LogGroup>(
    const fbuffers::LogGroup* group,
    const std::string& delimiter,
    const LogAttributes& parent_attributes);

// fbuffers::Directory => DirectoryNode
template <>
std::unique_ptr<DirectoryNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<DirectoryNode,
                                              fbuffers::Directory>(
    const fbuffers::Directory* dir,
    DirectoryNode* parent,
    const std::string& delimiter,
    bool apply_defaults);

// fbuffers::Directory => DirectoryNode
template <>
std::unique_ptr<DirectoryNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<DirectoryNode,
                                              fbuffers::Directory>(
    const fbuffers::Directory* dir,
    const std::string& delimiter);

// fbuffers::LogGroup => LogGroupNode
template <>
std::unique_ptr<LogGroupNode>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogGroupNode, fbuffers::LogGroup>(
    const fbuffers::LogGroup* group,
    const std::string& delimiter);

// fbuffers::LogGroupInDirectory => FlatLogGroupInDirectory
template <>
std::unique_ptr<LogGroupWithParentPath>
FBuffersLogsConfigCodec::fbuffers_deserialize<LogGroupWithParentPath,
                                              fbuffers::LogGroupInDirectory>(
    const fbuffers::LogGroupInDirectory* group,
    const std::string& delimiter);

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
    const std::string& delimiter);

// Header => DeltaHeader
template <>
DeltaHeader
FBuffersLogsConfigCodec::fbuffers_deserialize<DeltaHeader, fbuffers::Header>(
    const fbuffers::Header* in);

// MkDir_Msg => MkDirectoryDelta
template <>
std::unique_ptr<MkDirectoryDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<MkDirectoryDelta,
                                              fbuffers::MkDir_Msg>(
    const fbuffers::MkDir_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// Rm_Msg => RemoveDelta
template <>
std::unique_ptr<RemoveDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<RemoveDelta, fbuffers::Rm_Msg>(
    const fbuffers::Rm_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// Rename_Msg => RenameDelta
template <>
std::unique_ptr<RenameDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<RenameDelta,
                                              fbuffers::Rename_Msg>(
    const fbuffers::Rename_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// SetAttrs_Msg => SetAttributesDelta
template <>
std::unique_ptr<SetAttributesDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<SetAttributesDelta,
                                              fbuffers::SetAttrs_Msg>(
    const fbuffers::SetAttrs_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// SetLogRange_Msg => SetLogRangeDelta
template <>
std::unique_ptr<SetLogRangeDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<SetLogRangeDelta,
                                              fbuffers::SetLogRange_Msg>(
    const fbuffers::SetLogRange_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// MkLog_Msg => MkLogGroupDelta
template <>
std::unique_ptr<MkLogGroupDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<MkLogGroupDelta,
                                              fbuffers::MkLog_Msg>(
    const fbuffers::MkLog_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// BatchDelta => BatchDelta
template <>
std::unique_ptr<BatchDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<BatchDelta, fbuffers::BatchDelta>(
    const fbuffers::BatchDelta* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// SetTree_Msg => SetTreeDelta
template <>
std::unique_ptr<SetTreeDelta>
FBuffersLogsConfigCodec::fbuffers_deserialize<SetTreeDelta,
                                              fbuffers::SetTree_Msg>(
    const fbuffers::SetTree_Msg* in,
    const DeltaHeader& header,
    const std::string& delimiter);

// fbuffers::Delta => Delta
template <>
std::unique_ptr<Delta>
FBuffersLogsConfigCodec::fbuffers_deserialize<Delta, fbuffers::Delta>(
    const fbuffers::Delta* in,
    const std::string& delimiter);
