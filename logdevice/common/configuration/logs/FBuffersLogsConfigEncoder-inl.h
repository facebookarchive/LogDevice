/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

// a template to generate a 1:1 mapping between buffers custom types and c++
// types
#define CONVERT_TO_BUFFERS_TYPE(from, to)                                 \
  template <>                                                             \
  flatbuffers::Offset<fbuffers::to>                                       \
  FBuffersLogsConfigCodec::fbuffers_serialize<const from&, fbuffers::to>( \
      flatbuffers::FlatBufferBuilder & builder, const from& in, bool flatten);

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
    bool flatten);

// Converts std::chrono::seconds to fbuffers::Long
template <>
flatbuffers::Offset<fbuffers::Long>
FBuffersLogsConfigCodec::fbuffers_serialize<const std::chrono::seconds&,
                                            fbuffers::Long>(
    flatbuffers::FlatBufferBuilder& builder,
    const std::chrono::seconds& in,
    bool flatten);

// Converts std::chrono::milliseconds to fbuffers::Long
template <>
flatbuffers::Offset<fbuffers::Long>
FBuffersLogsConfigCodec::fbuffers_serialize<const std::chrono::milliseconds&,
                                            fbuffers::Long>(
    flatbuffers::FlatBufferBuilder& builder,
    const std::chrono::milliseconds& in,
    bool flatten);

// Converts NodeLocationScope to fbuffers::UInt8
template <>
flatbuffers::Offset<fbuffers::UInt8>
FBuffersLogsConfigCodec::fbuffers_serialize<const NodeLocationScope&,
                                            fbuffers::UInt8>(
    flatbuffers::FlatBufferBuilder& builder,
    const NodeLocationScope& in,
    bool flatten);

// LogAttributes => fbuffers::LogAttrs
template <>
flatbuffers::Offset<fbuffers::LogAttrs>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogAttributes& attributes,
    bool flatten);

// logid_range_t => fbuffers::LogRange
template <>
flatbuffers::Offset<fbuffers::LogRange>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const logid_range_t& range,
    bool flatten);

// LogGroupNode => fbuffers::LogGroup
template <>
flatbuffers::Offset<fbuffers::LogGroup>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogGroupNode& log_group,
    bool flatten);

// DirectoryNode => fbuffers::Directory
template <>
flatbuffers::Offset<fbuffers::Directory>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const DirectoryNode& dir,
    bool flatten);

// LogGroupWithParentPath => fbuffers::LogGroupInDirectory
template <>
flatbuffers::Offset<fbuffers::LogGroupInDirectory>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogGroupWithParentPath& log_group_in_directory,
    bool flatten);

// LogsConfigTree => fbuffers::LogsConfig (buffers)
template <>
flatbuffers::Offset<fbuffers::LogsConfig>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const LogsConfigTree& tree,
    bool flatten);

// fbuffers::Delta => Delta
flatbuffers::Offset<fbuffers::Delta>
fbuffers_serialize_polymorphic(flatbuffers::FlatBufferBuilder& builder,
                               const Delta* in);

template <>
// MkDirectoryDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const MkDirectoryDelta& in,
    bool flatten);

template <>
// MkLogGroupDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const MkLogGroupDelta& in,
    bool flatten);

template <>
// RemoveDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const RemoveDelta& in,
    bool flatten);

template <>
// RenameDelta => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const RenameDelta& in,
    bool flatten);

template <>
// SetLogRangeDelta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const SetLogRangeDelta& in,
    bool flatten);

template <>
// SetAttributesDelta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const SetAttributesDelta& in,
    bool flatten);

template <>
// BatchDelta  => fbuffers::Delta
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const BatchDelta& in,
    bool flatten);

// SetTreeDelta => fbuffers::Delta
template <>
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const SetTreeDelta& in,
    bool flatten);

// Delta => fbuffers::Delta
template <>
flatbuffers::Offset<fbuffers::Delta>
FBuffersLogsConfigCodec::fbuffers_serialize(
    flatbuffers::FlatBufferBuilder& builder,
    const Delta& in,
    bool flatten);
