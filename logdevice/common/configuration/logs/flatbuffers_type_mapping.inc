/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

template<>
class FBuffersLogsConfigCodec::TypeMapping<LogsConfigTree> {
 public:
  using to = fbuffers::LogsConfig;
  // Potentially big, compressing to avoid hitting the maximum payload size
  static constexpr bool shouldBeCompressed = true;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<LogGroupNode> {
 public:
  using to = fbuffers::LogGroup;
  static constexpr bool shouldBeCompressed = false;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<DirectoryNode> {
 public:
  using to = fbuffers::Directory;
  // This can get really big if you are returning the root directory of a large
  // tree to the client as a result of getDirectory() call
  static constexpr bool shouldBeCompressed = true;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<LogAttributes> {
 public:
  using to = fbuffers::LogAttrs;
  static constexpr bool shouldBeCompressed = false;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<Delta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<MkDirectoryDelta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<MkLogGroupDelta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};

template<>
class FBuffersLogsConfigCodec::TypeMapping<RemoveDelta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};
template<>
class FBuffersLogsConfigCodec::TypeMapping<RenameDelta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};
template<>
class FBuffersLogsConfigCodec::TypeMapping<SetAttributesDelta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};
template<>
class FBuffersLogsConfigCodec::TypeMapping<SetLogRangeDelta> {
 public:
  using to = fbuffers::Delta;
  static constexpr bool shouldBeCompressed = false;
};
template<>
class FBuffersLogsConfigCodec::TypeMapping<BatchDelta> {
 public:
  using to = fbuffers::Delta;
  // There is a possibility of this getting big
  static constexpr bool shouldBeCompressed = true;
};
template<>
class FBuffersLogsConfigCodec::TypeMapping<SetTreeDelta> {
 public:
  using to = fbuffers::Delta;
  // This holds the full LogsConfigTree, used in Import/Migration
  static constexpr bool shouldBeCompressed = true;
};
template<>
class FBuffersLogsConfigCodec::TypeMapping<LogGroupWithParentPath> {
 public:
  using to = fbuffers::LogGroupInDirectory;
  static constexpr bool shouldBeCompressed = false;
};
