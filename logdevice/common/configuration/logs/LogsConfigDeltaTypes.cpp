/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace logsconfig {

/**
 * Returns delta operation name from DeltaOpType enum value
 */
std::string deltaOpTypeName(DeltaOpType op) {
  std::string t;
  switch (op) {
#define DELTA_TYPE(d)  \
  case DeltaOpType::d: \
    t = #d;            \
    break;
#include "logdevice/common/configuration/logs/delta_types.inc"
  }
  return t;
}

std::string Delta::describe() const {
  return folly::format("LogsConfigDelta[Op={}, Path={}]",
                       deltaOpTypeName(delta_type_),
                       getPath().value_or("none"))
      .str();
}

int MkDirectoryDelta::apply(LogsConfigTree& tree,
                            std::string& failure_reason) const {
  if (tree.addDirectory(
          path, should_make_intermediates, attrs, failure_reason) != nullptr) {
    return 0;
  }
  return -1;
}

int MkLogGroupDelta::apply(LogsConfigTree& tree,
                           std::string& failure_reason) const {
  if (tree.addLogGroup(
          path, range, attrs, should_make_intermediates, failure_reason) !=
      nullptr) {
    return 0;
  }
  return -1;
}

int RenameDelta::apply(LogsConfigTree& tree,
                       std::string& failure_reason) const {
  return tree.rename(from_path, to_path, failure_reason);
}

int SetAttributesDelta::apply(LogsConfigTree& tree,
                              std::string& failure_reason) const {
  return tree.setAttributes(path, attrs, failure_reason);
}

int SetLogRangeDelta::apply(LogsConfigTree& tree,
                            std::string& failure_reason) const {
  std::shared_ptr<LogGroupNode> result = tree.findLogGroup(path);
  if (!result) {
    failure_reason = folly::format("Log group '{}' was not found!", path).str();
    err = E::NOTFOUND;
    return -1;
  }
  LogGroupNode replacement = result->withRange(range);
  if (!tree.replaceLogGroup(path, replacement, failure_reason)) {
    return -1;
  }
  return 0;
}

int RemoveDelta::apply(LogsConfigTree& tree,
                       std::string& failure_reason) const {
  if (is_directory) {
    return tree.deleteDirectory(path, recursive, failure_reason);
  }
  return tree.deleteLogGroup(path, failure_reason);
}

int SetTreeDelta::apply(LogsConfigTree& state,
                        std::string& /* unused */) const {
  if (tree_) {
    state = *tree_;
  } else {
    ld_warning("Trying to set the LogsConfigTree to a nullptr tree via a "
               "SetTreeDelta, delta ignored!");
  }
  return 0;
}
}}} // namespace facebook::logdevice::logsconfig
