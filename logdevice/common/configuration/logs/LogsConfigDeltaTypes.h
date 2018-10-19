/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace logsconfig {

enum class ConflictResolutionMode : uint8_t { STRICT = 0, AUTO };

enum class DeltaOpType {
#define DELTA_TYPE(name) name,
#include "logdevice/common/configuration/logs/delta_types.inc"
};

std::string deltaOpTypeName(DeltaOpType op);

class DeltaHeader {
 public:
  DeltaHeader()
      : base_version_(config_version_t(-1)),
        resolution_mode_(ConflictResolutionMode::AUTO) {}
  DeltaHeader(const config_version_t& base_version,
              ConflictResolutionMode resolution_mode)
      : base_version_(base_version), resolution_mode_(resolution_mode) {}

  const config_version_t& base_version() const {
    return base_version_;
  }
  ConflictResolutionMode resolution_mode() const {
    return resolution_mode_;
  }

 private:
  /**
   * The base version at which the delta should validate to, -1 means that
   * we don't really have or care about the version (this usually is set
   * when ConflictResolutionMode is Auto)
   */
  config_version_t base_version_ = config_version_t(-1);
  ConflictResolutionMode resolution_mode_ = ConflictResolutionMode::STRICT;
};

/**
 * The abstract class representing a delta, deltas must implement apply()
 * method which defines the effect of the application of this delta on a tree
 */
class Delta {
 public:
  Delta() : header_(DeltaHeader()), delta_type_(DeltaOpType::INVALID) {}
  Delta(const DeltaHeader& header, DeltaOpType type)
      : header_(header), delta_type_(type) {}

  const DeltaHeader& header() const {
    return header_;
  }
  virtual DeltaOpType type() const {
    return delta_type_;
  }

  virtual std::string describe() const;

  /**
   * The abstract parent for delta applications, children must implement this.
   * This mutates the input tree with the delta contents.
   *
   * @param failure_reason is a reference to a string that will be filled by a
   * string representaiton of why the application failed (if it failed).
   * @return 0 if successful, -1 otherwise, and `err` is set.
   */
  virtual int apply(LogsConfigTree& tree,
                    std::string& failure_reason) const = 0;
  virtual folly::Optional<std::string> getPath() const {
    return folly::none;
  }

  virtual ~Delta() {}

 protected:
  const DeltaHeader header_;
  const DeltaOpType delta_type_;
};

class MkDirectoryDelta : public Delta {
 public:
  MkDirectoryDelta(const DeltaHeader& header,
                   const std::string& path,
                   bool make_intermediates,
                   const LogAttributes& attrs)
      : Delta(header, DeltaOpType::MK_DIRECTORY),
        path(path),
        should_make_intermediates(make_intermediates),
        attrs(attrs) {}
  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;
  const std::string path;
  const bool should_make_intermediates;
  const LogAttributes attrs;

  folly::Optional<std::string> getPath() const override {
    return path;
  }
};

class MkLogGroupDelta : public Delta {
 public:
  MkLogGroupDelta(const DeltaHeader& header,
                  const std::string& path,
                  const logid_range_t& range,
                  bool make_intermediates,
                  const LogAttributes& attrs)
      : Delta(header, DeltaOpType::MK_LOG_GROUP),
        path(path),
        range(range),
        should_make_intermediates(make_intermediates),
        attrs(attrs) {}

  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;
  const std::string path;
  const logid_range_t range;
  const bool should_make_intermediates;
  const LogAttributes attrs;

  folly::Optional<std::string> getPath() const override {
    return path;
  }
};

class RenameDelta : public Delta {
 public:
  RenameDelta(const DeltaHeader& header,
              const std::string& from_path,
              const std::string& to_path)
      : Delta(header, DeltaOpType::RENAME),
        from_path(from_path),
        to_path(to_path) {}

  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;

  const std::string from_path;
  const std::string to_path;

  folly::Optional<std::string> getPath() const override {
    return from_path;
  }
};

class SetAttributesDelta : public Delta {
 public:
  SetAttributesDelta(const DeltaHeader& header,
                     const std::string& path,
                     const LogAttributes& attrs)
      : Delta(header, DeltaOpType::SET_ATTRS), path(path), attrs(attrs) {}

  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;
  const std::string path;
  const LogAttributes attrs;

  folly::Optional<std::string> getPath() const override {
    return path;
  }
};

class SetLogRangeDelta : public Delta {
 public:
  SetLogRangeDelta(const DeltaHeader& header,
                   const std::string& path,
                   const logid_range_t& range)
      : Delta(header, DeltaOpType::SET_LOG_RANGE), path(path), range(range) {}

  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;
  const std::string path;
  const logid_range_t range;

  folly::Optional<std::string> getPath() const override {
    return path;
  }
};

class RemoveDelta : public Delta {
 public:
  RemoveDelta(const DeltaHeader& header,
              const std::string& path,
              bool is_directory,
              bool recursive)
      : Delta(header, DeltaOpType::REMOVE),
        path(path),
        is_directory(is_directory),
        recursive(recursive) {}

  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;
  const std::string path;
  const bool is_directory;
  const bool recursive;

  folly::Optional<std::string> getPath() const override {
    return path;
  }
};

class BatchDelta : public Delta {
 public:
  BatchDelta(const DeltaHeader& header,
             std::vector<std::unique_ptr<Delta>> deltas)
      : Delta(header, DeltaOpType::REMOVE), deltas(std::move(deltas)) {}

  int apply(LogsConfigTree& /* unused */,
            std::string& /* unused */) const override {
    // Batch deltas are not supported for now, they are designed but might be
    // implemented in the future.
    err = E::NOTSUPPORTED;
    return -1;
  }

  std::vector<std::unique_ptr<Delta>> deltas;
};

class SetTreeDelta : public Delta {
 public:
  SetTreeDelta(const DeltaHeader& header, std::unique_ptr<LogsConfigTree> tree)
      : Delta(header, DeltaOpType::SET_TREE), tree_(std::move(tree)) {}

  int apply(LogsConfigTree& tree, std::string& failure_reason) const override;

  const LogsConfigTree& getTree() const {
    return *tree_;
  }

 private:
  std::unique_ptr<LogsConfigTree> tree_;
};

}}} // namespace facebook::logdevice::logsconfig
