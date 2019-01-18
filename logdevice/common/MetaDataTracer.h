/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/SampledTracer.h"

namespace facebook { namespace logdevice {

class TraceLogger;
struct NodeID;

constexpr auto METADATA_TRACER = "metadata";

class MetaDataTracer {
 public:
  enum class Action {
    // no traceable action, not logging
    NONE = 0,
    // sequencer activation
    SEQUENCER_ACTIVATION,
    // metadata being provisioned in epoch store
    PROVISION_METADATA,
    // setting written bit
    SET_WRITTEN_BIT,
    // disable
    DISABLE,
    // metadata log records written
    WRITE_METADATA_LOG,
    // clearing the written bit in epoch store
    CLEAR_WRITTEN_BIT,
    UPDATE_NODESET_PARAMS,
  };
  explicit MetaDataTracer(std::shared_ptr<TraceLogger> logger,
                          logid_t log_id,
                          Action a)
      : logger_(std::move(logger)),
        log_id_(log_id),
        action_(a),
        start_time_(std::chrono::steady_clock::now()) {}

  MetaDataTracer() {}

  void trace(Status st, lsn_t record_lsn = LSN_INVALID);

  void setAction(Action a) {
    action_ = a;
  }
  void setOldMetaData(EpochMetaData val) {
    old_metadata_.assign(std::move(val));
  }
  void setNewMetaData(EpochMetaData val) {
    new_metadata_ = std::move(val);
  }

 private:
  std::string actionToStr(Action a) {
    switch (a) {
      case Action::SEQUENCER_ACTIVATION:
        return "SEQUENCER_ACTIVATION";
      case Action::PROVISION_METADATA:
        return "PROVISION_METADATA";
      case Action::SET_WRITTEN_BIT:
        return "SET_WRITTEN_BIT";
      case Action::DISABLE:
        return "DISABLE";
      case Action::WRITE_METADATA_LOG:
        return "WRITE_METADATA_LOG";
      case Action::CLEAR_WRITTEN_BIT:
        return "CLEAR_WRITTEN_BIT";
      case Action::UPDATE_NODESET_PARAMS:
        return "UPDATE_NODESET_PARAMS";
      case Action::NONE:
        ld_check(false);
        return "";
    }
    ld_check(false);
    return "";
  }

  std::shared_ptr<TraceLogger> logger_;

  // data log id, for which metadata is being written
  logid_t log_id_;

  // action being performed
  Action action_;

  // metadata read from the epoch store, set only if action is
  // PROVISION_METADATA
  folly::Optional<EpochMetaData> old_metadata_;

  // Metadata that is being written
  EpochMetaData new_metadata_;

  // Time when the operation started, to measure its duration
  std::chrono::steady_clock::time_point start_time_;
};

}} // namespace facebook::logdevice
