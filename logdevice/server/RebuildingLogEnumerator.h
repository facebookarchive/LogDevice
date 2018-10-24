/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class UpdateableConfig;

class RebuildingLogEnumerator {
 public:
  class Listener {
   public:
    virtual void
    onLogsEnumerated(uint32_t shard_idx,
                     lsn_t version,
                     std::unordered_map<logid_t, RecordTimestamp> logs,
                     std::chrono::milliseconds maxBacklogDuration) = 0;
    virtual ~Listener() {}
  };

  struct Options {
    bool rebuild_metadata_logs;
    bool rebuild_internal_logs;
    RecordTimestamp min_timestamp;
  };

  RebuildingLogEnumerator(
      std::shared_ptr<UpdateableConfig> config,
      uint32_t shard_idx,
      lsn_t version,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      Options options,
      uint32_t num_shards,
      Listener* callback)
      : config_(std::move(config)),
        shard_idx_(shard_idx),
        version_(version),
        min_timestamp_(options.min_timestamp),
        rebuilding_settings_(rebuilding_settings),
        rebuild_metadata_logs_(options.rebuild_metadata_logs),
        rebuild_internal_logs_(options.rebuild_internal_logs),
        num_shards_(num_shards),
        callback_(callback),
        ref_holder_(this) {
    ld_check(callback_);
  }

  void start();
  void onMetaDataLogsStorageTaskDone(Status, std::vector<logid_t> log_ids);
  void onMetaDataLogsStorageTaskDropped();

 private:
  std::shared_ptr<UpdateableConfig> config_;
  const uint32_t shard_idx_;
  const lsn_t version_;
  const RecordTimestamp min_timestamp_;
  const UpdateableSettings<RebuildingSettings> rebuilding_settings_;
  const bool rebuild_metadata_logs_;
  const bool rebuild_internal_logs_;
  const uint32_t num_shards_;
  std::chrono::milliseconds maxBacklogDuration_{0};
  Listener* const callback_;
  std::unordered_map<logid_t, RecordTimestamp> result_;
  WeakRefHolder<RebuildingLogEnumerator> ref_holder_;

  bool finalize_called_{false};

  void finalize();
  void putStorageTask();
};

}} // namespace facebook::logdevice
