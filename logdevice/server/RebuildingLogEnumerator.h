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
  struct Parameters {
    bool rebuild_metadata_logs;
    RecordTimestamp min_timestamp;
    lsn_t version;
    inline bool operator==(const Parameters& o) const {
      return rebuild_metadata_logs == o.rebuild_metadata_logs &&
          min_timestamp == o.min_timestamp && version == o.version;
    }
  };

  using ParametersPerShard = std::unordered_map<uint32_t, Parameters>;
  using Results = std::unordered_map<logid_t, RecordTimestamp, logid_t::Hash>;

  class Listener {
   public:
    virtual void
    onLogsEnumerated(Results logs,
                     std::chrono::milliseconds maxBacklogDuration) = 0;
    virtual ~Listener() {}
  };

  RebuildingLogEnumerator(
      ParametersPerShard parameters,
      bool rebuild_internal_logs,
      std::shared_ptr<UpdateableConfig> config,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      uint32_t max_num_shards,
      Listener* callback)
      : parameters_(std::move(parameters)),
        rebuild_internal_logs_(rebuild_internal_logs),
        config_(std::move(config)),
        rebuilding_settings_(rebuilding_settings),
        max_num_shards_(max_num_shards),
        callback_(callback),
        ref_holder_(this) {
    ld_check(callback_);
  }

  void start();
  void onMetaDataLogsStorageTaskDone(Status,
                                     uint32_t shard_idx,
                                     std::vector<logid_t> log_ids);
  void onMetaDataLogsStorageTaskDropped(uint32_t shard_idx);

  void abortShardIdx(shard_index_t shard_idx);

 private:
  ParametersPerShard parameters_;
  const bool rebuild_internal_logs_;
  std::shared_ptr<UpdateableConfig> config_;
  const UpdateableSettings<RebuildingSettings> rebuilding_settings_;
  std::chrono::milliseconds maxBacklogDuration_{0};
  const uint32_t max_num_shards_;
  Listener* const callback_;
  Results results_;
  WeakRefHolder<RebuildingLogEnumerator> ref_holder_;
  std::set<uint32_t> shard_storage_tasks_remaining_;

  bool finalized_{false};

  void maybeFinalize();
  void putStorageTask(uint32_t shard_idx);
};

}} // namespace facebook::logdevice
