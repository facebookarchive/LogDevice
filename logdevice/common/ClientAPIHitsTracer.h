/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "folly/Synchronized.h"
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

constexpr auto API_HITS_TRACER = "api_hits_tracer";
class ClientAPIHitsTracer : public SampledTracer {
 public:
  explicit ClientAPIHitsTracer(std::shared_ptr<TraceLogger> logger);

  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 0.05;
  }

  void traceFindTime(int64_t msec_resp_time,
                     logid_t in_logid,
                     std::chrono::milliseconds in_timestamp,
                     FindKeyAccuracy in_accuracy,
                     FailedShardsMap&& failed_shards,
                     Status out_status,
                     lsn_t out_lsn = LSN_INVALID);

  void traceFindKey(int64_t msec_resp_time,
                    logid_t in_logid,
                    std::string in_key,
                    FindKeyAccuracy in_accuracy,
                    FailedShardsMap&& failed_shards,
                    Status out_status,
                    lsn_t out_lsn_lo = LSN_INVALID,
                    lsn_t out_lsn_hi = LSN_INVALID);

  void traceGetTailAttributes(int64_t msec_resp_time,
                              logid_t in_logid,
                              Status out_status,
                              LogTailAttributes* out_log_tail_attributes);

  void traceGetHeadAttributes(int64_t msec_resp_time,
                              logid_t in_logid,
                              FailedShardsMap&& failed_shards,
                              Status out_status,
                              LogHeadAttributes* out_log_head_attributes);

  void traceGetTailLSN(int64_t msec_resp_time,
                       logid_t in_logid,
                       Status out_status,
                       lsn_t out_lsn = LSN_INVALID);

  void traceIsLogEmpty(int64_t msec_resp_time,
                       logid_t in_logid,
                       FailedShardsMap&& failed_shards,
                       Status out_status,
                       bool out_bool,
                       int version = 1);

  void traceIsLogEmptyV2(int64_t msec_resp_time,
                         logid_t in_logid,
                         Status out_status,
                         bool out_bool);

  void traceDataSize(int64_t msec_resp_time,
                     logid_t in_logid,
                     std::chrono::milliseconds start_timestamp,
                     std::chrono::milliseconds end_timestamp,
                     DataSizeAccuracy in_accuracy,
                     FailedShardsMap&& failed_shards,
                     Status out_status,
                     size_t out_size);

  void traceTrim(int64_t msec_resp_time,
                 logid_t in_logid,
                 lsn_t in_lsn,
                 FailedShardsMap&& failed_shards,
                 Status out_status);

 private:
  bool assessIsLogEmptyFlappiness(Status st, logid_t logid, bool empty);

  struct FlappinessRecord {
    /**
     * Steps are:
     * 0   =   no flappiness
     * 1   =   went from empty to not empty before
     */
    unsigned int step : 1;
    unsigned int prev_val : 1;
  };

  folly::Synchronized<
      std::unordered_map<logid_t, FlappinessRecord, logid_t::Hash>>
      is_log_empty_record_;
};

}} // namespace facebook::logdevice
