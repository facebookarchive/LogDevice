/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RebuildingTracer.h"

#include <algorithm>
#include <memory>
#include <string>

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice {

RebuildingTracer::RebuildingTracer(std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void RebuildingTracer::traceRecordRebuild(const logid_t logid,
                                          const lsn_t lsn,
                                          const lsn_t rebuilding_version,
                                          const int64_t latency_us,
                                          const RebuildingSet& rebuilding_set,
                                          const copyset_t& existing_copyset,
                                          const copyset_t& new_copyset,
                                          const uint32_t waves,
                                          const size_t payload_size,
                                          const int cur_stage,
                                          const char* event_type,
                                          const char* status) {
  auto sample_builder = [&]() {
    auto sample = std::make_unique<TraceSample>();

    std::vector<std::string> rebuilding_shards;
    std::vector<std::string> existing_copyset_ids;
    std::vector<std::string> new_copyset_ids;
    std::vector<std::string> target_node_ids;
    std::vector<std::string> target_node_ips;

    for (auto& rec : rebuilding_set.shards) {
      rebuilding_shards.push_back(rec.first.toString());
    }

    for (auto& c : existing_copyset) {
      existing_copyset_ids.push_back("N" + c.toString());
    }

    for (auto& c : new_copyset) {
      std::string shard = "N" + c.toString();

      auto iter =
          std::find(existing_copyset.begin(), existing_copyset.end(), c);
      if (iter == existing_copyset.end()) {
        // new node to the copyset
        target_node_ids.push_back(shard);
        // resolve into IP
        const auto ip = logger_->nodeIDToIPAddress(c.node());
        if (!ip.empty()) {
          target_node_ips.push_back(ip);
        }
      }
      new_copyset_ids.push_back(shard);
    }

    auto config = logger_->getConfiguration();
    std::string log_range =
        config->getLogGroupPath(logid).value_or("<UNKNOWN>");

    sample->addIntValue("log_id", logid.val());
    sample->addNormalValue("log_range", log_range);
    sample->addNormalValue("lsn", std::to_string(lsn));
    sample->addNormalValue(
        "rebuilding_version", std::to_string(rebuilding_version));
    sample->addIntValue("latency_us", latency_us);
    sample->addNormVectorValue("rebuilding_set", rebuilding_shards);

    sample->addNormVectorValue("old_copyset_node_ids", existing_copyset_ids);
    sample->addNormVectorValue("new_copyset_node_ids", new_copyset_ids);
    sample->addNormVectorValue("target_node_ids", target_node_ids);
    sample->addNormVectorValue("target_node_ips", target_node_ips);
    sample->addIntValue("waves", waves);
    sample->addIntValue("payload_size", payload_size);
    sample->addIntValue("current_stage", cur_stage);
    sample->addNormalValue("event_type", event_type);
    if (status != nullptr) {
      sample->addNormalValue("status", status);
    }

    return sample;
  };
  publish(REBUILDING_TRACER, sample_builder);
}

void RebuildingTracer::traceRecordRebuildingAmend(
    const logid_t logid,
    const lsn_t lsn,
    const lsn_t rebuilding_version,
    const int64_t latency_us,
    const RebuildingSet& rebuilding_set,
    const uint32_t waves,
    const char* event_type,
    const char* status) {
  auto sample_builder = [&]() {
    auto sample = std::make_unique<TraceSample>();
    std::vector<std::string> rebuilding_shards;
    for (auto& rec : rebuilding_set.shards) {
      rebuilding_shards.push_back(rec.first.toString());
    }
    sample->addIntValue("log_id", logid.val());
    sample->addNormalValue("lsn", std::to_string(lsn));
    sample->addNormalValue(
        "rebuilding_version", std::to_string(rebuilding_version));
    sample->addIntValue("latency_us", latency_us);
    sample->addNormVectorValue("rebuilding_set", rebuilding_shards);
    sample->addIntValue("waves", waves);
    sample->addIntValue(
        "current_stage",
        4 /*Denotes Amend from RecordRebuildingAmend state machine*/);
    sample->addNormalValue("event_type", event_type);
    if (status != nullptr) {
      sample->addNormalValue("status", status);
    }

    return sample;
  };
  publish(REBUILDING_TRACER, sample_builder);
}

}} // namespace facebook::logdevice
