/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppenderTracer.h"

#include <memory>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Recipient.h"
#include "logdevice/common/RecipientSet.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

AppenderTracer::AppenderTracer(std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void AppenderTracer::traceAppend(
    size_t full_appender_size,
    epoch_t seen_epoch,
    bool chaining,
    const ClientID& client_id,
    const Sockaddr& client_sock_addr,
    int64_t latency_us,
    RecipientSet& recipient_set,
    logid_t log_id,
    lsn_t lsn,
    folly::Optional<std::chrono::seconds> backlog_duration,
    uint32_t waves,
    std::string client_status,
    std::string internal_status) {
  auto sample_builder = [&]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    const auto& recipients = recipient_set.getRecipients();
    std::vector<std::string> recipient_ids; // Node Ids
    std::vector<std::string> recipient_ips; // IP addresses

    for (auto& rec : recipients) {
      auto shard_id = rec.getShardID();
      recipient_ids.push_back(shard_id.toString());
      recipient_ips.push_back(logger_->nodeIDToIPAddress(shard_id.node()));
    }
    // We might not have the logs config locally if we run sequencers and
    // appenders on clients in the future.
    auto config = logger_->getConfiguration();
    ld_check(config->logsConfig()->isLocal());
    std::string log_range =
        config->getLogGroupPath(log_id).value_or("<UNKNOWN>");

    sample->addIntValue("is_chaining", chaining);
    sample->addNormalValue("client_id", client_id.toString());

    std::string client_sock_str = client_sock_addr.valid()
        ? client_sock_addr.toStringNoPort()
        : std::string("UNKNOWN");
    sample->addNormalValue("client_sock_addr", client_sock_str);
    sample->addIntValue("appender_size", full_appender_size);
    sample->addIntValue("seen_epoch", seen_epoch.val());
    sample->addIntValue("latency_us", latency_us);
    sample->addIntValue("log_id", log_id.val());
    sample->addIntValue("lsn", lsn);
    if (backlog_duration) {
      sample->addNormalValue(
          "backlog_duration", std::to_string(backlog_duration->count()));
    } else {
      // 'inf' means infinite duration, we don't have a backlog_duration for
      // this log
      sample->addNormalValue("backlog_duration", "inf");
    }
    sample->addNormalValue("log_range", log_range);
    sample->addNormVectorValue("recipient_ids", std::move(recipient_ids));
    sample->addNormVectorValue("recipient_ips", std::move(recipient_ips));
    sample->addIntValue("waves", waves);
    sample->addNormalValue("client_status", client_status);
    sample->addNormalValue("internal_status", internal_status);
    sample->addNormalValue("thread_name", ThreadID::getName());
    return sample;
  };

  publish(APPENDER_TRACER, sample_builder);
}

}} // namespace facebook::logdevice
