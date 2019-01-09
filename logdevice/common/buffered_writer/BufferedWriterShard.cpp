/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriterShard.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

void BufferedWriterShard::append(AppendChunk chunk, bool atomic) {
  if (parent_->isShuttingDown()) {
    // This should be rare as long as Workers process their requests in FIFO
    // order, but can still happen if appends are started just before calling
    // BufferedWriter()::shutDown().
    BufferedWriterImpl::AppendCallbackInternal* cb = parent_->getCallback();

    for (auto& append : chunk) {
      BufferedWriter::AppendCallback::ContextSet appends;
      appends.emplace_back(
          std::move(append.context), std::move(append.payload));
      cb->onFailureInternal(
          append.log_id, std::move(appends), E::SHUTDOWN, NodeID());
    }

    RATELIMIT_ERROR(
        std::chrono::seconds(10), 1, "Append received after shutdown.");
    return;
  }

  auto single_log_instance = [&](logid_t log_id) -> BufferedWriterSingleLog& {
    auto it = logs_.find(log_id);
    if (it == logs_.end()) {
      // First time we're seeing this log; create the BufferedWriterSingleLog
      // instance.
      it = logs_
               .emplace(std::piecewise_construct,
                        std::forward_as_tuple(log_id),
                        std::forward_as_tuple(this, log_id, get_log_options_))
               .first;
    }
    return it->second;
  };

  if (atomic && !chunk.empty()) {
    const logid_t log_id = chunk.front().log_id;
    single_log_instance(log_id).append(std::move(chunk));
  } else {
    for (auto& append : chunk) {
      const logid_t log_id = append.log_id;
      AppendChunk temp_chunk;
      temp_chunk.emplace_back(std::move(append));
      // TODO could pass defer_size_trigger = true for all but the last append
      single_log_instance(log_id).append(std::move(temp_chunk));
    }
  }
}

void BufferedWriterShard::flushAll() {
  if (parent_->isShuttingDown()) {
    return;
  }

  StatsHolder* stats{parent_->processor()->stats_};
  while (!flushable_logs_.empty()) {
    BufferedWriterSingleLog* log = &flushable_logs_.front();
    STAT_INCR(stats, buffered_writer_manual_flush);
    log->flush();
    // The log we just flushed should no longer be flushable and so no longer
    // on the list.  This ensures the loop will terminate.
    if (!flushable_logs_.empty()) {
      ld_check(log != &flushable_logs_.front());
      ld_assert(log != &flushable_logs_.back());
    }
  }
}

void BufferedWriterShard::quiesce() {
  ld_check(parent_->isShuttingDown());

  for (auto& logIdAndLog : logs_) {
    logIdAndLog.second.quiesce();
  }
}

}} // namespace facebook::logdevice
