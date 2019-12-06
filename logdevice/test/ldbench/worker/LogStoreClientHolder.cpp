/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"

#include <chrono>
#include <functional>
#include <memory>

#include <folly/Random.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/test/ldbench/worker/BenchStats.h"
#include "logdevice/test/ldbench/worker/BenchTracer.h"
#include "logdevice/test/ldbench/worker/FileBasedEventStore.h"
#include "logdevice/test/ldbench/worker/FileBasedStatsStore.h"
#include "logdevice/test/ldbench/worker/LogDeviceClient.h"
#include "logdevice/test/ldbench/worker/LogStoreClient.h"
#include "logdevice/test/ldbench/worker/LogStoreReader.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/RecordWriterInfo.h"

#ifdef BUILDKAFKA
#include "logdevice/test/ldbench/worker/KafkaClient.h"
#endif

namespace facebook { namespace logdevice { namespace ldbench {

LogStoreClientHolder::LogStoreClientHolder() {
  bench_stats_holder_ = std::make_shared<BenchStatsHolder>(options.bench_name);
  if (options.publish_dir != "") {
    std::string filename = options.bench_name;
    if (options.restart_backlog_probability > 0) {
      // distinguish tailing from backfill
      filename = "backfill";
    }
    // create everything about stats here
    // create stats_store_
    std::string stats_file = folly::to<std::string>(options.publish_dir,
                                                    "/stats_",
                                                    filename,
                                                    options.worker_id_index,
                                                    "_.csv");
    stats_store_ = std::make_shared<FileBasedStatsStore>(stats_file);
    // create collection thread for stats
    collect_thread_ = std::make_unique<BenchStatsCollectionThread>(
        bench_stats_holder_, stats_store_, options.stats_interval);
    // create event store
    if (options.event_ratio > 0 && filename != "backfill") {
      std::string event_file = folly::to<std::string>(options.publish_dir,
                                                      "/event_",
                                                      filename,
                                                      options.worker_id_index,
                                                      "_.csv");
      event_store_ = std::make_shared<FileBasedEventStore>(event_file);
      // create event tracer
      bench_tracer_ =
          std::make_unique<BenchTracer>(event_store_, options.event_ratio);
    }
  }
  if (options.sys_name == "logdevice") {
    client_ = std::make_unique<LogDeviceClient>(options);
#ifdef BUILDKAFKA
  } else if (options.sys_name == "kafka") {
    client_ = std::make_unique<KafkaClient>(options);
#endif
  } else {
    ld_error("Unsupported system: %s", options.sys_name.c_str());
    throw ConstructorFailed();
  }
  ld_check(client_);
  client_->setAppendStatsUpdateCallBack(
      std::bind(&LogStoreClientHolder::onAppendDone,
                this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                std::placeholders::_4));
  client_->setReadStatsUpdateCallBack(
      std::bind(&LogStoreClientHolder::onReadDone,
                this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                std::placeholders::_4,
                std::placeholders::_5));
}

LogStoreClientHolder::~LogStoreClientHolder() {}

std::unique_ptr<LogStoreReader> LogStoreClientHolder::createReader() {
  return client_->createReader();
}

bool LogStoreClientHolder::append(LogIDType log_id,
                                  std::string payload,
                                  Context context) {
  auto rv = client_->append(log_id, std::move(payload), context);
  if (rv) {
    bench_stats_holder_->getOrCreateTLStats()->incStat(StatsType::INFLIGHT, 1);
  } else {
    bench_stats_holder_->getOrCreateTLStats()->incStat(StatsType::FAILURE, 1);
  }
  return rv;
}

bool LogStoreClientHolder::getTail(
    LogIDType logid,
    std::function<void(bool, LogPositionType)> worker_cb) {
  return client_->getTail(logid, std::move(worker_cb));
}

bool LogStoreClientHolder::findTime(
    LogIDType log,
    std::chrono::milliseconds timestamp,
    std::function<void(bool, LogPositionType)> cb) {
  return client_->findTime(log, timestamp, std::move(cb));
}

bool LogStoreClientHolder::getLogs(std::vector<uint64_t>* all_logs) {
  return client_->getLogs(all_logs);
}

folly::dynamic LogStoreClientHolder::getAllStats() {
  return bench_stats_holder_->aggregateAllStats();
}

std::shared_ptr<void> LogStoreClientHolder::getRawClient() {
  return client_->getRawClient();
}

BenchStatsHolder* LogStoreClientHolder::getBenchStatsHolder() {
  return bench_stats_holder_.get();
}

void LogStoreClientHolder::setWorkerCallBack(
    write_worker_callback_t worker_cb) {
  worker_cb_ = worker_cb;
}

void LogStoreClientHolder::doSample(LogIDType log_id,
                                    LogPositionType lsn,
                                    const std::string& payload_str,
                                    std::string& event_name) {
  // we can get the end-to-end latency using the timestamp in the payload
  // The ts in paylaod is based_on system_clock
  auto now_us = std::chrono::time_point_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now());
  uint64_t now_ts = now_us.time_since_epoch().count();
  RecordWriterInfo info;
  Payload payload(payload_str.c_str(), payload_str.size());
  if (info.deserialize(payload) != 0) {
    // do not sample event since we can not get end-to-end latency without
    // record info
    return;
  }
  uint64_t lat_us = (now_us - info.client_timestamp).time_since_epoch().count();
  // "wlat" means write latency
  // We use log_id-lsn as event id
  std::string event_id =
      folly::to<std::string>(std::to_string(log_id), "-", std::to_string(lsn));
  EventRecord event_record(now_ts, event_id, event_name, lat_us);
  bench_tracer_->doSample(event_record);
}

void LogStoreClientHolder::onAppendDone(bool successful,
                                        LogIDType log_id,
                                        LogPositionType lsn,
                                        ContextSet contexts) {
  if (successful) {
    bench_stats_holder_->getOrCreateTLStats()->incStat(
        StatsType::SUCCESS, contexts.size());
  } else {
    bench_stats_holder_->getOrCreateTLStats()->incStat(
        StatsType::FAILURE, contexts.size());
  }
  bench_stats_holder_->getOrCreateTLStats()->incStat(
      StatsType::INFLIGHT, -1 * static_cast<int64_t>(contexts.size()));
  uint64_t payload_size = 0;
  for (auto it = contexts.begin(); it != contexts.end(); it++) {
    payload_size += it->second.size();
    // start to sample event
    // Current example is sampling latency
    if (bench_tracer_) {
      if (successful && options.record_writer_info &&
          bench_tracer_->isSample()) {
        std::string event_name =
            options.use_buffered_writer ? "bufferedWriter" : "Writer";
        doSample(log_id, lsn, it->second, event_name);
      }
    }
  }
  if (successful) {
    bench_stats_holder_->getOrCreateTLStats()->incStat(
        StatsType::SUCCESS_BYTE, payload_size);
  }

  // further report result to worker
  if (worker_cb_) {
    worker_cb_(log_id,
               successful,
               options.use_buffered_writer,
               contexts.size(),
               payload_size);
  }
  return;
}

void LogStoreClientHolder::onReadDone(bool successful,
                                      LogIDType log_id,
                                      LogPositionType lsn,
                                      uint64_t num_records,
                                      std::string payload) {
  if (successful) {
    bench_stats_holder_->getOrCreateTLStats()->incStat(
        StatsType::SUCCESS, num_records);
    bench_stats_holder_->getOrCreateTLStats()->incStat(
        StatsType::SUCCESS_BYTE, payload.size());
  } else {
    bench_stats_holder_->getOrCreateTLStats()->incStat(
        StatsType::FAILURE, num_records);
  }
  if (bench_tracer_) {
    if (successful && options.record_writer_info && bench_tracer_->isSample()) {
      doSample(log_id, lsn, payload, options.bench_name);
    }
  }
}
}}} // namespace facebook::logdevice::ldbench
