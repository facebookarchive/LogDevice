/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <condition_variable>

#include <librdkafka/rdkafkacpp.h>

#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"
#include "logdevice/test/ldbench/worker/LogStoreReader.h"
#include "logdevice/test/ldbench/worker/Options.h"

using namespace facebook::logdevice::ldbench;

int main() {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::INFO;
  options.log_range_names.push_back("test_topic");
  options.config_path = "logdevice/test/ldbench/worker/Kafka_conf.json";
  options.sys_name = "kafka";
  options.record_writer_info = true;

  std::mutex mutex;
  std::condition_variable cv;
  LogPositionType consume_offset = 0;

  std::atomic<uint64_t> append_success{0};
  write_worker_callback_t append_cb =
      [&](LogIDType, bool successful, bool, uint64_t num_records, uint64_t) {
        if (successful) {
          append_success += num_records;
        }
      };

  auto kafka_client_holder = std::make_unique<LogStoreClientHolder>();
  kafka_client_holder->setWorkerCallBack(append_cb);
  std::vector<LogIDType> logs;
  kafka_client_holder->getLogs(&logs);
  ld_info("Get logs return %ld total logs:", logs.size());
  int rv = 0;

  std::vector<std::unique_ptr<LogStoreReader>> readers(logs.size());
  for (auto i : logs) {
    readers[i] = kafka_client_holder->createReader();
    ld_info("Start reading for partition %ld", i);
    rv = readers[i]->startReading(
        i, RdKafka::Topic::OFFSET_END, RdKafka::Topic::OFFSET_END);
    if (rv == 0) {
      ld_error("Start reading failed!");
    }
  }
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(2000));
  }
  int count = 1000;
  while (count--) {
    for (auto i : logs) {
      ld_info("Appending to partition %lu", i);
      std::string payload = "hello_0724_" + std::to_string(i);
      uint64_t payload_size = payload.size();
      rv = kafka_client_holder->append(
          i, std::move(payload), reinterpret_cast<void*>(payload_size));
      if (rv == 0) {
        ld_error("Append failed!");
      }
    }
  }

  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(2000));
  }

  ld_info("Total successful appends: %ld\n", append_success.load());

  rv = 0;
  for (auto i : logs) {
    ld_info("Stop reading for partition %ld", i);
    rv = readers[i]->stopReading(i);
    if (rv == 0) {
      ld_error("Start reading failed!");
    }
  }

  rv = 0;
  auto worker_cb = [&](bool successful, LogPositionType offset) {
    if (successful) {
      consume_offset = offset;
      ld_info("Get offset successfully. Offset: %ld", offset);
    } else {
      ld_error("Get offset failed");
    }
    std::unique_lock<std::mutex> lock(mutex);
    cv.notify_one();
  };

  ld_info("Getting tail");
  kafka_client_holder->getTail(0, worker_cb);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(1000));
  }

  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  millis -= std::chrono::milliseconds(5000);
  kafka_client_holder->findTime(0, millis, worker_cb);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(1000));
  }

  ld_info("Reading from offset: %ld", consume_offset);
  rv = readers[0]->startReading(0, consume_offset, RdKafka::Topic::OFFSET_END);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(1000));
  }
  return 0;
}
