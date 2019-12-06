/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/KafkaClient.h"

#include <condition_variable>

#include "logdevice/test/ldbench/worker/KafkaReader.h"
#include "logdevice/test/ldbench/worker/LogStoreReader.h"
#include "logdevice/test/ldbench/worker/Options.h"

using namespace facebook::logdevice::ldbench;

int main() {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::DEBUG;
  options.log_range_names.push_back("test_topic");
  options.config_path = "logdevice/test/ldbench/worker/Kafka_conf.json";
  std::mutex mutex;
  std::condition_variable cv;
  LogPositionType consume_offset = 0;

  auto kafka_client = new KafkaClient(options);
  auto consumer = kafka_client->createReader();
  int rv = 0;
  ld_info("Start reading for partition 0");
  rv = consumer->startReading(
      0, RdKafka::Topic::OFFSET_END, RdKafka::Topic::OFFSET_END);
  if (rv == 0) {
    ld_error("Start reading failed!");
  }
  ld_info("Appending to partition 0");
  for (int i = 0; i < 10; i++) {
    std::string payload = "hello_0729_" + std::to_string(i);
    rv = kafka_client->append(0, payload, nullptr);
    if (rv == 0) {
      ld_error("Append failed!");
    }
    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait_for(lock, std::chrono::milliseconds(1000));
    }
  }
  rv = 0;
  ld_info("Stop reading from partition 0");
  rv = consumer->stopReading(0);
  if (rv == 0) {
    ld_error("Stop reading failed!");
  }

  std::vector<LogIDType> logs;
  kafka_client->getLogs(&logs);
  ld_info("Get logs return %ld total logs:", logs.size());

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
  kafka_client->getTail(0, worker_cb);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(100));
  }

  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  millis -= std::chrono::milliseconds(5000);
  kafka_client->findTime(0, millis, worker_cb);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(100));
  }

  ld_info("Reading from offset: %ld", consume_offset);
  consumer->startReading(0, consume_offset, RdKafka::Topic::OFFSET_END);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(1000));
  }
  delete kafka_client;
  return 0;
}
