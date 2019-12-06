/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <librdkafka/rdkafkacpp.h>

#include "logdevice/test/ldbench/worker/LogStoreReader.h"
#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice { namespace ldbench {

class KafkaConsumeCb;
class KafkaClient;

class KafkaReader : public LogStoreReader {
 public:
  explicit KafkaReader(const Options& options, KafkaClient* kfclient);
  ~KafkaReader() override;
  bool startReading(LogIDType partition_id,
                    LogPositionType start,
                    LogPositionType end) override;
  bool stopReading(LogIDType partition_id) override;

  bool resumeReading(LogIDType partition_id) override;

  // The client who creates the reader
  KafkaClient* owner_client_;

 private:
  // Applications using Kafka are required to call the consume function
  // periodically to serve the returned message. We use a separate thread.
  void consumingLoop();

  Options options_;
  // Conf objs will be taken from the client
  RdKafka::Conf* global_conf_;
  RdKafka::Conf* topic_conf_;
  // topic to read. Consumer has to create topic obj by itself
  std::unique_ptr<RdKafka::Topic> topic_;

  // A message queue to store returned message, It helps us to serve message
  // from different partitions with a single point. Otherwise, messages will
  // be return for each topic-partiton, and we need invoke a callback
  // periordically for each of the partitions.
  std::unique_ptr<RdKafka::Queue> message_queue_;

  // Callback function. But it has to be invoked periodically
  std::unique_ptr<KafkaConsumeCb> kafka_consume_cb_;
  std::unique_ptr<RdKafka::Consumer> consumer_; // consumer to perform read
  std::unique_ptr<std::thread> consume_thread_;
  std::string err_str_;
  std::atomic<bool> stop_consume_{false}; // stop the consume thread
  uint64_t timeout_ms_;
};

class KafkaConsumeCb : public RdKafka::ConsumeCb {
 public:
  explicit KafkaConsumeCb(KafkaReader* kfreader) : owner_reader_(kfreader) {}
  void consume_cb(RdKafka::Message& msg, void* opaque) override;
  KafkaReader* owner_reader_;
};

}}} // namespace facebook::logdevice::ldbench
