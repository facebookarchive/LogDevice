/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/KafkaReader.h"

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/test/ldbench/worker/KafkaClient.h"

namespace facebook { namespace logdevice { namespace ldbench {
KafkaReader::KafkaReader(const Options& options, KafkaClient* kfclient)
    : owner_client_(kfclient), options_(options), timeout_ms_(3000) {
  // Get global_conf from client
  global_conf_ = owner_client_->getGlobalConf();
  ld_check(global_conf_);
  topic_conf_ = owner_client_->getTopicConf();
  ld_check(topic_conf_);

  // Create consume callback
  kafka_consume_cb_ = std::make_unique<KafkaConsumeCb>(this);

  global_conf_->set("consume_cb", kafka_consume_cb_.get(), err_str_);
  if (!err_str_.empty()) {
    ld_error("Failed to set consume_cb with error %s", err_str_.c_str());
    throw ConstructorFailed();
  }

  // Create a consumer
  consumer_.reset(RdKafka::Consumer::create(global_conf_, err_str_));
  if (consumer_ == nullptr) {
    ld_error("Failed to create consumer with error %s", err_str_.c_str());
    throw ConstructorFailed();
  }

  // Create a topic. We can not take topic from client directly since
  // a topic needs to specify a handler
  if (!options.log_range_names.empty()) {
    topic_.reset(RdKafka::Topic::create(
        consumer_.get(), options.log_range_names[0], topic_conf_, err_str_));
    if (!topic_) {
      ld_error("Failed to create Topic with error %s", err_str_.c_str());
      throw ConstructorFailed();
    }
  }
  ld_check(topic_);
  // Create a message queue
  message_queue_.reset(RdKafka::Queue::create(consumer_.get()));
  if (!message_queue_) {
    ld_error("Failed to create message queue");
    throw ConstructorFailed();
  }

  // start the consume thread
  consume_thread_ = std::make_unique<std::thread>(
      std::bind(&KafkaReader::consumingLoop, this));
}

KafkaReader::~KafkaReader() {
  stop_consume_ = true;
  consume_thread_->join();
}

bool KafkaReader::startReading(LogIDType logid,
                               LogPositionType start,
                               LogPositionType end) {
  // In ReadWorker, we only pass LSN::MAX to the end.
  // The start() function of consumer in librdkafka does not have and end
  // parameter, therefore it should be fine to ignore the end here.
  ld_assert(start <= end);
  ld_check(topic_);
  int rv = consumer_->start(topic_.get(), logid, start, message_queue_.get());
  return rv == 0;
}
bool KafkaReader::stopReading(LogIDType logid) {
  ld_check(topic_);
  int rv = consumer_->stop(topic_.get(), logid);
  return rv == 0;
}

bool KafkaReader::resumeReading(LogIDType logid) {
  auto topic_partition = RdKafka::TopicPartition::create(topic_->name(), logid);
  std::vector<RdKafka::TopicPartition*> tps;
  tps.push_back(topic_partition);
  int rv = consumer_->resume(tps);
  delete topic_partition;
  return rv == 0;
}

void KafkaReader::consumingLoop() {
  while (!stop_consume_) {
    consumer_->consume_callback(
        message_queue_.get(), timeout_ms_, kafka_consume_cb_.get(), nullptr);
  }
}

void KafkaConsumeCb::consume_cb(RdKafka::Message& msg, void*) {
  ld_check(owner_reader_);
  if (msg.err() == 0) {
    // report results to readworker
    std::string payload((char*)msg.payload(), msg.len());
    ld_debug("Message received: %s\n", msg.payload());
    if (owner_reader_->worker_record_callback_) {
      owner_reader_->worker_record_callback_(
          msg.partition(),
          msg.offset(),
          std::chrono::milliseconds(msg.timestamp().timestamp),
          payload);
    }
    // report to client holder
    if (owner_reader_->owner_client_->reader_stats_updates_cb_) {
      owner_reader_->owner_client_->reader_stats_updates_cb_(
          true, msg.partition(), msg.offset(), 1, payload);
    }
  } else {
    if (owner_reader_->worker_gap_callback_) {
      LogStoreGapType type;
      // We don't really know which of the ~80 kafka error codes can be passed
      // to consume_cb, and in which cases offset() is valid. Kafka
      // documentation doesn't seem to have that information. The 2 cases
      // handled here are just a guess - we don't know if they're even
      // reachable. Would be nice to handle more cases.
      switch (msg.err()) {
        case RdKafka::ErrorCode::ERR__AUTHENTICATION:
          type = LogStoreGapType::ACCESS;
          break;
        case RdKafka::ErrorCode::ERR_KAFKA_STORAGE_ERROR:
          type = LogStoreGapType::DATALOSS;
          break;
        default:
          type = LogStoreGapType::OTHER;
          break;
      }
      owner_reader_->worker_gap_callback_(
          type, msg.partition(), msg.offset(), msg.offset());
    }
    // report to client holder
    if (owner_reader_->owner_client_->reader_stats_updates_cb_) {
      owner_reader_->owner_client_->reader_stats_updates_cb_(
          false, msg.partition(), 0, 1, "");
    }
  }
}

}}} // namespace facebook::logdevice::ldbench
