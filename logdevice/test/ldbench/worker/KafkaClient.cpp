/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/KafkaClient.h"

#include <folly/FileUtil.h>
#include <folly/json.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/test/ldbench/worker/KafkaReader.h"
#include "logdevice/test/ldbench/worker/LogStoreReader.h"

namespace facebook { namespace logdevice { namespace ldbench {

KafkaClient::KafkaClient(const Options& options)
    : options_(options), timeout_ms_(3000) {
  global_conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!global_conf_) {
    ld_error("Create the configuration Failed");
    throw ConstructorFailed();
  }
  topic_conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  if (!topic_conf_) {
    ld_error("Create the configuration Failed");
    throw ConstructorFailed();
  }

  if (!initFromConfigFile()) {
    ld_error("Setting Kafka Client from configuration file failed");
    throw ConstructorFailed();
  }

  // Set delivery report callback
  kafka_delivery_recort_cb_ = std::make_unique<KafkaDeliveryReportCb>(this);
  global_conf_->set("dr_cb", kafka_delivery_recort_cb_.get(), err_str_);
  if (!err_str_.empty()) {
    ld_error(
        "Setting delivery call back failed with error %s", err_str_.c_str());
    throw ConstructorFailed();
  }

  // Create a producer with the global configuration
  producer_.reset(RdKafka::Producer::create(global_conf_.get(), err_str_));
  if (!producer_) {
    ld_error("Failed to create Producer with error %s", err_str_.c_str());
    throw ConstructorFailed();
  }

  // Create a topic with an exsiting name can return the topic directly.
  // Otherwise, the function will create a new topic with default configurations
  ld_check(!options.log_range_names.empty());
  if (!options.log_range_names.empty()) {
    topic_.reset(RdKafka::Topic::create(producer_.get(),
                                        options.log_range_names[0],
                                        topic_conf_.get(),
                                        err_str_));
    if (!topic_) {
      ld_error("Failed to create Topic with error %s", err_str_.c_str());
      throw ConstructorFailed();
    }
  }
}

KafkaClient::~KafkaClient() {
  // Stop the polling thread
  stop_poll_ = true;
  for (int i = 0; i < poll_threads_.size(); i++) {
    poll_threads_[i]->join();
  }
}

bool KafkaClient::setConfig(std::shared_ptr<RdKafka::Conf> conf,
                            std::string param,
                            std::string val) {
  std::string err_str;
  conf->set(param, val, err_str);
  if (!err_str.empty()) {
    ld_error("Config setting failed with error: %s", err_str.c_str());
    return false;
  }
  return true;
}

bool KafkaClient::initFromConfigFile() {
  // read the configuration file
  std::string conf_str;
  if (!folly::readFile(options_.config_path.data(), conf_str)) {
    ld_error("Reading Kafka configuration file failed");
    return false;
  }
  folly::dynamic conf_obj = folly::parseJson(conf_str);

  if (conf_obj.count("poll-thread-count") == 0) {
    // default 10
    poll_thread_count_ = 10;
  } else {
    poll_thread_count_ = conf_obj["poll-thread-count"].asInt();
  }
  std::string global_conf_name = "global-config";
  std::string topic_conf_name = "topic-config";
  // check if the configuration file includes the global-conf
  if (conf_obj.count(global_conf_name) == 0) {
    ld_error("global-conf is missing!");
    return false;
  }
  auto global_conf_obj = conf_obj[global_conf_name];

  if (global_conf_obj.count("bootstrap.servers") == 0) {
    ld_error(
        "bootstrap.servers must be confugred in %s", global_conf_name.c_str());
    return false;
  }

  // start to set global config
  for (auto& params : global_conf_obj.items()) {
    // the set function only accepts string parameters
    if (!setConfig(
            global_conf_, params.first.asString(), params.second.asString())) {
      return false;
    }
  }

  // check if the configuration file includes the topic-conf
  // topic config is optional. So it is not an error if there is no topic-conf
  if (conf_obj.count(topic_conf_name) == 0) {
    return true;
  }

  auto topic_conf_obj = conf_obj[topic_conf_name];
  // start to set global config
  for (auto& params : topic_conf_obj.items()) {
    // the set function only accepts string parameters
    if (!setConfig(
            topic_conf_, params.first.asString(), params.second.asString())) {
      return false;
    }
  }
  global_conf_->set("default_topic_conf", topic_conf_.get(), err_str_);
  if (!err_str_.empty()) {
    ld_error(
        "Setting default_topic_conf failed with error: %s", err_str_.c_str());
    return false;
  }
  return true;
}

bool KafkaClient::append(LogIDType logid,
                         std::string payload,
                         Context context) {
  ld_check(producer_ && topic_);
  folly::call_once(create_poll_flag_, &KafkaClient::createPollThread, this);
  int rv = producer_->produce(topic_.get(),
                              logid,
                              RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                              payload.data(),
                              payload.size(),
                              nullptr,
                              context);
  return rv == 0;
}

bool KafkaClient::getLogs(std::vector<LogIDType>* all_logs) {
  // For testing Kakfa, we must have one and only one topic
  std::unique_ptr<RdKafka::Metadata> metadata;
  RdKafka::Metadata* p_metadata = nullptr;
  ld_check(topic_);
  producer_->metadata(false, topic_.get(), &p_metadata, timeout_ms_);
  if (p_metadata == nullptr) {
    ld_error("Failed to get partitions");
    return false;
  }
  metadata.reset(p_metadata);
  auto topic_metadata = metadata->topics()->front();
  auto partition_metadata_vec = topic_metadata->partitions();
  for (auto partition_metadata = partition_metadata_vec->begin();
       partition_metadata != partition_metadata_vec->end();
       partition_metadata++) {
    all_logs->push_back((*partition_metadata)->id());
  }
  return true;
}

std::shared_ptr<void> KafkaClient::getRawClient() {
  // we do not use this function in Kafka
  return nullptr;
}

bool KafkaClient::getTail(LogIDType,
                          std::function<void(bool, LogPositionType)> cb) {
  // When reading from OFFSET_END, the consumer will read message from the
  // end of the partition.
  cb(true, RdKafka::Topic::OFFSET_END);
  return true;
}

bool KafkaClient::findTime(
    LogIDType logid,
    std::chrono::milliseconds timestamp,
    std::function<void(bool, LogPositionType)> worker_cb) {
  // TopicPartion stores metadata for a partion in a topic.
  // Before we seek a timestamp, we create a topic_partition obj.
  // 1. we store the time stamp in the offset field.
  // 2. Then we pass the topic_partition obj to offsetsForTimes function.
  // 3. The function will replace the timestamp in topic_partion.offset with
  //    the returned offset.
  std::unique_ptr<RdKafka::TopicPartition> topic_partition;
  topic_partition.reset(RdKafka::TopicPartition::create(topic_->name(), logid));
  topic_partition->set_offset(timestamp.count());
  std::vector<RdKafka::TopicPartition*> offsets;
  offsets.push_back(topic_partition.get());
  int rv = producer_->offsetsForTimes(offsets, timeout_ms_);
  if (rv != 0) {
    ld_error("Failed to find time");
    worker_cb(false, 0);
    return false;
  }
  worker_cb(true, topic_partition->offset());
  return true;
}

void KafkaClient::pollingLoop() {
  while (!stop_poll_) {
    // poll is thread-safe
    producer_->poll(timeout_ms_);
  }
}

void KafkaClient::createPollThread() {
  poll_threads_.resize(poll_thread_count_);
  for (int i = 0; i < poll_thread_count_; i++) {
    poll_threads_[i] = std::make_unique<std::thread>(
        std::bind(&KafkaClient::pollingLoop, this));
  }
}

std::unique_ptr<LogStoreReader> KafkaClient::createReader() {
  try {
    auto kafka_reader = std::make_unique<KafkaReader>(options_, this);
    return kafka_reader;
  } catch (const ConstructorFailed&) {
    return nullptr;
  }
}

void KafkaDeliveryReportCb::dr_cb(RdKafka::Message& message) {
  std::string status_name;
  ContextSet contexts;
  std::string payload((char*)message.payload(), message.len());
  auto context = std::make_pair(message.msg_opaque(), payload);
  contexts.emplace_back(context);
  if (owner_ && owner_->writer_stats_update_cb_) {
    owner_->writer_stats_update_cb_(message.err() == 0,
                                    message.partition(),
                                    message.offset(),
                                    std::move(contexts));
  }
}
}}} // namespace facebook::logdevice::ldbench
