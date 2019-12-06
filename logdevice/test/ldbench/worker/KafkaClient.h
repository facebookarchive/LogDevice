/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include <folly/synchronization/CallOnce.h>
#include <librdkafka/rdkafkacpp.h>

#include "logdevice/test/ldbench/worker/LogStoreClient.h"
#include "logdevice/test/ldbench/worker/LogStoreTypes.h"
#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice { namespace ldbench {

class KafkaDeliveryReportCb;
class KafkaReader;

/**
 * Terms in Kafka and Logdevice correspond with each other. In our design, we
 * make the following Correspondence:
 * Logdevice  :  Kafka
 *  log_range : topic
 *  log       : partition
 *
 * Sometime, we may still use "log" in this file. In kafka, it means partition.
 *
 * A log id in logdevice can identify a log since it is unique. To
 * operate a log, a function only needs to provide a log id. However, in Kafka,
 * the partition id is not unique. Functions need to provide both the topic and
 * the partition. To simply the implementation, our benchmark tool only
 * operates in one topic. In this way, a partition id can identify a partition.
 * And it will not influence the performance results.
 */
class KafkaClient : public LogStoreClient {
 public:
  explicit KafkaClient(const Options& options);
  ~KafkaClient() override;
  /**
   * In logdevice, a client can be created using the logdevice configuration
   * file directly. However, we can not do the same thing in Kafka. We have to
   * manually set the parameters to create a client (producer) for Kakfa.
   * Therefore, we also provides a Kafka configuration file. And this function
   * is responsible to parse the file (json format) and set the parameters.
   * @return
   *  true if initializations are successful
   */
  bool initFromConfigFile();

  /**
   * Append to Kafka
   * @param
   *  partition_id: partition id
   *  payload
   */
  bool append(LogIDType partition_id, std::string payload, Context) override;

  /**
   * It is an sync operation
   * @param
   *  all_logs -- returned partition id
   */
  bool getLogs(std::vector<LogIDType>* all_partitions) override;

  /**
   * Get Raw client object
   * We do not use this function for Kafka
   */
  std::shared_ptr<void> getRawClient() override;

  /*
   * Get tail of a partition
   */
  bool getTail(LogIDType partition_id,
               std::function<void(bool, LogPositionType)> worker_cb) override;
  /**
   * Non-blocking return the position of the timestamp in a partition
   */
  bool findTime(LogIDType partition_id,
                std::chrono::milliseconds timestamp,
                std::function<void(bool, LogPositionType)> worker_cb) override;
  /**
   * Callback functions will be queued. Kafka requires to call the poll function
   * periodically to serve the queued callbacks.
   * This pollingLoop function will be called once if there are appends.
   * It uses an infinite loop to allow producer to call its poll function.
   */
  void pollingLoop();

  /**
   * Create a consumer
   */
  std::unique_ptr<LogStoreReader> createReader() override;

  /**
   * Get the configuration
   */
  RdKafka::Conf* getGlobalConf() {
    return global_conf_.get();
  }
  RdKafka::Conf* getTopicConf() {
    return topic_conf_.get();
  }

 private:
  /**
   * Set parameters in a RdKafka obj
   * It wraps the ld_error check
   * @return
   *  -- true if successfully set
   */
  bool setConfig(std::shared_ptr<RdKafka::Conf>,
                 std::string param,
                 std::string val);

  /**
   * Create polling threads
   */
  void createPollThread();

  Options options_;
  std::string err_str_;                        // the error message
  std::shared_ptr<RdKafka::Conf> global_conf_; // a global configuration
  std::shared_ptr<RdKafka::Conf> topic_conf_;  // a topic specific configuration
  // We will only operate this topic. It can be
  // specified by options.log_range_names
  std::unique_ptr<RdKafka::Topic> topic_;
  std::unique_ptr<KafkaDeliveryReportCb>
      kafka_delivery_recort_cb_; // callback for appends
  std::unique_ptr<RdKafka::Producer> producer_;

  // As mention, a kafka client needs to call poll()
  // periordically to serve callback functions. We
  // use separate threads to do it.
  // Based on the current results, using multiple polling threads has
  // very limited influences on the performance. But we still keep this option.
  std::vector<std::unique_ptr<std::thread>> poll_threads_;
  uint64_t poll_thread_count_;

  std::atomic<bool> stop_poll_{false}; // stop the poll thread
  folly::once_flag create_poll_flag_;
  uint64_t timeout_ms_;
};

/**
 * A callback class when a produced (appended) message (record)
 * is accepted by Kafka.
 */
class KafkaDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  explicit KafkaDeliveryReportCb(KafkaClient* kfclient) : owner_(kfclient) {}
  void dr_cb(RdKafka::Message& message) override;
  KafkaClient* owner_;
};
}}} // namespace facebook::logdevice::ldbench
