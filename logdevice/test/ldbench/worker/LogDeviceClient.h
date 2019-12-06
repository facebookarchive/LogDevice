/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <mutex>
#include <vector>

#include <folly/synchronization/CallOnce.h>

#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/test/ldbench/worker/LogStoreClient.h"

namespace facebook { namespace logdevice { namespace ldbench {

class BufferedWriterCallback;
class LogStoreReader;

class LogDeviceClient : public LogStoreClient {
 public:
  explicit LogDeviceClient(const Options& options);
  ~LogDeviceClient() override;
  /**
   * Append to logdevice
   */
  bool append(LogIDType id, std::string payload, Context context) override;

  /**
   * It is an aync operation
   * @param
   *  all_logs -- returned log ids
   */
  bool getLogs(std::vector<LogIDType>* all_logs) override;

  /**
   * Get Raw client object
   */
  std::shared_ptr<void> getRawClient() override;

  /*
   * Get tail of a log
   */
  bool getTail(LogIDType logid,
               std::function<void(bool, LogPositionType)> worker_cb) override {
    // non-blocking return the tail
    // add an indirection to convert logdevice status
    auto cb = [worker_cb](Status status, lsn_t tail_lsn) {
      worker_cb(status == E::OK, tail_lsn);
    };
    int rv = client_->getTailLSN(logid_t(logid), cb);
    return rv == 0;
  }

  /**
   * Non-blocking return the position of the timestamp in a log
   */
  bool findTime(LogIDType logid,
                std::chrono::milliseconds timestamp,
                std::function<void(bool, LogPositionType)> worker_cb) override {
    auto cb = [worker_cb](Status status, lsn_t tail_lsn) {
      worker_cb(status == E::OK, tail_lsn);
    };
    int rv = client_->findTime(logid_t(logid), timestamp, cb);
    return rv == 0;
  }

  /**
   * Create a buffered writer
   * This function will be only called once
   */
  void createBufferedWriter();

  /**
   * Create a reader
   */
  std::unique_ptr<LogStoreReader> createReader() override;

 private:
  std::unique_ptr<BufferedWriterCallback> writer_callback_;
  std::unique_ptr<BufferedWriter> buffered_writer_;
  std::shared_ptr<Client> client_;
  folly::once_flag create_writer_flag_;
  // options from command line
  Options options_;
};

class BufferedWriterCallback : public BufferedWriter::AppendCallback {
 public:
  explicit BufferedWriterCallback(LogDeviceClient* owner) : owner_(owner) {}

  /**
   * Callback for sucessful records
   */
  void onSuccess(logid_t,
                 ContextSet contexts,
                 const DataRecordAttributes& attr) override;

  /**
   * Callback for failed records.
   * BufferedWriter exhausted all retries it was configured to do.
   */
  void onFailure(logid_t log_id, ContextSet contexts, Status status) override;

 private:
  LogDeviceClient* owner_;
};
}}} // namespace facebook::logdevice::ldbench
