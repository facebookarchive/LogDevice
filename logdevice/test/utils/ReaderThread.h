/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <thread>

#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Reader.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

/**
 * A utility that reads a log from the beginning in a background thread.
 */
class ReaderThread {
 public:
  explicit ReaderThread(std::shared_ptr<Client> client, logid_t logid)
      : client_(std::move(client)), logid_(logid) {
    client_->setTimeout(std::chrono::seconds(5));
  }
  ~ReaderThread() {
    stop();
  }

  /**
   * Start reading the log from the beginning.
   */
  void start() {
    stopped_.store(false);
    thread_ = std::thread(&ReaderThread::loop, this);
  }

  /**
   * Stop reading the log.
   */
  void stop() {
    stopped_.store(true);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  /**
   * Blocks until the reader read the log up to this lsn.
   */
  void sync(lsn_t lsn) const {
    wait_until(
        "syncing reader", [&]() { return last_read_lsn_.load() >= lsn; });
  }

  /**
   * Blocks until the reader read the log up to the tail lsn.
   */
  void syncToTail() const {
    const lsn_t tail_lsn = client_->getTailLSNSync(logid_);
    sync(tail_lsn);
  }

  /**
   * Blocks until the reader reports that it's connection is unhealthy, ie it is
   * stalled. Used to verify that readers end up stalling under certain
   * scenarios, ie if rebuilding is non authoritative and there is data to
   * recover.
   */
  void waitUntilStalled() const {
    ClientImpl& real_client = static_cast<ClientImpl&>(*client_);
    std::string reason = "read stream to stall. State:\n";
    reason += AllClientReadStreams::getAllReadStreamsDebugInfo(
        true, false, real_client.getProcessor());
    wait_until(reason.c_str(), [&]() { return !is_healthy_.load(); });
  }

  /**
   * @returns True if the reader saw some DATALOSS gaps.
   */
  bool foundDataLoss() const {
    return has_data_loss_;
  }

  size_t getNumRecordsRead() const {
    return n_read_;
  }

 private:
  std::shared_ptr<Client> client_;
  logid_t logid_;
  std::thread thread_;
  std::atomic_bool stopped_;
  std::atomic<lsn_t> last_read_lsn_{LSN_INVALID};
  size_t n_read_{0};
  std::atomic_bool is_healthy_{true};

  bool has_data_loss_{false};

  void loop() {
    stopped_.store(false);
    std::unique_ptr<Reader> reader(client_->createReader(1));
    reader->setTimeout(std::chrono::milliseconds{200});
    reader->startReading(logid_, 1);

    std::vector<std::unique_ptr<DataRecord>> records;
    GapRecord gap;
    ssize_t nread;
    while (!stopped_) {
      records.clear();
      nread = reader->read(1, &records, &gap);
      if (nread == -1) {
        ASSERT_EQ(E::GAP, err);
        if (gap.type == GapType::DATALOSS) {
          has_data_loss_ = true;
        }
        last_read_lsn_.store(gap.hi);
      } else if (nread > 0) {
        ld_check(records.size() == nread);
        last_read_lsn_.store(records[nread - 1]->attrs.lsn);
        ++n_read_;
      }
      is_healthy_.store(reader->isConnectionHealthy(logid_));
    }
  }
};

}}} // namespace facebook::logdevice::IntegrationTestUtils
