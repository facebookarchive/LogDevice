/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <thread>
#include <atomic>

#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

/**
 * A utility that writes to a log in a background thread.
 */
class AppendThread {
 public:
  explicit AppendThread(std::shared_ptr<Client> client, logid_t logid)
      : client_(std::move(client)), logid_(logid) {}
  ~AppendThread() {
    stop();
  }

  void start() {
    stopped_.store(false);
    thread_ = std::thread(&AppendThread::loop, this);
  }

  void stop() {
    stopped_.store(true);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  size_t getNumRecordsAppended() const {
    return n_appended_;
  }

 private:
  std::shared_ptr<Client> client_;
  logid_t logid_;
  std::thread thread_;
  std::atomic_bool stopped_;
  size_t n_appended_{0};

  void loop() {
    while (!stopped_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      lsn_t lsn = client_->appendSync(logid_, Payload("test", 4));
      // Appends may fail during our tests. Count the ones that don't.
      if (lsn != LSN_INVALID) {
        ++n_appended_;
      }
    }
  }
};

}}} // namespace facebook::logdevice::IntegrationTestUtils
