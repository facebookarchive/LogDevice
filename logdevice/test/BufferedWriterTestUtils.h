/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <mutex>
#include <string>
#include <vector>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/BufferedWriter.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

// Null context for appenders that don't need one
constexpr BufferedWriter::AppendCallback::Context NULL_CONTEXT(nullptr);

/**
 * Universal callback for use in different tests.
 */
class TestCallback : public BufferedWriter::AppendCallback {
 public:
  void onSuccess(logid_t,
                 ContextSet contexts,
                 const DataRecordAttributes& attrs) override {
    std::lock_guard<std::mutex> guard(mutex_);
    if (lsn_range.first == LSN_INVALID) {
      lsn_range.first = attrs.lsn;
    }
    lsn_range.first = std::min(lsn_range.first, attrs.lsn);
    lsn_range.second = std::max(lsn_range.second, attrs.lsn);
    for (auto& ctx : contexts) {
      payloads_succeeded.push_back(std::move(ctx.second));
      sem.post();
    }
  }

  void onFailure(logid_t /*log_id*/,
                 ContextSet contexts,
                 Status /*status*/) override {
    for (auto& ctx : contexts) {
      sem.post();
    }
  }

  // Each complete append (success or failure) posts to this semaphore
  Semaphore sem;
  // Collects payloads for all successful writes
  std::vector<std::string> payloads_succeeded;
  // Don't care about duplicates?
  std::set<std::string> payloadsSucceededAsSet() const {
    return std::set<std::string>(
        payloads_succeeded.begin(), payloads_succeeded.end());
  }
  // LSN range that was written
  std::pair<lsn_t, lsn_t> lsn_range{LSN_INVALID, LSN_INVALID};

 private:
  std::mutex mutex_;
};
