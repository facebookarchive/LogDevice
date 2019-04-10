/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/FindKeyTracer.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FINDKEY_Message.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

class FindKeyTracer;

/**
 * @file Implements the findTime() and findKey() client APIs on a storage node.
 * In the case of findTime(), given a target timestamp, looks for the narrowest
 * range of LSNs (lo, hi] such that the earliest record with a timestamp >=
 * target timestamp is surely in the range. The range may be empty (lo == hi) if
 * we are at the end of the log.
 * In the case of findKey(), the task performs the same operation for record
 * keys instead of timestamps.
 */

class FindKeyStorageTask : public StorageTask {
 public:
  FindKeyStorageTask(ClientID reply_to,
                     request_id_t client_rqid,
                     logid_t log_id,
                     std::chrono::milliseconds target_timestamp,
                     folly::Optional<std::string> target_key,
                     lsn_t last_released_lsn,
                     lsn_t trim_point,
                     FINDKEY_flags_t flags,
                     std::chrono::steady_clock::time_point task_deadline,
                     FindKeyTracer tracer,
                     Sockaddr client_address = Sockaddr());

  void execute() override;
  void onDone() override;
  void onDropped() override;

  ThreadType getThreadType() const override {
    return ThreadType::SLOW;
  }

  StorageTaskPriority getPriority() const override {
    // FindKeyStorageTask does not read from data record space and it is usually
    // very fast to complete. Give them a very high priority to avoid head of
    // line blocking caused by other read tasks.
    return StorageTaskPriority::VERY_HIGH;
  }

  Principal getPrincipal() const override {
    return Principal::FINDKEY;
  }

  // Workhorse of execute(), LocalLogStore passed from above for testability
  void executeImpl(const LocalLogStore& store, StatsHolder* stats = nullptr);

  // The result, public for tests
  Status result_status_; // OK or FAILED
  lsn_t result_lo_;
  lsn_t result_hi_;

 private:
  const ClientID reply_to_;
  const request_id_t client_rqid_;
  const logid_t log_id_;
  const std::chrono::milliseconds target_timestamp_;
  const folly::Optional<std::string> target_key_;
  const lsn_t last_released_lsn_;
  const lsn_t trim_point_;
  const FINDKEY_flags_t flags_
#ifdef __clang__
      __attribute__((__unused__))
#endif
      ;
  // deadline after which the store operation is presumed to have timed out
  const std::chrono::steady_clock::time_point task_deadline_;
  FindKeyTracer tracer_;

  // used for debugging
  Sockaddr client_address_;

  void sendReply();

  void getDebugInfoDetailed(StorageTaskDebugInfo&) const override;
};

}} // namespace facebook::logdevice
