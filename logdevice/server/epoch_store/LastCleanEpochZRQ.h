/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdio>
#include <cstring>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/LogTailAttributes.h"
#include "logdevice/server/epoch_store/ZookeeperEpochStoreRequest.h"

namespace facebook { namespace logdevice {

/**
 * @file  this is the parent class of GetLastCleanEpochZRQ and
 *        SetLastCleanEpochZRQ requests. It contains the functions common
 *        to both set and get operations on the znode that stores last
 *        clean epoch and log tail attributes.
 *        Note that logdevice stores data for both data log and
 *        its corresponding metadata log, and these lce_s are stored in separate
 *        znodes under the same <datalog_id> directory:
 *            data log:     <root_path>/<datalog_id>/lce
 *            metadata log: <root_path>/<datalog_id>/metadatalog_lce
 */

class LastCleanEpochZRQ : public ZookeeperEpochStoreRequest {
 public:
  LastCleanEpochZRQ(logid_t logid, epoch_t epoch, EpochStore::CompletionLCE cf)
      : ZookeeperEpochStoreRequest(logid, epoch), cf_lce_(std::move(cf)) {
    ld_check(cf_lce_);
  }

  // see ZookeeperEpochStoreRequest.h
  std::string getZnodePath(const std::string& rootpath) const override {
    const logid_t datalog_id = MetaDataLog::dataLogID(logid_);
    const char* leaf_name = MetaDataLog::isMetaDataLog(logid_)
        ? znodeNameMetaDataLog
        : znodeNameDataLog;

    return rootpath + "/" + std::to_string(datalog_id.val_) + "/" + leaf_name;
  }

  void postCompletion(Status st, RequestExecutor& executor) override {
    auto completion = std::make_unique<EpochStore::CompletionLCERequest>(
        cf_lce_, worker_idx_, worker_type_, st, logid_, epoch_, tail_record_);

    std::unique_ptr<Request> rq(std::move(completion));
    int rv = executor.postWithRetrying(rq);

    if (rv != 0 && err != E::SHUTDOWN) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Got an unexpected status "
                      "code %s from Processor::postWithRetrying(), dropping "
                      "request for log %lu",
                      error_name(err),
                      logid_.val_);
      ld_check(false);
    }
  }

  static constexpr const char* znodeNameDataLog = "lce";
  static constexpr const char* znodeNameMetaDataLog = "metadatalog_lce";

 protected:
  const EpochStore::CompletionLCE cf_lce_;
  TailRecord tail_record_;

 public:
};

}} // namespace facebook::logdevice
