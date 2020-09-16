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
#include "logdevice/server/epoch_store/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/server/epoch_store/LogMetaData.h"
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
  LastCleanEpochZRQ(logid_t logid, EpochStore::CompletionLCE cf)
      : ZookeeperEpochStoreRequest(logid), cf_lce_(std::move(cf)) {
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

  Status
  legacyDeserializeIntoLogMetaData(std::string value,
                                   LogMetaData& log_metadata) const override {
    epoch_t parsed_epoch;
    TailRecord parsed_tail;

    int rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
        value.data(), value.size(), logid_, &parsed_epoch, &parsed_tail);

    if (rv != 0) {
      return E::BADMSG;
    }

    auto [epoch_ref, tail_record_ref] = referenceFromLogMetaData(log_metadata);

    epoch_ref = parsed_epoch;
    tail_record_ref = std::move(parsed_tail);

    return Status::OK;
  }

  void postCompletion(Status st,
                      LogMetaData&& log_metadata,
                      RequestExecutor& executor) override {
    auto [epoch_ref, tail_record_ref] = referenceFromLogMetaData(log_metadata);

    epoch_t epoch = epoch_ref;
    TailRecord tail_record = std::move(tail_record_ref);

    auto completion = std::make_unique<EpochStore::CompletionLCERequest>(
        cf_lce_,
        worker_idx_,
        worker_type_,
        st,
        logid_,
        epoch,
        std::move(tail_record));

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
  /**
   * LastCleanEpoch works on both data and metadata log. This function given
   * a log_metadata it returns a reference to the correct fields that this
   * request should deal with.
   */
  std::pair<epoch_t&, TailRecord&>
  referenceFromLogMetaData(LogMetaData& log_metadata) const {
    if (!MetaDataLog::isMetaDataLog(logid_)) {
      return {
          log_metadata.data_last_clean_epoch, log_metadata.data_tail_record};
    } else {
      return {log_metadata.metadata_last_clean_epoch,
              log_metadata.metadata_tail_record};
    }
  }

 protected:
  const EpochStore::CompletionLCE cf_lce_;
};

}} // namespace facebook::logdevice
