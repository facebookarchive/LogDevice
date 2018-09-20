/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/WeakRefHolder.h"

namespace facebook { namespace logdevice {

/**
 *  @file a request posted by the sequencer to write a metadata log record.
 */

class WriteMetaDataLogRequest : public FireAndForgetRequest {
 public:
  using callback_t =
      std::function<void(Status, std::shared_ptr<const EpochMetaData>)>;
  WriteMetaDataLogRequest(
      logid_t log_id,
      std::shared_ptr<const EpochMetaData> epoch_store_metadata,
      callback_t callback);

  // see FireAndForgetRequest.h
  void executionBody() override;

 private:
  // Appends the metadata log record
  void writeRecord();

  // Callback for when the append completes
  void onAppendResult(Status st, lsn_t lsn);

  // If append completed successfully, this method sets the written bit in the
  // epoch store
  void setMetaDataWrittenES();

  // Callback for the epoch store write
  void onEpochStoreUpdated(Status st,
                           logid_t log_id,
                           std::unique_ptr<const EpochMetaData> metadata);

  // Finishes execution of the request, calls the callback if st is not
  // E::SHUTDOWN and calls FireAndForgetRequest::destroy()
  void destroyRequest(Status st);

  // Returns the epoch store instance
  virtual EpochStore& getEpochStore();

  logid_t log_id_;

  // for checking whether the request is still alive in callbacks
  WeakRefHolder<WriteMetaDataLogRequest> ref_holder_;

  std::shared_ptr<const EpochMetaData> epoch_store_metadata_;
  callback_t callback_;
  std::string serialized_payload_;
  int checksum_bits_;
  std::unique_ptr<ExponentialBackoffTimer> append_retry_timer_;
  std::unique_ptr<ExponentialBackoffTimer> epoch_store_retry_timer_;
  MetaDataTracer tracer_;
};

}} // namespace facebook::logdevice
