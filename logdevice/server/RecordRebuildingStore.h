/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/small_vector.h>

#include "logdevice/server/RecordRebuildingAmend.h"
#include "logdevice/server/RecordRebuildingBase.h"

/**
 * @file RecordRebuildingStore is a state machine for rebuilding a single record
 */

namespace facebook { namespace logdevice {

class RecordRebuildingStore : public RecordRebuildingBase {
 public:
  /**
   * @param block_id              Used for seeding the rng for the copyset
   *                              selector
   * @param shard                 Local shard for which this state machine is
   *                              running
   * @param record                RawRecord object.
   * @param owner                 Object that owns this
   *                              RecordRebuildingStore object.
   * @param replication           How copysets should be selected for the
   *                              current epoch.
   * @param scratch_payload_holder If `record` doesn't own its memory
   *                              (owned = false) then this shared_ptr owns that
   *                              memory (usually that memory is part of a chunk
   *                              containing multiple records, and the
   *                              shared_ptr owns the whole chunk), as well as
   *                              a PayloadHolder object that
   *                              RecordRebuildingStore needs for internal use.
   * @param node_availability     Object to use for checking which nodes are
   *                              alive. Only tests override it.
   */
  RecordRebuildingStore(
      size_t block_id,
      shard_index_t shard,
      RawRecord record,
      RecordRebuildingOwner* owner,
      std::shared_ptr<ReplicationScheme> replication,
      std::shared_ptr<PayloadHolder> scratch_payload_holder = nullptr,
      const NodeAvailabilityChecker* node_availability =
          NodeAvailabilityChecker::instance());

  ~RecordRebuildingStore() override;

  void start(bool read_only = false) override;

  const RawRecord& getRecord() const {
    return record_;
  }
  size_t getPayloadSize() const;

  // RecordRebuildingAmendState contains all information necessary to start
  // amend state machines once stores are deemed durable.
  std::unique_ptr<RecordRebuildingAmendState> getRecordRebuildingAmendState();

 private:
  void traceEvent(const char* event_type, const char* status) override;

  copyset_size_t replicationFactor_;
  size_t blockID_;
  RawRecord record_;

  // -1 means pick copyset, non-negative value is index in stages_.
  int curStage_ = -1;
  folly::small_vector<StageRecipients, 3> stages_;
  std::shared_ptr<PayloadHolder> payloadHolder_;

  // @return -1 if record is malformed.
  int parseRecord();

  // @return  if can't pick copyset, returns -1 and sets err to:
  //          FAILED  failed, should retry after a timeout;
  //          EXISTS  record doesn't need rebuilding.
  int pickCopyset();
  void pickCopysetAndStart();

  // Populates newCopyset_ with recipients from existingCopyset_ that we want to
  // keep.
  void buildNewCopysetBase();
  void sendCurrentStage(bool resend_inflight_stores = false);
  void nextStage();
  // Start a new wave. If immediate==true, start it immediately. Otherwise,
  // activate the timer and start the new wave when it triggers.
  void startNewWave(bool immediate);

 protected:
  // Can be overridden in tests.
  // A part of pickCopyset() extracted to be overridden in tests.
  virtual int pickCopysetImpl();
  void onComplete() override;
  void onRetryTimeout() override;
  void onStoreTimeout() override;
  void onStoreFailed() override;
  void onStageComplete() override;
  std::shared_ptr<PayloadHolder> getPayloadHolder() const override;
};

}} // namespace facebook::logdevice
