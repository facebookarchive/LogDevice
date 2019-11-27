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
   * @param block_id           Used for seeding the rng for the copyset
   *                           selector
   * @param shard              Local shard for which this state machine is
   *                           running
   * @param raw_record         Record bytes, as read from local log store,
   *                           including header (see LocalLogStoreRecordFormat)
   * @param owner              Object that owns this
   *                           RecordRebuildingStore object.
   * @param replication        How copysets should be selected for the
   *                           current epoch.
   * @param node_availability  Object to use for checking which nodes are
   *                           alive. Only tests override it.
   */
  RecordRebuildingStore(size_t block_id,
                        shard_index_t shard,
                        lsn_t lsn,
                        folly::IOBuf&& raw_record,
                        RecordRebuildingOwner* owner,
                        std::shared_ptr<ReplicationScheme> replication,
                        const NodeAvailabilityChecker* node_availability =
                            NodeAvailabilityChecker::instance());

  ~RecordRebuildingStore() override;

  void start(bool read_only = false) override;

  size_t getRecordSize() const;

  // RecordRebuildingAmendState contains all information necessary to start
  // amend state machines once stores are deemed durable.
  std::unique_ptr<RecordRebuildingAmendState> getRecordRebuildingAmendState();

 private:
  void traceEvent(const char* event_type, const char* status) override;

  copyset_size_t replicationFactor_;
  size_t blockID_;

  // Before parseRecord(): raw record, to be parsed using
  // LocalLogStoreRecordFormat::parse().
  // After parseRecord(): only the payload.
  // We're being greedy and reuse the same IOBuf for these two purposes
  // because sizeof(IOBuf) is pretty big: 56 bytes.
  folly::IOBuf recordOrPayload_;

  // -1 means pick copyset, non-negative value is index in stages_.
  int curStage_ = -1;
  folly::small_vector<StageRecipients, 3> stages_;

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
  PayloadHolder getPayloadHolder() const override;
};

}} // namespace facebook::logdevice
