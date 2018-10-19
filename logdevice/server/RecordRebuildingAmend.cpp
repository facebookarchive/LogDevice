/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordRebuildingAmend.h"

#include <folly/Random.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/PerShardHistograms.h"

namespace facebook { namespace logdevice {

RecordRebuildingAmend::RecordRebuildingAmend(
    lsn_t lsn,
    shard_index_t shard,
    RecordRebuildingOwner* owner,
    std::shared_ptr<ReplicationScheme> replication,
    STORE_Header storeHeader,
    LocalLogStoreRecordFormat::flags_t flags,
    copyset_t newCopyset,
    copyset_t amendRecipients,
    uint32_t rebuildingWave,
    const NodeAvailabilityChecker* node_availability)
    : RecordRebuildingBase(lsn, shard, owner, replication, node_availability) {
  storeHeader_ = storeHeader;
  recordFlags_ = flags;
  newCopyset_ = newCopyset;
  amendRecipients_ = amendRecipients;
  rebuildingWave_ = rebuildingWave;
}

RecordRebuildingAmend::~RecordRebuildingAmend() {
  if (started_) {
    WORKER_STAT_DECR(record_rebuilding_amend_in_progress);
  }
}

void RecordRebuildingAmend::start(bool /*unused*/) {
  ld_check(!started_);
  started_ = true;
  ld_spew("RecordRebuildingAmend for log:%lu, lsn:%s started",
          owner_->getLogID().val_,
          lsn_to_string(lsn_).c_str());
  WORKER_STAT_INCR(record_rebuilding_amend_in_progress);

  StageRecipients amend_stage(StageRecipients::Type::AMEND);
  StageRecipients amend_self_stage(StageRecipients::Type::AMEND);

  for (int i = (int)amendRecipients_.size() - 1; i >= 0; i--) {
    RecipientNode r(*this, amendRecipients_[i]);

    if (amendRecipients_[i] == getMyShardID()) {
      amend_self_stage.recipients.push_back(std::move(r));
    } else {
      amend_stage.recipients.push_back(std::move(r));
    }
  }

  if (!amend_stage.recipients.empty()) {
    stages_.push_back(std::move(amend_stage));
  }

  if (!amend_self_stage.recipients.empty()) {
    stages_.push_back(std::move(amend_self_stage));
  }

  if (stages_.empty()) {
    // Nothing to amend.
    onComplete();
    return;
  }

  int rv = sendStage(&stages_[curStage_], false);
  ld_check(rv == 0);
}

void RecordRebuildingAmend::onStageComplete() {
  resetStoreTimer();
  resetRetryTimer();
  ++curStage_;
  if (curStage_ == stages_.size()) {
    onComplete();
  } else {
    int rv = sendStage(&stages_[curStage_], false);
    ld_check(rv == 0);
  }
}

void RecordRebuildingAmend::onRetryTimeout() {
  WORKER_STAT_INCR(record_rebuilding_amend_retries);

  if (!checkEveryoneStillInConfig()) {
    owner_->onCopysetInvalid(lsn_);
    return;
  }

  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "Retrying RecordRebuildingAmend Log %lu, LSN %s, stage: %s",
                 owner_->getLogID().val_,
                 lsn_to_string(lsn_).c_str(),
                 stageDebugInfo(&stages_[curStage_]).c_str());

  int rv = sendStage(&stages_[curStage_], false);
  ld_check(rv == 0);
}

void RecordRebuildingAmend::onStoreTimeout() {
  WORKER_STAT_INCR(record_rebuilding_amend_timeouts);

  if (!checkEveryoneStillInConfig()) {
    owner_->onCopysetInvalid(lsn_);
    return;
  }

  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "Rebuilding amend timed out. Log %lu, LSN %s, stage: %s",
                 owner_->getLogID().val_,
                 lsn_to_string(lsn_).c_str(),
                 stageDebugInfo(&stages_[curStage_]).c_str());
  int rv = sendStage(&stages_[curStage_], /* resend_inflight_stores */ true);
  ld_check(rv == 0);
}

void RecordRebuildingAmend::onStoreFailed() {
  WORKER_STAT_INCR(record_rebuilding_amend_failed);
  activateRetryTimer();
}

void RecordRebuildingAmend::onComplete() {
  auto flushTokenMap = std::make_unique<FlushTokenMap>();
  for (int i = 0; i < stages_.size(); i++) {
    for (auto& r : stages_[i].recipients) {
      ld_check(r.succeeded);
      if (r.flush_token != FlushToken_INVALID) {
        auto key = std::make_pair(r.shard_.node(), r.server_instance_id);
        flushTokenMap->emplace(key, r.flush_token);
      }
    }
  }
  owner_->onAllAmendsReceived(lsn_, std::move(flushTokenMap));
}

void RecordRebuildingAmend::traceEvent(const char* event_type,
                                       const char* status) {
  tracer_.traceRecordRebuildingAmend(owner_->getLogID(),
                                     lsn_,
                                     owner_->getRebuildingVersion(),
                                     usec_since(creation_time_),
                                     owner_->getRebuildingSet(),
                                     rebuildingWave_,
                                     event_type,
                                     status);
}

}} // namespace facebook::logdevice
