/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SequencerMetaDataLogManager.h"

#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerBackgroundActivator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WriteMetaDataLogRequest.h"

namespace facebook { namespace logdevice {

void SequencerMetaDataLogManager::considerWritingMetaDataLogRecord(
    std::shared_ptr<const EpochMetaData> epoch_store_metadata,
    const std::shared_ptr<Configuration>& cfg) {
  if (MetaDataLog::isMetaDataLog(owner_->getLogID())) {
    // there are no metadata logs for metadata logs
    return;
  }
  auto& ml_conf = cfg->serverConfig()->getMetaDataLogsConfig();
  if (!ml_conf.sequencers_write_metadata_logs) {
    // metadata logs should not be written by sequencers
    return;
  }

  // Sequencers are responsible for writing metadata logs, we should check the
  // written bit
  if (epoch_store_metadata->writtenInMetaDataLog()) {
    // the written bit is set, this record should already be in the metadata
    // log
    return;
  }

  // This loop ensures that there is no more than one outstanding request for
  // every effective_since so that subsequent sequencer reactivations do not
  // cause the same metadata log record to be written multiple times
  while (true) {
    uint32_t already_writing = last_metadata_log_writer_effective_since_.load();
    if (already_writing == epoch_store_metadata->h.effective_since.val()) {
      // A previous sequencer activation has started a request that is writing
      // the metadata log record with this effective since, and it hasn't
      // completed yet. Not adding another duplicate.
      return;
    } else if (already_writing >
               epoch_store_metadata->h.effective_since.val()) {
      dd_assert(false,
                "Trying to write a metadata log record for data log %lu with "
                "a lower effective_since than another outstanding request. "
                "This should not normally happen. Attempting to write S:%u, "
                "already writing S:%u",
                owner_->getLogID().val(),
                epoch_store_metadata->h.effective_since.val(),
                already_writing);
      return;
    } else {
      if (last_metadata_log_writer_effective_since_.compare_exchange_strong(
              already_writing, epoch_store_metadata->h.effective_since.val())) {
        break;
      }
      // If we failed the swap, re-read and re-try
    }
  }

  auto callback = [this](Status st,
                         std::shared_ptr<const EpochMetaData> es_metadata) {
    if (st == E::OK) {
      std::shared_ptr<EpochSequencer> current_epoch =
          owner_->getCurrentEpochSequencer();

      if (current_epoch == nullptr) {
        ld_info("Not setting metadata written in sequencer because log %lu "
                "is no longer in config or sequencer in PERMANENT_ERROR. "
                "Current sequencer state: %s.",
                owner_->getLogID().val(),
                Sequencer::stateString(owner_->getState()));
        return;
      }

      std::shared_ptr<const EpochMetaData> current_metadata =
          current_epoch->getMetaData();
      ld_check(current_metadata != nullptr);

      // comparing metadata except for the epoch field
      if (!es_metadata->identicalInMetaDataLog(*current_metadata)) {
        // current metadata is different from what we wrote, not modifying
        ld_info("Metadata mismatch for log %lu, epoch store: %s, "
                "current: %s.",
                owner_->getLogID().val(),
                es_metadata->toString().c_str(),
                current_metadata->toString().c_str());
        return;
      }

      bool res = current_epoch->setMetaDataWritten();
      if (res) {
        ld_spew("Successfully finished modifying metadata for log %lu",
                owner_->getLogID().val());
      } else {
        ld_info(
            "Failed to modify metadata for log %lu", owner_->getLogID().val());
      }

      // If config (e.g. nodeset_size) was changed, SequencerBackgroundActivator
      // may want to update sequencer's nodeset. It didn't have a chance to do
      // that up until now, because the previous nodeset needs to be written to
      // metadata log before we can switch to a new nodeset.
      // Tell SequencerBackgroundActivator to check if update is needed.
      SequencerBackgroundActivator::requestSchedule(
          Worker::onThisThread()->processor_, {owner_->getLogID()});
    }

    uint32_t expected_effective_since = es_metadata->h.effective_since.val();
    last_metadata_log_writer_effective_since_.compare_exchange_strong(
        expected_effective_since, 0);

    if (st == E::PREEMPTED || st == E::ABORTED) {
      // If the sequencer on this node got reactivated in a higher epoch since
      // we started doing this request, upon getting these errors, we should
      // retry the write, as it's possible this node is running the sequencer
      // for the latest epoch now
      std::shared_ptr<const EpochMetaData> cur_metadata =
          owner_->getCurrentMetaData();
      // cur_metadata may be null if the sequencer was disabled in config or
      // hit a permanent error, in which case we just bail.
      if (cur_metadata == nullptr) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       5,
                       "Sequencer of log %lu is no longer in config or "
                       "sequencer in PERMANENT_ERROR. "
                       "Current sequencer state: %s.",
                       owner_->getLogID().val(),
                       Sequencer::stateString(owner_->getState()));
        return;
      }
      if (cur_metadata->h.epoch.val() > es_metadata->h.epoch.val()) {
        considerWritingMetaDataLogRecord(
            cur_metadata, Worker::onThisThread()->getConfig());
      }
    } else if (st == E::NOTFOUND || st == E::NOTINSERVERCONFIG) {
      ld_warning("Write to metadata log failed for log %lu: %s, the log is "
                 "no longer in config.",
                 owner_->getLogID().val(),
                 error_description(st));
      // TODO 18147886: evict all epochs from the sequencer
    }
  };
  std::unique_ptr<Request> rq = std::make_unique<WriteMetaDataLogRequest>(
      owner_->getLogID(), epoch_store_metadata, callback);
  Worker::onThisThread()->processor_->postWithRetrying(rq);
}

}} // namespace facebook::logdevice
