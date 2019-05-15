/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochSequencer.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

/**
 * @file a MetaSequencer is special type of Sequencer that manages the special
 *       metadata log. It is owned by the parent controller WriteMetaDataRecord,
 *       which is owned by the corresponding data log sequencer. It is created
 *       on demand when there is a need to write a metadata log record, and gets
 *       destroyed with the parent controller once it finishes its duties.
 *       Different from sequencers for data logs, a MetaSequencer object runs
 *       on single worker thread for its life time.
 */

class WriteMetaDataRecord;

// The purpose for subclassing from the base Sequencer class is to
// hook function calls so that the parent controller can be notified on
// progresses and events of the meta sequencer. Also, log recovery is pinned
// on the same worker thread that runs the sequencer.

class MetaEpochSequencer : public EpochSequencer {
 public:
  using EpochSequencer::EpochSequencer;

  void retireAppender(Status st, lsn_t lsn, Appender::Reaper& reaper) override;
};

class MetaSequencer : public Sequencer {
 public:
  MetaSequencer(logid_t logid,
                UpdateableSettings<Settings> settings,
                StatsHolder* stats,
                WriteMetaDataRecord* controller)
      : Sequencer(logid, settings, stats), controller_(controller) {
    // must be created with a metadata log id
    ld_check(MetaDataLog::isMetaDataLog(logid));
    ld_check(controller != nullptr);
    const Worker* worker =
        static_cast<Worker*>(Worker::onThisThread(false /*enforce_worker*/));
    if (worker) {
      created_on_ = worker->idx_;
    }
  }

  std::shared_ptr<EpochSequencer>
  createEpochSequencer(epoch_t epoch,
                       std::unique_ptr<EpochMetaData> metadata) override;

  // Run recovery on the same worker thread on which it is created
  worker_id_t recoveryWorkerThread() override {
    return created_on_;
  }

  void onRecoveryCompleted(
      Status status,
      epoch_t epoch,
      TailRecord previous_epoch_tail,
      std::unique_ptr<const RecoveredLSNs> recovered_lsns) override;

  void noteReleaseSuccessful(ShardID shard,
                             lsn_t released_lsn,
                             ReleaseType release_type) override {
    Sequencer::noteReleaseSuccessful(shard, released_lsn, release_type);
    // TODO T15517759: if we ever use FLS for metadata nodesets, this needs
    // to be changed for tracking real f-majority
    controller_->onReleaseSentSuccessful(
        shard.node(), released_lsn, release_type);
  }

  void notePreempted(epoch_t epoch, NodeID preempted_by) override {
    ld_spew("PREEMPTED by %s for epoch %u of log %lu",
            preempted_by.toString().c_str(),
            epoch.val(),
            getLogID().val());
    Sequencer::notePreempted(epoch, preempted_by);
    controller_->notePreempted(epoch, preempted_by);
  }

  void onAppenderRetired(Status st, lsn_t lsn) {
    controller_->onAppenderRetired(st, lsn);
  }

 private:
  // back pointer to the parent controller
  WriteMetaDataRecord* const controller_;
  // worker thread that the sequencer is created on
  worker_id_t created_on_{-1};
};

}} // namespace facebook::logdevice
