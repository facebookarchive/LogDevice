/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/admincommands/SequencerDeactivationRequest.h"

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

void SequencerDeactivationRequest::onTimeout() {
  int deactivate_size = logs_.size() < batch_ ? logs_.size() : batch_;
  for (int i = 0; i < deactivate_size; i++) {
    auto log_id = logs_.front();
    logs_.pop();
    std::shared_ptr<Sequencer> sequencer = nullptr;
    sequencer =
        Worker::onThisThread()->processor_->allSequencers().findSequencer(
            log_id);
    if (sequencer) {
      sequencer->deactivateSequencer();
      ld_debug("The sequencer for a logId %lu has been disalbed", log_id.val_);
    }
  }
  ld_info("%d sequencers have been disalbed", deactivate_size);

  if (!logs_.empty()) {
    setupTimer();
  } else {
    destroy();
    ld_info("All active sequencers specified by the sequencer_stop command "
            "have been disabled");
  }
}

void SequencerDeactivationRequest::setupTimer() {
  timer_.assign([this] { onTimeout(); });
  timer_.activate(timer_wait_);
}

void SequencerDeactivationRequest::executionBody() {
  setupTimer();
}

WorkerType SequencerDeactivationRequest::getWorkerTypeAffinity() {
  return WorkerType::BACKGROUND;
}

}} // namespace facebook::logdevice
