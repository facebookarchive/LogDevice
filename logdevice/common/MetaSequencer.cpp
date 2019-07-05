/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaSequencer.h"

#include "logdevice/common/LogRecoveryRequest.h"

namespace facebook { namespace logdevice {

void MetaEpochSequencer::retireAppender(Status st,
                                        lsn_t lsn,
                                        Appender::Reaper& reaper) {
  EpochSequencer::retireAppender(st, lsn, reaper);
  auto parent = getParent();
  ld_assert(dynamic_cast<MetaSequencer*>(parent) != nullptr);
  static_cast<MetaSequencer*>(parent)->onAppenderRetired(st, lsn);
}

std::shared_ptr<EpochSequencer>
MetaSequencer::createEpochSequencer(epoch_t epoch,
                                    std::unique_ptr<EpochMetaData> metadata) {
  auto epoch_seq =
      std::make_shared<MetaEpochSequencer>(getLogID(),
                                           epoch,
                                           std::move(metadata),
                                           EpochSequencerImmutableOptions(),
                                           this);
  epoch_seq->createOrUpdateCopySetManager(
      getClusterConfig(), getNodesConfiguration(), settings());
  ld_check(epoch_seq->getCopySetManager() != nullptr);
  return epoch_seq;
}

void MetaSequencer::onRecoveryCompleted(
    Status status,
    epoch_t epoch,
    TailRecord previous_epoch_tail,
    std::unique_ptr<const RecoveredLSNs> recovered_lsns) {
  Sequencer::onRecoveryCompleted(
      status, epoch, std::move(previous_epoch_tail), std::move(recovered_lsns));
  controller_->onRecoveryCompleted(status, epoch);
}
}} // namespace facebook::logdevice
