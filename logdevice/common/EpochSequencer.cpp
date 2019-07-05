/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochSequencer.h"

#include "logdevice/common/AbortAppendersEpochRequest.h"
#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/Appender.h"
#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/CopySetSelectorFactory.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

EpochSequencer::EpochSequencer(
    logid_t log_id,
    epoch_t epoch,
    std::unique_ptr<EpochMetaData> metadata,
    const EpochSequencerImmutableOptions& immutable_options,
    Sequencer* parent)
    : parent_(parent),
      log_id_(log_id),
      epoch_(epoch),
      immutable_options_(immutable_options),
      metadata_(std::shared_ptr<EpochMetaData>(std::move(metadata))),
      state_(State::ACTIVE),
      window_(epoch,
              immutable_options_.window_size,
              immutable_options_.esn_max),
      lng_(compose_lsn(epoch, ESN_INVALID)),
      last_reaped_(compose_lsn(epoch, ESN_INVALID)) {}

bool EpochSequencerImmutableOptions::
operator==(const EpochSequencerImmutableOptions& rhs) {
  static_assert(sizeof(EpochSequencerImmutableOptions) == 12,
                "Don't forget to update operator==() when adding fields.");
  auto tup = [](const EpochSequencerImmutableOptions& o) {
    return std::make_tuple(
        o.extra_copies, o.synced_copies, o.window_size, o.esn_max);
  };
  return tup(*this) == tup(rhs);
}
bool EpochSequencerImmutableOptions::
operator!=(const EpochSequencerImmutableOptions& rhs) {
  return !(*this == rhs);
}
std::string EpochSequencerImmutableOptions::toString() const {
  return folly::sformat("extras: {}, synced: {}, window: {}, esn_max: {}",
                        extra_copies,
                        synced_copies,
                        window_size,
                        esn_max.val());
}

EpochSequencerImmutableOptions::EpochSequencerImmutableOptions(
    const logsconfig::LogAttributes& log_attrs,
    const Settings& settings) {
  extra_copies = log_attrs.extraCopies().value();
  synced_copies = log_attrs.syncedCopies().value();
  window_size = log_attrs.maxWritesInFlight().value();
  const size_t ESN_T_BITS = 8 * sizeof(esn_t::raw_type);
  ld_check(settings.esn_bits > 0);
  ld_check(settings.esn_bits <= ESN_T_BITS);
  esn_max = esn_t(~esn_t::raw_type(0) >> (ESN_T_BITS - settings.esn_bits));
}

RunAppenderStatus EpochSequencer::runAppender(Appender* appender) {
  if (!appender || appender->started()) {
    ld_check(false);
    err = E::INVALID_PARAM;
    return RunAppenderStatus::ERROR_DELETE;
  }

  if (state_.load() != State::ACTIVE) {
    // sequencer is no longer in ACTIVE state
    err = E::NOSEQUENCER;
    return RunAppenderStatus::ERROR_DELETE;
  }

  lsn_t lsn = window_.grow(appender);
  if (lsn == LSN_INVALID) {
    // 1. window full;
    // 2. no more LSN available to issue;
    // 3. sliding window is disabled. This is possible because the
    //    state transition and window growth is _not_ atomic. The window
    //    can be disabled while the state of EpochSequencer is still in
    //    ACTIVE
    ld_check(err == E::NOBUFS || err == E::TOOBIG || err == E::DISABLED);
    return RunAppenderStatus::ERROR_DELETE;
  }

  // There can be a race condition between SlidingWindow::grow() and
  // processNextBytes() which will cause appender with bigger esn register
  // its size before its predecessors. This error will not accumulate
  // over time and should not be a big problem since byte offset is best-effort
  // estimation.
  if (getSettings().byte_offsets) {
    processNextBytes(appender);
  }

  int rv = appender->start(shared_from_this(), lsn);
  if (rv != 0) {
    ld_check(err == E::SYSLIMIT); // INTERNAL asserts in debug mode
    return RunAppenderStatus::ERROR_DELETE;
  }

  return RunAppenderStatus::SUCCESS_KEEP;
}

void EpochSequencer::processNextBytes(Appender* appender) {
  ld_check(appender != nullptr);
  uint64_t in_payload_checksum_bytes = appender->getChecksumBytes();
  OffsetMap payload_size_map;
  // TODO(T33977412) Add record counter offset based on settings
  payload_size_map.setCounter(
      BYTE_OFFSET, appender->getPayload()->size() - in_payload_checksum_bytes);
  OffsetMap offsets = offsets_within_epoch_.fetchAdd(payload_size_map);
  appender->setLogOffset(std::move(offsets));
}

void EpochSequencer::transitionTo(State next) {
  // only allows transition to DRAINING and DYING state through
  // this function. In specific, supported transitions:
  // 1) ACTIVE -> DRAINING
  // 2) ACTIVE -> DYING
  // 3) DRAINING -> DYING
  ld_check(next == State::DRAINING || next == State::DYING);

  State current = state_.load();
  if (current >= next) {
    // already in a later state, nothing to do
    return;
  }

  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    current = state_.load();
    if (current >= next) {
      // check again with the lock held
      return;
    }

    if (current == State::ACTIVE) {
      // atomically disable the sliding window to reject all further appends
      lsn_t next_lsn = window_.disable();
      ld_check(next_lsn != LSN_INVALID);
      ld_check(lsn_to_esn(next_lsn) != ESN_INVALID);
      // this must be the first time we set draining_target_
      ld_check(draining_target_.load() == LSN_MAX);
      draining_target_.store(next_lsn - 1);
    } else {
      // EpochSequencer is performing transition of DRAINING -> DYING,
      // window must have been disabled and draining target must be set already
      ld_check(window_.is_disabled());
      ld_check(current == State::DRAINING);
      ld_check(next == State::DYING);
    }

    // finally complete the state transition _after_ window is disabled
    // and draining_target_ is set
    state_.store(next);
  }

  // Note: due to a race condition between setting draining_target_ and
  // checking it in noteAppenderReaped(), it is possible that
  // that the last Appender is reaped before draining_target_ is set. It is
  // necessary to check if the epoch is already quiescent after we set
  // draining_target_. Otherwise, we might miss the event of epoch becoming
  // quiescent.
  if (epochQuiescent()) {
    onEpochQuiescent();
  } else if (next == State::DYING) {
    // We need to destroy the epoch sequencer soon but the epoch is not
    // quiescent, which means that there are still Appenders
    // running on Workers. We need to abort them as soon as possible. Send
    // requests to all workers asking them to abort these Appenders.
    abortAppenders();
  }
}

void EpochSequencer::startDraining() {
  transitionTo(State::DRAINING);
}

void EpochSequencer::startDestruction() {
  transitionTo(State::DYING);
}

void EpochSequencer::onEpochQuiescent() {
  ld_check(window_.is_disabled());
  ld_check(last_reaped_.load() == draining_target_.load());

  State prev;
  {
    // we still need to hold the state_mutex_ to eliminate race conditions
    // caused by draining_target_ and state_ are not set atomically (e.g.,
    // state_ could still be ACTIVE when draining_target_ is already set)
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    prev = state_.exchange(State::QUIESCENT);
    ld_check(prev != State::ACTIVE);
  }

  if (prev == State::DRAINING) {
    if (epochDrained()) {
      // draining has completed, notify Sequencer about the result
      noteDrainingCompleted(E::OK);
    } else {
      // TODO 7467469: improve error reporting
      noteDrainingCompleted(E::FAILED);
    }
  }
}

bool EpochSequencer::noteAppenderReaped(Appender::FullyReplicated replicated,
                                        lsn_t reaped_lsn,
                                        std::shared_ptr<TailRecord> tail_record,
                                        epoch_t* last_released_epoch_out,
                                        bool* lng_changed_out) {
  ld_check(last_released_epoch_out != nullptr);
  ld_check(lng_changed_out != nullptr);
  ld_check(tail_record != nullptr);

  // Step 1: update LNG if necessary
  lsn_t reaped_lsn_minus_one;
  switch (replicated) {
    case Appender::FullyReplicated::YES:
      reaped_lsn_minus_one = reaped_lsn - 1;
      ld_check(lsn_to_epoch(reaped_lsn) == lsn_to_epoch(reaped_lsn_minus_one));

      *lng_changed_out =
          lng_.compare_exchange_strong(reaped_lsn_minus_one, reaped_lsn);
      break;
    case Appender::FullyReplicated::NO:
      // the appender was aborted, shouldn't advance LNG
      *lng_changed_out = false;
      break;
  }

  // Step 1.5: if lng changed, update tail record for the epoch
  if (*lng_changed_out) {
    // no need to do compare and swap here since this function is called
    // sequentially as Appenders are reaped
    tail_record_.store(std::move(tail_record));
  }

  // Step 2: update last_reaped_lsn_ and check for the status of
  // draining or destruction

  // an LSN can only be reaped once and should be reaped in lsn order by the
  // sliding window
  ld_assert(reaped_lsn > last_reaped_.load());
  last_reaped_.store(reaped_lsn);

  // also assert that reaped lsn should never exceed the draining target
  // (LSN_MAX if not in draining)
  ld_assert(reaped_lsn <= draining_target_.load());

  if (epochQuiescent()) {
    onEpochQuiescent();
  }

  // Step 3: update last released LSN with the parent Sequencer object
  if (*lng_changed_out) {
    return updateLastReleased(reaped_lsn, last_released_epoch_out);
  }

  // if LNG is not increased, then there is no chance that reaping this Appender
  // can increase last released lsn for the epoch
  err = E::ABORTED;
  return false;
}

bool EpochSequencer::epochQuiescent() const {
  ld_check(last_reaped_.load() <= draining_target_.load());
  return last_reaped_.load() == draining_target_.load();
}

bool EpochSequencer::epochDrained() const {
  ld_assert(lng_.load() <= draining_target_.load());
  return lng_.load() == draining_target_.load();
}

void EpochSequencer::abortAppenders() {
  Processor* processor = getProcessor();
  for (worker_id_t worker_idx{0};
       worker_idx.val_ < processor->getWorkerCount(WorkerType::GENERAL);
       ++worker_idx.val_) {
    std::unique_ptr<Request> rq = std::make_unique<AbortAppendersEpochRequest>(
        worker_idx, log_id_, epoch_);

    int rv = processor->postImportant(rq);
    if (rv != 0 && err != E::SHUTDOWN) {
      ld_critical("Got unexpected err %s for posting requests to abort "
                  "appenders with log %lu",
                  error_name(err),
                  log_id_.val_);
      ld_check(false);
    }
  }
}

void EpochSequencer::noteDrainingCompleted(Status status) {
  parent_->noteDrainingCompleted(epoch_, status);
}

void EpochSequencer::noteAppenderPreempted(epoch_t epoch, NodeID preempted_by) {
  parent_->notePreempted(epoch, preempted_by);
}

NodeID EpochSequencer::checkIfPreempted(epoch_t epoch) const {
  return parent_->checkIfPreempted(epoch);
}

bool EpochSequencer::epochMetaDataAvailable(epoch_t epoch) const {
  return parent_->epochMetaDataAvailable(epoch);
}

bool EpochSequencer::updateLastReleased(lsn_t reaped_lsn,
                                        epoch_t* last_released_epoch_out) {
  return parent_->noteAppenderReaped(reaped_lsn, last_released_epoch_out);
}

std::shared_ptr<const EpochMetaData> EpochSequencer::getMetaData() const {
  return metadata_.get();
}

bool EpochSequencer::setMetaDataWritten() {
  auto current_metadata = metadata_.get();
  if (current_metadata->writtenInMetaDataLog()) {
    // already marked as written
    return false;
  }

  auto new_metadata = std::make_shared<EpochMetaData>(*current_metadata);
  ld_check(new_metadata->h.epoch == current_metadata->h.epoch);
  new_metadata->h.flags |= MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
  ld_check(new_metadata->writtenInMetaDataLog());
  bool res = metadata_.compare_and_swap(current_metadata, new_metadata);
  // if we lost the race, then the metadata prevented us from the update must
  // already have the written flag set
  ld_check(res || current_metadata->writtenInMetaDataLog());
  return res;
}

bool EpochSequencer::setNodeSetParams(
    const EpochMetaData::NodeSetParams& params) {
  auto current_metadata = metadata_.get();
  ld_check(current_metadata->writtenInMetaDataLog());
  if (current_metadata->nodeset_params == params) {
    return false;
  }

  auto new_metadata = std::make_shared<EpochMetaData>(*current_metadata);
  ld_check(new_metadata->h.epoch == current_metadata->h.epoch);
  new_metadata->nodeset_params = params;
  bool res = metadata_.compare_and_swap(current_metadata, new_metadata);
  // Caller makes sure that these updates don't race against each other.
  ld_check(res);
  return res;
}

void EpochSequencer::createOrUpdateCopySetManager(
    const std::shared_ptr<Configuration>& cfg,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    const Settings& settings) {
  auto metadata = metadata_.get();
  ld_check(metadata);
  std::shared_ptr<NodeSetState> nodeset_state;
  auto copyset_manager = copyset_manager_.get();
  if (copyset_manager) {
    // Try using the existing NodeSetState so that the node availability status
    // are preserved. Note that it is safe to do so because we do not filter
    // nodes in NodeSetState and it always contain all nodes in the immutable
    // nodeset for the epoch.
    nodeset_state = copyset_manager->getNodeSetState();
  } else {
    nodeset_state = std::make_shared<NodeSetState>(
        metadata->shards, log_id_, NodeSetState::HealthCheck::ENABLED);
  }

  auto log_group = cfg->getLogGroupByIDShared(log_id_);

  copyset_manager_.update(CopySetSelectorFactory::createManager(
      log_id_,
      *metadata,
      std::move(nodeset_state),
      nodes_configuration,
      getProcessor()->getOptionalMyNodeID(),
      log_group ? &log_group->attrs() : nullptr,
      settings,
      settings.enable_sticky_copysets &&
          settings
              .write_sticky_copysets_deprecated, /* disable sticky copysets
                                                    if either one is false */
      settings.sticky_copysets_block_size,
      settings.sticky_copysets_block_max_time));
}

void EpochSequencer::noteConfigurationChanged(
    const std::shared_ptr<Configuration>& cfg,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    const Settings& settings) {
  ld_check(cfg != nullptr);
  auto copyset_manager = copyset_manager_.get();
  if (copyset_manager &&
      !copyset_manager->matchesConfig(*nodes_configuration)) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Updating copyset selector for log %lu epoch %u because of "
                   "config update",
                   log_id_.val_,
                   epoch_.val_);
    createOrUpdateCopySetManager(cfg, nodes_configuration, settings);
  }
}

bool EpochSequencer::checkNodeSet() const {
  std::shared_ptr<CopySetManager> copyset_manager_ptr = getCopySetManager();
  std::shared_ptr<const EpochMetaData> epoch_metadata = getMetaData();
  ld_check(copyset_manager_ptr != nullptr);
  ld_check(epoch_metadata != nullptr);

  auto nodeset_state = copyset_manager_ptr->getNodeSetState();
  ld_check(nodeset_state);
  const nodeset_ssize_t max_unavailable_nodes = nodeset_state->numShards() -
      std::min((size_t)epoch_metadata->replication.getReplicationFactor(),
               nodeset_state->numShards());

  // refresh the nodeset state if permitted
  nodeset_state->refreshStates();

  // Note: number of unavailable nodes in the NodeSet is updated by concurrently
  // running Appenders with the assumption that they will iterate over the node
  // set to update the number based on the current time. As a result, this may
  // not be an accurate number but rather an estimate.
  nodeset_ssize_t total_unavailable_nodes, max_reason_nodes;
  auto max_reason = NodeSetState::NotAvailableReason::NONE;

  auto check_node_availability = [&]() {
    total_unavailable_nodes = 0;
    max_reason_nodes = 0;

    for (uint8_t i = 0;
         i < static_cast<uint8_t>(NodeSetState::NotAvailableReason::Count);
         ++i) {
      NodeSetState::NotAvailableReason reason =
          static_cast<NodeSetState::NotAvailableReason>(i);
      if (nodeset_state->consideredAvailable(reason)) {
        continue;
      }

      nodeset_ssize_t num = nodeset_state->numNotAvailableShards(reason);
      total_unavailable_nodes += num;
      if (num > max_reason_nodes) {
        max_reason = reason;
        max_reason_nodes = num;
      }
    }
  };

  // First try, excludes gray listed nodes
  check_node_availability();

  // If we are about to report failure, let's try to re-pick
  // the slow nodes as well
  if (total_unavailable_nodes > max_unavailable_nodes) {
    // Clear graylists
    nodeset_state->resetGrayList(
        NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);
    auto worker = Worker::onThisThread(false);
    if (worker) {
      worker->resetGraylist();
    }

    // Second try, includes gray listed nodes
    check_node_availability();
  } else if (nodeset_state->shouldClearGrayList()) {
    nodeset_state->resetGrayList(
        NodeSetState::GrayListResetReason::THRESHOLD_REACHED);
  }

  // at any given time, sum of unavailable nodes could be
  // off by the number of workers.
  static const int C = static_cast<int>(getSettings().num_workers);

  ld_check(total_unavailable_nodes >= -C);

  nodeset_ssize_t max_possible = nodeset_state->numShards() + C;
  ld_check(total_unavailable_nodes <= max_possible);

  if (total_unavailable_nodes > max_unavailable_nodes) {
    // the Sequencer cannot complete the request at this time,
    // set err to the most occurred reason
    switch (max_reason) {
      case NodeSetState::NotAvailableReason::NONE:
      case NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC:
      case NodeSetState::NotAvailableReason::Count:
        // must have a valid unavailable reason
        RATELIMIT_CRITICAL(
            std::chrono::seconds(1),
            2,
            "total_unavailable_nodes:%d, max_unavailable_nodes:%d"
            ", max_reason:%s",
            total_unavailable_nodes,
            max_unavailable_nodes,
            NodeSetState::reasonString(max_reason));
        ld_check(false);
        err = E::INTERNAL;
        break;
      case NodeSetState::NotAvailableReason::OVERLOADED:
        err = E::OVERLOADED;
        break;
      case NodeSetState::NotAvailableReason::NO_SPC:
        err = E::NOSPC;
        break;
      case NodeSetState::NotAvailableReason::UNROUTABLE:
        err = E::UNROUTABLE;
        break;
      case NodeSetState::NotAvailableReason::STORE_DISABLED:
        err = E::DISABLED;
        break;
      case NodeSetState::NotAvailableReason::SLOW:
        err = E::INTERNAL;
        ld_check(false);
        break;
      case NodeSetState::NotAvailableReason::PROBING:
        err = E::DISABLED;
        break;
        // no default so that the compiler can check that we listed
        // all values of enum class NodeSetState::NotAvailableReason
    }

    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "total_unavailable_nodes=%d, max_unavailable_nodes:%d, "
                   "err:%s, reason:%s, max_reason_nodes:%u",
                   total_unavailable_nodes,
                   max_unavailable_nodes,
                   error_description(err),
                   NodeSetState::reasonString(max_reason),
                   max_reason_nodes);
    return false;
  }

  // return true if we still have at least r nodes available
  return true;
}

void EpochSequencer::schedulePeriodicReleases() {
  parent_->schedulePeriodicReleases();
}

const Settings& EpochSequencer::getSettings() const {
  return Worker::settings();
}

Processor* EpochSequencer::getProcessor() const {
  return parent_->getProcessor();
}

}} // namespace facebook::logdevice
