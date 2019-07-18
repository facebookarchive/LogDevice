/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"

#include "folly/container/Array.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBuffer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamSenderState.h"
#include "logdevice/common/stats/Histogram.h"

namespace facebook { namespace logdevice {

ClientReadStreamScd::ClientReadStreamScd(ClientReadStream* owner, Mode mode)
    : owner_(owner),
      mode_(mode),
      shards_down_failover_timer_(
          this,
          owner_->deps_->getSettings().scd_timeout,
          [this](small_shardset_t down) {
            return shardsDownFailoverTimerCallback(down);
          }),
      all_send_all_failover_timer_(
          this,
          owner_->deps_->getSettings().scd_all_send_all_timeout,
          [this](small_shardset_t down) {
            return allSendAllFailoverTimerCallback(down);
          }) {
  if (isActive()) {
    shards_down_failover_timer_.activate();
    all_send_all_failover_timer_.activate();
  }
}

ClientReadStreamScd::~ClientReadStreamScd(){};

void ClientReadStreamScd::configureOutlierDetector() {
  const auto& settings = owner_->deps_->getSettings();
  auto state = settings.reader_slow_shards_detection;

  if (MetaDataLog::isMetaDataLog(owner_->log_id_)) {
    // Not enabling outlier detector for metadata logs.
    return;
  }

  // Case 1: outlier detector was enabled and we need to disable it.
  // If there are outliers, make sure to schedule a rewind to clear them.
  if (outlier_detector_ &&
      state == Settings::ReaderSlowShardDetectionState::DISABLED) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Disabling outlier detector for log %lu",
                   owner_->log_id_.val_);
    rewindWithOutliers(ShardSet{}, "outlier detector disabled");
    outlier_detector_.reset();
    return;
  }

  // Case 2: Outlier detector was not enabled (in read only mode or not) but now
  // needs to be. Create and configure the outlier detector.
  // We check for `current_metadata_` because we can't enable it if we don't
  // have a storage set yet. `updateStorageShardsSet` will call this function
  // again.
  if (!outlier_detector_ &&
      state != Settings::ReaderSlowShardDetectionState::DISABLED &&
      owner_->current_metadata_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Enabling outlier detector for log %lu",
                   owner_->log_id_.val_);
    outlier_detector_ = std::make_unique<ClientReadStreamFailureDetector>(
        owner_->current_metadata_->replication,
        settings.reader_slow_shards_detection_settings);
    auto cb = [this](ShardSet outliers, std::string reason) {
      onOutliersChanged(std::move(outliers), std::move(reason));
    };
    outlier_detector_->setCallback(cb);
    updateFailureDetectorWorkingSet();
    outlier_detector_->start();
    outlier_detector_->onWindowSlid(
        owner_->server_window_.high, owner_->filter_version_);
    return;
  }

  // Case 3: We already have an outlier detectior, make sure it picks up the
  // latest settings and that if we switched between observe-only and enabled
  // mode, we rebuild the list of outliers.
  if (outlier_detector_) {
    outlier_detector_->setSettings(
        settings.reader_slow_shards_detection_settings);
    rewindWithOutliers(
        outlier_detector_->getCurrentOutliers(),
        folly::format("outlier detector switched to {} mode",
                      state == Settings::ReaderSlowShardDetectionState::ENABLED
                          ? "enabled"
                          : "observe-only")
            .str());
  }
}

void ClientReadStreamScd::updateFailureDetectorWorkingSet() {
  if (!outlier_detector_) {
    return;
  }

  // Set of shards that we should measure latencies for (shards that are not
  // empty).
  StorageSet tracking_set;
  // Set of shards that we may mark as outlier (shards that are not down and not
  // empty).
  StorageSet candidate_set;

  size_t n_down_non_empty = 0;
  for (auto& it : owner_->storage_set_states_) {
    if (it.second.getAuthoritativeStatus() !=
        AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
      tracking_set.push_back(it.first);
      if (it.second.blacklist_state !=
          ClientReadStreamSenderState::BlacklistState::DOWN) {
        candidate_set.push_back(it.first);
      } else {
        ++n_down_non_empty;
      }
    }
  }

  // Maximum amount of outliers allowed. We substract the replication - 1 with
  // the number of shards that are down and non empty. This helps guarantee that
  // at no point in time the set of shards that are both considered down and in
  // the outlier list do not form a valid copyset.
  size_t replication =
      owner_->current_metadata_->replication.getReplicationFactor();
  size_t max_outliers = 0;
  if (n_down_non_empty <= replication - 1) {
    max_outliers = replication - 1 - n_down_non_empty;
  }

  ld_check(outlier_detector_);
  outlier_detector_->changeWorkingSet(
      std::move(tracking_set), std::move(candidate_set), max_outliers);
}

void ClientReadStreamScd::onSettingsUpdated() {
  // Me may need to activate/deactivate the outlier detector.
  configureOutlierDetector();
}

void ClientReadStreamScd::onShardNextLsnChanged(ShardID shard, lsn_t next) {
  if (outlier_detector_) {
    outlier_detector_->onShardNextLsnChanged(shard, next);
  }
}

void ClientReadStreamScd::setShardAuthoritativeStatus(
    ShardID /* unused */,
    AuthoritativeStatus /* unused */) {
  updateFailureDetectorWorkingSet();
}

bool ClientReadStreamScd::isSlowShardsDetectionEnabled() {
  return owner_->deps_->getSettings().reader_slow_shards_detection ==
      Settings::ReaderSlowShardDetectionState::ENABLED;
}

bool ClientReadStreamScd::shardsDownFailoverTimerCallback(
    small_shardset_t down) {
  ld_check(isActive());

  if (isSlowShardsDetectionEnabled()) {
    // If slow shards detection is enabled, the shards that are stuck and
    // haven't sent a record for a long time will be marked as slow instead of
    // down (see onOutliersChanged()).
    // TODO(T21282553): remove this function once the outlier detector has
    // proven stable.
    return false;
  }

  // Rewind the stream if the shards down list changed. If the shards down list
  // contains all the shards in the read set, do nothing as the
  // all_send_all_failover_timer_ timer will take care of failing over to all
  // send all mode.
  if (!down.empty() &&
      getShardsDown().size() + down.size() < owner_->readSetSize()) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "Could not make progress for log %lu during %lums with "
        "shard down = {%s}, shards slow = {%s}; scheduling rewind with {%s} "
        "added in the shards down list. %s",
        owner_->log_id_.val_,
        shards_down_failover_timer_.period_.count(),
        toString(getShardsDown()).c_str(),
        toString(getShardsSlow()).c_str(),
        toString(down).c_str(),
        owner_->getDebugInfoStr().c_str());
    for (ShardID shard : down) {
      filtered_out_.deferredAddShardDown(shard);
    }
    owner_->scheduleRewind(folly::format("shards down list changed: {} added",
                                         toString(down).c_str())
                               .str());
  }

  return true;
}

bool ClientReadStreamScd::allSendAllFailoverTimerCallback(
    small_shardset_t down) {
  ld_check(isActive());

  // TODO(T21282553): consider removing this function once the outlier detector
  // has proven stable. We could also keep it as a last resort if there is a bug
  // preventing the reader to make progress while in SCD.

  // Failover to all send all mode if:
  // - all the shards are in the shards down list (nobody sent anything) OR
  // - the shards down list did not change since the last tick

  if (down.empty() ||
      getShardsDown().size() + down.size() == owner_->readSetSize()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Failing over to ALL_SEND_ALL mode for log %lu because "
                   "either all storage shards are in the shards down list or "
                   "we could not make progress for %lums with the current "
                   "filtered out list. Shards down list = {%s}, "
                   "shards slow list = {%s}. %s",
                   owner_->log_id_.val_,
                   all_send_all_failover_timer_.period_.count(),
                   toString(getShardsDown()).c_str(),
                   toString(getShardsSlow()).c_str(),
                   owner_->getDebugInfoStr().c_str());
    scheduleRewindToMode(
        Mode::ALL_SEND_ALL, "Failing over to ALL_SEND_ALL: timeout");
    return false;
  }
  return true;
}

bool ClientReadStreamScd::FilteredOut::applyDeferredChanges() {
  small_shardset_t temp_shards_down;
  small_shardset_t temp_shards_slow;
  small_shardset_t temp_all_shards;

  for (const auto& shard : new_shards_down_) {
    temp_shards_down.push_back(shard);
    temp_all_shards.push_back(shard);
  }

  for (const auto& shard : new_shards_slow_) {
    // It is possible both ClientReadStream and ClientReadStreamFailureDetector
    // decide to mark the same shard down and slow at the same time. Make sure
    // marking a shard down takes precedence.
    if (std::find(temp_shards_down.begin(), temp_shards_down.end(), shard) ==
        temp_shards_down.end()) {
      temp_shards_slow.push_back(shard);
      temp_all_shards.push_back(shard);
    }
  }

  auto equal = [](small_shardset_t l, small_shardset_t r) {
    std::sort(l.begin(), l.end());
    std::sort(r.begin(), r.end());
    return l == r;
  };

  if (equal(temp_shards_slow, shards_slow_) &&
      equal(temp_shards_down, shards_down_)) {
    ld_assert(equal(temp_all_shards, all_shards_));
    return false;
  }

  shards_down_ = std::move(temp_shards_down);
  shards_slow_ = std::move(temp_shards_slow);
  all_shards_ = std::move(temp_all_shards);
  return true;
}

bool ClientReadStreamScd::FilteredOut::deferredAddShardDown(ShardID shard) {
  if (!new_shards_down_.insert(shard).second) {
    return false;
  }

  new_shards_slow_.erase(shard);
  return true;
}

bool ClientReadStreamScd::FilteredOut::deferredRemoveShardDown(ShardID shard) {
  return (new_shards_down_.erase(shard) > 0);
}

bool ClientReadStreamScd::FilteredOut::deferredChangeShardsSlow(
    ShardSet outliers) {
  if (new_shards_slow_ == outliers) {
    return false;
  }
  new_shards_slow_ = std::move(outliers);
  return true;
}

void ClientReadStreamScd::FilteredOut::clear() {
  shards_down_.clear();
  shards_slow_.clear();
  all_shards_.clear();
  new_shards_down_.clear();
  new_shards_slow_.clear();
}

bool ClientReadStreamScd::FilteredOut::eraseShard(ShardID shard) {
  new_shards_down_.erase(shard);
  new_shards_slow_.erase(shard);
  auto remove = [](small_shardset_t& shard_set, const ShardID& shard_id) {
    auto removed = std::remove(shard_set.begin(), shard_set.end(), shard_id);
    if (removed != shard_set.end()) {
      shard_set.erase(removed, shard_set.end());
      return true;
    }
    return false;
  };

  remove(shards_down_, shard);
  remove(shards_slow_, shard);

  return remove(all_shards_, shard);
}

void ClientReadStreamScd::applyScheduledChanges() {
  if (!scheduledTransitionTo(Mode::ALL_SEND_ALL) &&
      filtered_out_.applyDeferredChanges()) {
    updateFailureDetectorWorkingSet();
    if (!owner_->deps_->getSettings()
             .read_stream_guaranteed_delivery_efficiency &&
        getShardsDown().size() == owner_->readSetSize()) {
      ld_check(getShardsSlow().empty());
      // Adding the shards to the shards down list would cause the shards down
      // list to have all shards in it. Failover to all send all mode instead.
      if (mode_ == Mode::ALL_SEND_ALL) {
        // We are already here, nothing to do.
        scheduled_mode_transition_.clear();
        filtered_out_.clear();
        return;
      }

      scheduleRewindToMode(Mode::ALL_SEND_ALL,
                           "Failing over to ALL_SEND_ALL: all shards in shards "
                           "down list");
    } else {
      // Cancel and re-activate the timer so that we have a full period before
      // shardDownFailoverTimerCallback() is called.
      shards_down_failover_timer_.cancel();
      shards_down_failover_timer_.activate();
    }
  }

  if (scheduledTransitionTo(Mode::ALL_SEND_ALL)) {
    // Switch to all send all.
    mode_ = Mode::ALL_SEND_ALL;
    filtered_out_.clear();
  } else if (scheduledTransitionTo(Mode::SCD)) {
    // Switch to SCD.
    mode_ = Mode::SCD;
    owner_->grace_period_->reset();
    shards_down_failover_timer_.activate();
    all_send_all_failover_timer_.activate();
  } else if (scheduledTransitionTo(Mode::LOCAL_SCD)) {
    // Switch to local SCD.
    mode_ = Mode::LOCAL_SCD;
    owner_->grace_period_->reset();
    shards_down_failover_timer_.activate();
    all_send_all_failover_timer_.activate();
  }
  scheduled_mode_transition_.clear();
}

void ClientReadStreamScd::scheduleRewindToMode(Mode mode, std::string reason) {
  ld_check(!owner_->done());

  if (mode == Mode::ALL_SEND_ALL) {
    ld_check(isActive());
    shards_down_failover_timer_.cancel();
    all_send_all_failover_timer_.cancel();
  } else {
    ld_check(mode == Mode::SCD || mode == Mode::LOCAL_SCD);
  }

  scheduled_mode_transition_ = mode;
  owner_->scheduleRewind(std::move(reason));
}

bool ClientReadStreamScd::checkNeedsFailoverToAllSendAll() {
  ld_check(isActive());
  if (owner_->done()) {
    return false;
  }

  if (scheduledTransitionTo(Mode::ALL_SEND_ALL)) {
    return false;
  }

  if (owner_->deps_->getSettings().read_stream_guaranteed_delivery_efficiency) {
    // When read_stream_guaranteed_delivery_efficiency setting is in use, don't
    // failover to ALL_SEND_ALL mode here.
    return false;
  }

  if (owner_->canDeliverRecordsNow()) {
    // There are records we can deliver right now, we'll only do failover when
    // we reach a state where we are stuck and can't deliver anymore.
    return false;
  }

  if (owner_->canSkipPartiallyTrimmedSection()) {
    // deliverFastForwardGap() should have been called to eagerly issue a trim
    // gap, we are here because delivering that gap to the user failed.
    return false;
  }

  // This is the number of shards that are not able to send us the next record.
  // owner_->numShardsInState(..) may account for shards that are in
  // the filtered_out_ list if these shards are back up. Therefore, we subtract
  // gap_shards_filtered_out_ which is the number of shards accounted both in
  // the filtered_out_ list and numShardsInState(*).
  // Note that it is expected that all shards in EMPTY or REBUILDING state are
  // in filtered_out_.
  using GapState = ClientReadStreamSenderState::GapState;
  const size_t gap_shards_total = owner_->numShardsInState(GapState::GAP) +
      owner_->numShardsInState(GapState::UNDER_REPLICATED) +
      getFilteredOut().size() - gap_shards_filtered_out_;

  ld_check(gap_shards_total <= owner_->readSetSize());
  if (gap_shards_total >= owner_->readSetSize() &&
      under_replicated_shards_not_blacklisted_ == 0) {
    // All shards reported that they don't have the next record or are in the
    // filtered out list. Failover to all send all mode.
    // Note that some shards might have been filtered out because they were
    // slow, so we could first rewind in SCD mode with the slow shards list
    // cleared and only afterwards failover to ALL_SEND_ALL if we are still
    // not making progress. However doing this requires more bookkeeping, and
    // this is a possible optimization to consider in the future..
    std::string reason =
        folly::format(
            "All storage shards are either in the filtered out list or "
            "sent a gap/record with lsn > next_lsn_to_deliver_. "
            "Shards down list = {}. Shards slow list = {}. {}",
            toString(getShardsDown()).c_str(),
            toString(getShardsSlow()).c_str(),
            owner_->getDebugInfoStr().c_str())
            .str();
    scheduleRewindToMode(Mode::ALL_SEND_ALL, reason);
    return true;
  }

  return false;
}

void ClientReadStreamScd::scheduleRewindIfShardBackUp(
    ClientReadStreamSenderState& state) {
  ld_check(isActive());
  // if the node has delivered a record and caught up to next_lsn_to_deliver_
  // during the last window, and believes its records are intact, allow it to
  // participate in SCD.
  if (state.max_data_record_lsn != 0 && state.getNextLsn() != 0 &&
      !state.should_blacklist_as_under_replicated) {
    if (filtered_out_.deferredRemoveShardDown(state.getShardID())) {
      ld_debug(
          "%s started delivering records or exited an under replicated region",
          state.getShardID().toString().c_str());
      owner_->scheduleRewind(
          folly::format("{} no longer down", state.getShardID().toString())
              .str());
    }
  }
}

void ClientReadStreamScd::onWindowSlid(lsn_t hi,
                                       filter_version_t filter_version) {
  if (outlier_detector_) {
    outlier_detector_->onWindowSlid(hi, filter_version);
  }
}

void ClientReadStreamScd::onOutliersChanged(ShardSet outliers,
                                            std::string reason) {
  if (owner_->deps_->getSettings().reader_slow_shards_detection ==
      Settings::ReaderSlowShardDetectionState::OBSERVE_ONLY) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "Outlier Detector for log %lu is marking shards %s as "
        "outliers (reason: %s), but it is enabled in observe-only mode.",
        owner_->log_id_.val_,
        toString(outliers).c_str(),
        reason.c_str());
    return;
  }
  rewindWithOutliers(std::move(outliers), std::move(reason));
}

void ClientReadStreamScd::rewindWithOutliers(ShardSet outliers,
                                             std::string reason) {
  if (mode_ == Mode::ALL_SEND_ALL) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "Outlier Detector for log %lu is marking shards %s as "
        "outliers (reason: %s). Not rewinding as we are in ALL_SEND_ALL mode.",
        owner_->log_id_.val_,
        toString(outliers).c_str(),
        reason.c_str());
    return;
  }
  ld_check(isActive());

  if (filtered_out_.deferredChangeShardsSlow(outliers)) {
    owner_->scheduleRewind(
        folly::format("Changing the outliers list from {} to {}: {}",
                      toString(getShardsSlow()).c_str(),
                      toString(outliers).c_str(),
                      reason.c_str())
            .str());
  }
}

bool ClientReadStreamScd::updateStorageShardsSet(
    const StorageSet& storage_set,
    const ReplicationProperty& replication) {
  configureOutlierDetector();

  bool did_remove_shards = false;
  // Erase any shards that have been removed from the filtered out list. We need
  // to remove from the current filtered out list, but also from the
  // new/deferred lists.

  // We make a separate copy of the filtered out set so that we can delete
  // elements from the filtered out set while iterating through the copy.
  auto filtered_out_copy = getFilteredOut();
  for (const auto& shard : filtered_out_copy) {
    if (owner_->storage_set_states_.count(shard) == 0) {
      // Remove from filtered_out_ these shards that are no longer in the config
      // Note: we are not calling filtered_out_.deferredRemoveShard..() here
      // because these are shards that were removed from the read set, we need
      // to make filtered_out_ consistent with that new read set immediately.
      filtered_out_.eraseShard(shard);
      did_remove_shards = true;
    }
  }
  auto new_shards_slow_sopy = filtered_out_.getNewShardsSlow();
  for (const auto& shard : new_shards_slow_sopy) {
    if (owner_->storage_set_states_.count(shard) == 0) {
      filtered_out_.eraseShard(shard);
    }
  }
  auto new_shards_down_copy = filtered_out_.getNewShardsDown();
  for (const auto& shard : new_shards_down_copy) {
    if (owner_->storage_set_states_.count(shard) == 0) {
      filtered_out_.eraseShard(shard);
    }
  }

  updateFailureDetectorWorkingSet();

  return maybeRewindAfterShardsUpdated(did_remove_shards);
}

bool ClientReadStreamScd::maybeRewindAfterShardsUpdated(bool shards_changed) {
  if (mode_ == Mode::ALL_SEND_ALL) {
    ld_check(getFilteredOut().empty());
    return false;
  }

  if (scheduledTransitionTo(Mode::ALL_SEND_ALL)) {
    // There is a rewind scheduled already to switch to all send all mode.
    return false;
  }

  // ClientReadStream updated the nodes in GAP and UNDER_REPLICATED gap states.
  // If the conditions needed to switch to all send all mode are now held,
  // switch now.
  if (checkNeedsFailoverToAllSendAll()) {
    return true;
  }

  // If filtered_out_ changed, schedule a rewind.
  if (shards_changed) {
    owner_->scheduleRewind("Shards removed from config");
    return true;
  }

  return false;
}

bool ClientReadStreamScd::addToShardsDownAndScheduleRewind(
    const ShardID& shard_id,
    std::string reason) {
  ld_check(isActive());
  if (std::find(filtered_out_.getShardsDown().begin(),
                filtered_out_.getShardsDown().end(),
                shard_id) != filtered_out_.getShardsDown().end()) {
    // The shard is already in the shards down list.

    return false;
  }

  if (!filtered_out_.deferredAddShardDown(shard_id)) {
    // The shard is already scheduled to be added.
    return true;
  }

  owner_->scheduleRewind(reason);

  return true;
}

void ClientReadStreamScd::onSenderGapStateChanged(
    ClientReadStreamSenderState& state,
    ClientReadStreamSenderState::GapState prev_gap_state) {
  using GapState = ClientReadStreamSenderState::GapState;
  if (state.blacklist_state ==
      ClientReadStreamSenderState::BlacklistState::NONE) {
    bool was_ur = prev_gap_state == GapState::UNDER_REPLICATED;
    bool is_ur = state.getGapState() == GapState::UNDER_REPLICATED;
    if (is_ur && !was_ur) {
      ++under_replicated_shards_not_blacklisted_;
    } else if (!is_ur && was_ur) {
      ld_check(under_replicated_shards_not_blacklisted_ > 0);
      --under_replicated_shards_not_blacklisted_;
    }
  } else {
    ld_check(!getFilteredOut().empty());
    bool was_gap = prev_gap_state != GapState::NONE;
    bool is_gap = state.getGapState() != GapState::NONE;
    if (is_gap && !was_gap) {
      ld_check(gap_shards_filtered_out_ < getFilteredOut().size());
      ++gap_shards_filtered_out_;
    } else if (!is_gap && was_gap) {
      ld_check(gap_shards_filtered_out_ > 0);
      --gap_shards_filtered_out_;
    }
  }
}

ClientReadStreamScd::FailoverTimer::FailoverTimer(
    ClientReadStreamScd* scd,
    std::chrono::milliseconds period,
    Callback cb)
    : period_(period),
      next_lsn_to_deliver_at_last_tick_(scd->owner_->next_lsn_to_deliver_),
      cb_(cb),
      scd_(scd) {
  if (period_.count() > 0) {
    timer_ = scd_->owner_->deps_->createTimer([this] { callback(); });
  }
}

void ClientReadStreamScd::FailoverTimer::cancel() {
  if (timer_) {
    timer_->cancel();
  }
}

void ClientReadStreamScd::FailoverTimer::activate() {
  if (timer_) {
    timer_->activate(period_);
  }
}

void ClientReadStreamScd::FailoverTimer::callback() {
  ld_check(!scd_->owner_->done());
  if (scd_->mode_ == Mode::ALL_SEND_ALL) {
    return;
  }

  const bool making_progress =
      scd_->owner_->next_lsn_to_deliver_ != next_lsn_to_deliver_at_last_tick_;
  next_lsn_to_deliver_at_last_tick_ = scd_->owner_->next_lsn_to_deliver_;

  if (making_progress) {
    // We are making progress. Nothing to do here. Schedule the next timer tick.
    activate();
    return;
  }

  if (scd_->owner_->canDeliverRecordsNow()) {
    // We are not making progress (next_lsn_to_deliver_ is stuck) but we do have
    // a record to deliver, which means we are stuck because the client is too
    // slow processing records. Let's not do SCD failover because of that.
    activate();
    return;
  }

  // Build the list of shards not blacklisted that have not sent anything
  // with lsn >= next_lsn_to_deliver_.
  small_shardset_t new_known_down;

  for (auto& it : scd_->owner_->storage_set_states_) {
    if (it.second.blacklist_state ==
            ClientReadStreamSenderState::BlacklistState::NONE &&
        it.second.getNextLsn() <= scd_->owner_->next_lsn_to_deliver_) {
      new_known_down.push_back(it.second.getShardID());
    }
  }

  ld_check(cb_);
  if (cb_(std::move(new_known_down))) {
    activate();
  }
}

}} // namespace facebook::logdevice
