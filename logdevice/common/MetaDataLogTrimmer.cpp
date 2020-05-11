/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaDataLogTrimmer.h"

#include <folly/Random.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/request_util.h"

using std::chrono::duration_cast;

namespace facebook { namespace logdevice {

// Sets the range (as fraction of original value) for trim interval. Each time
// the new trimming  round is scheduled the delay is picked randomly from
// [interval * (1 - jitter), interval * (1 + jitter)].
constexpr double kTrimIntervalJitter = 0.25;

MetaDataLogTrimmer::MetaDataLogTrimmer(Sequencer* sequencer)
    : sequencer_(sequencer),
      processor_(sequencer->getProcessor()),
      log_id_(sequencer->getLogID()),
      worker_id_(Worker::onThisThread()->idx_),
      worker_type_(Worker::onThisThread()->worker_type_) {
  ld_check(sequencer);
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  state_ = std::make_shared<MetaDataLogTrimmer::State>();
  state_->timer = std::make_unique<Timer>();
}

// Called from any thread
void MetaDataLogTrimmer::setRunInterval(
    std::chrono::milliseconds run_interval) {
  std::function<void()> func = [this, s = state_, run_interval]() {
    updateRunInterval(s, run_interval);
  };
  auto request = FuncRequest::make(
      worker_id_, worker_type_, RequestType::MISC, std::move(func));
  auto rv = processor_->postRequest(request);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    1,
                    "Posting update function failed with error(%s)",
                    error_name(err));
  }
}

// Called from Worker thread, but scheduled from any thread
void MetaDataLogTrimmer::updateRunInterval(
    std::shared_ptr<MetaDataLogTrimmer::State> state,
    std::chrono::milliseconds run_interval) {
  if (state->stopped.load()) {
    return;
  }
  if (state->run_interval != run_interval) {
    state->run_interval = run_interval;
    if (run_interval.count() == 0) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     1,
                     "Stopping periodic trimming log for %lu",
                     log_id_.val_);
      state->timer->cancel();
    } else {
      schedulePeriodicTrimming();
    }
  }
}

// Called from any thread
void MetaDataLogTrimmer::shutdown() {
  // blocks until the passed lambda executes
  // Check if it is stopped already
  if (state_->stopped.exchange(true)) {
    return;
  }
  run_on_worker(processor_, worker_id_.val_, worker_type_, [s = state_]() {
    s->timer.release();
    return 0;
  });
}

folly::Optional<lsn_t> MetaDataLogTrimmer::getTrimPoint() {
  return state_->metadata_trim_point.loadOptional();
}

std::chrono::milliseconds MetaDataLogTrimmer::State::pickRandomizedDelay() {
  if (run_interval.count() == 0) {
    return run_interval;
  }
  static_assert(kTrimIntervalJitter > 0 && kTrimIntervalJitter < 1);
  auto next_delay_us =
      duration_cast<std::chrono::microseconds>(run_interval).count();
  std::uniform_int_distribution<int64_t> dist_us(
      next_delay_us * (1 - kTrimIntervalJitter),
      next_delay_us * (1 + kTrimIntervalJitter));
  folly::ThreadLocalPRNG rng;
  return duration_cast<std::chrono::milliseconds>(
      std::chrono::microseconds(dist_us(rng)));
}

// Called from Worker thread
void MetaDataLogTrimmer::schedulePeriodicTrimming() {
  ld_check(!state_->stopped.load());
  RATELIMIT_INFO(std::chrono::seconds(10),
                 1,
                 "Will run periodic trimming every ~%lu s for %lu",
                 state_->run_interval.count(),
                 log_id_.val_);
  state_->timer->setCallback([this, s = state_]() {
    // Called from worker but may run after destruction
    if (s->stopped.load()) {
      // Callback called after shutdown, skipping it
      return;
    }
    // Re-activate timer for next round
    s->timer->activate(s->pickRandomizedDelay());
    trim();
  });
  state_->timer->activate(state_->pickRandomizedDelay());
}

// Called from Worker thread
folly::Optional<lsn_t> MetaDataLogTrimmer::findMetadataTrimLSN() {
  auto metadata_extras = sequencer_->getMetaDataExtrasMap();
  if (metadata_extras == nullptr) {
    ld_debug("Failed to load metadata log extras, skipping trimming metadata "
             "log for %lu",
             log_id_.val_);
    return folly::none;
  }
  auto data_trim_point = sequencer_->getTrimPoint();
  if (data_trim_point == LSN_INVALID) {
    ld_debug("Data trim point is not loaded, skipping trimming metadata "
             "log for %lu",
             log_id_.val_);
    return folly::none;
  }

  auto trim_point_epoch = lsn_to_epoch(data_trim_point);
  // We will iterate over all available epochs and look for 2 largest epoch
  // numbers below trim point. The largest of these maximums we need to keep
  // (candidate) and the smaller one (confirmed) we will trim.
  // Why second maximum? Consider example: we have one record in metadata log
  // with effective range [1, MAX] and 2 more epoch started without records. So
  // if the current data trim point is from epoch 3 then with naive approach we
  // will trim epoch 1 since it start epoch is strictly less than 3.
  folly::Optional<lsn_t> candidate_lsn, confirmed_lsn;
  for (const auto& [epoch, extras] : *metadata_extras) {
    // Skipping all epoch above the trim point
    if (epoch > trim_point_epoch) {
      continue;
    }
    // If this epoch is bigger then candidate (the max so far) then advance
    // candidate to new epoch and second best to previous max. Otherwise new
    // epoch might be worse then max found so far but still worse than the
    // second best. If this is the case we advance second best to new epoch.
    if (!candidate_lsn.has_value() || *candidate_lsn < extras.lsn) {
      confirmed_lsn = candidate_lsn;
      candidate_lsn = extras.lsn;
    } else if (!confirmed_lsn.has_value() || *confirmed_lsn < extras.lsn) {
      confirmed_lsn = extras.lsn;
    }
  }
  return confirmed_lsn;
}

// Called from Worker thread
void MetaDataLogTrimmer::trim() {
  auto metadata_trim_lsn = findMetadataTrimLSN();
  if (!metadata_trim_lsn.has_value()) {
    // We either could not find a trim point or there is nothing to trim
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    1,
                    "Did not find anything to trim, skipping...");
    return;
  }
  auto last_trim_point = getTrimPoint();
  if (last_trim_point.has_value() &&
      last_trim_point.value() >= metadata_trim_lsn.value()) {
    RATELIMIT_DEBUG(
        std::chrono::seconds(10),
        1,
        "Metadata log is trimmed up to maximum possible LSN, skipping...");
    return;
  }
  ld_info("Trimming MetaDataLog to %s for data log %lu",
          lsn_to_string(metadata_trim_lsn.value()).c_str(),
          log_id_.val_);
  sendTrimRequest(metadata_trim_lsn.value());
}

// Called from Worker thread
void MetaDataLogTrimmer::sendTrimRequest(lsn_t metadata_lsn) {
  ld_check(metadata_lsn != LSN_INVALID);

  STAT_INCR(processor_->stats_, metadata_log_trims);

  auto request = std::make_unique<TrimRequest>(
      nullptr,
      MetaDataLog::metaDataLogID(log_id_),
      metadata_lsn,
      sequencer_->settings().metadata_log_trim_timeout,
      [log_id = log_id_, s = state_, metadata_lsn](Status status) {
        onTrimComplete(s, log_id, status, metadata_lsn);
      });

  request->bypassWriteTokenCheck();
  // This is a hack to work around SyncSequencerRequest returning underestimated
  // next_lsn for metadata logs.
  request->bypassTailLSNCheck();

  std::unique_ptr<Request> req(std::move(request));
  int rv = processor_->postRequest(req);
  if (rv != 0 && err != E::SHUTDOWN) {
    ld_error("Got unexpected err %s for posting trim request for metadata log "
             "for %lu",
             error_name(err),
             log_id_.val_);
  }
}

/* static */
void MetaDataLogTrimmer::onTrimComplete(
    std::shared_ptr<MetaDataLogTrimmer::State> state,
    logid_t log_id,
    Status status,
    lsn_t trim_point) {
  if (status == E::OK) {
    state->metadata_trim_point.store(trim_point);
    ld_info("Successfully trimmed MetaDataLog for data log %lu up to %s",
            log_id.val_,
            lsn_to_string(trim_point).c_str());
  } else {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    1,
                    "Error on trimming metadata log for data log %lu: %s ",
                    log_id.val_,
                    error_description(status));
  }
}

}} // namespace facebook::logdevice
