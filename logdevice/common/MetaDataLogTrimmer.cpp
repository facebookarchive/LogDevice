/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaDataLogTrimmer.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/TrimRequest.h"

namespace facebook { namespace logdevice {

MetaDataLogTrimmer::MetaDataLogTrimmer(Sequencer* sequencer)
    : sequencer_(sequencer),
      log_id_(sequencer->getLogID()),
      current_run_interval_(0) {
  ld_check(sequencer);
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
}

void MetaDataLogTrimmer::setRunInterval(
    std::chrono::milliseconds run_interval) {
  if (current_run_interval_ == run_interval) {
    // No changes, skipping update
    return;
  }
  current_run_interval_ = run_interval;
  if (current_run_interval_.count() == 0) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Stopping periodic trimming log for %lu",
                   log_id_.val_);
    trim_timer_.cancel();
  } else {
    schedulePeriodicTrimming();
  }
}

folly::Optional<lsn_t> MetaDataLogTrimmer::getTrimPoint() {
  return metadata_trim_point_.loadOptional();
}

void MetaDataLogTrimmer::schedulePeriodicTrimming() {
  ld_check(current_run_interval_.count() > 0);
  RATELIMIT_INFO(std::chrono::seconds(10),
                 1,
                 "Will run periodic trimming every %lu s for %lu",
                 current_run_interval_.count(),
                 log_id_.val_);
  trim_timer_.setCallback([this]() {
    // Re-activate timer for next round
    trim_timer_.activate(current_run_interval_);
    trim();
  });
  trim_timer_.activate(current_run_interval_);
}

folly::Optional<lsn_t> MetaDataLogTrimmer::findMetadataTrimLSN() {
  auto metadata_extras = sequencer_->getMetaDataExtrasMap();
  if (metadata_extras == nullptr) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "Failed to load metadata log extras, skipping trimming metadata "
        "log for %lu",
        log_id_.val_);
    return folly::none;
  }
  auto data_trim_point = sequencer_->getTrimPoint();
  if (data_trim_point == LSN_INVALID) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Data trim point is not loaded, skipping trimming metadata "
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
    if (!candidate_lsn.hasValue() || *candidate_lsn < extras.lsn) {
      confirmed_lsn = candidate_lsn;
      candidate_lsn = extras.lsn;
    } else if (!confirmed_lsn.hasValue() || *confirmed_lsn < extras.lsn) {
      confirmed_lsn = extras.lsn;
    }
  }
  return confirmed_lsn;
}

void MetaDataLogTrimmer::trim() {
  auto metadata_trim_lsn = findMetadataTrimLSN();
  if (!metadata_trim_lsn.hasValue()) {
    // We either could not find a trim point or there is nothing to trim
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    1,
                    "Did not find anything to trim, skipping...");
    return;
  }
  auto last_trim_point = getTrimPoint();
  if (last_trim_point.hasValue() &&
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

void MetaDataLogTrimmer::sendTrimRequest(lsn_t metadata_lsn) {
  ld_check(metadata_lsn != LSN_INVALID);

  STAT_INCR(sequencer_->getProcessor()->stats_, metadata_log_trims);

  auto request = std::make_unique<TrimRequest>(
      nullptr,
      MetaDataLog::metaDataLogID(log_id_),
      metadata_lsn,
      sequencer_->settings().metadata_log_trim_timeout,
      [this, metadata_lsn](Status st) { onTrimComplete(st, metadata_lsn); });

  request->bypassWriteTokenCheck();
  // This is a hack to work around SyncSequencerRequest returning underestimated
  // next_lsn for metadata logs.
  request->bypassTailLSNCheck();

  std::unique_ptr<Request> req(std::move(request));
  int rv = sequencer_->getProcessor()->postRequest(req);
  if (rv != 0 && err != E::SHUTDOWN) {
    ld_error("Got unexpected err %s for posting trim request for metadata log "
             "for %lu",
             error_name(err),
             log_id_.val_);
  }
}

void MetaDataLogTrimmer::onTrimComplete(Status status, lsn_t trim_point) {
  if (status == E::OK) {
    metadata_trim_point_.store(trim_point);
    ld_info("Successfully trimmed MetaDataLog for data log %lu up to %s",
            log_id_.val_,
            lsn_to_string(trim_point).c_str());
  } else {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    1,
                    "Error on trimming metadata log for data log %lu: %s ",
                    log_id_.val_,
                    error_description(status));
  }
}

}} // namespace facebook::logdevice
