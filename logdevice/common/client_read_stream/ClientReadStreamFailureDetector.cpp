/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/client_read_stream/ClientReadStreamFailureDetector.h"

#include <algorithm>

#include <folly/Format.h>
#include <folly/Random.h>

#include "logdevice/common/OutlierDetection.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

static constexpr int kNumBuckets = 10;
static constexpr float kFuzzRatio = 0.1;
static constexpr int kNumDeviationsFromCenter = 5;

#define SCOPE_EXIT_CHECK_CONSISTENCY() \
  SCOPE_EXIT {                         \
    checkConsistency();                \
  }

ClientReadStreamFailureDetector::ClientReadStreamFailureDetector(
    ReplicationProperty replication,
    ClientReadStreamFailureDetectorSettings settings)
    : settings_(settings),
      replication_(replication),
      required_margin_(initRequiredMargin()) {}

ClientReadStreamFailureDetector::~ClientReadStreamFailureDetector() {}

ExponentialBackoffAdaptiveVariable
ClientReadStreamFailureDetector::initRequiredMargin() const {
  return ExponentialBackoffAdaptiveVariable(
      /*min=*/settings_.required_margin,
      /*initial=*/settings_.required_margin,
      /*max=*/std::numeric_limits<double>::max(),
      /*multiplier=*/2,
      /*decrease_rate=*/settings_.required_margin_decrease_rate,
      /*fuzz_factor=*/kFuzzRatio);
}

ClientReadStreamFailureDetector::OutlierDuration
ClientReadStreamFailureDetector::initOutlierDuration() const {
  return OutlierDuration(settings_.outlier_duration.initial_delay,
                         settings_.outlier_duration.initial_delay,
                         settings_.outlier_duration.max_delay,
                         settings_.outlier_duration.multiplier,
                         settings_.outlier_duration_decrease_rate,
                         kFuzzRatio);
}

void ClientReadStreamFailureDetector::start() {
  // If we are not on a worker (unit tests), expect the test to call
  // `removeExpiredOutliers()` and `checkForShardsBlockingWindow()` manually.
  Worker* worker = Worker::onThisThread(false);
  if (worker) {
    expiry_timer_ =
        std::make_unique<Timer>([this] { removeExpiredOutliers(TS::now()); });
    timer_ = std::make_unique<Timer>(
        [this] { checkForShardsBlockingWindow(TS::now()); });
  }
}

void ClientReadStreamFailureDetector::setSettings(
    ClientReadStreamFailureDetectorSettings settings) {
  auto prev = std::move(settings_);
  settings_ = std::move(settings);

  // Re-create the TimeSeries for each shard if the moving average duration has
  // changed.
  if (prev.moving_avg_duration != settings_.moving_avg_duration) {
    for (auto& p : shards_) {
      auto prev_moving_avg = std::move(p.second.moving_avg);
      auto now = TS::now();
      prev_moving_avg->update(now);
      p.second.moving_avg = std::make_unique<ShardState::TimeSeries>(
          kNumBuckets, settings_.moving_avg_duration);
      p.second.moving_avg->addValueAggregated(
          TS::now(), prev_moving_avg->sum(), prev_moving_avg->count());
    }
  }

  // If we changed settings related to the outlier duration, make sure to adjust
  // the `outlier_duration` for all shards.
  if (prev.outlier_duration != settings_.outlier_duration ||
      prev.outlier_duration_decrease_rate !=
          settings_.outlier_duration_decrease_rate) {
    for (auto& p : shards_) {
      const auto prev = p.second.outlier_duration.getCurrentValue();
      p.second.outlier_duration = initOutlierDuration();
      p.second.outlier_duration.setCurrentValue(prev);
    }
  }

  const auto prev_required_margin = required_margin_.getCurrentValue();
  required_margin_ = initRequiredMargin();
  required_margin_.setCurrentValue(prev_required_margin);
}

void ClientReadStreamFailureDetector::changeWorkingSet(
    StorageSet tracking_set,
    StorageSet allowed_outliers,
    size_t max_outliers) {
  SCOPE_EXIT_CHECK_CONSISTENCY();

  decltype(shards_) tmp;
  std::swap(shards_, tmp);

  // Populate the num `shards_` map.
  for (const ShardID& shard : tracking_set) {
    auto it_tmp = tmp.find(shard);
    if (it_tmp != tmp.end()) {
      it_tmp->second.is_candidate_ = false;
      shards_.emplace(shard, std::move(it_tmp->second));
    } else {
      ShardState state(initOutlierDuration());
      state.moving_avg = std::make_unique<ShardState::TimeSeries>(
          kNumBuckets, settings_.moving_avg_duration);
      shards_.emplace(shard, std::move(state));
    }
  }

  for (const ShardID& s : allowed_outliers) {
    auto it = shards_.find(s);
    ld_check(it != shards_.end());
    it->second.is_candidate_ = true;
  }
  n_candidates_ = allowed_outliers.size();
  max_outliers_allowed_ = max_outliers;

  // Recompute per-window counters.
  windowForEach([&](WindowState& window) {
    window.n_candidates_complete = recomputeCounterForWindow(window);
  });

  // Make sure we clean-up current_outliers_.
  for (auto it = current_outliers_.begin(); it != current_outliers_.end();) {
    auto it_o = shards_.find(*it);
    if (it_o == shards_.end() || !it_o->second.is_candidate_) {
      it = current_outliers_.erase(it);
    } else {
      ++it;
    }
  }
}

void ClientReadStreamFailureDetector::onWindowSlid(
    lsn_t hi,
    filter_version_t filter_version) {
  onWindowSlid(hi, filter_version, TS::now());
}

void ClientReadStreamFailureDetector::onWindowSlid(
    lsn_t hi,
    filter_version_t filter_version,
    TS now) {
  SCOPE_EXIT_CHECK_CONSISTENCY();

  // Clear everything if the filter version changed (we rewound).
  if (filter_version != filter_version_) {
    filter_version_ = filter_version;
    cur_window_.clear();
    next_window_.clear();
    for (auto& p : shards_) {
      p.second.next_lsn = LSN_OLDEST;
    }
  }

  WindowState wstate{hi, now, /*n_candidates_complete=*/0};
  // Count how many shards already sent a gap past this window.
  wstate.n_candidates_complete = recomputeCounterForWindow(wstate);

  // Rotate the buffer if needed.
  if (cur_window_.hasValue()) {
    if (next_window_.hasValue()) {
      cur_window_ = std::move(next_window_.value());
      next_window_.clear();
    }
    next_window_ = std::move(wstate);
  } else {
    ld_check(!next_window_.hasValue());
    cur_window_ = std::move(wstate);
  }

  // We are making progress, make sure the required_margin_ decreases.
  required_margin_.positiveFeedback(now);
}

void ClientReadStreamFailureDetector::onShardNextLsnChanged(ShardID shard,
                                                            lsn_t next) {
  onShardNextLsnChanged(shard, next, TS::now());
}

void ClientReadStreamFailureDetector::onShardNextLsnChanged(ShardID shard,
                                                            lsn_t next,
                                                            TS now) {
  SCOPE_EXIT_CHECK_CONSISTENCY();

  auto it_m = shards_.find(shard);
  if (it_m == shards_.end()) {
    // This shard is not in the tracking set.
    return;
  }

  // be robust against ClientReadStream decreasing next_lsn.
  if (next <= it_m->second.next_lsn) {
    return;
  }

  auto prev_next = it_m->second.next_lsn;
  it_m->second.next_lsn = next;

  // Check if the shard completed the current or next window so we can add
  // latency samples.
  windowForEach([&](WindowState& window) {
    if (prev_next <= window.hi && next > window.hi) {
      onShardCompletedWindow(shard, window, now);
    }
  });
}

void ClientReadStreamFailureDetector::onShardCompletedWindow(
    ShardID shard,
    WindowState& window,
    TS now) {
  auto it_m = shards_.find(shard);
  if (it_m == shards_.end()) {
    // This shard is not in the tracking set.
    return;
  }

  if (it_m->second.is_candidate_) {
    ++window.n_candidates_complete;
  }

  // Compute how long it took and add a latency sample.
  auto delta = TS(now - window.time_slid).toMilliseconds();
  it_m->second.moving_avg->addValue(now, delta.count());

  // If this shard is not an outlier, decrease its `outlier_duration`
  // value as this is positive signal that the shard is able to send without
  // being declared an outlier. The more windows it completes, the faster we
  // will try to remove it from the outliers list the next time it is added.
  if (!current_outliers_.count(shard)) {
    it_m->second.outlier_duration.positiveFeedback(now);
  }

  // If this shard just completed the last window, re-run the outlier detection
  // algorithm to see if that's enough information to find outliers.
  if (window.hi == getLastWindow().hi) {
    checkForShardsBlockingWindow(now);
  }
}

void ClientReadStreamFailureDetector::checkForShardsBlockingWindow(TS now) {
  cancelTimer();

  // Use the last window to detect outliers.
  WindowState& window = getLastWindow();

  const size_t n_pending = n_candidates_ - window.n_candidates_complete;

  // Do nothing if all shards completed the window (in that case
  // ClientReadStream will slide it), or if too many shards have not completed
  // the window such that we know they can't be marked outliers.
  if (n_pending == 0 || n_pending > max_outliers_allowed_) {
    return;
  }

  // Run the detection.
  OutlierChangedReason reason;
  Samples result;
  std::chrono::milliseconds threshold = std::chrono::milliseconds::max();
  const bool found_outliers = findShardsBlockingWindow(
      now, kNumDeviationsFromCenter, n_pending, result, threshold, reason);
  ld_check(threshold.count() >= 0);

  if (found_outliers) {
    ld_check(!result.empty());
    changeOutliers(now, std::move(result), folly::join(". ", reason));
    // make sure we are less agressive for the next window.
    required_margin_.negativeFeedback();
  } else if (threshold != std::chrono::milliseconds::max()) {
    auto window_duration = TS(now - window.time_slid).toMilliseconds();
    // `delta` measures how long we should wait until we are able to consider
    // all shards that have not yet completed the window as outliers.
    auto delta = threshold - window_duration;
    if (delta.count() <= 0) {
      // We can be here if the shards that have not completed the window yet are
      // already outliers. There is no point in calling this function again
      // until the window is slid.
      return;
    }
    // schedule a timer to call this function again.
    activateTimer(delta + std::chrono::milliseconds{10});
  }
}

ClientReadStreamFailureDetector::Samples
ClientReadStreamFailureDetector::generateSamples(WindowState& window, TS now) {
  const std::chrono::milliseconds delta =
      TS(now - window.time_slid).toMilliseconds();
  Samples samples;
  samples.reserve(shards_.size());
  for (const auto& p : shards_) {
    if (current_outliers_.count(p.first)) {
      samples.emplace_back(p.first, p.second.last_outlier_val);
    } else if (!p.second.is_candidate_) {
      // This shard is not a candidate, do not include it in the samples.
      continue;
    } else if (p.second.next_lsn <= window.hi) {
      samples.emplace_back(p.first, delta.count());
    } else {
      p.second.moving_avg->update(now);
      if (p.second.moving_avg->count() > 0) {
        samples.emplace_back(p.first, p.second.moving_avg->avg());
      }
    }
  }
  return samples;
}

bool ClientReadStreamFailureDetector::findShardsBlockingWindow(
    TS now,
    size_t num_deviations,
    size_t max_outliers,
    ClientReadStreamFailureDetector::Samples& samples_out,
    std::chrono::milliseconds& threshold,
    OutlierChangedReason& reason) {
  if (!cur_window_.hasValue()) {
    return false;
  }

  // Use the last window to detect outliers.
  WindowState& window = getLastWindow();

  Samples samples = generateSamples(window, now);

  // We want this function to only try to find outliers within the shards that
  // have not completed the window or are already outliers.
  std::function<bool(const Sample& s)> outlier_filter =
      [this, &window](const Sample& s) -> bool {
    auto it = shards_.find(s.first);
    ld_check(it != shards_.end());
    return it->second.next_lsn <= window.hi ||
        current_outliers_.count(s.first) != 0;
  };

  auto res = OutlierDetection::findOutliers(OutlierDetection::Method::RMSD,
                                            std::move(samples),
                                            num_deviations,
                                            max_outliers,
                                            required_margin_.getCurrentValue(),
                                            outlier_filter);
  if (res.threshold == std::numeric_limits<float>::max()) {
    threshold = std::chrono::milliseconds::max();
  } else {
    threshold =
        std::chrono::milliseconds(static_cast<long>(std::ceil(res.threshold)));
  }

  // Find out which shards amongst the outliers were just added.
  Samples added(res.outliers);
  added.erase(std::remove_if(added.begin(),
                             added.end(),
                             [this](const Sample& s) {
                               return current_outliers_.count(s.first);
                             }),
              added.end());
  if (added.empty()) {
    return false;
  }

  std::chrono::milliseconds delta = TS(now - window.time_slid).toMilliseconds();
  reason.push_back(
      folly::format(
          "samples {} did not complete the window up to {} after {}ms, "
          "which is above threshold {}ms while the rest of the samples have "
          "a mean latency of {}",
          toString(added),
          lsn_to_string(window.hi),
          delta.count(),
          threshold.count(),
          res.center)
          .str());
  samples_out = std::move(res.outliers);
  return true;
}

void ClientReadStreamFailureDetector::scheduleExpiryTimer(TS now) {
  // Find the earliest time point when an outlier will expire.
  TS next_expiry = TS::max();
  for (ShardID s : current_outliers_) {
    auto it_shard = shards_.find(s);
    ld_check(it_shard != shards_.end());
    next_expiry = std::min(next_expiry, it_shard->second.outlier_expiry_ts);
  }

  if (next_expiry == TS::max()) {
    // There are no outliers to expire.
    return;
  }

  // Schedule the call to `removeExpiredOutliers`.
  std::chrono::milliseconds delta = TS(next_expiry - now).toMilliseconds();
  delta = std::max(std::chrono::milliseconds{0}, delta) +
      std::chrono::milliseconds{50};
  activateExpiryTimer(delta);
}

void ClientReadStreamFailureDetector::removeExpiredOutliers(TS now) {
  OutlierChangedReason reason;
  Samples new_outliers;
  for (ShardID s : current_outliers_) {
    auto it_shard = shards_.find(s);
    ld_check(it_shard != shards_.end());
    if (now >= it_shard->second.outlier_expiry_ts) {
      reason.push_back(
          folly::format("removing {} from the list of outliers", s.toString())
              .str());
      it_shard->second.outlier_duration.positiveFeedback(now);
      // Because this shard was considered an outlier, the data samples we had
      // for it must be dropped as they should not be considered "normal" for
      // further runs of the outlier detection algorithm.
      it_shard->second.moving_avg->clear();
    } else {
      // This shard remains an outlier.
      new_outliers.push_back(Sample{s, it_shard->second.last_outlier_val});
    }
  }

  if (new_outliers.size() != current_outliers_.size()) {
    // Apply the change.
    changeOutliers(now, std::move(new_outliers), folly::join(". ", reason));
  }

  // Schedule the timer again for the remaining outliers (if any).
  scheduleExpiryTimer(now);
}

void ClientReadStreamFailureDetector::changeOutliers(
    TS now,
    Samples new_outlier_samples,
    std::string reason) {
  ShardSet new_outliers;
  for (const auto& o : new_outlier_samples) {
    new_outliers.insert(o.first);
  }

  if (new_outliers == current_outliers_) {
    return;
  }

  for (const Sample& s : new_outlier_samples) {
    ShardID shard = s.first;
    if (current_outliers_.count(shard)) {
      continue;
    }
    // We have a new outlier.
    auto it = shards_.find(shard);
    ld_check(it != shards_.end());
    // Compute the timestamp for when it should be reinstated.
    it->second.outlier_expiry_ts =
        now + it->second.outlier_duration.getCurrentValue();
    // Remember the value that was used to mark it an outlier.
    it->second.last_outlier_val = s.second;
    // Double the shard's potential to remain an outlier.
    it->second.outlier_duration.negativeFeedback();
  }

  current_outliers_ = std::move(new_outliers);
  last_change_ts_ = now;
  if (cb_) {
    cb_(current_outliers_, reason);
  }

  // Schedule a timer to expire these outliers.
  scheduleExpiryTimer(now);
}

const ShardSet& ClientReadStreamFailureDetector::getCurrentOutliers() const {
  return current_outliers_;
}

ClientReadStreamFailureDetector::WindowState&
ClientReadStreamFailureDetector::getLastWindow() {
  return const_cast<WindowState&>(
      const_cast<const ClientReadStreamFailureDetector*>(this)
          ->getLastWindow());
}

const ClientReadStreamFailureDetector::WindowState&
ClientReadStreamFailureDetector::getLastWindow() const {
  ld_check(cur_window_.hasValue());
  return next_window_.hasValue() ? next_window_.value() : cur_window_.value();
}

size_t ClientReadStreamFailureDetector::recomputeCounterForWindow(
    const WindowState& w) const {
  return std::count_if(
      shards_.begin(), shards_.end(), [&](const ShardMap::value_type& p) {
        return p.second.is_candidate_ && p.second.next_lsn > w.hi;
      });
}

void ClientReadStreamFailureDetector::checkConsistency() {
  if (!folly::kIsDebug) {
    return;
  }
  // Make sure the `n_candidates_complete` counter for each window is
  // consistent.
  windowForEach([&](WindowState& window) {
    ld_assert_eq(
        recomputeCounterForWindow(window), window.n_candidates_complete);
  });

  // Make sure `current_outliers_` is in sync with a changing storage set.
  for (ShardID s : current_outliers_) {
    ld_assert(shards_.count(s));
  }
}

void ClientReadStreamFailureDetector::activateTimer(
    std::chrono::milliseconds timeout) {
  ld_check(timer_);
  timer_->activate(timeout);
}

void ClientReadStreamFailureDetector::cancelTimer() {
  ld_check(timer_);
  timer_->cancel();
}

void ClientReadStreamFailureDetector::activateExpiryTimer(
    std::chrono::milliseconds timeout) {
  ld_check(expiry_timer_);
  expiry_timer_->activate(timeout);
}

}} // namespace facebook::logdevice
