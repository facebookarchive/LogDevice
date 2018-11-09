/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>
#include <unordered_map>

#include <folly/Optional.h>
#include <folly/stats/BucketedTimeSeries-defs.h>
#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/settings/ClientReadStreamFailureDetectorSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"

/**
 * @file ClientReadStreamFailureDetector is a utility that detects slow storage
 * shards that should be put in the "greylist" when Single Copy Delivery is
 * active. Adding a shard to the greylist means instructing other shards to send
 * the data that it ought to send.
 *
 * When Single Copy Delivery is active, every record is to be sent by one and
 * only one shard in the storage set only. This means that the slowest shard in
 * the storage set will drive the slowness of the reader.
 *
 * This utility monitors how fast storage shards are sending and runs an outlier
 * detection algorithm to decide which shard should be put in the greylist at
 * any point in time.
 *
 * == How we measure how fast a shard is ==
 *
 * The metric used to measure how fast storage shards are sending consists in
 * measuring how quickly they complete each new window as the window is slid by
 * ClientReadStream.
 *
 * ClientReadStream calls two methods of this class: 1/ `onWindowSlid` to inform
 * that it slid the "window high" up to some LSN, 2/ `onShardNextLsnChanged` to
 * inform that a shard has sent a record/gap past some LSN.
 *
 * This utility keeps track of the timestamp of each time the window is slid
 * with the timestamp of each time a storage shard's next lsn moves past the
 * window. Because ClientReadStream does not wait until all storage shards have
 * completed the current window before sliding it (this is controlled by
 * --client-read-flow-control-threshold), we maintain a "rotating buffer" of two
 * "window slid" events materialized by cur_window_ and next_window_.
 *
 * == Outlier Detection Algorithm ==
 *
 * This utility keeps track of which shards have completed which window. Once
 * the number of shards remaining to complete the last window is below the
 * `maxOutliersAllowed()` threshold, we run an outlier detection algorithm
 * to determine if these shards should be greylisted to increase the speed
 * of window movement and hence record delivery.
 *
 * We leverage the utility in logdevice/commor/OutlierDetecton.h which can tell
 * us if these shards should be considered outliers or if we should wait more.
 * If we should wait more, we schedule a timer to try again after some time
 * suggested by the OutlierDetection utility. The detection may be tried again
 * before the timer triggers if another shard completes the window.
 *
 * == Adaptive sensitivity ==
 *
 * The OutlierDetection utility is given a `required_margin` parameter that
 * defines how sensitive the outlier detection is. For instance if the
 * value is 2, we will only declare outliers if they are 200% slower than the
 * average of the other shards.
 *
 * This parameter is adaptive. The more rewinds we perform due to changing
 * the list of outliers, the less sensitive the detection will become. The
 * more time spent successfully reading (higher rate of window slide events),
 * the more sensitive the detection becomes.
 *
 * The adaptive nature of the required margin helps protect ourselves against
 * degenerate cases where many shards alternate being slow.
 *
 * == Reinstating outliers ==
 *
 * When a shard is in the outlier list, we keep sending WINDOW messages to it
 * and the shard will try to send RECORDs/GAPs anyway. We cannot reinstate a
 * shard that is in the outlier list by simply waiting for it to complete the
 * window and measuring the latency. Indeed, because the shard is an outlier and
 * in the SCD known down list, ClientReadStream won't wait for it to chime in
 * before sliding the window.
 *
 * We instead use a strategy of proactively reinstating the shard after some
 * time. The time before an outlier is reinstated is also adaptive. It grows
 * exponentially as we keep detecting it as an outlier, and decreases linearly
 * when we are successfully reading (sliding the window) while the shard is not
 * in the list.
 *
 * == A cap on the allowed number of outliers ==
 *
 * A cap on the maximum amount of shards that can be considered outliers is
 * defined by the user through a call to changeWorkingSet.
 * ClientReadStreamScd uses it to ensure that there would never be R+ shards
 * (that are not AUTHORITATIVE_EMPTY) in the known down list, otherwise there
 * would not be any benefit of adding them as we would eventually encounter
 * a record fully replicated on these shards.
 */

namespace facebook { namespace logdevice {

class Timer;

class ClientReadStreamFailureDetector {
 public:
  using TS = SteadyTimestamp;
  using Callback = std::function<void(ShardSet outliers, std::string reason)>;
  ClientReadStreamFailureDetectorSettings settings_;

  /**
   * Create a ClientReadStreamFailureDetector.
   * The user is expected to call changeWorkingSet() to configure the set of
   * shards to track and which shards are allowed to be outliers.
   *
   * @param replication Replication factor. Used to cap the maximum allowed
   *                    number of outliers.
   */
  ClientReadStreamFailureDetector(
      ReplicationProperty replication,
      ClientReadStreamFailureDetectorSettings settings);

  virtual ~ClientReadStreamFailureDetector();

  /**
   * Start the state machine.
   */
  void start();

  /**
   * Change the settings.
   *
   * @param settings New settings to use.
   */
  void setSettings(ClientReadStreamFailureDetectorSettings settings);

  /**
   * @param tracking_set     Set of shards for which to track latency.
   * @param allowed_outliers Set of shards that are allowed to be treated
   *                         outliers at the moment. This function expects this
   *                         to be a subset of `tracking_set`;
   * @param max_outliers     Maximum amount of shards that can be treated
   *                         outliers.
   */
  void changeWorkingSet(StorageSet tracking_set,
                        StorageSet allowed_outliers,
                        size_t max_outliers);

  /**
   * Set a callback to be called by this utility each time it requires a change
   * of the outlier list.
   */
  void setCallback(Callback cb) {
    cb_ = std::move(cb);
  }

  /**
   * Called by ClientReadStream when it slides the window.
   *
   * @param lo Low end of the window
   * @param hi High end of the window
   * @param v  Filter version for the window.
   */
  void onWindowSlid(lsn_t hi, filter_version_t v);
  void onWindowSlid(lsn_t hi, filter_version_t v, TS now);

  /**
   * Called by ClientReadStream each time a storage shard's next LSN changed.
   * Checks if the storage shard completed a window, if that's the case log a
   * latency sample.
   *
   * @param shard Shard that made progress.
   * @param next  New next lsn for that shard.
   */
  void onShardNextLsnChanged(ShardID shard, lsn_t next);
  void onShardNextLsnChanged(ShardID shard, lsn_t next, TS now);

  /**
   * @return current list of outliers.
   */
  const ShardSet& getCurrentOutliers() const;

  // The following is public for testing.

  using Sample = std::pair<ShardID, TS::rep>;
  using Samples = std::vector<Sample>;

  // Look at which shards have not completed the current window. If the number
  // of such shards is small enough, run the outlier detection algorithm (by
  // calling findShardsBlockingWindow()). If the algorithm does not detect
  // outliers but provides a time threshold after which these shards would be
  // marked outliers, schedule another try after that threshold.
  // Public so that tests can call it when simulating the `timer_` firing.
  void checkForShardsBlockingWindow(TS now);

  // Go through each outlier and remove it from the list if it has been in there
  // for an amount of time equal to `outlier_duration`.
  // Public so that tests can call it when simulating the `expiry_timer_`
  // firing.
  void removeExpiredOutliers(TS now);

 protected:
  /* Overridden by tests */
  virtual void activateTimer(std::chrono::milliseconds timeout);
  virtual void cancelTimer();
  virtual void activateExpiryTimer(std::chrono::milliseconds timeout);

 private:
  // Used to check if a shard that has not yet completed the window should be
  // marked an outlier.
  std::unique_ptr<Timer> timer_;

  // Used to schedule reinstating shards that are outliers.
  std::unique_ptr<Timer> expiry_timer_;

  Callback cb_;
  ReplicationProperty replication_;

  // number of shards that are candidate for being treated outlier
  size_t n_candidates_ = {0};

  // maximum number of shards that can be outliers.
  size_t max_outliers_allowed_ = {0};

  // An adaptive variable that defines how long an outlier shard should remain
  // as such before we try reinstating them.
  using OutlierDuration =
      ChronoExponentialBackoffAdaptiveVariable<std::chrono::seconds>;

  struct ShardState {
    explicit ShardState(OutlierDuration d) : outlier_duration(std::move(d)) {}
    using TimeSeries = folly::BucketedTimeSeries<size_t, TS::clock>;
    // Average latency of completing windows for the shard over a time period.
    std::unique_ptr<TimeSeries> moving_avg;
    // Next lsn that this storage shard will send.
    lsn_t next_lsn = LSN_OLDEST;
    // If the shard is an outlier, timestamp of when it should be reinstated.
    TS outlier_expiry_ts = TS::max();
    // Value that was last used to determine this shard was an outlier.
    size_t last_outlier_val = 0;
    // When a shard is declared an outlier, how long until it is reinstated.
    OutlierDuration outlier_duration;
    // set to true if this shard is allowed to be an outlier
    bool is_candidate_ = false;
  };

  using ShardMap = std::unordered_map<ShardID, ShardState, ShardID::Hash>;
  ShardMap shards_;

  // A rotating buffer of two "window sliding" events.
  struct WindowState {
    lsn_t hi;
    // Time the window was slid.
    TS time_slid;
    // How many outlier candidates have completed this window.
    // Tracking this number helps bypassing the outlier detection algorithm when
    // we know too many candidates have not responded.
    size_t n_candidates_complete;
  };
  folly::Optional<WindowState> cur_window_;
  folly::Optional<WindowState> next_window_;

  // Current filter version. If ClientReadStream rotates the window using a new
  // filter version, we clear the rotating buffer as this means it rewound.
  filter_version_t filter_version_{0};

  ShardSet current_outliers_;

  // Timestamp of the last time we changed the outlier list.
  TS last_change_ts_{TS::zero()};

  ExponentialBackoffAdaptiveVariable required_margin_;

  // A list of human readable strings that explain how the list of outliers was
  // mutated. Useful for logging.
  using OutlierChangedReason = std::vector<std::string>;

  // Create an ExponentialBackoffAdaptiveVariable for the required-margin from
  // the settings.
  ExponentialBackoffAdaptiveVariable initRequiredMargin() const;

  // Create a ChronoExponentialBackoffAdaptiveVariable for the outlier duration
  // from the settings.
  ChronoExponentialBackoffAdaptiveVariable<std::chrono::seconds>
  initOutlierDuration() const;

  // Called when a shard completes the window. Add the latency sample in the
  // moving average. Calls checkForShardsBlockingWindow() to try and
  // find outlier shards that are blocking the window.
  void onShardCompletedWindow(ShardID shard, WindowState& window, TS now);

  // Generate a set of latency samples where:
  // * for shards that completed the window we account for their average latency
  //   over the aggregated time period;
  // * for shards that are already outliers we account for the latency that was
  //   used to mark them outliers;
  // * for shards that did not complete the window we log a latency sample equal
  //   to how long ago the window was slid.
  Samples generateSamples(WindowState& window, TS now);

  // Compute the outliers that are blocking progress because they are not
  // completing the current window.
  // Returns true if some outliers were added, and samples gets populated with
  // the new outlier list.
  // Populates `threshold` with the latency threshold that was used for the
  // detection (set to -1 if the function returned false because it does not
  // have enough data to do the detection).
  // When this function returns true, it populates `reason` with a human
  // readable reason for detecting outliers.
  bool findShardsBlockingWindow(TS now,
                                size_t num_deviations,
                                size_t max_outliers,
                                Samples& samples,
                                std::chrono::milliseconds& threshold,
                                OutlierChangedReason& reason);

  // Find the outlier that is due to expire first and schedule the
  // `expiry_timer_` to eventually call `removeExpiredOutliers`.
  void scheduleExpiryTimer(TS now);

  // Change the list of outliers and call the user provided callback to notify
  // of the change.
  void changeOutliers(TS now, Samples new_outlier_samples, std::string reason);

  // Return the last window that was slid.
  WindowState& getLastWindow();
  const WindowState& getLastWindow() const;

  // Execute a function on each window.
  template <typename F>
  void windowForEach(F fn) {
    if (cur_window_.hasValue()) {
      fn(cur_window_.value());
    }
    if (next_window_.hasValue()) {
      fn(next_window_.value());
    }
  }

  // Mark a shard as down or not. Adjusts all counters.
  void markShardDown(ShardState& state, bool down);

  // Recompute `WindowState::n_candidates_complete`.
  size_t recomputeCounterForWindow(const WindowState& w) const;

  // Called at the end of each method to verify that `WindowState::n_completed`,
  // `n_down_shards_non_empty_` and `n_down_shards_` are consistent.
  void checkConsistency();
};

}} // namespace facebook::logdevice
