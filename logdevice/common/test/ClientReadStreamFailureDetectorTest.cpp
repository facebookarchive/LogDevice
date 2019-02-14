/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/ClientReadStreamFailureDetector.h"

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/settings/ClientReadStreamFailureDetectorSettings.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)
#define N9 ShardID(9, 0)
#define N10 ShardID(10, 0)
#define N11 ShardID(11, 0)
#define N12 ShardID(12, 0)
#define N13 ShardID(13, 0)
#define N14 ShardID(14, 0)
#define N15 ShardID(15, 0)

namespace facebook { namespace logdevice {

using namespace std::chrono_literals;

static StorageSet storage_set =
    {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9, N10, N11, N12, N13, N14, N15};

static constexpr size_t replication = 3;

class MockClientReadStreamFailureDetector
    : public ClientReadStreamFailureDetector {
 public:
  MockClientReadStreamFailureDetector(
      ReplicationProperty replication,
      ClientReadStreamFailureDetectorSettings settings)
      : ClientReadStreamFailureDetector(std::move(replication),
                                        std::move(settings)) {}
  void activateTimer(std::chrono::milliseconds /* timeout */) override {
    timer_active_ = true;
  }
  void cancelTimer() override {
    timer_active_ = false;
  }
  void activateExpiryTimer(std::chrono::milliseconds /* timeout */) override {
    expiry_timer_active_ = true;
  }

  bool timerIsActive() const {
    return timer_active_;
  }

  bool expiryTimerIsActive() const {
    return expiry_timer_active_;
  }

  void onTimerExpired(TS::TimePoint time) {
    checkForShardsBlockingWindow(time);
    timer_active_ = false;
  }

  void onExpiryTimerExpired(TS::TimePoint time) {
    removeExpiredOutliers(time);
    expiry_timer_active_ = false;
  }

 private:
  bool timer_active_{false};
  bool expiry_timer_active_{false};
};

class ClientReadStreamFailureDetectorTest : public ::testing::Test {
 public:
  using TS = ClientReadStreamFailureDetector::TS;
  using TimePoint = TS::TimePoint;

  void SetUp() override {
    ClientReadStreamFailureDetectorSettings settings;
    settings.moving_avg_duration = std::chrono::seconds{10};
    settings.required_margin = 1.0;
    settings.required_margin_decrease_rate = 0.25;
    settings.outlier_duration =
        chrono_expbackoff_t<std::chrono::seconds>(1min, 1min, 2.0);
    settings.outlier_duration_decrease_rate = 1.0;

    detector = std::make_unique<MockClientReadStreamFailureDetector>(
        ReplicationProperty(replication, NodeLocationScope::NODE), settings);

    detector->changeWorkingSet(storage_set, storage_set, /*max_outliers=*/2);

    detector->setCallback([](ShardSet outliers, std::string reason) {
      ld_info("Outliers: %s, reason: %s",
              toString(outliers).c_str(),
              reason.c_str());
    });

    time = TS::now();
  }

  void advanceTime(std::chrono::milliseconds delta) {
    time += delta;
  }

  void onWindowSlid(lsn_t lsn, filter_version_t version = filter_version_t{1}) {
    detector->onWindowSlid(lsn, version, time);
  }

  void setKnownDown(small_shardset_t shards) {
    known_down_ = std::unordered_set<ShardID, ShardID::Hash>(
        shards.begin(), shards.end());

    StorageSet candidates;
    for (ShardID s : storage_set) {
      if (!known_down_.count(s)) {
        candidates.push_back(s);
      }
    }

    size_t max_outliers = replication - 1 - shards.size();
    detector->changeWorkingSet(storage_set, candidates, max_outliers);
  }

  void onShardNextLsnChanged(ShardID shard, lsn_t lsn) {
    detector->onShardNextLsnChanged(shard, lsn, time);
  }

  template <typename... Args>
  void onShardsNextLsnChanged(lsn_t lsn, Args... shards) {
    for (ShardID s : {shards...}) {
      onShardNextLsnChanged(s, lsn);
    }
  }

  bool timerIsActive() const {
    return detector->timerIsActive();
  }
  bool expiryTimerIsActive() const {
    return detector->expiryTimerIsActive();
  }

  void triggerTimer() {
    ASSERT_TRUE(timerIsActive());
    detector->onTimerExpired(time);
  }

  void triggerExpiryTimer() {
    ASSERT_TRUE(expiryTimerIsActive());
    detector->onExpiryTimerExpired(time);
  }

  ClientReadStreamFailureDetectorSettings settings;
  std::unique_ptr<MockClientReadStreamFailureDetector> detector;
  TimePoint time;
  std::unordered_set<ShardID, ShardID::Hash> known_down_;
};

// We manage to make progress through a couple windows but then N0 is slow and
// has to be added an outlier. We manage to make progress through a couple more
// windows with N0 in the known down list but then N0 is established back.
TEST_F(ClientReadStreamFailureDetectorTest, Scenario1) {
  /**
   * Simulate shards completed successfully a few windows with an average
   * latency of 60ms.
   */

  // Slide the window up to LSN 100
  onWindowSlid(lsn_t(100));

  // All shards complete 75% of the window in 20ms +/- 10%
  advanceTime(18ms);
  onShardsNextLsnChanged(lsn_t(78), N0, N1, N2, N3, N4);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(78), N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(78), N9, N10, N11, N12);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(78), N13, N14);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(78), N15);

  // Timer should not be active, there are no outlier candidates yet.
  ASSERT_FALSE(timerIsActive());

  // Advance 10 more ms...
  advanceTime(10ms);

  // slide the window up to lsn 170
  onWindowSlid(lsn_t(170));

  // All shards complete 100% of the window at LSN 100 after another 20ms +/-
  // 10% - that's ~40ms total to complete window up to LSN 100.
  advanceTime(18ms);
  onShardsNextLsnChanged(lsn_t(100), N0, N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(100), N3, N4, N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(100), N9, N10, N11);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(100), N12, N13);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(100), N14, N15);

  // Timer should not be active, there are no outlier candidates yet.
  ASSERT_FALSE(timerIsActive());

  // All shards make some more progress after 10ms +/- 10%
  advanceTime(8ms);
  onShardsNextLsnChanged(lsn_t(150), N0, N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(150), N3, N4, N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(150), N9, N10, N11);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(150), N12, N13);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(150), N14, N15);

  advanceTime(15ms);

  // Timer should not be active, there are no outlier candidates yet.
  ASSERT_FALSE(timerIsActive());

  // slide the window up to lsn 250
  onWindowSlid(lsn_t(250));

  // All shards complete the window at LSN 175 after another 10ms +/- 10%.
  advanceTime(8ms);
  onShardsNextLsnChanged(lsn_t(200), N0, N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(200), N3, N4, N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(200), N9, N10, N11);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(200), N12, N13);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(200), N14, N15);

  // Timer should not be active, there are no outlier candidates yet.
  ASSERT_FALSE(timerIsActive());

  advanceTime(15ms);

  /**
   * N0 does not complete the window. Check that after 75ms, we consider it an
   * outlier.
   */

  // All shards but N0 take another 35ms to complete the window at LSN 250.
  advanceTime(33ms);
  onShardsNextLsnChanged(lsn_t(252), N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(253), N3, N4, N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(290), N9, N10, N11);
  advanceTime(2ms);
  onShardsNextLsnChanged(lsn_t(400), N12, N13);
  ASSERT_FALSE(timerIsActive()); // Too many shards did not complete the window.
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(251), N14);
  ASSERT_TRUE(timerIsActive()); // N15, N0 are candidates
  onShardsNextLsnChanged(lsn_t(251), N15);

  // N0 still has not completed the window. Because the required margin is 1.0,
  // we must wait 60ms + 100% of 60ms = 120ms at least before N0 will be
  // declared an outlier.
  advanceTime(70ms);
  triggerTimer();
  auto expected = ShardSet{N0};
  ASSERT_EQ(expected, detector->getCurrentOutliers());

  // We rewind (filter version = 2, known down = N0).
  onWindowSlid(lsn_t(250), filter_version_t{2});
  setKnownDown(small_shardset_t{N0});

  // All shards take 35ms to make some progress.
  advanceTime(33ms);
  onShardsNextLsnChanged(lsn_t(230), N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(230), N3, N4, N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(230), N9, N10, N11);
  advanceTime(2ms);
  onShardsNextLsnChanged(lsn_t(230), N12, N13);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(230), N14);
  onShardsNextLsnChanged(lsn_t(230), N15);
  ASSERT_FALSE(timerIsActive()); // None of the shards completed the window.

  // Window is slid to 330
  onWindowSlid(lsn_t(330), filter_version_t{2});

  // All shards take another 25ms to make some progress and complete window up
  // to LSN 250.
  advanceTime(23ms);
  onShardsNextLsnChanged(lsn_t(264), N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(272), N3, N4, N5, N6, N7, N8);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(800), N9, N10, N11);
  advanceTime(2ms);
  onShardsNextLsnChanged(lsn_t(280), N12, N13);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(280), N14);
  onShardsNextLsnChanged(lsn_t(278), N15);
  // Only N9, N10, N11 completed the window (up to LSN 330).
  ASSERT_FALSE(timerIsActive());

  // Some shards complete window up to LSN 330 after another 10ms
  advanceTime(8ms);
  onShardsNextLsnChanged(lsn_t(331), N1, N2);
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(333), N3, N4, N5, N6, N7, N8);
  advanceTime(2ms);
  onShardsNextLsnChanged(lsn_t(340), N12, N13);
  advanceTime(1ms);
  ASSERT_FALSE(timerIsActive()); // Only allowed 1 outlier
  onShardsNextLsnChanged(lsn_t(338), N14);
  ASSERT_TRUE(timerIsActive()); // N15 is candidate
  advanceTime(1ms);
  onShardsNextLsnChanged(lsn_t(331), N15);
  ASSERT_FALSE(timerIsActive());

  // Window is slid to 450
  onWindowSlid(lsn_t(450), filter_version_t{2});

  advanceTime(1min);
  advanceTime(10s);
  // It's been a bit more than a minute that N0 has been an outlier, the expiry
  // timer should be active.
  ASSERT_TRUE(expiryTimerIsActive());
  triggerExpiryTimer();
  ASSERT_TRUE(detector->getCurrentOutliers().empty());
}

}} // namespace facebook::logdevice
