/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PeriodicReleases.h"

#include <chrono>
#include <mutex>
#include <queue>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

using namespace facebook::logdevice;

namespace {

class MockPeriodicReleases;

class PeriodicReleasesTest : public ::testing::Test {
 public:
  dbg::Level log_level_ = dbg::Level::DEBUG;
  Alarm alarm_{DEFAULT_TEST_TIMEOUT};

  static constexpr logid_t LOG_ID{2333};

  Settings settings_;
  UpdateableSettings<Settings> updateable_settings_;
  StatsHolder stats_;

  std::shared_ptr<PeriodicReleases> pr_;

  lsn_t last_released_{0};
  lsn_t lng_{0};

  bool retry_timer_active_{false};

  bool broadcast_timer_active_{false};

  std::shared_ptr<const EpochMetaDataMap> metadata_map_;

  // release messages sent to storage shards
  std::map<ReleaseType, std::set<ShardID>> releases_sent_;

  explicit PeriodicReleasesTest();

  void updateMetaDataMap(StorageSet s, epoch_t since, epoch_t until);

  void setUp();
};

class MockPeriodicReleases : public PeriodicReleases {
 public:
  explicit MockPeriodicReleases(PeriodicReleasesTest* test)
      : PeriodicReleases(nullptr), test_(test) {}

  logid_t getLogID() const override {
    return test_->LOG_ID;
  }

  lsn_t getLastKnownGood() const override {
    return test_->lng_;
  }

  lsn_t getLastReleased() const override {
    return test_->last_released_;
  }

  int sendReleaseToStorageSets(
      lsn_t lsn,
      ReleaseType release_type,
      const Sequencer::SendReleasesPred& pred) override {
    EXPECT_NE(nullptr, test_->metadata_map_);
    std::unique_ptr<StorageSet> shards =
        test_->metadata_map_->getUnionStorageSet();
    EXPECT_NE(nullptr, shards);
    for (const auto shard : *shards) {
      if (!pred || pred(lsn, release_type, shard)) {
        test_->releases_sent_[release_type].insert(shard);
      }
    }
    return 0;
  }

  void startPeriodicReleaseTimer(Type type) override {
    switch (type) {
      case Type::BROADCAST: {
        ASSERT_FALSE(test_->broadcast_timer_active_);
        test_->broadcast_timer_active_ = true;
      } break;
      case Type::RETRY: {
        ASSERT_FALSE(test_->retry_timer_active_);
        test_->retry_timer_active_ = true;
      } break;
    }
  }

  void cancelPeriodicReleaseTimer(ExponentialBackoffTimerNode* /*unused*/,
                                  Type type) override {
    switch (type) {
      case Type::BROADCAST: {
        ASSERT_TRUE(test_->broadcast_timer_active_);
        test_->broadcast_timer_active_ = false;
      } break;
      case Type::RETRY: {
        ASSERT_TRUE(test_->retry_timer_active_);
        test_->retry_timer_active_ = false;
      } break;
    }
  }

  void activatePeriodicReleaseTimer(ExponentialBackoffTimerNode* /*unused*/,
                                    Type type) override {
    // reactivation happened only in timer callbacks
    switch (type) {
      case Type::BROADCAST: {
        ASSERT_TRUE(test_->broadcast_timer_active_);
      } break;
      case Type::RETRY: {
        ASSERT_TRUE(test_->retry_timer_active_);
      } break;
    }
  }

  bool shouldSendBroadcast() const override {
    return true;
  }

  void postBroadcastingRequest(
      std::unique_ptr<StartBroadcastingReleaseRequest> request) override {
    request->execute();
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }

 private:
  PeriodicReleasesTest* const test_;
};

PeriodicReleasesTest::PeriodicReleasesTest()
    : settings_(create_default_settings<Settings>()),
      stats_(StatsParams().setIsServer(true)) {}

void PeriodicReleasesTest::updateMetaDataMap(StorageSet s,
                                             epoch_t since,
                                             epoch_t until) {
  EpochMetaData m(std::move(s),
                  ReplicationProperty(1, NodeLocationScope::NODE),
                  since,
                  since);

  if (metadata_map_ == nullptr) {
    EpochMetaDataMap::Map map;
    map.insert(std::make_pair(since, m));
    metadata_map_ = EpochMetaDataMap::create(
        std::make_shared<EpochMetaDataMap::Map>(map), until);
    ASSERT_NE(nullptr, metadata_map_);
    return;
  }

  metadata_map_ = metadata_map_->withNewEntry(m, until);
  ASSERT_NE(nullptr, metadata_map_);
}

void PeriodicReleasesTest::setUp() {
  dbg::currentLevel = log_level_;
  pr_ = std::make_shared<MockPeriodicReleases>(this);
  updateMetaDataMap({N1, N2}, epoch_t(1), epoch_t(1));
  pr_->onMetaDataMapUpdate(metadata_map_);
}

#define CHECK_RELEASE(type, ...)                                         \
  do {                                                                   \
    ASSERT_EQ(std::set<ShardID>({__VA_ARGS__}), releases_sent_[(type)]); \
    releases_sent_[(type)].clear();                                      \
  } while (0)

#define CHECK_NO_RELEASE(type)                   \
  do {                                           \
    ASSERT_TRUE(releases_sent_[(type)].empty()); \
  } while (0)

constexpr logid_t PeriodicReleasesTest::LOG_ID;

lsn_t lsn(epoch_t::raw_type epoch, esn_t::raw_type esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

TEST_F(PeriodicReleasesTest, Basic) {
  setUp();
  last_released_ = lsn(4, 9);
  lng_ = lsn(4, 7);
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  // should only send global release since it covers lng
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
  CHECK_NO_RELEASE(ReleaseType::PER_EPOCH);
  lng_ = lsn(5, 1);
  last_released_ = lsn(5, 0);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
  CHECK_RELEASE(ReleaseType::PER_EPOCH, N1, N2);
}

TEST_F(PeriodicReleasesTest, Reactivation) {
  setUp();
  last_released_ = lsn(4, 9);
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  // reactivate before timer fired
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
  // timer should be still on
  ASSERT_TRUE(retry_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  last_released_ = lsn(4, 17);
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
}

TEST_F(PeriodicReleasesTest, Retry) {
  setUp();
  last_released_ = lsn(4, 9);
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  // no confirmation, will keep sending
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
  // N1 and N2 are successfully sent
  pr_->noteReleaseSuccessful(N1, last_released_, ReleaseType::GLOBAL);
  pr_->noteReleaseSuccessful(N2, last_released_, ReleaseType::GLOBAL);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  // releases are up-to-date, no need to resend
  CHECK_NO_RELEASE(ReleaseType::GLOBAL);
}

TEST_F(PeriodicReleasesTest, UpdateShardMap) {
  setUp();
  last_released_ = lsn(4, 9);
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  updateMetaDataMap({N3, N4}, epoch_t(5), epoch_t(7));
  pr_->onMetaDataMapUpdate(metadata_map_);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2, N3, N4);
  pr_->noteReleaseSuccessful(N1, last_released_, ReleaseType::GLOBAL);
  pr_->noteReleaseSuccessful(N3, last_released_, ReleaseType::GLOBAL);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  // new nodes should be tracked
  CHECK_RELEASE(ReleaseType::GLOBAL, N2, N4);
  pr_->noteReleaseSuccessful(N2, last_released_, ReleaseType::GLOBAL);
  pr_->noteReleaseSuccessful(N4, last_released_, ReleaseType::GLOBAL);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_NO_RELEASE(ReleaseType::GLOBAL);
  // update map again for some new nodes, but it should remember the progress
  // of existing nodes
  updateMetaDataMap({N1, N5}, epoch_t(8), epoch_t(9));
  pr_->onMetaDataMapUpdate(metadata_map_);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_RELEASE(ReleaseType::GLOBAL, N5);
}

TEST_F(PeriodicReleasesTest, InvalidateReleaseOfNode) {
  setUp();
  // S1 and N1 are on the same node
  const ShardID S1(1, 1);
  last_released_ = lsn(2, 1);
  updateMetaDataMap({N1, S1, N2}, epoch_t(2), epoch_t(2));
  pr_->onMetaDataMapUpdate(metadata_map_);
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, S1, N2);
  pr_->noteReleaseSuccessful(N1, last_released_, ReleaseType::GLOBAL);
  pr_->noteReleaseSuccessful(S1, last_released_, ReleaseType::GLOBAL);
  pr_->noteReleaseSuccessful(N2, last_released_, ReleaseType::GLOBAL);
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  CHECK_NO_RELEASE(ReleaseType::GLOBAL);
  // N1 disconnected
  pr_->invalidateLastLsnsOfNode(N1.node());
  pr_->schedule();
  pr_->timerCallback(nullptr, PeriodicReleases::Type::RETRY);
  // the release state of N1 and S1 should be invalidated
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, S1);
}

TEST_F(PeriodicReleasesTest, Broadcast) {
  setUp();
  last_released_ = lsn(4, 9);
  pr_->schedule();
  ASSERT_TRUE(retry_timer_active_);
  // broadcast not activated
  pr_->schedule();
  ASSERT_FALSE(pr_->isBroadcasting());
  pr_->startBroadcasting();
  // broadcast should be enabled with timer scheduled
  ASSERT_TRUE(pr_->isBroadcasting());
  ASSERT_TRUE(broadcast_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::BROADCAST);
  // should send to N1 and N2
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
  // shouldn't affect the other timer
  ASSERT_TRUE(retry_timer_active_);
  // timer should be still activte
  ASSERT_TRUE(broadcast_timer_active_);
  pr_->timerCallback(nullptr, PeriodicReleases::Type::BROADCAST);
  // should still send to N1 and N2 regardless of the progress
  CHECK_RELEASE(ReleaseType::GLOBAL, N1, N2);
}

} // anonymous namespace
