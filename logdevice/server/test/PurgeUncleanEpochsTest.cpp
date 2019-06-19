/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/PurgeUncleanEpochs.h"

#include <memory>
#include <vector>

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <gtest/gtest.h>

#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/storage/PurgeSingleEpoch.h"

#define N0 ShardID(0, 0)

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::NodeSetTestUtil;

using State = PurgeUncleanEpochs::State;
using RecordSource = MetaDataLogReader::RecordSource;

class MockPurgeUncleanEpochs;

class PurgeUncleanEpochsTest : public ::testing::Test {
 public:
  PurgeUncleanEpochsTest() : settings_(create_default_settings<Settings>()) {}

  void setUp();

  State getState() const {
    ld_check(purge_ != nullptr);
    return purge_->state_;
  }

  const std::map<epoch_t, PurgeSingleEpoch>& getPurgeSingleEpochsMap() const {
    ld_check(purge_ != nullptr);
    return purge_->purge_epochs_;
  }

  std::vector<epoch_t> getActivePurgeEpochs() {
    std::vector<epoch_t> active_epochs_;
    for (const auto& entry : purge_->purge_epochs_) {
      active_epochs_.push_back(entry.first);
    }
    return active_epochs_;
  }

  BackoffTimer* getRetryTimer() {
    ld_check(purge_ != nullptr);
    return purge_->retry_timer_.get();
  }

  bool epochMetaDataFinalized() const {
    ld_check(purge_ != nullptr);
    return purge_->epoch_metadata_finalized_;
  }

  std::unique_ptr<EpochMetaData>
  genMetaData(epoch_t epoch, folly::Optional<epoch_t> since = folly::none) {
    return std::make_unique<EpochMetaData>(
        StorageSet{ShardID(0, 0)},
        ReplicationProperty(1, NodeLocationScope::NODE),
        epoch,
        since.value_or(epoch));
  }

  std::shared_ptr<const EpochMetaDataMap::Map>
  genMetaDataMap(std::set<epoch_t::raw_type> since_epochs) {
    EpochMetaDataMap::Map map;
    for (const auto e : since_epochs) {
      map.insert(std::make_pair(epoch_t(e), *genMetaData(epoch_t(e))));
    }
    return std::make_shared<const EpochMetaDataMap::Map>(std::move(map));
  }

  void genEpochMetaDataMap(std::set<epoch_t::raw_type> since_epochs,
                           epoch_t until) {
    epoch_metadata_map_ = EpochMetaDataMap::create(
        genMetaDataMap(std::move(since_epochs)), until);
  }

  void onHistoricalMetadata(Status status) {
    purge_->onHistoricalMetadata(status);
  }

  void checkGetEpochRecoveryMetadataRequestPosted(epoch_t start, epoch_t end) {
    auto it = std::find(std::begin(pendingEpochRecoveryMetadataRequests),
                        std::end(pendingEpochRecoveryMetadataRequests),
                        std::make_pair(start, end));

    ASSERT_TRUE(it != std::end(pendingEpochRecoveryMetadataRequests));
    if (it != std::end(pendingEpochRecoveryMetadataRequests)) {
      pendingEpochRecoveryMetadataRequests.erase(it);
    }
  }

  void onGetEpochRecoveryMetadataComplete(
      Status status,
      const EpochRecoveryStateMap& epochRecoveryStateMap) {
    purge_->onGetEpochRecoveryMetadataComplete(status, epochRecoveryStateMap);
  }

  logid_t log_id_{222};
  const shard_index_t shard_{0};
  folly::Optional<epoch_t> current_last_clean_epoch_;
  epoch_t purge_to_{EPOCH_INVALID};
  epoch_t new_last_clean_epoch_{EPOCH_INVALID};
  NodeID node_{1, 1};
  std::shared_ptr<Configuration> config_;
  Settings settings_;
  std::unique_ptr<PurgeUncleanEpochs> purge_;
  Status completion_{E::UNKNOWN};
  std::vector<std::unique_ptr<StorageTask>> tasks_;
  std::pair<epoch_t, epoch_t> metadata_epochs_{EPOCH_INVALID, EPOCH_INVALID};
  std::vector<std::pair<epoch_t, epoch_t>> pendingEpochRecoveryMetadataRequests;
  int metadata_read_attempt_ = 0;
  std::shared_ptr<const EpochMetaDataMap> epoch_metadata_map_;
  bool test_metadata_log_ = false;
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
};

class MockPurgeUncleanEpochs : public PurgeUncleanEpochs {
 public:
  explicit MockPurgeUncleanEpochs(PurgeUncleanEpochsTest* test)
      : PurgeUncleanEpochs(nullptr,
                           test->log_id_,
                           test->shard_,
                           test->current_last_clean_epoch_,
                           test->purge_to_,
                           test->new_last_clean_epoch_,
                           test->node_,
                           nullptr),
        test_(test) {}

  std::unique_ptr<BackoffTimer> createTimer() override {
    auto timer = std::make_unique<MockBackoffTimer>();
    return std::move(timer);
  }

  void startStorageTask(std::unique_ptr<StorageTask>&& task) override {
    test_->tasks_.push_back(std::move(task));
  }

  const std::shared_ptr<const Configuration> getClusterConfig() const override {
    return test_->config_;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return test_->config_->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

  const std::shared_ptr<LogsConfig> getLogsConfig() const override {
    return test_->config_->logsConfig();
  }

  void startReadingMetaData() override {
    test_->metadata_read_attempt_++;
  }

  std::shared_ptr<const EpochMetaDataMap> getEpochMetadataMap() override {
    return test_->epoch_metadata_map_;
  }

  void startPurgingEpochs() override {
    ld_info("Starting PurgeSingleEpoch for log %lu for epochs [%s].",
            log_id_.val_,
            getActiveEpochsStr().c_str());
  }

  void complete(Status status) override {
    ASSERT_EQ(E::UNKNOWN, test_->completion_);
    ld_info("PurgeUncleanEpochs for epoch [%u, %u] for log %lu completed "
            "with status %s.",
            current_last_clean_epoch_.value().val_,
            purge_to_.val_,
            log_id_.val_,
            error_name(status));
    test_->completion_ = status;
  }

  void postEpochRecoveryMetadataRequest(
      epoch_t start,
      epoch_t end,
      std::shared_ptr<EpochMetaData> /* unused */) override {
    test_->pendingEpochRecoveryMetadataRequests.push_back(
        std::make_pair(start, end));
    num_running_get_erm_requests_++;
  }

  NodeID getMyNodeID() const override {
    return NodeID(0, 0);
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }

 private:
  PurgeUncleanEpochsTest* const test_;
};

void PurgeUncleanEpochsTest::setUp() {
  dbg::currentLevel = dbg::Level::DEBUG;
  dbg::assertOnData = true;

  Configuration::Nodes nodes;
  addNodes(&nodes, 1, 1, "....", 1);
  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), log_id_, 1, 0, 1, {});

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 1);

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest("purge_unclean_epochs_test",
                                 std::move(nodes_config),
                                 std::move(meta_config)),
      std::move(logs_config));

  if (test_metadata_log_) {
    log_id_ = MetaDataLog::metaDataLogID(log_id_);
  }
  purge_ = std::make_unique<MockPurgeUncleanEpochs>(this);
  settings_.get_erm_for_empty_epoch = true;
}

#define CHECK_STORAGE_TASK(name)                         \
  do {                                                   \
    ASSERT_FALSE(tasks_.empty());                        \
    if (folly::kIsDebug) {                               \
      auto task_ptr = tasks_.back().get();               \
      ASSERT_NE(nullptr, dynamic_cast<name*>(task_ptr)); \
    }                                                    \
    tasks_.clear();                                      \
  } while (0)

// a test to see if the state machine can complete a successful purge workflow
TEST_F(PurgeUncleanEpochsTest, BasicWorkFlow) {
  // last clean epoch: 5 but initially unknown
  // purge up to: 200
  // new lce: 201
  // should perform purge in epochs [6, 200] then set the LCE to 201
  // non-empty epochs: 7, 13, 18, 19
  // epoch metadata interval: [2, 14] [15, 15],[16, 18], [19, 210]
  purge_to_ = epoch_t(200);
  new_last_clean_epoch_ = epoch_t(201);
  setUp();

  ASSERT_EQ(nullptr, getRetryTimer());
  purge_->start();

  ASSERT_NE(nullptr, getRetryTimer());
  ASSERT_FALSE(getRetryTimer()->isActive());

  // current LCE is unknown, we should read the LCE
  ASSERT_EQ(State::READ_LAST_CLEAN, getState());
  CHECK_STORAGE_TASK(PurgeReadLastCleanTask);

  // local lce is read from the local log store
  purge_->onLastCleanRead(E::OK, epoch_t(5));

  // state machine should advance to the next state
  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  // SealStorageTask should be created to get information about epochs
  CHECK_STORAGE_TASK(SealStorageTask);

  // the seal task got dropped at first
  purge_->onGetPurgeEpochsDone(E::DROPPED, nullptr);
  // we should expect a retry
  ASSERT_TRUE(getRetryTimer()->isActive());
  // simulate retry timer expired
  static_cast<MockBackoffTimer*>(getRetryTimer())->trigger();

  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  CHECK_STORAGE_TASK(SealStorageTask);

  auto epoch_map = new SealStorageTask::EpochInfoMap({
      {7, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {13, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {18, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {19, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
  });

  purge_->onGetPurgeEpochsDone(
      E::OK, std::unique_ptr<SealStorageTask::EpochInfoMap>(epoch_map));
  ASSERT_EQ(State::GET_EPOCH_METADATA, getState());

  genEpochMetaDataMap({2, 15, 16, 19}, epoch_t(210));

  // No purge state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());
  // not concluded yet
  ASSERT_EQ(State::GET_EPOCH_METADATA, getState());

  onHistoricalMetadata(E::OK);

  ASSERT_TRUE(epochMetaDataFinalized());

  // GetEpochRecoveryMetadataRequest should be posted for [15]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(15), epoch_t(15));
  // GetEpochRecoveryMetadataRequest should be posted for [16, 18]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(16), epoch_t(18));
  // GetEpochRecoveryMetadataRequest should be posted for [6,14]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(6), epoch_t(14));
  // GetEpochRecoveryMetadataRequest should be posted for [19, 200]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(19), epoch_t(200));
  ASSERT_TRUE(pendingEpochRecoveryMetadataRequests.empty());

  // We have gotten all EpochMetaData. Move to next stage
  ASSERT_EQ(State::GET_EPOCH_RECOVERY_METADATA, getState());

  // Say GetEpochRecoveryMetadataRequest completes for E15
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm(epoch_t(16),
                            esn_t(2),
                            esn_t(4),
                            0,
                            tail_record,
                            epoch_size_map,
                            epoch_end_offsets);
  EpochRecoveryStateMap map = {{15, {E::OK, erm}}};
  onGetEpochRecoveryMetadataComplete(E::OK, map);

  // PurgeSinlgeEpoch state machine should be created for (15)
  ASSERT_EQ(1, getPurgeSingleEpochsMap().size());
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(15)));

  // Say GetEpochRecoveryMetadataRequest completes for [16, 18]
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm1(epoch_t(17),
                             esn_t(2),
                             esn_t(4),
                             0,
                             tail_record,
                             epoch_size_map,
                             epoch_end_offsets);
  EpochRecoveryMetadata erm2;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm3(epoch_t(19),
                             esn_t(2),
                             esn_t(4),
                             0,
                             tail_record,
                             epoch_size_map,
                             epoch_end_offsets);
  EpochRecoveryStateMap map2 = {
      {16, {E::OK, erm1}}, {17, {E::EMPTY, erm2}}, {18, {E::OK, erm3}}};
  onGetEpochRecoveryMetadataComplete(E::OK, map2);

  // PurgeSingleEpoch state machine (16, 18) should be created
  ASSERT_EQ(3, getPurgeSingleEpochsMap().size());
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(16)));
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(18)));

  // No state machine should be created for epoch 17 since it
  // is empty localy and status is also empty
  ASSERT_EQ(0, getPurgeSingleEpochsMap().count(epoch_t(17)));

  // Now GetEpochRecoveryMetadataRequest completes for [6, 14]
  EpochRecoveryStateMap map3;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 6; epoch <= 14; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata md(epoch_t(epoch),
                               esn_t(2),
                               esn_t(4),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
      map3.emplace(epoch, std::make_pair(E::OK, md));
    } else {
      map3.emplace(epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
    }
  }
  onGetEpochRecoveryMetadataComplete(E::OK, map3);

  // PurgeSingleEpoch state machine (6,7,8,10,12,13,14) should be created
  ASSERT_EQ(10, getPurgeSingleEpochsMap().size());
  for (auto epoch = 6; epoch <= 14; epoch++) {
    if (epoch % 2 == 0 || epoch == 7 || epoch == 13) {
      ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(epoch)));
    }
  }

  EpochRecoveryStateMap map4;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 19; epoch <= 200; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata md(epoch_t(epoch),
                               esn_t(2),
                               esn_t(4),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
      map4.emplace(epoch, std::make_pair(E::OK, md));
    } else {
      map4.emplace(epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
    }
  }
  onGetEpochRecoveryMetadataComplete(E::OK, map4);

  // PurgeSingleEpoch state machine (19 and every even epoch
  // numbered epoch after 19) should be created
  for (auto epoch = 19; epoch <= 200; epoch++) {
    if (epoch == 19 || epoch % 2 == 0) {
      ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(epoch)));
    }
  }

  // got all epoch metadata, should advance to the next stage
  ASSERT_EQ(State::RUN_PURGE_EPOCHS, getState());

  // finish PurgeSingleEpoch machines
  for (auto epoch : getActivePurgeEpochs()) {
    purge_->onPurgeSingleEpochDone(epoch, E::OK);
  }

  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());

  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);

  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);

  // all done!
  ASSERT_EQ(Status::OK, completion_);
}

TEST_F(PurgeUncleanEpochsTest, EmptyEpochs) {
  // last clean epoch: 6 already known
  // purge up to: 20
  // new lce: 20
  // should perform purge in epochs [6, 20] then set the LCE to 20
  // non-empty epochs: none
  purge_to_ = epoch_t(20);
  current_last_clean_epoch_.assign(epoch_t(6));
  new_last_clean_epoch_ = epoch_t(20);
  setUp();

  purge_->start();
  ASSERT_NE(nullptr, getRetryTimer());
  ASSERT_FALSE(getRetryTimer()->isActive());

  // current LCE is known, we should direct transition to GET_PURGE_EPOCHS
  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  // SealStorageTask should be created to get information about epochs
  CHECK_STORAGE_TASK(SealStorageTask);

  // all epochs are empty
  auto epoch_map = new SealStorageTask::EpochInfoMap({});

  purge_->onGetPurgeEpochsDone(
      E::OK, std::unique_ptr<SealStorageTask::EpochInfoMap>(epoch_map));

  // We should still get EpochMetaData and EpochRecoveryMetadata for these
  // epochs
  genEpochMetaDataMap({2}, epoch_t(20));

  // No purse state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());

  onHistoricalMetadata(E::OK);

  // GetEpochRecoveryMetadataRequest should be posted for [7, 20]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(7), epoch_t(20));

  // Now GetEpochRecoveryMetadataRequest completes for [7, 20]
  EpochRecoveryStateMap map;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 7; epoch <= 20; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata md(epoch_t(epoch),
                               esn_t(2),
                               esn_t(4),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
      map.emplace(epoch, std::make_pair(E::OK, md));
    } else {
      map.emplace(epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
    }
  }
  onGetEpochRecoveryMetadataComplete(E::OK, map);

  // got all epoch metadata, should advance to the next stage
  ASSERT_EQ(State::RUN_PURGE_EPOCHS, getState());

  for (auto epoch = 7; epoch <= 20; epoch++) {
    if (epoch % 2 == 0) {
      purge_->onPurgeSingleEpochDone(epoch_t(epoch), E::OK);
    }
  }

  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());

  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);

  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);
  ASSERT_EQ(Status::OK, completion_);
}

TEST_F(PurgeUncleanEpochsTest, EmptyEpochsMetaDataLog) {
  // last clean epoch: 6 already known
  // purge up to: 20
  // new lce: 20
  // should perform purge in epochs [6, 20] then set the LCE to 20
  // non-empty epochs: none
  test_metadata_log_ = true;
  purge_to_ = epoch_t(20);
  current_last_clean_epoch_.assign(epoch_t(6));
  new_last_clean_epoch_ = epoch_t(20);
  setUp();

  purge_->start();
  ASSERT_NE(nullptr, getRetryTimer());
  ASSERT_FALSE(getRetryTimer()->isActive());

  // current LCE is known, we should direct transition to GET_PURGE_EPOCHS
  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  // SealStorageTask should be created to get information about epochs
  CHECK_STORAGE_TASK(SealStorageTask);

  // all epochs are empty
  auto epoch_map = new SealStorageTask::EpochInfoMap({});

  // No purse state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());

  purge_->onGetPurgeEpochsDone(
      E::OK, std::unique_ptr<SealStorageTask::EpochInfoMap>(epoch_map));

  // GetEpochRecoveryMetadataRequest should be posted for [7, 20]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(7), epoch_t(20));

  // Now GetEpochRecoveryMetadataRequest completes for [7, 20]
  EpochRecoveryStateMap map;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 7; epoch <= 20; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata md(epoch_t(epoch),
                               esn_t(2),
                               esn_t(4),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
      map.emplace(epoch, std::make_pair(E::OK, md));
    } else {
      map.emplace(epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
    }
  }
  onGetEpochRecoveryMetadataComplete(E::OK, map);

  // got all epoch metadata, should advance to the next stage
  ASSERT_EQ(State::RUN_PURGE_EPOCHS, getState());

  for (auto epoch = 7; epoch <= 20; epoch++) {
    if (epoch % 2 == 0) {
      purge_->onPurgeSingleEpochDone(epoch_t(epoch), E::OK);
    }
  }

  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());

  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);

  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);
  ASSERT_EQ(Status::OK, completion_);
}

TEST_F(PurgeUncleanEpochsTest, EmptyEpochsDoNotFetchERM) {
  // last clean epoch: 6 already known
  // purge up to: 20
  // new lce: 20
  // should perform purge in epochs [6, 20] then set the LCE to 20
  // non-empty epochs: 15,16,19
  purge_to_ = epoch_t(20);
  current_last_clean_epoch_.assign(epoch_t(6));
  new_last_clean_epoch_ = epoch_t(20);
  setUp();
  settings_.get_erm_for_empty_epoch = false;

  purge_->start();
  ASSERT_NE(nullptr, getRetryTimer());
  ASSERT_FALSE(getRetryTimer()->isActive());

  // current LCE is known, we should direct transition to GET_PURGE_EPOCHS
  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  // SealStorageTask should be created to get information about epochs
  CHECK_STORAGE_TASK(SealStorageTask);

  // all epochs are empty
  auto epoch_map = new SealStorageTask::EpochInfoMap(
      {{15, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
       {16, SealStorageTask::EpochInfo{esn_t(10), esn_t(20)}},
       {19, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}}});

  purge_->onGetPurgeEpochsDone(
      E::OK, std::unique_ptr<SealStorageTask::EpochInfoMap>(epoch_map));

  // We should still get EpochMetaData and EpochRecoveryMetadata for these
  // epochs
  genEpochMetaDataMap({2, 16}, epoch_t(16));

  // No purge state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());

  onHistoricalMetadata(E::OK);

  // GetEpochRecoveryMetadataRequest should be posted ONLY for non empty epochs
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(15), epoch_t(15));
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(16), epoch_t(16));
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(19), epoch_t(19));

  // Now GetEpochRecoveryMetadataRequest completes
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata md(epoch_t(15),
                           esn_t(2),
                           esn_t(4),
                           0,
                           tail_record,
                           epoch_size_map,
                           epoch_end_offsets);

  onGetEpochRecoveryMetadataComplete(
      E::OK, EpochRecoveryStateMap{{15, std::make_pair(E::OK, md)}});

  onGetEpochRecoveryMetadataComplete(
      E::OK,
      EpochRecoveryStateMap{
          {16, std::make_pair(E::EMPTY, EpochRecoveryMetadata())}});

  onGetEpochRecoveryMetadataComplete(
      E::OK,
      EpochRecoveryStateMap{
          {19, std::make_pair(E::EMPTY, EpochRecoveryMetadata())}});

  // got all epoch metadata, should advance to the next stage
  ASSERT_EQ(State::RUN_PURGE_EPOCHS, getState());
  purge_->onPurgeSingleEpochDone(epoch_t(15), E::OK);
  purge_->onPurgeSingleEpochDone(epoch_t(16), E::OK);
  purge_->onPurgeSingleEpochDone(epoch_t(19), E::OK);

  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());

  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);

  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);
  ASSERT_EQ(Status::OK, completion_);
}

TEST_F(PurgeUncleanEpochsTest, NoPurgeNeededButAdvanceLCE) {
  // last clean epoch: 6 already known
  // purge up to: 6
  // new lce: 7
  // should NOT perform any purge but set the LCE to 7
  purge_to_ = epoch_t(6);
  current_last_clean_epoch_.assign(epoch_t(6));
  new_last_clean_epoch_ = epoch_t(7);
  setUp();

  purge_->start();
  ASSERT_NE(nullptr, getRetryTimer());
  ASSERT_FALSE(getRetryTimer()->isActive());

  // we should skip all intermediate phases and directly perform
  // WRITE_LAST_CLEAN
  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());

  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);

  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);
  ASSERT_EQ(Status::OK, completion_);
}

TEST_F(PurgeUncleanEpochsTest, NoOp) {
  // last clean epoch: 6 already known
  // purge up to: 6
  // new lce: 6
  // should complete synchronously
  purge_to_ = epoch_t(6);
  current_last_clean_epoch_.assign(epoch_t(6));
  new_last_clean_epoch_ = epoch_t(6);
  setUp();

  purge_->start();
  ASSERT_EQ(Status::UPTODATE, completion_);
}

TEST_F(PurgeUncleanEpochsTest, DoNotPurgeBeyondPurgeTo) {
  // last clean epoch: 5 but initially unknown
  // purge up to: 6
  // new lce: 7
  // but there are data written in epoch 8 and 9
  // should perform purge in epochs [6, 6] then set the LCE to 7
  // should not purge epoch 8 or 9
  current_last_clean_epoch_.assign(epoch_t(5));
  purge_to_ = epoch_t(6);
  new_last_clean_epoch_ = epoch_t(7);
  setUp();

  purge_->start();

  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  // SealStorageTask should be created to get information about epochs
  CHECK_STORAGE_TASK(SealStorageTask);

  auto epoch_map = new SealStorageTask::EpochInfoMap({
      {6, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {7, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
  });

  purge_->onGetPurgeEpochsDone(
      E::OK, std::unique_ptr<SealStorageTask::EpochInfoMap>(epoch_map));

  ASSERT_EQ(State::GET_EPOCH_METADATA, getState());
  genEpochMetaDataMap({2}, epoch_t(20));

  // No purse state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());

  onHistoricalMetadata(E::OK);
  ASSERT_TRUE(epochMetaDataFinalized());

  ASSERT_EQ(State::GET_EPOCH_RECOVERY_METADATA, getState());
  // GetEpochRecoveryMetadataRequest should be posted for [6,6]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(6), epoch_t(6));
  ASSERT_TRUE(pendingEpochRecoveryMetadataRequests.empty());

  // Now GetEpochRecoveryMetadataRequest completes for [6, 6]
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata md(epoch_t(6),
                           esn_t(2),
                           esn_t(4),
                           0,
                           tail_record,
                           epoch_size_map,
                           epoch_end_offsets);
  EpochRecoveryStateMap map;
  map.emplace(6, std::make_pair(E::OK, md));
  onGetEpochRecoveryMetadataComplete(E::OK, map);

  // got all epoch metadata, should advance to the next stage
  ASSERT_EQ(State::RUN_PURGE_EPOCHS, getState());

  // only one PurgeSingleEpoch state machine (epoch 6) should be created
  ASSERT_EQ(1, getPurgeSingleEpochsMap().size());
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(6)));

  purge_->onPurgeSingleEpochDone(epoch_t(6), E::OK);
  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());
  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);
  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);
  // all done!
  ASSERT_EQ(Status::OK, completion_);
}

// similar to the basic workflow, test PurgeUncleanEpochs can function well
// with corrupted epoch metadata
TEST_F(PurgeUncleanEpochsTest, BadEpochMetadata) {
  // last clean epoch: 5
  // purge up to: 200
  // new lce: 201
  // should perform purge in epochs [6, 200] then set the LCE to 201
  // non-empty epochs: 7, 13, 18, 19
  // epoch metadata interval: [7, 14] [15, 15],[16, 18], [19, 200]
  current_last_clean_epoch_.assign(epoch_t(5));
  purge_to_ = epoch_t(200);
  new_last_clean_epoch_ = epoch_t(201);
  setUp();

  purge_->start();

  // state machine should advance to the next state
  ASSERT_EQ(State::GET_PURGE_EPOCHS, getState());
  // SealStorageTask should be created to get information about epochs
  CHECK_STORAGE_TASK(SealStorageTask);

  auto epoch_map = new SealStorageTask::EpochInfoMap({
      {7, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {13, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {18, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
      {19, SealStorageTask::EpochInfo{esn_t(1), esn_t(2)}},
  });

  ASSERT_EQ(0, metadata_read_attempt_);

  purge_->onGetPurgeEpochsDone(
      E::OK, std::unique_ptr<SealStorageTask::EpochInfoMap>(epoch_map));
  ASSERT_EQ(State::GET_EPOCH_METADATA, getState());

  ASSERT_EQ(1, metadata_read_attempt_);

  onHistoricalMetadata(E::TIMEDOUT);

  // No purge state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());

  // we should expect a retry
  ASSERT_TRUE(getRetryTimer()->isActive());
  // simulate retry timer expired
  static_cast<MockBackoffTimer*>(getRetryTimer())->trigger();

  ASSERT_EQ(2, metadata_read_attempt_);
  genEpochMetaDataMap({2, 15, 16, 19}, epoch_t(198));

  // No purse state machine is created yet
  ASSERT_EQ(0, getPurgeSingleEpochsMap().size());

  onHistoricalMetadata(E::OK);
  ASSERT_TRUE(epochMetaDataFinalized());

  // GetEpochRecoveryMetadataRequest should be posted for [15]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(15), epoch_t(15));
  // GetEpochRecoveryMetadataRequest should be posted for [16, 18]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(16), epoch_t(18));
  // GetEpochRecoveryMetadataRequest should be posted for [6,14]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(6), epoch_t(14));
  // GetEpochRecoveryMetadataRequest should be posted for [19, 200]
  checkGetEpochRecoveryMetadataRequestPosted(epoch_t(19), epoch_t(200));
  ASSERT_TRUE(pendingEpochRecoveryMetadataRequests.empty());

  // We have gotten all EpochMetaData. Move to next stage
  ASSERT_EQ(State::GET_EPOCH_RECOVERY_METADATA, getState());

  // Say GetEpochRecoveryMetadataRequest completes for E15
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm(epoch_t(16),
                            esn_t(2),
                            esn_t(4),
                            0,
                            tail_record,
                            epoch_size_map,
                            epoch_end_offsets);
  EpochRecoveryStateMap map = {{15, {E::OK, erm}}};
  onGetEpochRecoveryMetadataComplete(E::OK, map);

  // PurgeSinlgeEpoch state machine should be created for (15)
  ASSERT_EQ(1, getPurgeSingleEpochsMap().size());
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(15)));

  // Say GetEpochRecoveryMetadataRequest completes for [16, 18]
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm1(epoch_t(17),
                             esn_t(2),
                             esn_t(4),
                             0,
                             tail_record,
                             epoch_size_map,
                             epoch_end_offsets);
  EpochRecoveryMetadata erm2;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  EpochRecoveryMetadata erm3(epoch_t(19),
                             esn_t(2),
                             esn_t(4),
                             0,
                             tail_record,
                             epoch_size_map,
                             epoch_end_offsets);
  EpochRecoveryStateMap map2 = {
      {16, {E::OK, erm1}}, {17, {E::EMPTY, erm2}}, {18, {E::OK, erm3}}};
  onGetEpochRecoveryMetadataComplete(E::OK, map2);

  // PurgeSingleEpoch state machine (16, 18) should be created
  ASSERT_EQ(3, getPurgeSingleEpochsMap().size());
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(16)));
  ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(18)));

  // No state machine should be created for epoch 17 since it
  // is empty localy and status is also empty
  ASSERT_EQ(0, getPurgeSingleEpochsMap().count(epoch_t(17)));

  // Now GetEpochRecoveryMetadataRequest completes for [6, 14]
  EpochRecoveryStateMap map3;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 6; epoch <= 14; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata md(epoch_t(epoch),
                               esn_t(2),
                               esn_t(4),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
      map3.emplace(epoch, std::make_pair(E::OK, md));
    } else {
      map3.emplace(epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
    }
  }
  onGetEpochRecoveryMetadataComplete(E::OK, map3);

  // PurgeSingleEpoch state machine (6,7,8,10,12,13,14) should be created
  ASSERT_EQ(10, getPurgeSingleEpochsMap().size());
  for (auto epoch = 6; epoch <= 14; epoch++) {
    if (epoch % 2 == 0 || epoch == 7 || epoch == 13) {
      ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(epoch)));
    }
  }

  EpochRecoveryStateMap map4;
  epoch_size_map.setCounter(BYTE_OFFSET, 200);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 100);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 100);
  for (auto epoch = 19; epoch <= 200; epoch++) {
    if (epoch % 2 == 0) {
      EpochRecoveryMetadata md(epoch_t(epoch),
                               esn_t(2),
                               esn_t(4),
                               0,
                               tail_record,
                               epoch_size_map,
                               epoch_end_offsets);
      map4.emplace(epoch, std::make_pair(E::OK, md));
    } else {
      map4.emplace(epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
    }
  }
  onGetEpochRecoveryMetadataComplete(E::OK, map4);

  // PurgeSingleEpoch state machine (19 and every even epoch
  // numbered epoch after 19) should be created
  for (auto epoch = 19; epoch <= 200; epoch++) {
    if (epoch == 19 || epoch % 2 == 0) {
      ASSERT_EQ(1, getPurgeSingleEpochsMap().count(epoch_t(epoch)));
    }
  }

  // got all epoch metadata, should advance to the next stage
  ASSERT_EQ(State::RUN_PURGE_EPOCHS, getState());

  // finish PurgeSingleEpoch machines
  for (auto epoch : getActivePurgeEpochs()) {
    purge_->onPurgeSingleEpochDone(epoch, E::OK);
  }

  ASSERT_TRUE(getPurgeSingleEpochsMap().empty());
  ASSERT_EQ(State::WRITE_LAST_CLEAN, getState());

  CHECK_STORAGE_TASK(PurgeWriteLastCleanTask);

  ASSERT_EQ(Status::UNKNOWN, completion_);
  purge_->onWriteLastCleanDone(E::OK);

  // all done!
  ASSERT_EQ(Status::OK, completion_);
}

// TODO: add test for persistent error / fail safe mode

}} // namespace facebook::logdevice
