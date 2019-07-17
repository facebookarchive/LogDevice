/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FileEpochStore.h"

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/test/TestNodeSetSelector.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

#define TEST_CLUSTER "nodeset_test" // fake LD cluster name to use

#define N1 ShardID(1, 1)
#define N2 ShardID(2, 1)
#define N3 ShardID(3, 1)
#define N4 ShardID(4, 1)

class MockFileEpochStore : public FileEpochStore {
 public:
  explicit MockFileEpochStore(
      std::string path,
      const std::shared_ptr<UpdateableNodesConfiguration>& config)
      : FileEpochStore(path, nullptr, config) {}

 protected:
  void postCompletionMetaData(
      EpochStore::CompletionMetaData cf,
      Status status,
      logid_t log_id,
      std::unique_ptr<EpochMetaData> metadata = nullptr,
      std::unique_ptr<EpochStoreMetaProperties> meta_prop = nullptr) override {
    cf(status, log_id, std::move(metadata), std::move(meta_prop));
  }
  void postCompletionLCE(EpochStore::CompletionLCE cf,
                         Status status,
                         logid_t log_id,
                         epoch_t epoch,
                         TailRecord tail_record) override {
    cf(status, log_id, epoch, tail_record);
  }
};

class FileEpochStoreTest : public ::testing::Test {
 public:
  FileEpochStoreTest()
      : temp_dir_(createTemporaryDir("FileEpochStoreTest", false)) {}

  void SetUp() override {
    dbg::assertOnData = true;
    std::shared_ptr<Configuration> cfg_in =
        Configuration::fromJsonFile(TEST_CONFIG_FILE(TEST_CLUSTER ".conf"));
    ld_check(cfg_in);
    cluster_config_ = std::make_shared<UpdateableConfig>(std::move(cfg_in));
    cluster_config_->updateableNodesConfiguration()->update(
        cluster_config_->getNodesConfigurationFromServerConfigSource());
    auto selector = NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM);
    auto config = cluster_config_->get();

    store_ = std::make_unique<MockFileEpochStore>(
        temp_dir_->path().string(),
        cluster_config_->updateableNodesConfiguration());

    int rv = store_->provisionMetaDataLogs(
        std::make_shared<CustomEpochMetaDataUpdater>(
            config,
            config->getNodesConfigurationFromServerConfigSource(),
            std::move(selector),
            true,
            true /* provision_if_empty */,
            false /* update_if_exists */),
        config);
    ASSERT_EQ(0, rv);
  }

 private:
  std::unique_ptr<folly::test::TemporaryDirectory> temp_dir_;

 public:
  std::shared_ptr<UpdateableConfig> cluster_config_;
  std::unique_ptr<MockFileEpochStore> store_;
};

TEST_F(FileEpochStoreTest, NextEpochWithMetaData) {
  store_->createOrUpdateMetaData(
      logid_t(1),
      std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
      [](Status status,
         logid_t,
         std::unique_ptr<EpochMetaData> info,
         std::unique_ptr<EpochStoreMetaProperties>) {
        ASSERT_EQ(E::OK, status);
        ASSERT_NE(nullptr, info);
        EXPECT_TRUE(info->isValid());
        EXPECT_EQ(2, info->h.epoch.val());
      },
      MetaDataTracer());
  store_->createOrUpdateMetaData(
      logid_t(1),
      std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
      [](Status status,
         logid_t,
         std::unique_ptr<EpochMetaData> info,
         std::unique_ptr<EpochStoreMetaProperties>) {
        ASSERT_EQ(E::OK, status);
        ASSERT_NE(nullptr, info);
        EXPECT_EQ(3, info->h.epoch.val());
      },
      MetaDataTracer());
}

TEST_F(FileEpochStoreTest, UpdateMetaData) {
  // get the current storage_set
  StorageSet shards;
  store_->createOrUpdateMetaData(
      logid_t(1),
      std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
      [&shards](Status status,
                logid_t,
                std::unique_ptr<EpochMetaData> info,
                std::unique_ptr<EpochStoreMetaProperties>) {
        ASSERT_EQ(E::OK, status);
        ASSERT_NE(nullptr, info);
        EXPECT_TRUE(info->isValid());
        EXPECT_EQ(3, info->shards.size()); // in nodeset_test.conf
        shards = info->shards;
      },
      MetaDataTracer());

  auto selector = std::make_shared<TestNodeSetSelector>();
  auto updater = std::make_shared<CustomEpochMetaDataUpdater>(
      cluster_config_->get(),
      cluster_config_->get()->getNodesConfigurationFromServerConfigSource(),
      selector,
      true,
      true);
  // change to a different storage_set
  shards = shards == StorageSet{N1, N2, N3} ? StorageSet{N2, N3, N4}
                                            : StorageSet{N1, N2, N3};
  selector->setStorageSet(shards);

  store_->createOrUpdateMetaData(
      logid_t(1),
      updater,
      [&shards, updater, this](Status status,
                               logid_t,
                               std::unique_ptr<EpochMetaData> info,
                               std::unique_ptr<EpochStoreMetaProperties>) {
        ASSERT_EQ(E::OK, status);
        ASSERT_NE(nullptr, info);
        EXPECT_TRUE(info->isValid());
        EXPECT_EQ(info->h.epoch, info->h.effective_since);
        EXPECT_EQ(shards, info->shards);
        epoch_t base_epoch = info->h.epoch;
        // do another update attempt with same shards,
        // verify that nothing is changed
        store_->createOrUpdateMetaData(
            logid_t(1),
            updater,
            [base_epoch, &shards](Status status_nested,
                                  logid_t,
                                  std::unique_ptr<EpochMetaData> info_nested,
                                  std::unique_ptr<EpochStoreMetaProperties>) {
              // shoud be E::UPTODATE not E::OK
              EXPECT_EQ(E::UPTODATE, status_nested);
              ASSERT_NE(nullptr, info_nested);
              EXPECT_TRUE(info_nested->isValid());
              EXPECT_EQ(info_nested->h.epoch, base_epoch);
              EXPECT_EQ(info_nested->h.epoch, info_nested->h.effective_since);
              EXPECT_EQ(shards, info_nested->shards);
            },
            MetaDataTracer());
      },
      MetaDataTracer());
}

TEST_F(FileEpochStoreTest, LastCleanEpoch) {
  // test accessing lce for both data log and metadata log
  std::vector<logid_t> log_ids{
      logid_t(1), MetaDataLog::metaDataLogID(logid_t(1))};
  for (auto logid : log_ids) {
    store_->getLastCleanEpoch(
        logid,
        [](Status status, logid_t logid_cb, epoch_t epoch, TailRecord tail) {
          ASSERT_EQ(E::OK, status);
          ASSERT_TRUE(tail.isValid());
          ASSERT_EQ(logid_cb, tail.header.log_id);
          EXPECT_EQ(EPOCH_INVALID, epoch);
          EXPECT_EQ(LSN_INVALID, tail.header.lsn);
          EXPECT_EQ(0, tail.header.timestamp);
          EXPECT_EQ(0, tail.offsets_map_.getCounter(BYTE_OFFSET));
        });

    TailRecord set_tail(
        {logid,
         lsn_t(200),
         15,
         {BYTE_OFFSET_INVALID /* deprecated, use OffsetMap instead */},
         0,
         {}},
        OffsetMap({{BYTE_OFFSET, 100}}),
        std::shared_ptr<PayloadHolder>());

    store_->setLastCleanEpoch(logid,
                              epoch_t(10),
                              set_tail,
                              [](Status status, logid_t, epoch_t, TailRecord) {
                                ASSERT_EQ(E::OK, status);
                              });

    store_->getLastCleanEpoch(
        logid,
        [](Status status, logid_t logid_cb, epoch_t epoch, TailRecord tail) {
          ASSERT_EQ(E::OK, status);
          ASSERT_EQ(logid_cb, tail.header.log_id);
          EXPECT_EQ(epoch_t(10), epoch);
          EXPECT_EQ(lsn_t(200), tail.header.lsn);
          EXPECT_EQ(15, tail.header.timestamp);
          EXPECT_EQ(100, tail.offsets_map_.getCounter(BYTE_OFFSET));
        });

    TailRecord set_tail2(
        {logid,
         lsn_t(100),
         30,
         {BYTE_OFFSET_INVALID /* deprecated, use OffsetMap instead */},
         0,
         {}},
        OffsetMap({{BYTE_OFFSET, 50}}),
        std::shared_ptr<PayloadHolder>());

    // Will not make any change because new value for lce is smaller than
    // previous stored value
    store_->setLastCleanEpoch(
        logid,
        epoch_t(5),
        set_tail2,
        [](Status /*status*/, logid_t, epoch_t, TailRecord) {});
    store_->getLastCleanEpoch(
        logid, [](Status status, logid_t, epoch_t epoch, TailRecord tail) {
          ASSERT_EQ(E::OK, status);
          EXPECT_EQ(epoch_t(10), epoch);
          EXPECT_EQ(lsn_t(200), tail.header.lsn);
          EXPECT_EQ(15, tail.header.timestamp);
          EXPECT_EQ(100, tail.offsets_map_.getCounter(BYTE_OFFSET));
        });
  }
}
