/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <thread>

#include <folly/Random.h>
#include <folly/hash/Checksum.h>
#include <gtest/gtest.h>

#include "logdevice/common/Checksum.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

namespace {

// various tests on readLogTail(), getTailAttribute() and tail optimized logs
class TailRecordIntegrationTest : public IntegrationTestBase {
 public:
  void init();
  std::shared_ptr<ClientImpl> createClient();
  inline void checkTail(std::shared_ptr<ClientImpl>& client,
                        lsn_t lsn,
                        uint64_t timstamp,
                        folly::Optional<uint64_t> byte_offset = folly::none,
                        folly::Optional<std::string> payload = folly::none);

  const logid_t LOG_ID{1};

  int replication_{3};
  int nodes_{6};

  bool tail_optimized_{true};
  int checksum_bits_{32};
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
};

void TailRecordIntegrationTest::init() {
  static const int cs_bits[3] = {0, 32, 64};

  tail_optimized_ = (folly::Random::rand64(2) == 0);
  checksum_bits_ = (cs_bits[folly::Random::rand64(3)]);

  ld_info("Log %lu tail optimized: %s, checksum bits %d.",
          LOG_ID.val_,
          tail_optimized_ ? "YES" : "NO",
          checksum_bits_);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(replication_);
  log_attrs.set_tailOptimized(tail_optimized_);

  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setLogGroupName("tail-record-test")
                     .setLogAttributes(log_attrs)
                     .deferStart()
                     .setParam("--reactivation-limit", "100/1s")
                     // enable byte offsets
                     .setParam("--byte-offsets");

  cluster_ = factory.create(nodes_);

  dbg::currentLevel = dbg::Level::INFO;
  dbg::assertOnData = true;
}

std::shared_ptr<ClientImpl> TailRecordIntegrationTest::createClient() {
  ld_check(cluster_ != nullptr);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  // using a small gap grace period to speed us reading when nodes are down
  EXPECT_EQ(
      0,
      client_settings->set("checksum-bits", toString(checksum_bits_).c_str()));

  auto client = cluster_->createClient(
      std::chrono::seconds(20), std::move(client_settings));
  return std::dynamic_pointer_cast<ClientImpl>(client);
}

std::string genPayloadString(size_t len) {
  static const char arr[] = "qwertyuiopasdfghjklzxcvbnm1234567890";
  const char c = arr[folly::Random::rand64(sizeof(arr))];
  return std::string(len, c);
}

inline void
TailRecordIntegrationTest::checkTail(std::shared_ptr<ClientImpl>& client,
                                     lsn_t lsn,
                                     uint64_t timestamp,
                                     folly::Optional<uint64_t> byte_offset,
                                     folly::Optional<std::string> payload) {
  // three thing to check: tail attributes, tail record and data record
  auto tail_attribute = client->getTailAttributesSync(LOG_ID);
  auto tail_record = client->getTailRecordSync(LOG_ID);
  auto data_record = client->readLogTailSync(LOG_ID);

  ASSERT_NE(nullptr, tail_attribute);
  ASSERT_NE(nullptr, tail_record);
  ASSERT_TRUE(tail_record->isValid());

  // consistency b/w these three
  EXPECT_EQ(tail_attribute->last_released_real_lsn, tail_record->header.lsn);
  EXPECT_EQ(tail_attribute->last_timestamp,
            std::chrono::milliseconds(tail_record->header.timestamp));
  EXPECT_FALSE(tail_record->containOffsetWithinEpoch());
  EXPECT_EQ(
      tail_attribute->offsets, OffsetMap::toRecord(tail_record->offsets_map_));
  if (data_record) {
    EXPECT_EQ(tail_attribute->last_released_real_lsn, data_record->attrs.lsn);
    EXPECT_EQ(tail_attribute->last_timestamp, data_record->attrs.timestamp);
    EXPECT_EQ(tail_attribute->offsets, data_record->attrs.offsets);
  }

  if (lsn == LSN_INVALID) {
    // log is empty / never released any record
    EXPECT_EQ(LSN_INVALID, tail_attribute->last_released_real_lsn);
    EXPECT_EQ(std::chrono::milliseconds(0), tail_attribute->last_timestamp);
    EXPECT_EQ(0, tail_attribute->offsets.getCounter(BYTE_OFFSET));
    return;
  }

  EXPECT_EQ(lsn, tail_record->header.lsn);
  EXPECT_EQ(timestamp, tail_record->header.timestamp);
  if (byte_offset.hasValue()) {
    EXPECT_EQ(
        byte_offset.value(), tail_record->offsets_map_.getCounter(BYTE_OFFSET));
  }

  if (tail_optimized_ && payload.hasValue()) {
    uint64_t expected_checksum = 0;
    const auto& pl = payload.value();
    if (checksum_bits_ == 32) {
      expected_checksum = checksum_32bit(Slice(pl.data(), pl.size()));
    } else if (checksum_bits_ == 64) {
      expected_checksum = checksum_64bit(Slice(pl.data(), pl.size()));
    }
    ld_debug("expected payload size: %lu, checksum %lu.",
             pl.size(),
             expected_checksum);
    ASSERT_NE(nullptr, data_record);
    EXPECT_EQ(pl,
              std::string((const char*)data_record->payload.data(),
                          data_record->payload.size()));
  }
}

// check the tail of the log that has never been written before
TEST_F(TailRecordIntegrationTest, EmptyLog) {
  init();
  auto client = createClient();
  cluster_->start();
  cluster_->waitForRecovery();
  checkTail(client, LSN_INVALID, 0, 0);
}

TEST_F(TailRecordIntegrationTest, Basic) {
  init();
  auto client = createClient();
  cluster_->start();
  lsn_t lsn;
  std::chrono::milliseconds timestamp;
  size_t byte_offset = 0;
  std::string pl;

  // write 15 records and check the tail after each append
  for (int i = 0; i < 15; ++i) {
    pl = genPayloadString(folly::Random::rand64(8192) + 1);
    byte_offset += pl.size();
    lsn = client->appendSync(LOG_ID, pl, AppendAttributes(), &timestamp);
    ASSERT_NE(LSN_INVALID, lsn);
    checkTail(client, lsn, timestamp.count(), byte_offset, pl);
  }
}

TEST_F(TailRecordIntegrationTest, SequencerFailOver) {
  init();
  auto client = createClient();
  cluster_->start();
  lsn_t lsn;
  std::chrono::milliseconds timestamp;
  size_t byte_offset = 0;
  std::string pl;

  for (int i = 0; i < 5; ++i) {
    pl = genPayloadString(folly::Random::rand64(256) + 1);
    byte_offset += pl.size();
    lsn = client->appendSync(LOG_ID, pl, AppendAttributes(), &timestamp);
    ASSERT_NE(LSN_INVALID, lsn);
  }
  // check the tail of the log
  checkTail(client, lsn, timestamp.count(), byte_offset, pl);
  // restarting the sequencer node to trigger a failover.
  cluster_->getNode(0).kill();
  ASSERT_EQ(0, cluster_->start({0}));
  cluster_->getNode(0).waitForRecovery(LOG_ID);
  // the tail should remain the same
  checkTail(client, lsn, timestamp.count(), byte_offset, pl);
  // reactivate sequencer
  ASSERT_EQ(1, LOG_ID.val_);
  std::string reply = cluster_->getNode(0).sendCommand("up --logid 1");
  ASSERT_NE(
      std::string::npos, reply.find("Started sequencer activation for log 1"));

  cluster_->getNode(0).waitForRecovery(LOG_ID);
  // the tail should still remain the same
  checkTail(client, lsn, timestamp.count(), byte_offset, pl);
}

// TODO: test empty record payload and large payload cases

} // namespace
