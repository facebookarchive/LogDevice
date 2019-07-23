/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochMetaData.h"

#include <cstdio>
#include <cstring>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/test/TestNodeSetSelector.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/ZookeeperEpochStore.h"

namespace facebook { namespace logdevice {

#define TEST_CLUSTER "nodeset_test" // LD cluster config for nodeset testing

#define N0 ShardID(0, 1)
#define N1 ShardID(1, 1)
#define N2 ShardID(2, 1)
#define N3 ShardID(3, 1)
#define N4 ShardID(4, 1)
#define N5 ShardID(5, 1)
#define N6 ShardID(6, 1)
#define N7 ShardID(7, 1)
#define N8 ShardID(8, 1)
#define N9 ShardID(9, 1)
#define N10 ShardID(10, 1)

class EpochMetaDataTest : public ::testing::Test {
 public:
  EpochMetaDataTest() {
    dbg::assertOnData = true;
  }

  std::shared_ptr<Configuration> parseConfig() {
    return Configuration::fromJsonFile(TEST_CONFIG_FILE(TEST_CLUSTER ".conf"));
  }

  const NodeLocationScope RACK = NodeLocationScope::RACK;
  const NodeLocationScope NODE = NodeLocationScope::NODE;
  using Map = EpochMetaDataMap::Map;

  EpochMetaData genValidEpochMetaData(
      folly::Optional<NodeLocationScope> sync_replication_scope =
          NodeLocationScope::RACK,
      epoch_t::raw_type epoch = 10,
      epoch_t::raw_type effective_since = 6,
      StorageSet storage_set = {N3, N4, N5},
      int replication_factor = 2) {
    if (!sync_replication_scope.hasValue()) {
      sync_replication_scope = NodeLocationScope::NODE;
    }
    ld_check(sync_replication_scope.value() != NodeLocationScope::ROOT);
    EpochMetaData meta(
        storage_set,
        ReplicationProperty(replication_factor, sync_replication_scope.value()),
        epoch_t(epoch),
        epoch_t(effective_since));
    meta.nodeset_params.target_nodeset_size = storage_set.size();
    meta.nodeset_params.signature = 0;
    return meta;
  }
};

TEST_F(EpochMetaDataTest, LogIDTest) {
  const logid_t LOG_ID(35);
  logid_t metadata_log = MetaDataLog::metaDataLogID(LOG_ID);
  EXPECT_EQ(
      ((1ul << (LOGID_BITS_INTERNAL - 1)) | LOG_ID.val_), metadata_log.val_);
  EXPECT_TRUE(MetaDataLog::isMetaDataLog(metadata_log));
  EXPECT_EQ(LOG_ID, MetaDataLog::dataLogID(metadata_log));

  // LOGID_MAX should be the max data log
  EXPECT_FALSE(MetaDataLog::isMetaDataLog(LOGID_MAX));
  EXPECT_TRUE(MetaDataLog::isMetaDataLog(LOGID_MAX_INTERNAL));
  EXPECT_EQ(LOGID_MAX, MetaDataLog::dataLogID(LOGID_MAX_INTERNAL));
}

TEST_F(EpochMetaDataTest, Valid) {
  EpochMetaData uninit_info;
  EXPECT_FALSE(MetaDataLogRecordHeader::isValid(uninit_info.h));
  EXPECT_FALSE(uninit_info.isValid());
  EXPECT_TRUE(uninit_info.isEmpty());

  EpochMetaData valid_info = genValidEpochMetaData();
  EXPECT_TRUE(MetaDataLogRecordHeader::isValid(valid_info.h));
  EXPECT_TRUE(valid_info.isValid());
  EXPECT_FALSE(valid_info.isEmpty());

  // future version is supported but version < MIN_SUPPORTED is not
  valid_info.h.version = epoch_metadata_version::CURRENT + 1;
  EXPECT_TRUE(valid_info.isValid());
  EXPECT_FALSE(valid_info.isEmpty());
  valid_info.h.version = epoch_metadata_version::MIN_SUPPORTED - 1;
  EXPECT_FALSE(valid_info.isValid());
  EXPECT_FALSE(valid_info.isEmpty());

  // Header::nodeset_size does not match the actual size of the vector
  valid_info = genValidEpochMetaData();
  valid_info.setShards(StorageSet{N3, N4, N5, N9});
  valid_info.h.nodeset_size = 3;
  EXPECT_FALSE(valid_info.isValid());
  EXPECT_FALSE(valid_info.isEmpty());

  // duplicate indices
  valid_info = genValidEpochMetaData();
  valid_info.setShards(StorageSet{N3, N4, N4});
  EXPECT_FALSE(valid_info.isValid());

  // out of order indices
  valid_info = genValidEpochMetaData();
  valid_info.setShards(StorageSet{N3, N4, N2});
  EXPECT_FALSE(valid_info.isValid());
}

TEST_F(EpochMetaDataTest, MetaDataLogWritten) {
  const logid_t LOGID(23);
  EpochMetaDataUpdateToWritten written_updater;
  EpochMetaData uninit_info;
  EXPECT_FALSE(MetaDataLogRecordHeader::isValid(uninit_info.h));
  EXPECT_FALSE(uninit_info.isValid());
  EXPECT_TRUE(uninit_info.isEmpty());
  EXPECT_FALSE(uninit_info.writtenInMetaDataLog());
  auto uptr = std::make_unique<EpochMetaData>(uninit_info);
  auto rv = written_updater(LOGID, uptr, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::FAILED, rv);

  auto valid_info = std::make_unique<EpochMetaData>(genValidEpochMetaData());
  EXPECT_TRUE(MetaDataLogRecordHeader::isValid(valid_info->h));
  EXPECT_TRUE(valid_info->isValid());
  EXPECT_FALSE(valid_info->isEmpty());
  EXPECT_FALSE(valid_info->writtenInMetaDataLog());
  rv = written_updater(LOGID, valid_info, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  EXPECT_TRUE(valid_info->writtenInMetaDataLog());

  rv = written_updater(LOGID, valid_info, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::UNCHANGED, rv);
  EXPECT_TRUE(valid_info->writtenInMetaDataLog());

  // update the metadata using a normal updater, make sure that the written
  // flag is cleared
  std::shared_ptr<Configuration> cfg = parseConfig();
  ASSERT_TRUE(cfg);
  ASSERT_NE(nullptr, cfg.get());
  auto selector = std::make_shared<TestNodeSetSelector>();
  CustomEpochMetaDataUpdater updater(
      cfg, cfg->getNodesConfigurationFromServerConfigSource(), selector, true);
  selector->setStorageSet(StorageSet{N1, N2, N3});
  rv = updater(LOGID, valid_info, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  EXPECT_FALSE(valid_info->writtenInMetaDataLog());
}

TEST_F(EpochMetaDataTest, Payload) {
  auto cfg = parseConfig();
  const EpochMetaData info = genValidEpochMetaData();

  std::string str_payload = info.toStringPayload();
  EXPECT_FALSE(str_payload.empty());
  EXPECT_EQ(info.sizeInPayload(), str_payload.size());

  // now converted it back to a new epoch metadata object
  EpochMetaData info_from_str;
  int rv = info_from_str.fromPayload(
      Payload(str_payload.data(), str_payload.size()),
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
  EXPECT_EQ(0, rv);
  EXPECT_TRUE(info_from_str.isValid());
  EXPECT_TRUE(info_from_str == info);

  // test with payload with not enough size
  str_payload.resize(str_payload.size() - 1);
  EpochMetaData info2;
  rv = info2.fromPayload(
      Payload(str_payload.data(), str_payload.size()),
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(E::BADMSG, err);
  rv =
      info.toPayload(const_cast<char*>(str_payload.data()), str_payload.size());
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(E::NOBUFS, err);

  // payload with larger size
  str_payload.resize(str_payload.size() + 500);
  rv =
      info.toPayload(const_cast<char*>(str_payload.data()), str_payload.size());

  EXPECT_EQ(info.sizeInPayload(), rv);
  rv = info2.fromPayload(
      Payload(str_payload.data(), rv),
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
  EXPECT_EQ(0, rv);
  EXPECT_TRUE(info2.isValid());
  EXPECT_TRUE(info2 == info);
}

TEST_F(EpochMetaDataTest, BackwardCompatibility) {
  auto cfg = parseConfig();

  // Constants to use for the test case across all of the scenarios.
  const auto time_replication_conf_changed_at =
      RecordTimestamp::from(std::chrono::milliseconds(1000));
  const auto time_epoch_incremented_at =
      RecordTimestamp::from(std::chrono::milliseconds(5000));

  // A bunch of checks of the form:
  //  1. build a legit EpochMetaData,
  //  2. serialize it and compare to a hard-coded string (to ensure that format
  //     wasn't changed accidentally),
  //  3. deserialize and check all fields.

  auto check = [time_replication_conf_changed_at, time_epoch_incremented_at](
                   const EpochMetaData& info2,
                   int v,
                   epoch_metadata_flags_t extra_flags = 0) {
    epoch_metadata_flags_t flags = extra_flags |
        MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG |
        MetaDataLogRecordHeader::HAS_TIMESTAMPS;

    ReplicationProperty replication;
    if (v < 2) {
      replication.assign({{NodeLocationScope::NODE, 4}});
    } else if (flags & MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY) {
      replication.assign({{NodeLocationScope::NODE, 4},
                          {NodeLocationScope::RACK, 3},
                          {NodeLocationScope::REGION, 2}});
    } else {
      replication.assign(
          {{NodeLocationScope::NODE, 4}, {NodeLocationScope::CLUSTER, 2}});
    }

    std::vector<double> weights;
    if (extra_flags & MetaDataLogRecordHeader::HAS_WEIGHTS) {
      weights = {11, 15, 13, 14, 12};
    }
    if (extra_flags & MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE) {
      auto config =
          Configuration::fromJsonFile(TEST_CONFIG_FILE(TEST_CLUSTER ".conf"));
      EXPECT_EQ(config->getNodesConfigurationFromServerConfigSource()
                    ->getStorageNodesHash(),
                info2.nodeset_params.signature);
    } else {
      EXPECT_EQ(0, info2.nodeset_params.signature);
    }
    if (extra_flags &
        MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED) {
      EXPECT_EQ(0x42, info2.nodeset_params.target_nodeset_size);
      EXPECT_EQ(0xABCDEFEDCBA01234ul, info2.nodeset_params.seed);
    } else {
      EXPECT_EQ(0, info2.nodeset_params.target_nodeset_size);
      EXPECT_EQ(0, info2.nodeset_params.seed);
    }

    EXPECT_EQ(v, info2.h.version);
    EXPECT_EQ(3, info2.h.epoch.val_);
    EXPECT_EQ(2, info2.h.effective_since.val_);
    EXPECT_EQ(5, info2.h.nodeset_size);
    EXPECT_EQ(StorageSet({N6, N7, N8, N9, N10}), info2.shards);
    EXPECT_EQ(replication.toString(), info2.replication.toString());

    EXPECT_EQ(weights, info2.weights);

    if (v >= 2) {
      EXPECT_EQ(flags, info2.h.flags);
      EXPECT_EQ(
          time_replication_conf_changed_at, info2.replication_conf_changed_at);
      EXPECT_EQ(time_epoch_incremented_at, info2.epoch_incremented_at);
    } else {
      EXPECT_EQ(0, info2.h.flags);
      EXPECT_EQ(RecordTimestamp(), info2.replication_conf_changed_at);
      EXPECT_EQ(RecordTimestamp(), info2.epoch_incremented_at);
    }
  };

  // v1

  EpochMetaData info;
  info.h.version = 1;
  info.h.epoch = epoch_t(3);
  info.h.effective_since = epoch_t(2);
  info.setShards(StorageSet{N6, N7, N8, N9, N10});
  info.replication = ReplicationProperty({{NodeLocationScope::NODE, 4}});

  ld_check(info.isValid());

  std::string str = info.toStringPayload();
  EXPECT_EQ("01000000030000000200000005000406000700080009000A00",
            hexdump_buf(str.data(), str.size()));

  {
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
    check(info2, 1);
  }

  {
    // Change serialized version from 1 to 2 and check that deserialization
    // fails.
    epoch_metadata_version::type v2 = 2;
    memcpy(&str[0], &v2, sizeof(v2));
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(-1, rv);

    // Change it back to 1 and check that it's uncorrupted.
    epoch_metadata_version::type v1 = 1;
    memcpy(&str[0], &v1, sizeof(v1));
    rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
  }

  // v2

  info.h.version = 2;
  info.h.flags = MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG |
      MetaDataLogRecordHeader::HAS_TIMESTAMPS;
  info.replication = ReplicationProperty(
      {{NodeLocationScope::NODE, 4}, {NodeLocationScope::CLUSTER, 2}});
  info.replication_conf_changed_at = time_replication_conf_changed_at;
  info.epoch_incremented_at = time_epoch_incremented_at;
  ld_check(info.isValid());

  str = info.toStringPayload();
  EXPECT_EQ("020000000300000002000000050004030201000006000700080009000A00E80300"
            "00000000008813000000000000",
            hexdump_buf(str.data(), str.size()));

  {
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
    check(info2, 2);
  }

  {
    // Change serialized version from 2 to 1 and check that deserialization
    // fails.
    epoch_metadata_version::type v1 = 1;
    memcpy(&str[0], &v1, sizeof(v1));
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(-1, rv);
  }

  // weights

  info.weights = {11, 15, 13, 14, 12};
  ld_check(info.isValid());

  str = info.toStringPayload();
  EXPECT_EQ("020000000300000002000000050004030A01000006000700080009000A00000000"
            "00000026400000000000002E400000000000002A400000000000002C4000000000"
            "00002840E8030000000000008813000000000000",
            hexdump_buf(str.data(), str.size()));

  {
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
    check(info2, 2, MetaDataLogRecordHeader::HAS_WEIGHTS);
  }

  // replication property

  info.replication = ReplicationProperty({{NodeLocationScope::NODE, 4},
                                          {NodeLocationScope::RACK, 3},
                                          {NodeLocationScope::REGION, 2}});
  ld_check(info.isValid());

  str = info.toStringPayload();
  EXPECT_EQ("020000000300000002000000050004051A01000006000700080009000A00000000"
            "00000026400000000000002E400000000000002A400000000000002C4000000000"
            "0000284003000000050000000200000001000000030000000000000004000000E8"
            "030000000000008813000000000000",
            hexdump_buf(str.data(), str.size()));

  {
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
    check(info2,
          2,
          MetaDataLogRecordHeader::HAS_WEIGHTS |
              MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY);
  }

  // config hash
  auto config =
      Configuration::fromJsonFile(TEST_CONFIG_FILE(TEST_CLUSTER ".conf"));
  uint64_t cfg_hash = config->getNodesConfigurationFromServerConfigSource()
                          ->getStorageNodesHash();

  // In real code signature usually isn't equal to storage nodes config hash.
  // Instead it's produced by NodeSetSelector and depends on nodeset selector
  // type. But for this test we can use any signature value, so we're using
  // a plain storage nodes config hash to also make sure hash function didn't
  // change.
  info.nodeset_params.signature = cfg_hash;
  ld_check(info.isValid());
  std::string hash_string = "D6E09254769F9FFF"; // if you changed the test
                                                // config, update this hash

  str = info.toStringPayload();
  EXPECT_EQ("020000000300000002000000050004053A01000006000700080009000A00000000"
            "00000026400000000000002E400000000000002A400000000000002C4000000000"
            "0000284003000000050000000200000001000000030000000000000004000000" +
                hash_string + "E8030000000000008813000000000000",
            hexdump_buf(str.data(), str.size()));

  {
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
    check(info2,
          2,
          MetaDataLogRecordHeader::HAS_WEIGHTS |
              MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY |
              MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE);
  }

  // target nodeset size and seed
  info.nodeset_params.target_nodeset_size = 0x42;
  info.nodeset_params.seed = 0xABCDEFEDCBA01234ul;
  ld_check(info.isValid());

  str = info.toStringPayload();
  EXPECT_EQ("02000000030000000200000005000405BA01000006000700080009000A00000000"
            "00000026400000000000002E400000000000002A400000000000002C4000000000"
            "0000284003000000050000000200000001000000030000000000000004000000" +
                hash_string +
                "42003412A0CBEDEFCDABE8030000000000008813000000000000",
            hexdump_buf(str.data(), str.size()));

  {
    EpochMetaData info2;
    int rv = info2.fromPayload(
        Payload(str.data(), str.size()),
        logid_t(1),
        *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());
    EXPECT_EQ(0, rv);
    check(info2,
          2,
          MetaDataLogRecordHeader::HAS_WEIGHTS |
              MetaDataLogRecordHeader::HAS_REPLICATION_PROPERTY |
              MetaDataLogRecordHeader::HAS_NODESET_SIGNATURE |
              MetaDataLogRecordHeader::HAS_TARGET_NODESET_SIZE_AND_SEED);
  }
}

TEST_F(EpochMetaDataTest, ZookeeperRecordZnodeBuffer) {
  std::shared_ptr<Configuration> cfg = parseConfig();

  const auto zk_record = genValidEpochMetaData();
  const folly::Optional<NodeID> nid(NodeID(1234, 2345));

  char zbuf[ZookeeperEpochStore::ZNODE_VALUE_WRITE_LEN_MAX];
  int rv = EpochStoreEpochMetaDataFormat::toLinearBuffer(
      zk_record, zbuf, sizeof(zbuf) / sizeof(zbuf[0]), nid);
  int expected_size =
      EpochStoreEpochMetaDataFormat::sizeInLinearBuffer(zk_record, nid);
  EXPECT_EQ(expected_size, rv);

  EpochMetaData record_from_buffer;
  NodeID nid_from_buffer;
  rv = EpochStoreEpochMetaDataFormat::fromLinearBuffer(
      zbuf,
      rv,
      &record_from_buffer,
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      &nid_from_buffer);
  EXPECT_EQ(0, rv);
  EXPECT_TRUE(zk_record == record_from_buffer);
  EXPECT_TRUE(nid == nid_from_buffer);

  // test again without node_id written
  rv = EpochStoreEpochMetaDataFormat::toLinearBuffer(
      zk_record, zbuf, sizeof(zbuf) / sizeof(zbuf[0]), folly::none);
  expected_size =
      EpochStoreEpochMetaDataFormat::sizeInLinearBuffer(zk_record, folly::none);
  EXPECT_EQ(expected_size, rv);

  rv = EpochStoreEpochMetaDataFormat::fromLinearBuffer(
      zbuf,
      rv,
      &record_from_buffer,
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      &nid_from_buffer);
  EXPECT_EQ(0, rv);
  EXPECT_TRUE(zk_record == record_from_buffer);
  EXPECT_FALSE(nid_from_buffer.isNodeID());

  std::string small;
  small.resize(
      EpochStoreEpochMetaDataFormat::sizeInLinearBuffer(zk_record, nid) - 1);

  rv = EpochStoreEpochMetaDataFormat::toLinearBuffer(
      zk_record, &small[0], small.size(), nid);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(E::NOBUFS, err);

  // should return E::EMPTY for empty record
  zbuf[0] = '\0';
  rv = EpochStoreEpochMetaDataFormat::fromLinearBuffer(
      zbuf,
      32,
      &record_from_buffer,
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      &nid_from_buffer);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ(E::EMPTY, err);
}

// test a corner case where the binary portion of znode buffer
// starts with something like "N0:0"
TEST_F(EpochMetaDataTest, ZookeeperEpochStoreCornerCase) {
  auto cfg = parseConfig();
  auto zk_record = genValidEpochMetaData();
  zk_record.h.epoch = epoch_t(809119822);
  ASSERT_TRUE(zk_record.isValid());
  char zbuf[ZookeeperEpochStore::ZNODE_VALUE_WRITE_LEN_MAX];
  EpochMetaData record_from_buffer;
  NodeID nid_from_buffer;
  int rv = EpochStoreEpochMetaDataFormat::toLinearBuffer(
      zk_record, zbuf, sizeof(zbuf) / sizeof(zbuf[0]), folly::none);
  int expected_size =
      EpochStoreEpochMetaDataFormat::sizeInLinearBuffer(zk_record, folly::none);
  EXPECT_EQ(expected_size, rv);
  rv = EpochStoreEpochMetaDataFormat::fromLinearBuffer(
      zbuf,
      rv,
      &record_from_buffer,
      logid_t(1),
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      &nid_from_buffer);
  EXPECT_EQ(0, rv);
  EXPECT_TRUE(zk_record == record_from_buffer);
  EXPECT_FALSE(nid_from_buffer.isNodeID());
}

TEST_F(EpochMetaDataTest, EpochMetaDataUpdaterTest) {
  std::shared_ptr<Configuration> cfg = parseConfig();
  ASSERT_NE(nullptr, cfg.get());
  ASSERT_NE(nullptr, cfg.get()->serverConfig());
  ASSERT_NE(nullptr, cfg.get()->logsConfig());
  auto logcfg = cfg->getLogGroupByIDShared(logid_t(2));
  LogsConfig::LogAttributes& attrs =
      const_cast<LogsConfig::LogAttributes&>(logcfg->attrs());
  auto selector = std::make_shared<TestNodeSetSelector>();
  CustomEpochMetaDataUpdater updater(
      cfg, cfg->getNodesConfigurationFromServerConfigSource(), selector, true);

  auto zk_record_default = std::make_unique<EpochMetaData>();
  auto rv =
      updater(logid_t(2), zk_record_default, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::FAILED, rv);
  EXPECT_EQ(E::EMPTY, err);

  EpochMetaData original_meta = genValidEpochMetaData(
      logcfg->attrs().syncReplicationScope().asOptional());
  auto zk_record = std::make_unique<EpochMetaData>(original_meta);
  auto replication_conf_changed_at = zk_record->replication_conf_changed_at;
  auto initial_epoch_incremented_at = zk_record->epoch_incremented_at;
  rv = updater(logid_t(9999),
               zk_record,
               /* MetaDataTracer */ nullptr); // log 9999 is not provisioned
  EXPECT_EQ(EpochMetaData::UpdateResult::FAILED, rv);
  EXPECT_EQ(E::NOTFOUND, err);
  selector->setStorageSet(StorageSet{N3, N4, N5});
  rv = updater(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::UNCHANGED, rv);
  EXPECT_EQ(original_meta, *zk_record);
  EXPECT_EQ(original_meta.toString(), zk_record->toString());
  EXPECT_EQ(
      replication_conf_changed_at, zk_record->replication_conf_changed_at);
  EXPECT_EQ(initial_epoch_incremented_at, zk_record->epoch_incremented_at);

  // change replication factor, expect metadata to be updated
  attrs.set_replicationFactor(1);
  rv = updater(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  EXPECT_TRUE(zk_record->isValid());
  EXPECT_NE(genValidEpochMetaData(
                logcfg->attrs().syncReplicationScope().asOptional()),
            *zk_record);
  // epoch should remain the same
  EXPECT_EQ(
      genValidEpochMetaData(logcfg->attrs().syncReplicationScope().asOptional())
          .h.epoch.val_,
      zk_record->h.epoch.val_);
  EXPECT_EQ(zk_record->h.effective_since.val_, zk_record->h.epoch.val_);
  EXPECT_EQ(
      replication_conf_changed_at, zk_record->replication_conf_changed_at);
  EXPECT_EQ(initial_epoch_incremented_at, zk_record->epoch_incremented_at);

  // change sync_replication_scope, expect metadata to be updated
  *zk_record = genValidEpochMetaData(NodeLocationScope::NODE);
  initial_epoch_incremented_at = zk_record->epoch_incremented_at;
  attrs.set_syncReplicationScope(NodeLocationScope::RACK);
  rv = updater(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  EXPECT_TRUE(zk_record->isValid());
  EXPECT_NE(genValidEpochMetaData(NodeLocationScope::NODE), *zk_record);
  // epoch should remain the same
  EXPECT_EQ(genValidEpochMetaData(NodeLocationScope::NODE).h.epoch.val_,
            zk_record->h.epoch.val_);
  EXPECT_EQ(zk_record->h.effective_since.val_, zk_record->h.epoch.val_);
  EXPECT_EQ(
      replication_conf_changed_at, zk_record->replication_conf_changed_at);
  EXPECT_EQ(initial_epoch_incremented_at, zk_record->epoch_incremented_at);
  replication_conf_changed_at = zk_record->replication_conf_changed_at;

  // change nodeset, expect metadata to be updated
  StorageSet new_storage_set{N2, N4, N6};
  *zk_record = genValidEpochMetaData(
      logcfg->attrs().syncReplicationScope().asOptional());
  initial_epoch_incremented_at = zk_record->epoch_incremented_at;
  selector->setStorageSet(new_storage_set);
  rv = updater(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  EXPECT_TRUE(zk_record->isValid());
  EXPECT_EQ(
      genValidEpochMetaData(logcfg->attrs().syncReplicationScope().asOptional())
          .h.epoch.val_,
      zk_record->h.epoch.val_);
  EXPECT_EQ(zk_record->h.effective_since.val_, zk_record->h.epoch.val_);
  EXPECT_EQ(new_storage_set, zk_record->shards);
  EXPECT_TRUE(replication_conf_changed_at <
              zk_record->replication_conf_changed_at);
  EXPECT_EQ(initial_epoch_incremented_at, zk_record->epoch_incremented_at);

  // test metadata initial provision
  CustomEpochMetaDataUpdater provisioning_updater(
      cfg,
      cfg->getNodesConfigurationFromServerConfigSource(),
      selector,
      true,
      true /* provision_if_empty */,
      false /* update_if_exists */);
  zk_record.reset();
  StorageSet shards{N1, N2, N3};
  attrs.set_replicationFactor(2);
  attrs.set_nodeSetSize(3);
  selector->setStorageSet(shards);
  rv =
      provisioning_updater(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::CREATED, rv);
  EXPECT_TRUE(zk_record->isValid());
  EXPECT_EQ(EPOCH_MIN, zk_record->h.epoch);
  EXPECT_EQ(EPOCH_MIN, zk_record->h.effective_since);
  EXPECT_EQ(shards, zk_record->shards);
  EXPECT_EQ(ReplicationProperty::fromLogAttributes(logcfg->attrs()).toString(),
            zk_record->replication.toString());
}

TEST_F(EpochMetaDataTest, EpochMetaDataUpdateToNextEpochTest) {
  std::shared_ptr<Configuration> cfg = parseConfig();
  ASSERT_NE(nullptr, cfg.get());
  ASSERT_NE(nullptr, cfg.get()->serverConfig());
  ASSERT_NE(nullptr, cfg.get()->logsConfig());
  auto logcfg = cfg->getLogGroupByIDShared(logid_t(2));
  EpochMetaDataUpdateToNextEpoch updater(
      cfg, cfg->getNodesConfigurationFromServerConfigSource());

  auto zk_record_default = std::make_unique<EpochMetaData>();
  auto rv =
      updater(logid_t(2), zk_record_default, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::FAILED, rv);
  EXPECT_EQ(E::FAILED, err);

  auto zk_record = std::make_unique<EpochMetaData>(genValidEpochMetaData(
      logcfg->attrs().syncReplicationScope().asOptional()));
  rv = updater(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  auto cmp = genValidEpochMetaData(
      logcfg->attrs().syncReplicationScope().asOptional());
  EXPECT_NE(cmp.epoch_incremented_at, zk_record->epoch_incremented_at);
  cmp.epoch_incremented_at = zk_record->epoch_incremented_at;
  ++cmp.h.epoch.val_;
  EXPECT_EQ(cmp, *zk_record);

  EpochMetaDataUpdateToNextEpoch updater_conditional(
      cfg,
      cfg->getNodesConfigurationFromServerConfigSource(),
      nullptr,
      cmp.h.epoch);

  rv = updater_conditional(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION, rv);
  EXPECT_EQ(cmp.epoch_incremented_at, zk_record->epoch_incremented_at);
  ++cmp.h.epoch.val_;
  EXPECT_EQ(cmp, *zk_record);

  rv = updater_conditional(logid_t(2), zk_record, /* MetaDataTracer */ nullptr);
  EXPECT_EQ(EpochMetaData::UpdateResult::FAILED, rv);
  EXPECT_EQ(E::ABORTED, err);
}

#define ASSERT_METADATA_RANGE(_map, _low, _high, _metadata)                  \
  do {                                                                       \
    for (uint32_t e = (_low); e < (_high); ++e) {                            \
      auto metadata = (_map)->getEpochMetaData(epoch_t(e));                  \
      ASSERT_NE(nullptr, metadata);                                          \
      const EpochMetaData& mref = (_map)->getValidEpochMetaData(epoch_t(e)); \
      ASSERT_EQ(*metadata, mref);                                            \
      ASSERT_EQ(_metadata, *metadata);                                       \
    }                                                                        \
  } while (0)

TEST_F(EpochMetaDataTest, EpochMetaDataMapBasic) {
  auto cfg = parseConfig();
  const logid_t logid = logid_t(1);
  std::map<epoch_t, EpochMetaData> epochs;
  epochs[epoch_t(7)] = genValidEpochMetaData(NodeLocationScope::RACK, 7, 7);
  epochs[epoch_t(10)] = genValidEpochMetaData(NodeLocationScope::NODE, 10, 10);
  epochs[epoch_t(11)] = genValidEpochMetaData(NodeLocationScope::NODE, 11, 11);
  epochs[epoch_t(23)] = genValidEpochMetaData(NodeLocationScope::RACK, 23, 23);
  epochs[epoch_t(77)] = genValidEpochMetaData(NodeLocationScope::RACK, 77, 77);

  auto gen_map = [](const std::map<epoch_t, EpochMetaData>& m) {
    return std::make_shared<const Map>(m);
  };

  // empty map
  auto result = EpochMetaDataMap::create(gen_map(Map()), epoch_t(20));
  ASSERT_EQ(nullptr, result);

  // bad map
  EpochMetaDataMap::Map bad_map;
  bad_map[epoch_t(7)] = epochs[epoch_t(10)];
  result = EpochMetaDataMap::create(gen_map(bad_map), epoch_t(20));
  ASSERT_EQ(nullptr, result);

  // bad effective until
  EpochMetaDataMap::Map good_map;
  good_map[epoch_t(7)] = epochs[epoch_t(7)];
  result = EpochMetaDataMap::create(gen_map(good_map), epoch_t(6));
  ASSERT_EQ(nullptr, result);

  // bad metadata
  ASSERT_TRUE(good_map[epoch_t(7)].isValid());
  good_map[epoch_t(7)].h.effective_since = epoch_t(999);
  ASSERT_FALSE(good_map[epoch_t(7)].isValid());
  result = EpochMetaDataMap::create(gen_map(good_map), epoch_t(99999));
  ASSERT_EQ(nullptr, result);

  // success
  good_map[epoch_t(7)] = epochs[epoch_t(7)];
  result = EpochMetaDataMap::create(gen_map(good_map), epoch_t(7));
  ASSERT_NE(nullptr, result);
  ASSERT_EQ(good_map, *result->getMetaDataMap());
  ASSERT_EQ(epoch_t(7), result->getEffectiveUntil());
  ASSERT_METADATA_RANGE(result, 0, 7, epochs[epoch_t(7)]);
  ASSERT_EQ(nullptr, result->getEpochMetaData(epoch_t(8)));
  ASSERT_EQ(E::INVALID_PARAM, err);

  // success 2
  result = EpochMetaDataMap::create(gen_map(epochs), epoch_t(95));
  ASSERT_NE(nullptr, result);
  ASSERT_EQ(epochs, *result->getMetaDataMap());
  ASSERT_EQ(epoch_t(95), result->getEffectiveUntil());
  ASSERT_METADATA_RANGE(result, 0, 10, epochs[epoch_t(7)]);
  ASSERT_METADATA_RANGE(result, 10, 11, epochs[epoch_t(10)]);
  ASSERT_METADATA_RANGE(result, 11, 23, epochs[epoch_t(11)]);
  ASSERT_METADATA_RANGE(result, 23, 77, epochs[epoch_t(23)]);
  ASSERT_METADATA_RANGE(result, 77, 95, epochs[epoch_t(77)]);
  ASSERT_EQ(nullptr, result->getEpochMetaData(epoch_t(96)));
  ASSERT_EQ(E::INVALID_PARAM, err);

  // serialzation and deserialization
  char buffer[4096];
  int written = result->serialize(buffer, 4096);
  ASSERT_GT(written, 0);
  ASSERT_EQ(written, result->sizeInLinearBuffer());

  size_t bytes_read;
  auto deserialized_result = EpochMetaDataMap::deserialize(
      {buffer, 4096},
      &bytes_read,
      logid,
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource());

  ASSERT_NE(nullptr, deserialized_result);
  ASSERT_EQ(written, bytes_read);
  ASSERT_EQ(epochs, *deserialized_result->getMetaDataMap());
  ASSERT_EQ(epoch_t(95), deserialized_result->getEffectiveUntil());
}

TEST_F(EpochMetaDataTest, UnionStorageSet) {
  auto cfg = parseConfig();
  std::map<epoch_t, EpochMetaData> epochs;
  epochs[epoch_t(7)] = genValidEpochMetaData(NODE, 7, 7, {N1, N2}, 2);
  epochs[epoch_t(10)] = genValidEpochMetaData(RACK, 10, 10, {N2, N3}, 2);
  epochs[epoch_t(11)] = genValidEpochMetaData(NODE, 11, 11, {N3, N4}, 1);
  epochs[epoch_t(23)] = genValidEpochMetaData(RACK, 23, 23, {N0, N4, N5}, 2);
  epochs[epoch_t(77)] =
      genValidEpochMetaData(RACK, 77, 77, {N7, ShardID(19, 1)}, 2);

  auto result = EpochMetaDataMap::create(
      std::make_shared<const Map>(epochs), epoch_t(95));
  ASSERT_NE(nullptr, result);
  auto s = result->getUnionStorageSet(EPOCH_MIN, epoch_t(99));
  ASSERT_EQ(nullptr, s);
  ASSERT_EQ(E::INVALID_PARAM, err);

  s = result->getUnionStorageSet(EPOCH_MIN, epoch_t(8));
  auto expected = StorageSet{N1, N2};
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(EPOCH_MIN, epoch_t(10));
  expected = StorageSet{N1, N2, N3};
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(EPOCH_MIN, epoch_t(11));
  expected = StorageSet{N1, N2, N3, N4};
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(EPOCH_MIN, epoch_t(77));
  expected = StorageSet{N0, N1, N2, N3, N4, N5, N7, ShardID(19, 1)};
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(EPOCH_MIN, epoch_t(95));
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(epoch_t(9), epoch_t(22));
  expected = StorageSet{N1, N2, N3, N4};
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(epoch_t(11), epoch_t(21));
  expected = StorageSet{N3, N4};
  ASSERT_EQ(expected, *s);

  s = result->getUnionStorageSet(
      *cfg->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      EPOCH_MIN,
      epoch_t(95));
  // should filter non-exist or non-storage nodes
  expected = StorageSet{N1, N2, N3, N4, N5, N7};
  ASSERT_EQ(expected, *s);

  auto r = result->getNarrowestReplication(EPOCH_MIN, epoch_t(95));
  ASSERT_NE(nullptr, r);
  auto r_expected = ReplicationProperty(1, NODE);
  ASSERT_EQ(r_expected, *r);
}

TEST_F(EpochMetaDataTest, ExtendMap) {
  auto cfg = parseConfig();
  std::map<epoch_t, EpochMetaData> epochs;
  epochs[epoch_t(7)] = genValidEpochMetaData(NODE, 7, 7, {N1, N2}, 2);
  epochs[epoch_t(10)] = genValidEpochMetaData(RACK, 10, 10, {N2, N3}, 2);
  epochs[epoch_t(11)] = genValidEpochMetaData(NODE, 11, 11, {N3, N4}, 1);
  epochs[epoch_t(23)] = genValidEpochMetaData(RACK, 23, 23, {N0, N4, N5}, 2);
  epochs[epoch_t(77)] =
      genValidEpochMetaData(RACK, 77, 77, {N7, ShardID(19, 1)}, 2);
  auto result = EpochMetaDataMap::create(
      std::make_shared<const Map>(epochs), epoch_t(81));
  ASSERT_NE(nullptr, result);
  auto until_92 = result->withNewEffectiveUntil(epoch_t(92));
  ASSERT_NE(nullptr, until_92);
  ASSERT_EQ(epoch_t(92), until_92->getEffectiveUntil());
  auto until_77 = result->withNewEffectiveUntil(epoch_t(77));
  ASSERT_NE(nullptr, until_77);
  ASSERT_EQ(epoch_t(77), until_77->getEffectiveUntil());
  auto until_60 = result->withNewEffectiveUntil(epoch_t(60));
  ASSERT_EQ(nullptr, until_60);
  auto new_entry = result->withNewEntry(
      genValidEpochMetaData(NODE, 81, 81, {N0, N4}, 2), epoch_t(90));
  ASSERT_EQ(nullptr, new_entry);
  auto new_metadata = genValidEpochMetaData(NODE, 82, 82, {N0, N4}, 2);
  new_entry = result->withNewEntry(new_metadata, epoch_t(97));
  ASSERT_NE(nullptr, new_entry);
  ASSERT_EQ(epoch_t(97), new_entry->getEffectiveUntil());
  ASSERT_METADATA_RANGE(new_entry, 82, 97, new_metadata);
}

}} // namespace facebook::logdevice
