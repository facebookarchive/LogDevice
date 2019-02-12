/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Conv.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Reader.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using IntegrationTestUtils::RocksDBType;

class ReadPastGlobalLastReleasedTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool /* mutablePerEpochLogMetadataEnabled */,
                     bool /* enable_record_cache */,
                     RocksDBType>> {};

TEST_P(ReadPastGlobalLastReleasedTest, RecoveryStuck) {
  static constexpr const char* LOG_LEVEL = "info";
  static constexpr logid_t LOG_ID{1};
  static constexpr size_t NNODES = 6;
  static constexpr std::chrono::seconds READ_TIMEOUT{2};
  static constexpr size_t NRECORDS = 10;

  static_assert(
      NRECORDS >= 4, "NRECORDS needs to be at least 4 for test to work");

  // Get parameters.
  const bool mutable_per_epoch_log_metadata_enabled = std::get<0>(GetParam());
  const bool enable_record_cache = std::get<1>(GetParam());
  const auto rocks_db_type = std::get<2>(GetParam());

  // Set desired log level.
  dbg::parseLoglevelOption(LOG_LEVEL);

  // Make sure metadata log is on a node that won't fail.
  IntegrationTestUtils::ClusterFactory factory;

  // Set metadata config fields
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({node_index_t(NNODES - 1)},
                               /*replication_factor*/ 1,
                               NodeLocationScope::NODE);
  factory.setMetaDataLogsConfig(meta_config);

  factory.setRocksDBType(rocks_db_type);

  // Create cluster, with mutable per-epoch log metadata and record cache
  // enabled or disabled, as given by test parameters.
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(NNODES -
                                                                       1);
  log_attrs.set_mutablePerEpochLogMetadataEnabled(
      mutable_per_epoch_log_metadata_enabled);
  factory.setLogAttributes(log_attrs);
  auto cluster = factory
                     .setParam("--enable-record-cache",
                               folly::to<std::string>(enable_record_cache))
                     .setParam("--loglevel", LOG_LEVEL)
                     .create(NNODES);
  cluster->waitForRecovery();

  // Create client and synchronous tail reader.
  std::shared_ptr<Client> client(cluster->createClient());
  std::unique_ptr<Reader> reader(client->createReader(1));
  reader->setTimeout(READ_TIMEOUT);
  std::vector<std::unique_ptr<DataRecord>> records;
  facebook::logdevice::GapRecord gap;

  // Append two records.
  lsn_t begin_lsn = client->appendSync(LOG_ID, "1");
  client->appendSync(LOG_ID, "2");

  // Read the records back.
  reader->startReading(LOG_ID, begin_lsn, LSN_MAX);
  ssize_t nread = reader->read(SIZE_MAX, &records, &gap);
  ASSERT_EQ(2, nread);

  // Kill three storage nodes and replace sequencer. Recovery will get stuck.
  cluster->getNode(1).kill();
  cluster->getNode(2).kill();
  cluster->getNode(3).kill();
  cluster->replace(0, false); // sequencer

  // Expect reader to be stuck. Too many dead nodes in node set, recovery stuck.
  nread = reader->read(SIZE_MAX, &records, &gap);
  ASSERT_EQ(0, nread);
  reader->stopReading(LOG_ID);

  // Append NRECORDS-2 more records, will land in next epoch.
  begin_lsn = client->appendSync(LOG_ID, "3");
  for (int i = 4; i <= NRECORDS; ++i) {
    client->appendSync(LOG_ID, std::to_string(i));
  }

  // Expect to be able to read everything written if mutable per-epoch log
  // metadata or the record cache are enabled.  (In those cases CatchupOneStream
  // should be able to read the latest LNG from the metadata or the record
  // cache, which are updated on receival of per-epoch RELEASE messages.)
  //
  // Otherwise, we only have the STORE messages to read LNGs from. We should be
  // able to read at least one record, but nothing is guaranteed beyond that.
  if (mutable_per_epoch_log_metadata_enabled || enable_record_cache) {
    // Expect all records can be read.
    reader->setTimeout(testTimeout());
    reader->startReading(LOG_ID, begin_lsn, LSN_MAX);
    size_t nread_rem = NRECORDS - 2;
    do {
      nread = reader->read(nread_rem, &records, &gap);
      ASSERT_LE(nread, nread_rem);
      ASSERT_NE(0, nread);
      nread_rem -= nread;
    } while (nread_rem);
  } else {
    // Expect at least one record can be read.
    reader->setTimeout(testTimeout());
    reader->startReading(LOG_ID, begin_lsn, LSN_MAX);
    nread = reader->read(1, &records, &gap);
    ASSERT_EQ(1, nread);
  }
  reader->stopReading(LOG_ID);

  // Finally, try reading from an epoch that does not exist yet. Should block
  // (but not crash).
  begin_lsn = compose_lsn(
      epoch_t(lsn_to_epoch(begin_lsn).val_ + 1), lsn_to_esn(begin_lsn));
  reader->stopReading(LOG_ID);
  reader->setTimeout(READ_TIMEOUT);
  reader->startReading(LOG_ID, begin_lsn, LSN_MAX);
  nread = reader->read(1, &records, &gap);
  ASSERT_EQ(0, nread);
  reader->stopReading(LOG_ID);
}

INSTANTIATE_TEST_CASE_P(
    Parametric,
    ReadPastGlobalLastReleasedTest,
    ::testing::Combine(
        ::testing::Bool() /* mutablePerEpochLogMetadataEnabled */,
        ::testing::Bool() /* enable_record_cache */,
        ::testing::Values(RocksDBType::SINGLE, RocksDBType::PARTITIONED)));
