/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <thread>
#include <unistd.h>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class ByteOffsetTest : public IntegrationTestBase {};

TEST_F(ByteOffsetTest, InBandByteOffsetBasic) {
  const int NRECORDS_PER_EPOCH = 20;
  const int RECORD_SIZE = 10;
  const logid_t LOG_ID(1);
  const epoch_t start_epoch(2);

  Configuration::Nodes nodes;
  for (int i = 0; i < 4; ++i) {
    auto& node = nodes[i];

    node.generation = 1;
    if (i == 0) {
      node.addSequencerRole();
    } else {
      node.addStorageRole(/*num_shards*/ 2);
    }
  }

  Configuration::NodesConfig nodes_config(nodes);

  // set replication factor for metadata log to be 2
  // otherwise recovery cannot complete for metadata log
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes.size(), 2);

  // set byte-offset-interval as one record size so that offset_within_epoch
  // will be written with each record.
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .setParam("--byte-offsets")
                     .setParam("--log-state-recovery-interval", "0ms")
                     .setMetaDataLogsConfig(meta_config)
                     .create(4);

  std::shared_ptr<const Configuration> config = cluster->getConfig()->get();
  ld_check(config->serverConfig()->getNode(0)->isSequencingEnabled());

  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();

  // write some records
  lsn_t first_lsn = LSN_INVALID;
  auto write_records = [&] {
    for (int i = 0; i < NRECORDS_PER_EPOCH; ++i) {
      std::string data;
      for (int j = 0; j < RECORD_SIZE; ++j) {
        data += 'a' + folly::Random::rand32() % 26;
      }
      lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
      if (first_lsn == LSN_INVALID) {
        first_lsn = lsn;
      }
      EXPECT_NE(LSN_INVALID, lsn);
    };
  };
  write_records();

  const size_t max_logs = 1;
  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;

  // We don't know if recovery for LogStorageState already happened and epoch
  // offset for start_epoch is known. But reading some data will trigger
  // LogStorageState recovery if epoch offset is unknown. We need to wait until
  // GetSeqStateRequests are finished.
  wait_until("GetSeqStateRequests are finished and epoch offset is propagated "
             "to storage nodes.",
             [&]() {
               records.clear();
               std::unique_ptr<Reader> reader(client->createReader(max_logs));
               reader->includeByteOffset();
               reader->startReading(LOG_ID, first_lsn);
               auto nread = reader->read(NRECORDS_PER_EPOCH, &records, &gap);
               EXPECT_EQ(nread, NRECORDS_PER_EPOCH);
               if (records[0]->attrs.offsets.isValid()) {
                 for (int i = 0; i < NRECORDS_PER_EPOCH; ++i) {
                   const DataRecord& r = *records[i];
                   ld_info("lsn %s", lsn_to_string(r.attrs.lsn).c_str());
                   EXPECT_EQ(
                       RecordOffset({{BYTE_OFFSET, (i + 1) * RECORD_SIZE}}),
                       r.attrs.offsets);
                 }
                 return true;
               }
               return false;
             });
}
