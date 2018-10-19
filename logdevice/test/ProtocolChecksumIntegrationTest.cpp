/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/SocketTest_fixtures.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

/**
 * @file Tests for Protocol layer Checksumming.
 */

using namespace facebook::logdevice;

class ProtocolChecksumIntegrationTest : public IntegrationTestBase {};

TEST_F(ProtocolChecksumIntegrationTest, ChecksummingEnabled) {
  const int NNODES = 4;
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setParam("--checksumming-enabled", "true")
                     .create(NNODES);

  for (node_index_t i = 0; i < NNODES; ++i) {
    cluster->getNode(i).waitUntilAvailable();
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  for (node_index_t i = 0; i < NNODES; ++i) {
    EXPECT_GT(cluster->getNode(i).stats()["protocol_checksum_matched"], 0);
    EXPECT_EQ(cluster->getNode(i).stats()["protocol_checksum_mismatch"], 0);
  }
}

TEST_F(ProtocolChecksumIntegrationTest, ChecksummingDisabled) {
  const int NNODES = 4;
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setParam("--checksumming-enabled", "false")
                     .create(NNODES);

  for (node_index_t i = 0; i < NNODES; ++i) {
    cluster->getNode(i).waitUntilAvailable();
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  for (node_index_t i = 0; i < NNODES; ++i) {
    EXPECT_EQ(cluster->getNode(i).stats()["protocol_checksum_matched"], 0);
    EXPECT_EQ(cluster->getNode(i).stats()["protocol_checksum_mismatch"], 0);
  }
}

TEST_F(ProtocolChecksumIntegrationTest, TestBlacklistedMessages) {
  const int NNODES = 4;
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  ld_check(sizeof(MessageType) == sizeof(char));
  std::string all_messages;
  for (int c = 1; c < 128; c++) {
    all_messages += c;
  }
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .useHashBasedSequencerAssignment()
          .setParam("--checksumming-enabled", "true")
          .setParam("--checksumming-blacklisted-messages", all_messages)
          .create(NNODES);

  for (node_index_t i = 0; i < NNODES; ++i) {
    cluster->getNode(i).waitUntilAvailable();
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  for (node_index_t i = 0; i < NNODES; ++i) {
    EXPECT_EQ(cluster->getNode(i).stats()["protocol_checksum_matched"], 0);
    EXPECT_EQ(cluster->getNode(i).stats()["protocol_checksum_mismatch"], 0);
  }
}
