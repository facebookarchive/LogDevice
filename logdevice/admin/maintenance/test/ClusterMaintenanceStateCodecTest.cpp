/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Demangle.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "thrift/lib/cpp2/protocol/BinaryProtocol.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

using namespace ::testing;
using namespace apache::thrift;
using namespace facebook::logdevice;
using namespace facebook::logdevice::thrift;
using namespace facebook::logdevice::maintenance;

MaintenanceDefinition genDefinition() {
  auto def1 = MaintenanceDefinition();
  def1.set_user("Automation");

  auto node1 = thrift::NodeID();
  node1.set_node_index(1);

  auto node2 = thrift::NodeID();
  node2.set_node_index(2);

  auto node3 = thrift::NodeID();
  auto node3_addr = thrift::SocketAddress();
  // this node will be matched by address.
  node3_addr.set_address("127.0.0.9");
  node3_addr.set_address_family(thrift::SocketAddressFamily::INET);
  node3.set_address(node3_addr);

  auto shard1 = thrift::ShardID();
  shard1.set_node(node1);
  shard1.set_shard_index(0);

  auto shard2 = thrift::ShardID();
  shard2.set_node(node2);
  shard2.set_shard_index(0);
  def1.set_shards({shard1, shard2});
  def1.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // A single group
  def1.set_group(true);
  // Group ID will be defined and set by the maintenance RSM.
  def1.set_group_id("911");
  return def1;
}

TEST(ClusterMaintenanceStateCodecTest, StateSerialization) {
  auto cluster_state = std::make_unique<thrift::ClusterMaintenanceState>();
  cluster_state->set_maintenances({genDefinition()});
  cluster_state->set_version(222);

  std::string payload =
      ThriftCodec::serialize<BinarySerializer>(*cluster_state);
  auto restored =
      ThriftCodec::deserialize<BinarySerializer, ClusterMaintenanceState>(
          Slice::fromString(payload));

  ASSERT_NE(nullptr, restored);
  ASSERT_EQ(222, restored->get_version());

  ASSERT_EQ(1, restored->get_maintenances().size());
}

TEST(ClusterMaintenanceStateCodecTest, BadDeserialize) {
  std::string payload = "Garbage Payload";
  auto restored =
      ThriftCodec::deserialize<BinarySerializer, ClusterMaintenanceState>(
          Slice::fromString(payload));
  ASSERT_EQ(nullptr, restored);
  ASSERT_EQ(E::BADMSG, err);
}

TEST(ClusterMaintenanceStateCodecTest, DeltaSerialization) {
  MaintenanceDelta apply_maintenance;
  MaintenanceDefinition definition = genDefinition();
  apply_maintenance.set_apply_maintenances({definition});

  std::string payload =
      ThriftCodec::serialize<BinarySerializer>(apply_maintenance);

  ASSERT_NE(0, payload.size());

  auto restored = ThriftCodec::deserialize<BinarySerializer, MaintenanceDelta>(
      Slice::fromString(payload));
  ASSERT_NE(nullptr, restored);
  ASSERT_EQ(MaintenanceDelta::Type::apply_maintenances, restored->getType());

  ASSERT_EQ(apply_maintenance, *restored);
  ASSERT_EQ(1, restored->get_apply_maintenances().size());
  ASSERT_EQ(definition, restored->get_apply_maintenances()[0]);
}
