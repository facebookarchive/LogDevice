/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/event_log/EventLogRebuildingSet.h"

#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"

/**
 * These tests validate the behavior of EventLogRebuildingSet and especially how
 * it computes the authoritative status of shards in the cluster.
 */

namespace facebook { namespace logdevice {

static const size_t kNumNodes{5};
static const size_t kNumShards{2};

namespace {

std::shared_ptr<UpdateableConfig> config;

static UpdateableSettings<Settings> settings;

std::shared_ptr<UpdateableConfig> buildConfig() {
  configuration::Nodes nodes;
  for (node_index_t nid = 0; nid < kNumNodes; ++nid) {
    Configuration::Node& node = nodes[nid];
    node.address = Sockaddr("::1", folly::to<std::string>(4440 + nid));
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(kNumShards);
  }
  Configuration::NodesConfig nodes_config(std::move(nodes));
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 2);
  auto config = std::make_shared<UpdateableConfig>();
  config->updateableServerConfig()->update(
      ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config));
  return config;
}

class EventLogRebuildingSetTest : public ::testing::Test {
 public:
  EventLogRebuildingSetTest() {
    dbg::assertOnData = true;
    config_ = buildConfig();
  }

  const std::shared_ptr<ServerConfig> getServerConfig() const {
    return config_->getServerConfig();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

 private:
  std::shared_ptr<UpdateableConfig> config_;
};

} // namespace

#define ASSERT_EVENTS_EQ(expected, got, T)                                \
  {                                                                       \
    ASSERT_EQ(                                                            \
        *dynamic_cast<const T*>(expected), *dynamic_cast<const T*>(got)); \
  }

#define ASSERT_SHARD_STATUS(set, nid, sid, status)                        \
  {                                                                       \
    const auto nc = buildConfig()                                         \
                        ->getServerConfig()                               \
                        ->getNodesConfigurationFromServerConfigSource();  \
    auto map = set.toShardStatusMap(*nc);                                 \
    EXPECT_EQ(AuthoritativeStatus::status, map.getShardStatus(nid, sid)); \
  }

#define EVENT(set, name, ...)                   \
  {                                             \
    name##_Header h = {__VA_ARGS__};            \
    auto e = std::make_shared<name##_Event>(h); \
    set.update(set.getLastSeenLSN() + 1,        \
               std::chrono::milliseconds(),     \
               *e,                              \
               *getNodesConfiguration());       \
  }

#define DIRTY_EVENT(set, rmm, ...)                                \
  {                                                               \
    SHARD_NEEDS_REBUILD_Header h = {__VA_ARGS__};                 \
    auto e = std::make_shared<SHARD_NEEDS_REBUILD_Event>(h, rmm); \
    set.update(set.getLastSeenLSN() + 1,                          \
               std::chrono::milliseconds(),                       \
               *e,                                                \
               *getNodesConfiguration());                         \
  }

::std::ostream& operator<<(::std::ostream& os, const AuthoritativeStatus& s) {
  os << toString(s);
  return os;
}

TEST_F(EventLogRebuildingSetTest, Simple) {
  EventLogRebuildingSet set;

  // N1 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{1});
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);

  // N2 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{2});
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);

  // N1 acks rebuilding
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(3));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);

  // N3 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{3});
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);

  // N3 acks rebuilding
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(6));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // N2 acks rebuilding
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(6));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
}

TEST_F(EventLogRebuildingSetTest, Simple2) {
  EventLogRebuildingSet set;

  // N1 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{1});
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);

  // All donors rebuild N1
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // N2 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{2});
  // N1 should be considered AUTHORITATIVE_EMPTY and N2 UNDERREPLICATION
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);

  // One donor rebuilds N2
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(8), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);

  // N3 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{3});
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);

  // One donor rebuilds N2/N3
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(10), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);

  // Another donor rebuilds N2/N3
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(10), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // N3 acks
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(10));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // N1 acks
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(10));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // N2 acks
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(10));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);

  ASSERT_TRUE(set.empty());
}

TEST_F(EventLogRebuildingSetTest, Simple3) {
  EventLogRebuildingSet set;

  // N3 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{3});
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);

  // N4 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{4}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{4});
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);

  // All donors rebuild N3 (but not N4). It's possible to receive the messages
  // in this order if donors finish rebuilding N3 before they see N4 needs to be
  // rebuilt as well.
  // N3 should not end-up empty because these donors skipped some records that
  // they thought N4 should rebuild.
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);

  // Actually, N4 had time to rebuild N3 right before it got rebuilt.
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);

  // N0 starts rebuilding. N1, N2 are donors
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{0});
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);

  // All donors rebuild everything.
  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(9), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(9), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // N0 and N3 acknowledge rebuilding
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(9));
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(9));
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // N4 restarts rebuilding...
  EVENT(set,
        SHARD_NEEDS_REBUILD,
        node_index_t{4},
        uint32_t{0},
        "",
        "",
        SHARD_NEEDS_REBUILD_Header::FORCE_RESTART);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{4});
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);

  // all nodes complete
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(15), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(15), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(15), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(15), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // N4 acks rebuildings
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(15));
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{5}, uint32_t{0}, FULLY_AUTHORITATIVE);

  ASSERT_TRUE(set.empty());
}

TEST_F(EventLogRebuildingSetTest, Simple4) {
  EventLogRebuildingSet set;

  // N0 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{0}, uint32_t{0});
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);

  // N4 rebuilt
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);

  // N1 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0});
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);

  // N2 and N3 rebuild as well
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(4), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(4), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);

  // N4 rebuilt as well
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(4), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
}

TEST_F(EventLogRebuildingSetTest, Simple5) {
  EventLogRebuildingSet set;

  // N0 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{0});
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);

  // N4 rebuilt
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);

  // N4 starts rebuilding
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{4}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{4});
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);

  // N1,N2,N3 rebuilt
  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNDERREPLICATION);
}

TEST_F(EventLogRebuildingSetTest, Simple6) {
  EventLogRebuildingSet set;

  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{0}, uint32_t{0});
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0});
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{3}, uint32_t{0});
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(6), 0u);

  // Underreplicated because N2 only rebuilt data of N0
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
}

TEST_F(EventLogRebuildingSetTest, Simple7) {
  EventLogRebuildingSet set;

  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{4}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, AUTHORITATIVE_EMPTY);
}

TEST_F(EventLogRebuildingSetTest, Simple8) {
  EventLogRebuildingSet set;

  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);

  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);

  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{4}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, UNAVAILABLE);
}

TEST_F(EventLogRebuildingSetTest, Abort) {
  EventLogRebuildingSet set;

  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}, "", "", 0);

  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);

  EVENT(set, SHARD_ABORT_REBUILD, node_index_t{2}, uint32_t{0}, lsn_t(3));

  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);

  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(4));
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(4));

  // We aborted rebuilding for node 2, verify that we wait for N2 to participate
  // in rebuilding of the new rebuilding set before changing the authoritative
  // status of N0 and N1 to AUTHORITATIVE_EMPTY.
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);

  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(4));

  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(set, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{4}, uint32_t{0}, FULLY_AUTHORITATIVE);
}

TEST_F(EventLogRebuildingSetTest, Relocate) {
  EventLogRebuildingSet set;

  // N1 is marked for draining in relocate mode
  EVENT(
      set,
      SHARD_NEEDS_REBUILD,
      node_index_t{1},
      uint32_t{0},
      "",
      "",
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // All donors rebuild N1
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // Including N1 since we are in RELOCATE and N1 serves as a donor
  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // Node is returned to normal service
  EVENT(set, SHARD_UNDRAIN, node_index_t{1}, uint32_t{0});

  // Node remains AUTHORITATIVE_EMPTY until it acks.
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  // N1 acks
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(1));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  ASSERT_TRUE(set.empty());
}

TEST_F(EventLogRebuildingSetTest, DirtyRanges) {
  EventLogRebuildingSet set;
  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));

  // N1 requests a rebuild of it's dirty ranges.
  RebuildingRangesMetadata rmm;
  rmm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  DIRTY_EVENT(set, &rmm, node_index_t{1}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // All donors rebuild N1
  EVENT(set, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(1), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(1), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // N1 acks
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(1));
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  ASSERT_TRUE(set.empty());
}

TEST_F(EventLogRebuildingSetTest, MiniRebuildingRemovedAfterAck) {
  EventLogRebuildingSet set;
  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));

  // N1 requests a rebuild of it's dirty ranges.
  RebuildingRangesMetadata rmm;
  rmm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  DIRTY_EVENT(set, &rmm, node_index_t{1}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // N0 goes down and is added to rebuilding set
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  EVENT(set, SHARD_UNRECOVERABLE, node_index_t{0});

  // All donors rebuild N0 and N1
  EVENT(set, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t(2), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t(2), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNDERREPLICATION);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t(2), 0u);
  EVENT(set, SHARD_IS_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(2), 0u);
  ASSERT_SHARD_STATUS(set, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);

  // N1 acks
  EVENT(set, SHARD_ACK_REBUILT, node_index_t{1}, uint32_t{0}, lsn_t(2));
  // N1 is no longer in rebuilding set
  ld_check(!set.getNodeInfo(node_index_t{1}, uint32_t{0}));
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, AUTHORITATIVE_EMPTY);

  ASSERT_TRUE(!set.empty());
}

TEST_F(EventLogRebuildingSetTest, DowngradeToMiniRebuilding) {
  EventLogRebuildingSet set;
  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));

  // N0 goes down and is added to rebuilding set
  EVENT(set, SHARD_NEEDS_REBUILD, node_index_t{0}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, UNAVAILABLE);

  // N0 comes back up and requests a rebuild of just it's dirty ranges.
  RebuildingRangesMetadata rmm;
  rmm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  DIRTY_EVENT(set, &rmm, node_index_t{0}, uint32_t{0}, "", "", 0);
  ASSERT_SHARD_STATUS(set, node_index_t{0}, uint32_t{0}, FULLY_AUTHORITATIVE);
}

}} // namespace facebook::logdevice
