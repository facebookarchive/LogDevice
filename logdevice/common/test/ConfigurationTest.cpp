/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/Configuration.h"

#include <memory>
#include <string>

#include <arpa/inet.h>
#include <folly/FBString.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/test/JsonTestUtil.h>
#include <gtest/gtest.h>
#include <netinet/in.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

// NOTE: file reading assumes the test is being run from the top-level fbcode
// dir

using facebook::logdevice::AuthenticationType;
using facebook::logdevice::ConfigParserOptions;
using facebook::logdevice::Configuration;
using facebook::logdevice::E;
using facebook::logdevice::err;
using facebook::logdevice::LOGID_INVALID;
using facebook::logdevice::logid_range_t;
using facebook::logdevice::logid_t;
using facebook::logdevice::node_index_t;
using facebook::logdevice::NodeID;
using facebook::logdevice::NodeLocation;
using facebook::logdevice::NodeLocationScope;
using facebook::logdevice::PermissionCheckerType;
using facebook::logdevice::Semaphore;
using facebook::logdevice::Sockaddr;
using facebook::logdevice::Status;
using facebook::logdevice::configuration::SecurityConfig;

using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice;

/**
 * Reads configs/sample_valid.conf and asserts some basic stuff.
 */
TEST(ConfigurationTest, SimpleValid) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(config, nullptr);

  const auto& nodes = config->serverConfig()->getNodes();
  ASSERT_EQ(
      true,
      (bool)std::dynamic_pointer_cast<LocalLogsConfig>(config->logsConfig()));
  ASSERT_TRUE(config->logsConfig()->isLocal());
  auto logs_config = config->localLogsConfig();

  ASSERT_EQ(5, nodes.size());

  // N0 and N5 are sequencer nodes. The list of sequencers is padded to size 43.
  const auto& seq_locator_config =
      config->serverConfig()
          ->getNodesConfigurationFromServerConfigSource()
          ->getSequencersConfig();
  EXPECT_EQ(43, seq_locator_config.nodes.size());
  EXPECT_EQ(43, seq_locator_config.weights.size());
  for (int i = 0; i <= 42; ++i) {
    if (i == 0) {
      EXPECT_EQ(NodeID(0, 3), seq_locator_config.nodes[i]);
      EXPECT_EQ(1, seq_locator_config.weights[i]);
    } else if (i == 5) {
      EXPECT_EQ(NodeID(5, 2), seq_locator_config.nodes[i]);
      EXPECT_EQ(1, seq_locator_config.weights[i]);
    } else if (i == 6) {
      EXPECT_EQ(NodeID(6, 2), seq_locator_config.nodes[i]);
      EXPECT_EQ(1, seq_locator_config.weights[i]);
    } else if (i == 42) {
      EXPECT_EQ(NodeID(42, 5), seq_locator_config.nodes[i]);
      EXPECT_EQ(1, seq_locator_config.weights[i]);
    } else {
      EXPECT_EQ(NodeID(), seq_locator_config.nodes[i]);
      EXPECT_EQ(0, seq_locator_config.weights[i]);
    }
  }

  // LogTracer Default Sampling Rate (percentage)
  ASSERT_FALSE(config->serverConfig()
                   ->getTracerSamplePercentage("UNKNOWN_TRACER")
                   .hasValue());
  EXPECT_DOUBLE_EQ(20.0, config->serverConfig()->getDefaultSamplePercentage());

  EXPECT_DOUBLE_EQ(
      15.4,
      config->serverConfig()->getTracerSamplePercentage("appender").value());

  char buf[256];

  {
    const Configuration::Node& node = nodes.at(0);
    EXPECT_EQ(AF_INET, node.address.family());

    ASSERT_EQ("server-0", node.name);
    struct sockaddr_storage ss;
    int len = node.address.toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    auto aptr = reinterpret_cast<const sockaddr_in*>(&ss);
    const char* result =
        inet_ntop(aptr->sin_family, &aptr->sin_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("127.0.0.1", buf);
    int expected_port = htons(4444);
    EXPECT_EQ(expected_port, aptr->sin_port);

    EXPECT_TRUE(node.ssl_address);
    EXPECT_EQ(AF_INET, node.ssl_address->family());

    len = node.ssl_address->toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    aptr = reinterpret_cast<const sockaddr_in*>(&ss);
    result = inet_ntop(aptr->sin_family, &aptr->sin_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("127.0.0.1", buf);
    expected_port = htons(4446);
    EXPECT_EQ(expected_port, aptr->sin_port);

    EXPECT_EQ(configuration::StorageState::READ_WRITE, node.getStorageState());
    EXPECT_EQ(1, node.storage_attributes->capacity);
    EXPECT_EQ(1, node.getWritableStorageCapacity());

    EXPECT_EQ(3, node.generation);
    EXPECT_TRUE(node.isSequencingEnabled());

    EXPECT_TRUE(node.location.hasValue());
    const NodeLocation& location = node.location.value();
    EXPECT_EQ("ash", location.getLabel(NodeLocationScope::REGION));
    EXPECT_EQ("ash2", location.getLabel(NodeLocationScope::DATA_CENTER));
    EXPECT_EQ("08", location.getLabel(NodeLocationScope::CLUSTER));
    EXPECT_EQ("k", location.getLabel(NodeLocationScope::ROW));
    EXPECT_EQ("z", location.getLabel(NodeLocationScope::RACK));
    EXPECT_EQ("ash.ash2.08.k.z", node.locationStr());
  }

  {
    const Configuration::Node& node = nodes.at(1);
    EXPECT_EQ(AF_INET6, node.address.family());

    ASSERT_EQ("server-1", node.name);
    struct sockaddr_storage ss;
    int len = node.address.toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    auto aptr = reinterpret_cast<const sockaddr_in6*>(&ss);
    const char* result =
        inet_ntop(aptr->sin6_family, &aptr->sin6_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("::1", buf);
    int expected_port = htons(6666);
    EXPECT_EQ(expected_port, aptr->sin6_port);

    EXPECT_TRUE(node.ssl_address);
    EXPECT_EQ(AF_INET6, node.ssl_address->family());

    len = node.ssl_address->toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    aptr = reinterpret_cast<const sockaddr_in6*>(&ss);
    result = inet_ntop(aptr->sin6_family, &aptr->sin6_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("::1", buf);
    expected_port = htons(6670);
    EXPECT_EQ(expected_port, aptr->sin6_port);

    EXPECT_EQ(configuration::StorageState::READ_WRITE,
              node.storage_attributes->state);
    EXPECT_EQ(4, node.storage_attributes->capacity);
    EXPECT_EQ(4, node.getWritableStorageCapacity());

    EXPECT_EQ(6, node.generation);
    EXPECT_FALSE(node.isSequencingEnabled());
    EXPECT_FALSE(node.location.hasValue());

    EXPECT_FALSE(node.hasRole(NodeRole::SEQUENCER));
    EXPECT_TRUE(node.hasRole(NodeRole::STORAGE));
  }

  {
    const Configuration::Node& node = nodes.at(5);
    EXPECT_EQ(AF_INET6, node.address.family());

    ASSERT_EQ("server-5", node.name);
    struct sockaddr_storage ss;
    int len = node.address.toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    auto aptr = reinterpret_cast<const sockaddr_in6*>(&ss);
    const char* result =
        inet_ntop(aptr->sin6_family, &aptr->sin6_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("::1", buf);
    int expected_port = htons(6669);
    EXPECT_EQ(expected_port, aptr->sin6_port);

    EXPECT_TRUE(node.ssl_address);
    EXPECT_EQ(AF_INET6, node.ssl_address->family());

    len = node.ssl_address->toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    aptr = reinterpret_cast<const sockaddr_in6*>(&ss);
    result = inet_ntop(aptr->sin6_family, &aptr->sin6_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("::1", buf);
    expected_port = htons(6673);
    EXPECT_EQ(expected_port, aptr->sin6_port);

    EXPECT_EQ(configuration::StorageState::READ_WRITE,
              node.storage_attributes->state);
    EXPECT_EQ(2, node.storage_attributes->capacity);
    EXPECT_EQ(2, node.getWritableStorageCapacity());

    EXPECT_EQ(2, node.generation);
    EXPECT_TRUE(node.isSequencingEnabled());

    ASSERT_TRUE(node.location.hasValue());
    EXPECT_EQ("ash.ash2.07.a.b", node.locationStr());

    EXPECT_TRUE(node.hasRole(NodeRole::SEQUENCER));
    EXPECT_TRUE(node.hasRole(NodeRole::STORAGE));
  }

  {
    const Configuration::Node& node = nodes.at(42);
    EXPECT_EQ(AF_INET6, node.address.family());

    ASSERT_EQ("server-42", node.name);
    struct sockaddr_storage ss;
    int len = node.address.toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    auto aptr = reinterpret_cast<const sockaddr_in6*>(&ss);
    const char* result =
        inet_ntop(aptr->sin6_family, &aptr->sin6_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("::1", buf);
    int expected_port = htons(6668);
    EXPECT_EQ(expected_port, aptr->sin6_port);

    EXPECT_TRUE(node.ssl_address);
    EXPECT_EQ(AF_INET6, node.ssl_address->family());

    len = node.ssl_address->toStructSockaddr(&ss);
    ASSERT_NE(len, -1);
    aptr = reinterpret_cast<const sockaddr_in6*>(&ss);
    result = inet_ntop(aptr->sin6_family, &aptr->sin6_addr, buf, sizeof buf);
    EXPECT_NE(result, nullptr);
    EXPECT_STREQ("::1", buf);
    expected_port = htons(6672);
    EXPECT_EQ(expected_port, aptr->sin6_port);

    EXPECT_EQ(configuration::StorageState::READ_WRITE,
              node.storage_attributes->state);
    EXPECT_EQ(4, node.storage_attributes->capacity);
    EXPECT_EQ(4, node.getWritableStorageCapacity());
    EXPECT_EQ(5, node.generation);
    EXPECT_TRUE(node.isSequencingEnabled());

    ASSERT_TRUE(node.location.hasValue());
    EXPECT_EQ("ash.ash2.07.a.b", node.locationStr());
  }

  EXPECT_EQ(nullptr, config->getLogGroupByIDShared(logid_t(7)));
  EXPECT_FALSE(config->logsConfig()->logExists(logid_t(7)));
  {
    Semaphore sem;
    std::shared_ptr<const LogsConfig::LogGroupNode> async_log_cfg;
    config->getLogGroupByIDAsync(
        logid_t(7),
        [&](const std::shared_ptr<const LogsConfig::LogGroupNode> log) {
          async_log_cfg = log;
          sem.post();
        });
    sem.wait();
    EXPECT_EQ(nullptr, async_log_cfg);
  }

  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      config->getLogGroupByIDShared(logid_t(3));
  EXPECT_TRUE(log);
  auto log_shared = config->getLogGroupByIDShared(logid_t(3));
  ASSERT_NE(log, nullptr);
  ASSERT_NE(log_shared, nullptr);
  ASSERT_TRUE(*log == *log_shared);
  ASSERT_TRUE(config->logsConfig()->logExists(logid_t(3)));
  {
    Semaphore sem;
    std::shared_ptr<const LogsConfig::LogGroupNode> async_log_cfg;
    config->getLogGroupByIDAsync(
        logid_t(3),
        [&](const std::shared_ptr<const LogsConfig::LogGroupNode> clog) {
          async_log_cfg = clog;
          sem.post();
        });
    sem.wait();
    EXPECT_TRUE(*log == *async_log_cfg);
  }

  EXPECT_EQ(3, log->attrs().replicationFactor().value());
  EXPECT_EQ(3, log->attrs().extraCopies().value());
  EXPECT_EQ(10, log->attrs().maxWritesInFlight().value());
  EXPECT_TRUE(log->attrs().singleWriter().value());
  EXPECT_EQ(
      NodeLocationScope::REGION, log->attrs().syncReplicationScope().value());
  // all fields in the config should be recognized.
  ASSERT_FALSE(log->attrs().extras().hasValue());

  auto check_log = [&](logid_t::raw_type log,
                       const logsconfig::LogGroupInDirectory& gid_log) {
    const Configuration::LogAttributes& log_attrs = gid_log.log_group->attrs();
    EXPECT_FALSE(log_attrs.backlogDuration().value().hasValue());
    EXPECT_EQ(log >= 11, log_attrs.replicateAcross().hasValue());
    if (log >= 8 && log <= 10) {
      EXPECT_EQ(3, log_attrs.replicationFactor());
      EXPECT_EQ(2, log_attrs.extraCopies());
      EXPECT_EQ(0, log_attrs.syncedCopies());
      EXPECT_EQ(10, log_attrs.maxWritesInFlight());
    }
    if (log != 3) {
      ASSERT_FALSE(log_attrs.syncReplicationScope().hasValue());
    }
  };

  {
    const int NLOGS = 9;
    logid_t::raw_type logids[NLOGS] = {1, 2, 3, 8, 9, 10, 11, 12, 13};
    int i = 0;

    for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd();
         ++it) {
      ASSERT_LT(i, NLOGS);
      EXPECT_EQ(it->first, logids[i]);

      check_log(logids[i], it->second);

      i++;
    }
    ASSERT_EQ(i, NLOGS);

    i = NLOGS - 1;

    for (auto it = logs_config->logsRBegin(); it != logs_config->logsREnd();
         ++it) {
      EXPECT_EQ(it->first, logids[i]);

      check_log(logids[i], it->second);

      --i;
    }
    ASSERT_EQ(i, -1);
  }

  {
    auto log = logs_config->getLogGroup("/with_replicate_across1");
    EXPECT_EQ(2, log->attrs().replicationFactor().value());
    EXPECT_FALSE(log->attrs().syncReplicationScope().hasValue());
    EXPECT_EQ(Configuration::LogAttributes::ScopeReplicationFactors(
                  {{NodeLocationScope::NODE, 2}}),
              log->attrs().replicateAcross().value());
    EXPECT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
              ReplicationProperty::fromLogAttributes(log->attrs()).toString());
  }

  {
    auto log = logs_config->getLogGroup("/with_replicate_across2");
    EXPECT_EQ(4, log->attrs().replicationFactor().value());
    EXPECT_FALSE(log->attrs().syncReplicationScope().hasValue());
    EXPECT_EQ(Configuration::LogAttributes::ScopeReplicationFactors(
                  {{NodeLocationScope::RACK, 3}}),
              log->attrs().replicateAcross().value());
    EXPECT_EQ(ReplicationProperty(
                  {{NodeLocationScope::NODE, 4}, {NodeLocationScope::RACK, 3}})
                  .toString(),
              ReplicationProperty::fromLogAttributes(log->attrs()).toString());
  }

  {
    auto log = logs_config->getLogGroup("/with_replicate_across3");
    EXPECT_FALSE(log->attrs().replicationFactor().hasValue());
    EXPECT_FALSE(log->attrs().syncReplicationScope().hasValue());
    EXPECT_EQ(
        Configuration::LogAttributes::ScopeReplicationFactors(
            {{NodeLocationScope::REGION, 2}, {NodeLocationScope::RACK, 3}}),
        log->attrs().replicateAcross().value());
    EXPECT_EQ(ReplicationProperty({{NodeLocationScope::REGION, 2},
                                   {NodeLocationScope::RACK, 3}})
                  .toString(),
              ReplicationProperty::fromLogAttributes(log->attrs()).toString());
  }

  {
    const auto meta_nodeset = config->serverConfig()->getMetaDataNodeIndices();
    const LogsConfig::LogAttributes& meta_attrs =
        config->serverConfig()->getMetaDataLogGroup()->attrs();
    const std::vector<node_index_t> expected_nodeset{0, 1, 5};
    EXPECT_EQ(expected_nodeset, meta_nodeset);
    EXPECT_EQ(2, meta_attrs.replicationFactor().value());
    EXPECT_EQ(2, meta_attrs.syncedCopies().value());
    EXPECT_EQ(2, meta_attrs.maxWritesInFlight().value());
    EXPECT_EQ(
        NodeLocationScope::CLUSTER, meta_attrs.syncReplicationScope().value());
    auto& ml_conf = config->serverConfig()->getMetaDataLogsConfig();
    EXPECT_TRUE(ml_conf.metadata_version_to_write.hasValue());
    EXPECT_EQ(1, ml_conf.metadata_version_to_write.value());
    EXPECT_EQ(NodeSetSelectorType::SELECT_ALL, ml_conf.nodeset_selector_type);
  }

  ASSERT_NE(nullptr, config->zookeeperConfig());
  const std::string zookeeper_quorum =
      config->zookeeperConfig()->getQuorumString();

  EXPECT_EQ(zookeeper_quorum, "1.2.3.4:2181,5.6.7.8:2181,9.10.11.12:2181");

  const std::chrono::milliseconds zookeeper_timeout =
      config->zookeeperConfig()->getSessionTimeout();

  EXPECT_EQ(zookeeper_timeout, std::chrono::milliseconds(30000));
  EXPECT_NE(config->serverConfig()->getCustomFields(), nullptr);
  EXPECT_EQ(config->serverConfig()->getCustomFields().size(), 1);
  folly::dynamic fields =
      folly::dynamic::object("custom_field_for_testing", "custom_value");
  EXPECT_EQ(config->serverConfig()->getCustomFields(), fields);
  auto timestamp = config->serverConfig()->getClusterCreationTime();
  EXPECT_TRUE(timestamp.hasValue());
  EXPECT_EQ(timestamp.value().count(), 1467928224);
}

/**
 * Attempts to load configs with overlapping log id ranges must fail
 * with E::INVALID_CONFIG.
 */
TEST(ConfigurationTest, OverlappingLogIdRanges) {
  using facebook::logdevice::E;
  using facebook::logdevice::err;

  std::shared_ptr<Configuration> config;

  config = Configuration::fromJsonFile(TEST_CONFIG_FILE("overlap1.conf"));
  EXPECT_EQ(config->logsConfig(), nullptr);
  EXPECT_EQ(err, E::INVALID_CONFIG);

  config = Configuration::fromJsonFile(TEST_CONFIG_FILE("overlap2.conf"));
  EXPECT_EQ(config->logsConfig(), nullptr);
  EXPECT_EQ(err, E::INVALID_CONFIG);

  config = Configuration::fromJsonFile(TEST_CONFIG_FILE("overlap3.conf"));
  EXPECT_EQ(config->logsConfig(), nullptr);
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

/**
 * Attempts to load configs with invalid roles config and checks that
 * they fail with E::INVALID_CONFIG.
 */
TEST(ConfigurationTest, InvalidRolesConfig) {
  using facebook::logdevice::E;
  using facebook::logdevice::err;

  std::shared_ptr<Configuration> config;

  config =
      Configuration::fromJsonFile(TEST_CONFIG_FILE("invalid_roles_empty.conf"));
  EXPECT_EQ(err, E::INVALID_CONFIG);

  config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_roles_unknown.conf"));
  EXPECT_EQ(err, E::INVALID_CONFIG);

  config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_roles_not_array.conf"));
  EXPECT_EQ(err, E::INVALID_CONFIG);

  config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_generation_with_non_storage_role.conf"));
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

/**
 * Attempts to load configs with properties that are only available
 * for some roles and ensure they fail with E::INVALID_CONFIG.
 */
TEST(ConfigurationTest, InvalidPropertiesRoles) {
  using facebook::logdevice::E;
  using facebook::logdevice::err;

  std::shared_ptr<Configuration> config;

  config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_properties_storage1.conf"));
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

/**
 * Exercises getNode().
 */
TEST(ConfigurationTest, LookupNodeByID) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(config, nullptr);

  using facebook::logdevice::E;
  using facebook::logdevice::err;

  const Configuration::Node* node;

  // Search for the first node (index=0).  Lookup should succeed with the
  // right generation.
  node = config->serverConfig()->getNode(NodeID(0, 3));
  EXPECT_NE(node, nullptr);
  EXPECT_EQ(
      configuration::StorageState::READ_WRITE, node->storage_attributes->state);
  EXPECT_EQ(1, node->storage_attributes->capacity);
  EXPECT_EQ(1, node->getWritableStorageCapacity());
  EXPECT_EQ(nullptr, config->serverConfig()->getNode(NodeID(0, 2)));
  EXPECT_EQ(E::NOTFOUND, err);
  EXPECT_EQ(nullptr, config->serverConfig()->getNode(NodeID(0, 4)));
  EXPECT_EQ(E::NOTFOUND, err);

  // Same for the second node
  node = config->serverConfig()->getNode(NodeID(1, 6));
  EXPECT_NE(node, nullptr);
  EXPECT_EQ(
      configuration::StorageState::READ_WRITE, node->storage_attributes->state);
  EXPECT_EQ(4, node->storage_attributes->capacity);
  EXPECT_EQ(4, node->getWritableStorageCapacity());
  EXPECT_EQ(nullptr, config->serverConfig()->getNode(NodeID(1, 5)));
  EXPECT_EQ(E::NOTFOUND, err);
  EXPECT_EQ(nullptr, config->serverConfig()->getNode(NodeID(1, 7)));
  EXPECT_EQ(E::NOTFOUND, err);

  node = config->serverConfig()->getNode(NodeID(5, 2));
  EXPECT_NE(node, nullptr);
  EXPECT_EQ(
      configuration::StorageState::READ_WRITE, node->storage_attributes->state);
  EXPECT_EQ(2, node->storage_attributes->capacity);
  EXPECT_EQ(2, node->getWritableStorageCapacity());
  EXPECT_EQ(nullptr, config->serverConfig()->getNode(NodeID(5, 1)));
  EXPECT_EQ(E::NOTFOUND, err);
  EXPECT_EQ(nullptr, config->serverConfig()->getNode(NodeID(5, 100)));
  EXPECT_EQ(E::NOTFOUND, err);

  node = config->serverConfig()->getNode(42);
  EXPECT_NE(node, nullptr);
  EXPECT_EQ(
      configuration::StorageState::READ_WRITE, node->storage_attributes->state);
  EXPECT_EQ(4, node->storage_attributes->capacity);
  EXPECT_EQ(4, node->getWritableStorageCapacity());
}

TEST(ConfigurationTest, SSLNodeToNode) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(config, nullptr);

// The int conversions below are used to circumvent an issue where
// ASSERT_EQ(<bool>, ...) doesn't build on gcc
#define TEST_NODE_TO_NODE_SSL(scope, exp_result)                               \
  do {                                                                         \
    const auto& nodes = config->serverConfig()->getNodes();                    \
    NodeID nid0 = NodeID(0, nodes.at(0).generation);                           \
    NodeID nid2 = NodeID(5, nodes.at(5).generation);                           \
    const auto nc =                                                            \
        config->serverConfig()->getNodesConfigurationFromServerConfigSource(); \
    ASSERT_EQ((int)exp_result,                                                 \
              configuration::nodes::getNodeSSL(                                \
                  *nc, nodes.at(0).location, nid2.index(), scope));            \
    ASSERT_EQ((int)exp_result,                                                 \
              configuration::nodes::getNodeSSL(                                \
                  *nc, nodes.at(5).location, nid0.index(), scope));            \
  } while (0)

  const auto& nodes = config->serverConfig()->getNodes();

  // Nodes in the same data center
  EXPECT_EQ("ash.ash2.08.k.z", nodes.at(0).locationStr());
  EXPECT_EQ("ash.ash2.07.a.b", nodes.at(5).locationStr());

  TEST_NODE_TO_NODE_SSL(NodeLocationScope::NODE, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::RACK, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::ROW, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::CLUSTER, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::DATA_CENTER, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::REGION, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::ROOT, false);

  // Nodes in different regions
  EXPECT_EQ(0,
            const_cast<NodeLocation&>(*nodes.at(5).location)
                .fromDomainString("lla.lla1.08.k.z"));

  auto recompute_config = [&]() {
    auto sc = config->serverConfig();
    config =
        std::make_shared<Configuration>(sc->withNodes(sc->getNodesConfig()),
                                        config->logsConfig(),
                                        config->zookeeperConfig());
    ld_check(config != nullptr);
  };

  recompute_config();
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::NODE, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::RACK, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::ROW, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::CLUSTER, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::DATA_CENTER, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::REGION, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::ROOT, false);

  // Nodes in the same rack
  EXPECT_EQ(0,
            const_cast<NodeLocation&>(
                *config->serverConfig()->getNodes().at(5).location)
                .fromDomainString("ash.ash2.08.k.z"));

  recompute_config();
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::NODE, true);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::RACK, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::ROW, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::CLUSTER, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::DATA_CENTER, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::REGION, false);
  TEST_NODE_TO_NODE_SSL(NodeLocationScope::ROOT, false);
}

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {
extern std::pair<std::string, std::string> parseIpPort(const std::string&);
}}}} // namespace facebook::logdevice::configuration::parser

/**
 * Exercises parseIpPort() which is a helper used during configuration
 * parsing.
 */
TEST(ConfigurationTest, ParseIpPort) {
  typedef std::pair<std::string, std::string> strpair;
  using facebook::logdevice::configuration::parser::parseIpPort;

  // Basic cases
  EXPECT_EQ(strpair("127.0.0.1", "12345"), parseIpPort("127.0.0.1:12345"));
  EXPECT_EQ(strpair("123::abc", "12345"), parseIpPort("[123::abc]:12345"));
  EXPECT_EQ(strpair("::1", "0"), parseIpPort("[::1]:0"));
  // unspecified address
  EXPECT_EQ(strpair("::", "80"), parseIpPort("[::]:80"));
  // mixed form
  EXPECT_EQ(strpair("0:0:0:0:0:FFFF:129.144.52.38", "12345"),
            parseIpPort("[0:0:0:0:0:FFFF:129.144.52.38]:12345"));

  // Error cases
  EXPECT_EQ(strpair(), parseIpPort(":8080"));              // no ip address
  EXPECT_EQ(strpair(), parseIpPort("]:8080"));             // no ip address
  EXPECT_EQ(strpair(), parseIpPort("[:8080"));             // no ip address
  EXPECT_EQ(strpair(), parseIpPort("[]:8080"));            // no ip address
  EXPECT_EQ(strpair(), parseIpPort("127.0.0.1"));          // no port
  EXPECT_EQ(strpair(), parseIpPort("127.0.0.1:"));         // no port
  EXPECT_EQ(strpair(), parseIpPort("127.0.0.a:12345"));    // illegal ipv4 digit
  EXPECT_EQ(strpair(), parseIpPort("[127::fffg]:12345"));  // illegal ipv6 digit
  EXPECT_EQ(strpair(), parseIpPort("123::abc:12345"));     // need brackets
  EXPECT_EQ(strpair(), parseIpPort("[123::abc]"));         // no port
  EXPECT_EQ(strpair(), parseIpPort("123.123.0.0]:12345")); // no open bracket
  EXPECT_EQ(strpair(), parseIpPort("[123::abc:123"));      // no closing bracket
  EXPECT_EQ(strpair(), parseIpPort("[123::abc]:"));        // no port
}

TEST(ConfigurationTest, Sockaddr) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(nullptr, config);

  const auto& nodes = config->serverConfig()->getNodes();

  ASSERT_TRUE(nodes.at(0).address.valid());
  ASSERT_TRUE(nodes.at(1).address.valid());

  EXPECT_EQ(4444, nodes.at(0).address.port());
  EXPECT_EQ(6666, nodes.at(1).address.port());

  EXPECT_EQ("127.0.0.1:4444", nodes.at(0).address.toString());
  EXPECT_EQ("[::1]:6666", nodes.at(1).address.toString());
}

TEST(ConfigurationTest, SockaddrFromString) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(nullptr, config);

  const auto& nodes = config->serverConfig()->getNodes();
  auto check_address = [](const facebook::logdevice::Sockaddr& a) {
    auto r = facebook::logdevice::Sockaddr::fromString(a.toString());
    EXPECT_TRUE(r.hasValue());
    EXPECT_EQ(a, r);
  };

  for (const auto& kv : nodes) {
    check_address(kv.second.address);
  }
}

TEST(ConfigurationTest, SockaddrDefaultConstructor) {
  facebook::logdevice::Sockaddr addr;
  ASSERT_FALSE(addr.valid());
  ASSERT_EQ("INVALID", addr.toString());

  ASSERT_FALSE(facebook::logdevice::Sockaddr::INVALID.valid());
}

TEST(ConfigurationTest, NamedRanges) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(nullptr, config);

  std::pair<logid_t, logid_t> foo_range =
      config->logsConfig()->getLogRangeByName("foo");
  EXPECT_EQ(foo_range.first, logid_t(8));
  EXPECT_EQ(foo_range.second, logid_t(10));

  std::pair<logid_t, logid_t> bar_range =
      config->logsConfig()->getLogRangeByName("bar");
  EXPECT_EQ(bar_range.first, LOGID_INVALID);
  EXPECT_EQ(bar_range.second, LOGID_INVALID);

  Semaphore sem;
  Status st;
  std::pair<logid_t, logid_t> foo_range2;
  config->logsConfig()->getLogRangeByNameAsync(
      "foo", [&](Status err, decltype(foo_range2) r) {
        foo_range2 = r;
        st = err;
        sem.post();
      });
  sem.wait();
  EXPECT_EQ(E::OK, st);
  EXPECT_EQ(foo_range, foo_range2);

  std::pair<logid_t, logid_t> no_range;
  config->logsConfig()->getLogRangeByNameAsync(
      "does_not_exist", [&](Status err, decltype(no_range) r) {
        no_range = r;
        st = err;
        sem.post();
      });
  sem.wait();
  EXPECT_EQ(E::NOTFOUND, st);
  EXPECT_EQ(std::make_pair(logid_t(0), logid_t(0)), no_range);
}

TEST(ConfigurationTest, DuplicateRangeNames) {
  using facebook::logdevice::E;
  using facebook::logdevice::err;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("dupname.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, DuplicateHost) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("duphost.conf")));
  ASSERT_EQ(nullptr, config);
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, DuplicateName) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("dup_server_name.conf")));
  ASSERT_EQ(nullptr, config);
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, NegativeReplicationFactor) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("negative_replication.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, ReplicationFactorTooBig) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("replication_too_big.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, DupMetaNodes) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("dup_metanodes.conf")));

  ASSERT_EQ(nullptr, config);
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, InvalidMetaDataNodeSet) {
  using namespace facebook::logdevice;
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_metadata_logs.conf")));
  EXPECT_EQ(err, E::INVALID_CONFIG);
  ASSERT_EQ(nullptr, config->logsConfig());
}

TEST(ConfigurationTest, InvalidNodeInMetaDataNodeSet) {
  using namespace facebook::logdevice;
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("bad_metadata_id.conf")));
  EXPECT_EQ(err, E::INVALID_CONFIG);
  ASSERT_EQ(nullptr, config->logsConfig());
}

TEST(ConfigurationTest, Defaults) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("defaults.conf")));

  ASSERT_NE(nullptr, config);
  std::shared_ptr<LogsConfig::LogGroupNode> log;

  // rate with no default override in cfg at all
  ASSERT_FALSE(config->serverConfig()
                   ->getTracerSamplePercentage("UNKNOWN_TRACER")
                   .hasValue());
  EXPECT_DOUBLE_EQ(0.1, config->serverConfig()->getDefaultSamplePercentage());

  log = config->getLogGroupByIDShared(logid_t(1));
  ASSERT_NE(nullptr, log);
  const auto& attrs = log->attrs();
  ASSERT_TRUE(config->logsConfig()->logExists(logid_t(1)));
  EXPECT_EQ(2, *attrs.replicationFactor());
  EXPECT_EQ(1, *attrs.extraCopies());
  EXPECT_EQ(0, *attrs.syncedCopies());
  EXPECT_EQ(1000, *attrs.maxWritesInFlight());
  EXPECT_EQ(0, *attrs.singleWriter());
  EXPECT_EQ(NodeLocationScope::RACK, *attrs.syncReplicationScope());
  EXPECT_FALSE(attrs.backlogDuration().value().hasValue());
  EXPECT_EQ(3, *attrs.nodeSetSize());
  EXPECT_EQ(std::chrono::milliseconds(10), *attrs.deliveryLatency());
  EXPECT_TRUE(*attrs.scdEnabled());

  auto range = config->logsConfig()->getLogRangeByName("weirdo");
  EXPECT_EQ(91, range.first.val_);
  EXPECT_EQ(9911, range.second.val_);

  log = config->getLogGroupByIDShared(logid_t(2));
  const auto& attrs2 = log->attrs();
  EXPECT_TRUE(attrs2.backlogDuration().value().hasValue());
  EXPECT_EQ(std::chrono::seconds(3600 * 24 * 4),
            attrs2.backlogDuration().value().value());

  log = config->getLogGroupByIDShared(logid_t(500));
  const auto& attrs3 = log->attrs();
  EXPECT_EQ(3, *attrs3.replicationFactor());
  EXPECT_EQ(0, *attrs3.extraCopies());
  EXPECT_EQ(0, *attrs3.syncedCopies());
  EXPECT_EQ(1001, *attrs3.maxWritesInFlight());
  EXPECT_TRUE(*attrs3.singleWriter());
  EXPECT_EQ(std::chrono::seconds(3600 * 24 * 5), *attrs3.backlogDuration());
  EXPECT_EQ(4, *attrs3.nodeSetSize());
  EXPECT_EQ(std::chrono::milliseconds(11), *attrs3.deliveryLatency());
  EXPECT_EQ(1, *attrs3.scdEnabled());

  EXPECT_NE(config->serverConfig()->getCustomFields(), nullptr);
  EXPECT_EQ(config->serverConfig()->getCustomFields().size(), 0);
  auto timestamp = config->serverConfig()->getClusterCreationTime();
  EXPECT_FALSE(timestamp.hasValue());
}

TEST(ConfigurationTest, BadID) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("bad_id.conf")));

  ASSERT_EQ(nullptr, config->logsConfig());
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

TEST(ConfigurationTest, SequencerWeights) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sequencer_weights.conf")));
  ASSERT_NE(nullptr, config.get());
  ASSERT_EQ(8, config->serverConfig()->getNodes().size());

  {
    // Validating the SequencerNodeAttributes structs
    std::vector<SequencerNodeAttributes> expected{
        {false, 0},
        {true, 1},
        {true, 2},
        {true, 4},
        {false, 8},
        {false, 8},
        {true, 1},
        {true, 0},
    };

    std::vector<SequencerNodeAttributes> got;
    const auto& nodes = config->serverConfig()->getNodes();
    for (short i = 0; i <= config->getNodesConfigurationFromServerConfigSource()
                               ->getMaxNodeIndex();
         i++) {
      node_index_t idx{i};
      if (nodes.find(idx) == nodes.end()) {
        continue;
      }
      got.push_back(*nodes.at(idx).sequencer_attributes);
    }

    EXPECT_EQ(expected, got);
  }

  {
    // Validating that SequencersConfig is properly calculated.
    const auto& seq_config = config->serverConfig()
                                 ->getNodesConfigurationFromServerConfigSource()
                                 ->getSequencersConfig();

    // check padded node IDs
    EXPECT_EQ(std::vector<NodeID>({NodeID(),
                                   NodeID(1, 1),
                                   NodeID(2, 1),
                                   NodeID(3, 1),
                                   NodeID(),
                                   NodeID(),
                                   NodeID(6, 1),
                                   NodeID()}),
              seq_config.nodes);

    // check normalized weights
    EXPECT_EQ(std::vector<double>({0, .25, .5, 1, 0, 0, 0.25, 0}),
              seq_config.weights);

    EXPECT_FALSE(config->serverConfig()->getNode(0)->isSequencingEnabled());
    EXPECT_TRUE(config->serverConfig()->getNode(1)->isSequencingEnabled());
    EXPECT_TRUE(config->serverConfig()->getNode(2)->isSequencingEnabled());
    EXPECT_TRUE(config->serverConfig()->getNode(3)->isSequencingEnabled());
    EXPECT_FALSE(config->serverConfig()->getNode(4)->isSequencingEnabled());
    EXPECT_FALSE(config->serverConfig()->getNode(5)->isSequencingEnabled());
    EXPECT_TRUE(config->serverConfig()->getNode(6)->isSequencingEnabled());
    EXPECT_FALSE(config->serverConfig()->getNode(7)->isSequencingEnabled());
  }
}

TEST(ConfigurationTest, Serialization) {
  std::string read_str =
      parser::readFileIntoString(TEST_CONFIG_FILE("serialization_test.conf"));
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("serialization_test.conf")));
  auto serialized_str = config->toString();
  FOLLY_EXPECT_JSON_EQ(read_str, serialized_str);
}

/**
 * Reads configs/included_logs.conf and checks if the log list from
 * configs/included_logs.inc was included.
 */
TEST(ConfigurationTest, IncludeLogs) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("included_logs.conf")));
  ASSERT_NE(nullptr, config);

  ASSERT_EQ(
      true,
      (bool)std::dynamic_pointer_cast<LocalLogsConfig>(config->logsConfig()));
  ASSERT_TRUE(config->logsConfig()->isLocal());
  auto& logs_config = config->localLogsConfig();
  {
    const int NLOGS = 6;
    logid_t::raw_type logids[NLOGS] = {1, 2, 3, 8, 9, 10};
    int i = 0;

    for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd();
         ++it) {
      ASSERT_LT(i, NLOGS);
      EXPECT_EQ(it->first, logids[i]);

      if (logids[i] >= 8 && logids[i] <= 10) {
        const logsconfig::LogGroupInDirectory& log = it->second;
        const Configuration::LogAttributes& log_attrs = log.log_group->attrs();
        EXPECT_EQ(3, log_attrs.replicationFactor());
        EXPECT_EQ(2, log_attrs.extraCopies());
        EXPECT_EQ(0, log_attrs.syncedCopies());
        EXPECT_EQ(10, log_attrs.maxWritesInFlight());
      }

      i++;
    }
    ASSERT_EQ(NLOGS, i);
  }

  EXPECT_NE(config->serverConfig()->getCustomFields(), nullptr);
  EXPECT_EQ(config->serverConfig()->getCustomFields().size(), 0);
  auto timestamp = config->serverConfig()->getClusterCreationTime();
  EXPECT_FALSE(timestamp.hasValue());
}

/*
 * Reads configs/included_logs.conf but doesn't include the log list.
 */

using RangeLookupMap = LogsConfig::NamespaceRangeLookupMap;

class TestLogsConfigStub : public LogsConfig {
  bool isLocal() const override {
    return false;
  }

  std::shared_ptr<LogsConfig::LogGroupNode>
  getLogGroupByIDShared(logid_t /* unused */) const override {
    return nullptr;
  }

  void getLogGroupByIDAsync(
      logid_t /* unused */,
      std::function<void(std::shared_ptr<LogsConfig::LogGroupNode>)> cb)
      const override {
    cb(nullptr);
  }

  bool logExists(logid_t /*id*/) const override {
    return false;
  }

  void getLogRangeByNameAsync(
      std::string,
      std::function<void(Status, std::pair<logid_t, logid_t>)> cb)
      const override {
    cb(E::NOTFOUND, std::make_pair(logid_t(0), logid_t(0)));
  }

  std::shared_ptr<logsconfig::LogGroupNode>
  getLogGroup(const std::string& /*path*/) const override {
    err = E::NOTFOUND;
    return nullptr;
  };

  void getLogRangesByNamespaceAsync(
      const std::string&,
      std::function<void(Status, RangeLookupMap)> cb) const override {
    cb(E::NOTFOUND, RangeLookupMap());
  }

  std::unique_ptr<LogsConfig> copy() const override {
    return std::unique_ptr<LogsConfig>(new TestLogsConfigStub(*this));
  }
};

TEST(ConfigurationTest, SkipIncludeLogs) {
  std::unique_ptr<LogsConfig> logs_config(new TestLogsConfigStub());
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("included_logs.conf"), std::move(logs_config)));
  ASSERT_NE(nullptr, config);
  ASSERT_FALSE(
      (bool)std::dynamic_pointer_cast<LocalLogsConfig>(config->logsConfig()));
  ASSERT_FALSE(config->logsConfig()->isLocal());

  bool callback_called = false;
  // This will actually get called synchronously
  config->getLogGroupByIDAsync(
      logid_t(1), [&](const std::shared_ptr<LogsConfig::LogGroupNode> ptr) {
        callback_called = true;
        ASSERT_EQ(nullptr, ptr.get());
      });
  ASSERT_TRUE(callback_called);
}

/**
 * Reads configs/invalid_included_logs1.conf and checks that the test fails
 */
TEST(ConfigurationTest, DoubleLogSpec) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_included_logs1.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
}

/**
 * Reads configs/invalid_included_logs2.conf and checks that the test fails
 */
TEST(ConfigurationTest, NonExistentIncludeLog) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_included_logs2.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
}

/**
 * Checks that logs with a hierarchical namespace structure work
 */
TEST(ConfigurationTest, LogsWithNamespaces) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("namespaced_logs.conf")));
  LogsConfig::NamespaceRangeLookupMap ranges;
  LogsConfig::NamespaceRangeLookupMap sub_ranges;

  ASSERT_NE(nullptr, config);
  auto range = config->logsConfig()->getLogRangeByName("ns1/sublog1");
  ASSERT_EQ(1, range.first.val_);
  ASSERT_EQ(1, range.second.val_);

  auto log = config->getLogGroupInDirectoryByIDRaw(range.first);
  ASSERT_EQ(2, log->log_group->attrs().replicationFactor().value());
  ranges[log->getFullyQualifiedName()] = range;

  range = config->logsConfig()->getLogRangeByName("ns1/ns2/subsublog1");
  ASSERT_EQ(2, range.first.val_);
  ASSERT_EQ(3, range.second.val_);

  log = config->getLogGroupInDirectoryByIDRaw(range.first);
  ASSERT_EQ(3, log->log_group->attrs().replicationFactor().value());
  ranges[log->getFullyQualifiedName()] = range;
  sub_ranges[log->getFullyQualifiedName()] = range;

  range = config->logsConfig()->getLogRangeByName("ns1/ns2/subsublog2");
  ASSERT_EQ(4, range.first.val_);
  ASSERT_EQ(4, range.second.val_);
  log = config->getLogGroupInDirectoryByIDRaw(range.first);
  ASSERT_EQ(1, log->log_group->attrs().replicationFactor().value());
  ranges[log->getFullyQualifiedName()] = range;
  sub_ranges[log->getFullyQualifiedName()] = range;

  range = config->logsConfig()->getLogRangeByName("log1");
  ASSERT_EQ(95, range.first.val_);
  ASSERT_EQ(100, range.second.val_);
  log = config->getLogGroupInDirectoryByIDRaw(range.first);
  ASSERT_EQ(1, log->log_group->attrs().replicationFactor().value());
  ranges[log->getFullyQualifiedName()] = range;

  range = config->logsConfig()->getLogRangeByName("log2");
  ASSERT_EQ(101, range.first.val_);
  ASSERT_EQ(101, range.second.val_);
  log = config->getLogGroupInDirectoryByIDRaw(range.first);
  ASSERT_EQ(1, log->log_group->attrs().replicationFactor().value());
  ranges[log->getFullyQualifiedName()] = range;

  // supposed to fetch all named logs
  auto fetched_ranges = config->logsConfig()->getLogRangesByNamespace("");
  ASSERT_EQ(5, fetched_ranges.size());
  ASSERT_EQ(ranges, fetched_ranges);

  Semaphore sem;
  Status st;
  config->logsConfig()->getLogRangesByNamespaceAsync(
      "", [&](Status err, decltype(ranges) r) {
        st = err;
        fetched_ranges = r;
        sem.post();
      });
  sem.wait();
  ASSERT_EQ(ranges, fetched_ranges);
  ASSERT_EQ(E::OK, st);

  config->logsConfig()->getLogRangesByNamespaceAsync(
      "does_not_exist", [&](Status err, decltype(ranges) r) {
        st = err;
        fetched_ranges = r;
        sem.post();
      });
  sem.wait();
  ASSERT_TRUE(fetched_ranges.empty());
  ASSERT_EQ(E::NOTFOUND, st);

  EXPECT_NE(config->serverConfig()->getCustomFields(), nullptr);
  EXPECT_EQ(config->serverConfig()->getCustomFields().size(), 0);
  auto timestamp = config->serverConfig()->getClusterCreationTime();
  EXPECT_FALSE(timestamp.hasValue());
}

/**
 * Checks that custom fields specified in log group config are read correctly
 */
TEST(ConfigurationTest, CustomFields) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("custom_fields.conf")));
  ASSERT_NE(nullptr, config);

  std::shared_ptr<LogsConfig::LogGroupNode> log =
      config->getLogGroupByIDShared(logid_t(1));
  ASSERT_NE(nullptr, log);
  ASSERT_EQ(1, log->attrs().extras().value().size());

  ASSERT_EQ("custom_value", log->attrs().extras().value().at("custom_key"));

  log = config->getLogGroupByIDShared(logid_t(2));
  ASSERT_NE(nullptr, log);
  ASSERT_EQ(1, log->attrs().extras().value().size());
  ASSERT_EQ("default_value", log->attrs().extras().value().at("custom_key"));

  EXPECT_NE(config->serverConfig()->getCustomFields(), nullptr);
  EXPECT_EQ(config->serverConfig()->getCustomFields().size(), 4);
  folly::dynamic customFields = folly::dynamic::object(
      "custom_field_test1", "custom_value1")(
      "custom_field_test2", "custom_value2")("custom_field_test3", 123)(
      "custom_field_test4", folly::dynamic::array("value_at_0", "value_at_1"));
  EXPECT_EQ(config->serverConfig()->getCustomFields(), customFields);
  auto timestamp = config->serverConfig()->getClusterCreationTime();
  EXPECT_FALSE(timestamp.hasValue());
}

template <typename T>
void test_wrapped_attribute(
    const logsconfig::Attribute<folly::Optional<T>>& val_a,
    const logsconfig::Attribute<folly::Optional<T>>& val_b) {
  ASSERT_EQ(val_a.hasValue(), val_b.hasValue());
  if (val_a.hasValue()) {
    ASSERT_EQ(val_a.value().hasValue(), val_b.value().hasValue());
    if (val_a.value().hasValue()) {
      ASSERT_EQ(*val_a.value(), *val_b.value());
    }
  }
}

TEST(ConfigurationTest, LogsConfigSerialization) {
  using namespace facebook::logdevice;

  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("defaults.conf")));

  ASSERT_NE(nullptr, config);
  const LogsConfig::LogGroupInDirectory* log;

  log = config->getLogGroupInDirectoryByIDRaw(logid_t(1));
  ASSERT_NE(nullptr, log);
  EXPECT_EQ(true, log->log_group->attrs().scdEnabled().value());
  std::string json = log->toJson();
  std::cout << json << std::endl;

  auto parsed = LogsConfig::LogGroupNode::createFromJson(json, "/");
  ASSERT_NE(parsed, nullptr);

  // Following are always present
  ASSERT_EQ(std::make_pair(logid_t(1), logid_t(1)), parsed->range());
  ASSERT_EQ(log->log_group->name(), parsed->name());
  ASSERT_EQ(*log->log_group->attrs().replicationFactor(),
            *parsed->attrs().replicationFactor());
  ASSERT_EQ(
      *log->log_group->attrs().extraCopies(), *parsed->attrs().extraCopies());
  ASSERT_EQ(
      *log->log_group->attrs().syncedCopies(), *parsed->attrs().syncedCopies());

  // Optional ones + double optional wrapping (to be fixed in T16481409)
  ASSERT_EQ(*log->log_group->attrs().maxWritesInFlight(),
            *parsed->attrs().maxWritesInFlight());
  ASSERT_EQ(
      *log->log_group->attrs().singleWriter(), *parsed->attrs().singleWriter());
  ASSERT_EQ(
      *log->log_group->attrs().scdEnabled(), *parsed->attrs().scdEnabled());
  ASSERT_EQ(*log->log_group->attrs().shadow(), *parsed->attrs().shadow());

  // Attributes which can be optional/null
  test_wrapped_attribute(
      log->log_group->attrs().backlogDuration(),
      parsed->attrs().backlogDuration()); /*
   test_wrapped_attribute(log->log_group->attrs().nodeSetSize(),
       parsed->attrs().nodeSetSize());
   test_wrapped_attribute(log->log_group->attrs().deliveryLatency(),
       parsed->attrs().deliveryLatency());
   test_wrapped_attribute(log->log_group->attrs().writeToken(),
       parsed->attrs().writeToken());
   test_wrapped_attribute(log->log_group->attrs().sequencerAffinity(),
       parsed->attrs().sequencerAffinity());*/
}

/**
 * Reads a config that has both ssl_host and ssl_port set for a node - should
 * fail.
 */
TEST(ConfigurationTest, NodeSSLDoubleConfigFail) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("ssl_double_fail.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Config with ssl set only for some of the hosts. Should fail.
 */
TEST(ConfigurationTest, PartialSSLNodes) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("partial_ssl_nodes.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Tests that parsing a valid config with security_info and permissions set
 */
TEST(ConfigurationTest, SecurityAndPermissionInfo) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("security_and_permission_info.conf")));
  ASSERT_NE(nullptr, config);

  // check that all security_information fields are set correctly
  ASSERT_TRUE(config->serverConfig()->allowUnauthenticated());
  ASSERT_EQ(AuthenticationType::SELF_IDENTIFICATION,
            config->serverConfig()->getAuthenticationType());
  ASSERT_EQ(PermissionCheckerType::CONFIG,
            config->serverConfig()->getPermissionCheckerType());

  ASSERT_TRUE(config->logsConfig()->logExists(logid_t(1)));
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      config->getLogGroupByIDShared(logid_t(1));
  ASSERT_NE(nullptr, log);
  const auto& permissions = log->attrs().permissions().value();

  ASSERT_EQ(4, permissions.size());

  // Check that custom permissions are intact
  auto it = permissions.find("allPass");
  ASSERT_TRUE(it != permissions.end());
  ASSERT_TRUE(it->second[0]);
  ASSERT_TRUE(it->second[1]);
  ASSERT_TRUE(it->second[2]);

  it = permissions.find("appendFail");
  ASSERT_TRUE(it != permissions.end());
  ASSERT_FALSE(it->second[0]);
  ASSERT_TRUE(it->second[1]);
  ASSERT_TRUE(it->second[2]);

  it = permissions.find("readFail");
  ASSERT_TRUE(it != permissions.end());
  ASSERT_TRUE(it->second[0]);
  ASSERT_FALSE(it->second[1]);
  ASSERT_TRUE(it->second[2]);

  it = permissions.find("trimFail");
  ASSERT_TRUE(it != permissions.end());
  ASSERT_TRUE(it->second[0]);
  ASSERT_TRUE(it->second[1]);
  ASSERT_FALSE(it->second[2]);

  // check that the default permissions are overwritten
  it = permissions.find("GlobalDefault");
  ASSERT_TRUE(it == permissions.end());

  it = permissions.find("unauthenticated");
  ASSERT_TRUE(it == permissions.end());

  // check that for logs that did not set any permissions, default permissions
  // propergate to them
  ASSERT_TRUE(config->logsConfig()->logExists(logid_t(11)));
  const std::shared_ptr<LogsConfig::LogGroupNode> log2 =
      config->getLogGroupByIDShared(logid_t(11));
  ASSERT_NE(nullptr, log2);

  const auto& permissions2 = log2->attrs().permissions().value();
  it = permissions2.find("GlobalDefault");
  ASSERT_TRUE(it != permissions2.end());
  ASSERT_EQ(2, permissions2.size());

  ASSERT_TRUE(it->second[0]);
  ASSERT_FALSE(it->second[1]);
  ASSERT_FALSE(it->second[2]);

  it = permissions2.find("default");
  ASSERT_TRUE(it != permissions2.end());

  ASSERT_FALSE(it->second[0]);
  ASSERT_TRUE(it->second[1]);
  ASSERT_FALSE(it->second[2]);

  // check that admin list is parsed properly
  SecurityConfig securityConf = config->serverConfig()->getSecurityConfig();
  auto admins = securityConf.admins;

  ASSERT_FALSE(admins.empty());
  ASSERT_EQ(3, admins.size());

  auto iter = admins.find("user1");
  ASSERT_NE(iter, admins.end());

  iter = admins.find("user2");
  ASSERT_NE(iter, admins.end());

  iter = admins.find("user3");
  ASSERT_NE(iter, admins.end());

  EXPECT_NE(config->serverConfig()->getCustomFields(), nullptr);
  EXPECT_EQ(config->serverConfig()->getCustomFields().size(), 0);
  auto timestamp = config->serverConfig()->getClusterCreationTime();
  EXPECT_FALSE(timestamp.hasValue());
}

/**
 * Tests that permissions propagate properly with Namespaces
 */
TEST(ConfigurationTest, NamespacedConfigWithPermissions) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("namespaced_logs_with_permissions.conf")));
  ASSERT_NE(nullptr, config);

  // check that all security_information fields are set correctly
  ASSERT_TRUE(config->serverConfig()->allowUnauthenticated());
  ASSERT_EQ(AuthenticationType::SELF_IDENTIFICATION,
            config->serverConfig()->getAuthenticationType());
  ASSERT_EQ(PermissionCheckerType::CONFIG,
            config->serverConfig()->getPermissionCheckerType());

  LogsConfig::NamespaceRangeLookupMap ranges;
  LogsConfig::NamespaceRangeLookupMap sub_ranges;

  ASSERT_NE(nullptr, config);
  auto range = config->logsConfig()->getLogRangeByName("ns1/sublog1");
  ASSERT_EQ(1, range.first.val_);
  ASSERT_EQ(1, range.second.val_);

  // check that ns1 still has GlobalDefault permissions
  auto log = config->getLogGroupByIDShared(range.first);
  auto it = (*log->attrs().permissions()).find("GlobalDefault");
  ASSERT_TRUE(it != (*log->attrs().permissions()).end());
  ASSERT_EQ(1, (*log->attrs().permissions()).size());

  ASSERT_TRUE(it->second[0]);
  ASSERT_FALSE(it->second[1]);
  ASSERT_FALSE(it->second[2]);

  // check that ns2 has overwritten the GlobalDefault with a sub default
  range = config->logsConfig()->getLogRangeByName("ns1/ns2/subsublog1");
  ASSERT_EQ(2, range.first.val_);
  ASSERT_EQ(3, range.second.val_);
  log = config->getLogGroupByIDShared(range.first);
  it = (*log->attrs().permissions()).find("ns2Default");
  ASSERT_TRUE(it != (*log->attrs().permissions()).end());
  ASSERT_EQ(1, (*log->attrs().permissions()).size());

  ASSERT_FALSE(it->second[0]);
  ASSERT_FALSE(it->second[1]);
  ASSERT_TRUE(it->second[2]);

  // check that subsublog2 has overwritten all defaults
  range = config->logsConfig()->getLogRangeByName("ns1/ns2/subsublog2");
  ASSERT_EQ(4, range.first.val_);
  ASSERT_EQ(4, range.second.val_);
  log = config->getLogGroupByIDShared(range.first);
  it = (*log->attrs().permissions()).find("log4");
  ASSERT_TRUE(it != (*log->attrs().permissions()).end());
  ASSERT_EQ(1, (*log->attrs().permissions()).size());

  ASSERT_TRUE(it->second[0]);
  ASSERT_FALSE(it->second[1]);
  ASSERT_TRUE(it->second[2]);

  // Check that log 1 is unaffected by other namespaces
  range = config->logsConfig()->getLogRangeByName("log1");
  ASSERT_EQ(95, range.first.val_);
  ASSERT_EQ(100, range.second.val_);
  log = config->getLogGroupByIDShared(range.first);
  it = (*log->attrs().permissions()).find("GlobalDefault");
  ASSERT_TRUE(it != (*log->attrs().permissions()).end());
  ASSERT_EQ(1, (*log->attrs().permissions()).size());

  ASSERT_TRUE(it->second[0]);
  ASSERT_FALSE(it->second[1]);
  ASSERT_FALSE(it->second[2]);

  // check that log3 can overwrite global defaults
  range = config->logsConfig()->getLogRangeByName("log3");
  ASSERT_EQ(101, range.first.val_);
  ASSERT_EQ(101, range.second.val_);
  log = config->getLogGroupByIDShared(range.first);
  it = (*log->attrs().permissions()).find("user101");
  ASSERT_TRUE(it != (*log->attrs().permissions()).end());
  ASSERT_EQ(1, (*log->attrs().permissions()).size());

  ASSERT_TRUE(it->second[0]);
  ASSERT_TRUE(it->second[1]);
  ASSERT_TRUE(it->second[2]);
}

/**
 * Test that the config will fail to parse if "authentication_type" is missing
 * when from security_information
 */
TEST(ConfigurationTest, MissingAuthenticatorType) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("missing_auth_type_info.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Checks that if the config contains a permission_checker_type that is
 * not defined in the parser, then the config is not created
 */
TEST(ConfigurationTest, MissingPermissionChckerType) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_permission_checker_type.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Checks that if the config contains a authentication_type that is
 * not defined in the parser, then the config is not created
 */
TEST(ConfigurationTest, MissingAuthenticationChckerType) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_authenticator_type.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Check that if the "permissions" field is included in the defaults section of
 * the config file and permission_checker_type is not set to "config" then
 * the Configuration object will not be created.
 */
TEST(ConfigurationTest, InvalidUseOfPermissionDefault) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_use_of_permission1.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
}

/**
 * Checks that if the "permissions" field is included in the logs section of the
 * config file, and the permission_checker_type is not set to "config" then
 * the Configuration object is not created.
 */
TEST(ConfigurationTest, InvalidUseOfPermissionLog) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_use_of_permission2.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Checks if there is an action that is not "READ", "APPEND" or "TRIM"
 * in the permission list, it fails to parse
 */
TEST(ConfigurationTest, InvalidActionInPermissions) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("config_permission_invalid_action.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
}

/**
 * Node has no node_id.
 */
TEST(ConfigurationTest, NoID) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("noid.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * Two nodes have the same node_id.
 */
TEST(ConfigurationTest, DupID) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("dupid.conf")));
  ASSERT_EQ(nullptr, config);
}

/**
 * A user log is within the internal range.
 */
TEST(ConfigurationTest, UserLogWithinInternalRange) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("user_log_within_internal_range.conf")));
  ASSERT_EQ(nullptr, config->logsConfig());
}

/**
 * An internal log is configured with a backlog duration, which is incorrect.
 */
TEST(ConfigurationTest, InternalLogIncorrectWithBacklog) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("internal_logs_incorrect_backlog.conf")));
  ASSERT_EQ(nullptr, config);
}

TEST(ConfigurationTest, InternalLogCorrect) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("internal_logs_correct.conf")));
  ASSERT_NE(nullptr, config);
  ASSERT_NE(nullptr, config->serverConfig());
}

TEST(ConfigurationTest, DefaultDscp) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("dscp_default.conf")));
  ASSERT_NE(nullptr, config);
  ASSERT_NE(nullptr, config->serverConfig());
  const std::string server_default_dscp = config->serverConfig()
                                              ->getServerSettingsConfig()
                                              .find("server-default-dscp")
                                              ->second;
  EXPECT_STREQ("34", server_default_dscp.c_str());
  const std::string client_default_dscp = config->serverConfig()
                                              ->getClientSettingsConfig()
                                              .find("client-default-dscp")
                                              ->second;
  EXPECT_STREQ("45", client_default_dscp.c_str());
}

TEST(ConfigurationTest, MetaDataLogsConfig) {
  auto get_config =
      [](std::string metadata_log_fields) -> std::unique_ptr<Configuration> {
    ld_info("Generating config with metadata log fields: %s",
            metadata_log_fields.c_str());
    std::string conf_str = R"(
{
  "cluster": "test_cluster",
  "nodes": [)";
    // Nodes config with 4 nodes.
    for (int i = 0; i < 4; ++i) {
      if (i) {
        conf_str += ",";
      }
      conf_str += R"(
    {
      "node_id": )" +
          std::to_string(i) + R"(,
      "name": "server-)" +
          std::to_string(i) + R"(",
      "location": "reg)" +
          std::to_string(i) + R"(.x.y.z.w",
      "host": "127.0.0.1:)" +
          std::to_string(4444 + i) + R"(",
      "generation": 1,
      "roles": [
        "sequencer",
        "storage"
      ],
      "sequencer": true,
      "weight": 1,
      "num_shards": 2
    })";
    }
    conf_str += R"(
  ],
  "logs": [],
  "metadata_logs": {
    "nodeset": [0, 1, 2, 3],
    "replication_factor": 4
)";
    if (!metadata_log_fields.empty()) {
      conf_str += ", " + metadata_log_fields;
    }
    conf_str += R"(
  },
})";
    auto config_from_json_str = [](const std::string& str) {
      auto json = parser::parseJson(str);
      // Make sure the parsed string is actually an object
      ld_check(json.isObject());
      return Configuration::fromJson(
          json, nullptr, nullptr, ConfigParserOptions());
    };
    auto config = config_from_json_str(conf_str);
    if (!config) {
      return nullptr;
    }

    // Re-serializing and de-serializing the config again to verify that
    // serialization writes all the fields we have written
    std::string serialized_str = config->toString();
    auto config2 = config_from_json_str(serialized_str);

    ld_check(config2);
    ld_check(config2->serverConfig());
    return config2;
  };
  {
    auto cfg = get_config("");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_FALSE(ml_config.metadata_version_to_write.hasValue());
    EXPECT_EQ(NodeSetSelectorType::CONSISTENT_HASHING,
              ml_config.nodeset_selector_type);
    ASSERT_EQ(epoch_metadata_version::CURRENT,
              epoch_metadata_version::versionToWrite(cfg->serverConfig()));
  }
  {
    auto cfg = get_config("\"metadata_version\": 1");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_TRUE(ml_config.metadata_version_to_write.hasValue());
    ASSERT_EQ(1, ml_config.metadata_version_to_write.value());
    ASSERT_EQ(1, epoch_metadata_version::versionToWrite(cfg->serverConfig()));
    EXPECT_EQ(NodeSetSelectorType::CONSISTENT_HASHING,
              ml_config.nodeset_selector_type);
  }
  {
    auto cfg = get_config("\"nodeset_selector\": \"random-crossdomain\"");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_FALSE(ml_config.metadata_version_to_write.hasValue());
    ASSERT_EQ(epoch_metadata_version::CURRENT,
              epoch_metadata_version::versionToWrite(cfg->serverConfig()));
    EXPECT_EQ(NodeSetSelectorType::RANDOM_CROSSDOMAIN,
              ml_config.nodeset_selector_type);
  }
  {
    auto cfg = get_config("\"nodeset_selector\": \"random\"");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_FALSE(ml_config.metadata_version_to_write.hasValue());
    ASSERT_EQ(epoch_metadata_version::CURRENT,
              epoch_metadata_version::versionToWrite(cfg->serverConfig()));
    EXPECT_EQ(NodeSetSelectorType::RANDOM, ml_config.nodeset_selector_type);
  }
  {
    auto cfg = get_config("\"nodeset_selector\": \"select-all\"");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_FALSE(ml_config.metadata_version_to_write.hasValue());
    ASSERT_EQ(epoch_metadata_version::CURRENT,
              epoch_metadata_version::versionToWrite(cfg->serverConfig()));
    EXPECT_EQ(NodeSetSelectorType::SELECT_ALL, ml_config.nodeset_selector_type);
  }
  {
    auto cfg = get_config("\"sync_replicate_across\": \"region\"");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_EQ(
        NodeLocationScope::REGION,
        ml_config.metadata_log_group->attrs().syncReplicationScope().value());
    ASSERT_EQ(
        NodeLocationScope::REGION,
        ml_config.metadata_log_group->attrs().syncReplicationScope().value());
  }
  {
    auto cfg = get_config(R"("replicate_across": {"rack": 4, "region": 3})");
    ASSERT_NE(cfg, nullptr);
    auto& ml_config = cfg->serverConfig()->getMetaDataLogsConfig();
    ASSERT_FALSE(ml_config.metadata_log_group->attrs()
                     .syncReplicationScope()
                     .hasValue());
    ASSERT_FALSE(ml_config.metadata_log_group->attrs()
                     .syncReplicationScope()
                     .hasValue());
    ASSERT_EQ(
        Configuration::LogAttributes::ScopeReplicationFactors(
            {{NodeLocationScope::REGION, 3}, {NodeLocationScope::RACK, 4}}),
        ml_config.metadata_log_group->attrs().replicateAcross().value());
    ASSERT_EQ(ReplicationProperty({{NodeLocationScope::REGION, 3},
                                   {NodeLocationScope::RACK, 4}})
                  .toString(),
              ReplicationProperty::fromLogAttributes(
                  ml_config.metadata_log_group->attrs())
                  .toString());
    ASSERT_EQ(ReplicationProperty({{NodeLocationScope::REGION, 3},
                                   {NodeLocationScope::RACK, 4}})
                  .toString(),
              ReplicationProperty::fromLogAttributes(
                  ml_config.metadata_log_group->attrs())
                  .toString());
  }
  {
    std::vector<std::string> invalid_params = {
        "\"metadata_version\": " +
            std::to_string(epoch_metadata_version::CURRENT + 1),
        "\"metadata_version\": 0",
        "\"metadata_version\": -1",
        "\"metadata_version\": \"2\"",
        "\"metadata_version\": \"abc\"",
        "\"nodeset_selector\": \"abc\"",
        "\"nodeset_selector\": 2",
    };
    for (auto& params : invalid_params) {
      err = E::OK;
      auto cfg = get_config(params);
      ASSERT_EQ(nullptr, cfg);
      ASSERT_EQ(E::INVALID_CONFIG, err);
    }
  }
}

TEST(ConfigurationTest, ACLS) {
  std::shared_ptr<Configuration> config(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_permission_acl_test.conf")));
  ASSERT_NE(nullptr, config);
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      config->getLogGroupByIDShared(logid_t(1));
  ASSERT_NE(log, nullptr);
  const Configuration::LogAttributes& log_attrs = log->attrs();

  EXPECT_EQ(
      Configuration::LogAttributes::ACLList({"SOME_CATEGORY.TestSomeACL"}),
      log_attrs.acls());
  EXPECT_EQ(Configuration::LogAttributes::ACLList(
                {"SOME_CATEGORY.TestSomeACLShadow"}),
            log_attrs.aclsShadow());
}

// Tests valid and invalid SequencerAffinity strings
TEST(ConfigurationTest, SequencerAffinity) {
  using facebook::logdevice::E;
  using facebook::logdevice::err;

  std::shared_ptr<Configuration> config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("sequencer_affinity_2nodes.conf"));
  ASSERT_NE(config, nullptr);
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      config->getLogGroupByIDShared(logid_t(1));
  ASSERT_NE(log, nullptr);
  const Configuration::LogAttributes& log_attrs = log->attrs();
  ASSERT_TRUE(log_attrs.sequencerAffinity().hasValue());
  EXPECT_EQ(log_attrs.sequencerAffinity().value(), "rgn1....");

  // Sequencer affinity attribute does not have the proper format. See
  // NodeLocation::fromDomainString for valid formats.
  config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("invalid_sequencer_affinity.conf"));
  EXPECT_EQ(config->logsConfig(), nullptr);
  EXPECT_EQ(err, E::INVALID_CONFIG);
}

// Tests various combinations of storage_capacity, storage and weight attributes
TEST(ConfigurationTest, StorageCapacityVsWeight) {
  using facebook::logdevice::configuration::StorageState;

  // Configs with storage_capacity or weight specified for writable nodes
  std::string path(TEST_CONFIG_FILE("storage_capacity_success1.conf"));
  ld_info("Loading %s, expecting it to succeed", path.c_str());
  auto config = Configuration::fromJsonFile(path.c_str());
  ASSERT_NE(config, nullptr);

  auto verify_node = [&](node_index_t node_id,
                         StorageState state,
                         double storage_capacity,
                         int weight,
                         bool exclude_from_nodesets) {
    ld_info("Verifying node %d", node_id);
    auto const node = config->serverConfig()->getNode(node_id);
    EXPECT_NE(nullptr, node);
    if (node->storage_attributes != nullptr) {
      EXPECT_EQ(state, node->storage_attributes->state);
      EXPECT_EQ(storage_capacity, node->storage_attributes->capacity);
      EXPECT_EQ(exclude_from_nodesets,
                node->storage_attributes->exclude_from_nodesets);
    } else {
      EXPECT_EQ(state, configuration::StorageState::DISABLED);
      EXPECT_EQ(storage_capacity, 0);
    }

    double expected_capacity = storage_capacity;
    if (state != StorageState::READ_WRITE) {
      expected_capacity = 0;
    }
    EXPECT_EQ(expected_capacity, node->getWritableStorageCapacity());
    EXPECT_EQ(weight, node->getLegacyWeight());
  };

  verify_node(1, StorageState::READ_WRITE, 1, 1, true);
  verify_node(2, StorageState::READ_ONLY, 1, 0, false);
  verify_node(3, StorageState::DISABLED, 1, -1, false);
  verify_node(4, StorageState::READ_WRITE, 0.2, 1, false);
  verify_node(5, StorageState::READ_ONLY, 0, 0, true);
  verify_node(6, StorageState::READ_ONLY, 1.2, 0, false);
  verify_node(7, StorageState::DISABLED, 5.7, -1, true);
  verify_node(8, StorageState::READ_WRITE, 1.8, 2, false);
  verify_node(9, StorageState::READ_ONLY, 0, 0, false);
  verify_node(10, StorageState::READ_ONLY, 1, 0, false);
  verify_node(11, StorageState::DISABLED, 1, -1, false);
  verify_node(12, StorageState::READ_WRITE, 2, 2, false);
  verify_node(13, StorageState::READ_ONLY, 1, 0, false);
  verify_node(14, StorageState::DISABLED, 1, -1, false);
  verify_node(18, StorageState::READ_WRITE, 2, 2, false);
  verify_node(19, StorageState::READ_ONLY, 2, 0, false);
  verify_node(20, StorageState::READ_ONLY, 0, 0, false);
  verify_node(21, StorageState::DISABLED, 0, -1, false);

  // Configs with storage_capacity/weight not specified for writable nodes
  path = TEST_CONFIG_FILE("storage_capacity_success2.conf");
  ld_info("Loading %s, expecting it to succeed", path.c_str());
  config = Configuration::fromJsonFile(path.c_str());
  ASSERT_NE(config, nullptr);

  verify_node(0, StorageState::DISABLED, 0, -1, false);
  verify_node(2, StorageState::READ_ONLY, 1, 0, false);
  verify_node(3, StorageState::DISABLED, 1, -1, false);
  verify_node(5, StorageState::READ_ONLY, 1, 0, true);
  verify_node(13, StorageState::READ_ONLY, 1, 0, false);
  verify_node(14, StorageState::DISABLED, 1, -1, false);
  verify_node(15, StorageState::READ_WRITE, 1, 1, false);
  verify_node(16, StorageState::READ_ONLY, 1, 0, false);
  verify_node(17, StorageState::DISABLED, 1, -1, false);

  std::vector<std::string> failing_configs = {
      // weight: -1 and storage_state: read-write
      TEST_CONFIG_FILE("storage_capacity_fail2.conf"),
      // weight: 0 and storage_state: read-write
      TEST_CONFIG_FILE("storage_capacity_fail3.conf"),
      // weight: 1 and storage_state: none
      TEST_CONFIG_FILE("storage_capacity_fail4.conf"),
      // weight: 1 and storage_state: read-only
      TEST_CONFIG_FILE("storage_capacity_fail5.conf"),
      // weight: 1 and storage_capacity: 1.6
      TEST_CONFIG_FILE("storage_capacity_fail6.conf"),
  };
  for (auto& config_path : failing_configs) {
    ld_info("Loading %s, expecting it to fail", config_path.c_str());
    config = Configuration::fromJsonFile(config_path.c_str());
    ASSERT_EQ(config, nullptr);
  }
}
