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

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/hash.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

/**
 * @file Tests for sequencer placement, activation etc
 */

using namespace facebook::logdevice;
struct SeqReactivationStats {
  uint64_t scheduled = 0;
  uint64_t completed = 0;
  uint64_t delayed = 0;
  uint64_t delayCompleted = 0;
  uint64_t reactivations = 0;

  bool statsMatch() {
    return ((scheduled == completed) && (delayed == delayCompleted));
  }
};

class SequencerIntegrationTest : public IntegrationTestBase {};

std::unique_ptr<ClientSettings> createClientSettings(
    const std::vector<std::pair<std::string, std::string>>& settings) {
  std::unique_ptr<ClientSettings> client_settings{ClientSettings::create()};

  ld_check(client_settings->set(settings) == 0);

  return client_settings;
}

TEST_F(SequencerIntegrationTest, SequencerReactivationTest) {
  using IntegrationTestUtils::ParamScope;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          // 8 bits for ESN means we'll need to roll over into a new epoch once
          // if we write 400 records
          .setParam("--esn-bits", "8", ParamScope::SEQUENCER)
          .doPreProvisionEpochMetaData() // to disable writing metadata logs by
                                         // the sequencer
          .create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t logid(2);
  const size_t num_records = 400;
  // a map <lsn, data> for appended records
  std::map<lsn_t, std::string> lsn_map;

  lsn_t first_lsn = LSN_INVALID, previous_lsn = LSN_INVALID;
  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    // verify that lsn is monotonically increasing
    ASSERT_TRUE(lsn > previous_lsn);

    if (first_lsn == LSN_INVALID) {
      first_lsn = lsn;
    }

    ASSERT_EQ(lsn_map.end(), lsn_map.find(lsn));
    lsn_map[lsn] = data;
    previous_lsn = lsn;
  }

  cluster->waitForRecovery();

  auto it = lsn_map.cbegin();
  bool has_payload = true;
  Semaphore sem;

  // should expect one gap for epoch bump
  size_t gap_count = 0;
  auto gap_cb = [&](const GapRecord&) {
    gap_count++;
    return true;
  };
  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    EXPECT_EQ(logid, r->logid);
    EXPECT_NE(lsn_map.cend(), it);
    EXPECT_EQ(it->first, r->attrs.lsn);
    const Payload& p = r->payload;
    if (has_payload) {
      EXPECT_NE(nullptr, p.data());
      EXPECT_EQ(it->second.size(), p.size());
      EXPECT_EQ(it->second, p.toString());
    } else {
      EXPECT_EQ(nullptr, p.data());
      EXPECT_EQ(0, p.size());
    }

    if (++it == lsn_map.cend()) {
      sem.post();
    }
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(logid, first_lsn);
  sem.wait();

  // TODO 5904734: bridge record: fix this when the read path is ready.
  // EXPECT_EQ(1, gap_count);

  Semaphore stop_sem;
  int rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();
}

// Sequencer activation may be performed immediately or postponed
// based on the information that is changing. Changes to important
// information like a logs config parameters need to be updated
// immediately while a change in the nodeset due to changes to
// the storage state can be applied after a delay.
//
// Test that the sequencer reactivation occurs immediately if one
// of the sequencer options like repl factor changes, even though
// sequencer-reactivation-delay-secs is set to a high value.
TEST_F(SequencerIntegrationTest,
       SequencerReactivationReplFactorChangeDelayTest) {
  Configuration::Nodes nodes;
  size_t num_nodes = 5;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_nodeSetSize(10);
  uint32_t numLogs = 32;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .setLogGroupName("test_range")
          .setLogAttributes(log_attrs)
          .setParam("--sequencer-reactivation-delay-secs", "1200s..2000s")
          .setParam("--nodeset-adjustment-period", "0s")
          .setParam("--nodeset-adjustment-min-window", "1s")
          .setParam("--nodeset-size-adjustment-min-factor", "0")
          .setNumLogs(numLogs)
          .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();
  ld_info("starting test...");

  // Append some records
  // for log 1..numLogs, send a write to activate the sequencer
  for (logid_t::raw_type logid_r = 1; logid_r <= numLogs; ++logid_r) {
    lsn_t lsn;
    do {
      lsn = client->appendSync(logid_t(logid_r), "dummy");
    } while (lsn == LSN_INVALID);
  }

  auto get_activations = [&]() {
    size_t activations = 0;
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      activations += stats["sequencer_activations"];
    }
    return activations;
  };

  const size_t initial_activations = get_activations();
  EXPECT_GE(initial_activations, numLogs);

  // Update the config by changing the replication factor
  auto full_config = cluster->getConfig()->get();
  ld_check(full_config);
  auto logs_config_changed = full_config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());

  auto start = RecordTimestamp::now().toSeconds();
  ld_info("Changing config by changing replication factor from 2 to 1");
  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;
  auto& attrs = log_in_directory.log_group->attrs();
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
            ReplicationProperty::fromLogAttributes(attrs).toString());
  auto new_attrs = attrs.with_replicationFactor(1);
  ASSERT_TRUE(local_logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withLogAttributes(new_attrs)));

  cluster->writeConfig(
      full_config->serverConfig().get(), logs_config_changed.get());

  cluster->waitForConfigUpdate();

  cluster->waitForRecovery();

  auto get_stats = [&]() {
    SeqReactivationStats stats;
    for (const auto& it : nodes) {
      auto nodeStats = cluster->getNode(it.first).stats();
      stats.scheduled +=
          nodeStats["background_sequencer_reactivation_checks_scheduled"];
      stats.completed +=
          nodeStats["background_sequencer_reactivation_checks_completed"];

      stats.delayed += nodeStats["sequencer_reactivations_delayed"];
      stats.delayCompleted +=
          nodeStats["sequencer_reactivations_delay_completed"];
      stats.reactivations +=
          nodeStats["sequencer_reactivations_for_metadata_update"];
    }
    return stats;
  };

  // Wait until the scheduled activations are completed
  wait_until("stats match up", [&]() {
    auto stats = get_stats();
    EXPECT_GT(stats.scheduled, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return stats.statsMatch();
  });

  // Check that reactivations completed without waiting for the specified delay
  // in the settings.
  auto delay = RecordTimestamp::now().toSeconds() - start;
  EXPECT_LT(delay.count(), 1200);

  auto stats = get_stats();
  EXPECT_EQ(stats.delayed, 0);
  EXPECT_EQ(stats.delayCompleted, 0);

  size_t num_activations_after_config_change = get_activations();
  EXPECT_GE(num_activations_after_config_change - initial_activations, numLogs);
}

// Sequencer activation may be performed immediately or postponed
// based on the information that is changing. Changes to important
// information like a logs config parameters need to be updated
// immediately while a change in the nodeset due to changes to
// the storage state can be applied after a delay.
//
// Test that the sequencer reactivation occurs immediately if one
// of the sequencer options like the window size changes, even though
// sequencer-reactivation-delay-secs is set to a high value.
TEST_F(SequencerIntegrationTest, WinSizeChangeDelayTest) {
  Configuration::Nodes nodes;
  size_t num_nodes = 5;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  // window size is initially 200
  log_attrs.set_maxWritesInFlight(200);
  uint32_t numLogs = 32;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .setLogGroupName("test_range")
          .setParam("--sequencer-reactivation-delay-secs", "1200s..2000s")
          .setParam("--nodeset-adjustment-period", "0s")
          .setParam("--nodeset-adjustment-min-window", "1s")
          .setParam("--nodeset-size-adjustment-min-factor", "0")
          .setLogAttributes(log_attrs)
          .setNumLogs(numLogs)
          .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }
  auto client = cluster->createClient();
  ld_info("starting test...");

  // for log 1..numLogs, send a write to activate the sequencer
  for (logid_t::raw_type logid_r = 1; logid_r <= numLogs; ++logid_r) {
    lsn_t lsn;
    do {
      lsn = client->appendSync(logid_t(logid_r), "dummy");
    } while (lsn == LSN_INVALID);
  }

  auto get_activations = [&]() {
    size_t activations = 0;
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      activations += stats["sequencer_activations"];
    }
    return activations;
  };

  const size_t initial_activations = get_activations();
  EXPECT_GE(initial_activations, numLogs);

  // change the sequencer window size in config
  auto start = RecordTimestamp::now().toSeconds();
  auto full_config = cluster->getConfig()->get();
  ld_check(full_config);
  auto logs_config_changed = full_config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());

  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;
  // changing window size to 300 for the whole log group
  bool rv = local_logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withLogAttributes(
          log_in_directory.log_group->attrs().with_maxWritesInFlight(300)));
  ASSERT_TRUE(rv);

  cluster->writeConfig(
      full_config->serverConfig().get(), logs_config_changed.get());

  cluster->waitForConfigUpdate();

  cluster->waitForRecovery();

  auto get_stats = [&]() {
    SeqReactivationStats stats;
    for (const auto& it : nodes) {
      auto nodeStats = cluster->getNode(it.first).stats();
      stats.scheduled +=
          nodeStats["background_sequencer_reactivation_checks_scheduled"];
      stats.completed +=
          nodeStats["background_sequencer_reactivation_checks_completed"];

      stats.delayed += nodeStats["sequencer_reactivations_delayed"];
      stats.delayCompleted +=
          nodeStats["sequencer_reactivations_delay_completed"];
      stats.reactivations +=
          nodeStats["sequencer_reactivations_for_metadata_update"];
    }
    return stats;
  };

  // Wait until the scheduled activations are completed
  wait_until("stats match up", [&]() {
    auto stats = get_stats();
    EXPECT_GT(stats.scheduled, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return stats.statsMatch();
  });

  // Check that reactivations completed without waiting for the specified delay
  // in the settings.
  auto delay = RecordTimestamp::now().toSeconds() - start;
  EXPECT_LT(delay.count(), 1200);

  auto stats = get_stats();
  EXPECT_EQ(stats.delayed, 0);
  EXPECT_EQ(stats.delayCompleted, 0);

  size_t num_activations_after_config_change = get_activations();
  EXPECT_GE(num_activations_after_config_change - initial_activations, numLogs);
}

// Sequencer activation may be performed immediately or postponed
// based on the information that is changing. Changes to important
// information like a logs config parameters need to be updated
// immediately while a change in the nodeset due to changes to
// the storage state can be applied after a delay.
//
// Test that the sequencer reactivation is delayed at least
// sequencer-reactivation-delay-secs if just the storage state of some of the
// nodes changes.
TEST_F(SequencerIntegrationTest, StorageStateChangeDelayTest) {
  Configuration::Nodes nodes;
  size_t num_nodes = 7;
  // 2 sequencer nodes and 5 storage nodes
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i < 2) {
      node.addSequencerRole();
    } else {
      node.addStorageRole();
    }
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  log_attrs.set_nodeSetSize(3);
  uint32_t numLogs = 1;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setLogGroupName("test_range")
                     .setLogAttributes(log_attrs)
                     .setParam("--sequencer-reactivation-delay-secs", "6s..12s")
                     .setParam("--nodeset-adjustment-period", "0s")
                     .setParam("--nodeset-adjustment-min-window", "1s")
                     .setParam("--nodeset-size-adjustment-min-factor", "0")
                     .setNumLogs(numLogs)
                     .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();
  ld_info("starting test...");

  // Append some records
  // for log 1..numLogs, send a write to activate the sequencer
  for (logid_t::raw_type logid_r = 1; logid_r <= numLogs; ++logid_r) {
    lsn_t lsn;
    do {
      lsn = client->appendSync(logid_t(logid_r), "dummy");
    } while (lsn == LSN_INVALID);
  }

  auto get_activations = [&]() {
    size_t activations = 0;
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      activations += stats["sequencer_activations"];
    }
    return activations;
  };

  const size_t initial_activations = get_activations();
  EXPECT_GE(initial_activations, numLogs);

  auto get_stats = [&]() {
    SeqReactivationStats stats;
    for (const auto& it : nodes) {
      auto nodeStats = cluster->getNode(it.first).stats();
      stats.scheduled +=
          nodeStats["background_sequencer_reactivation_checks_scheduled"];
      stats.completed +=
          nodeStats["background_sequencer_reactivation_checks_completed"];

      stats.delayed += nodeStats["sequencer_reactivations_delayed"];
      stats.delayCompleted +=
          nodeStats["sequencer_reactivations_delay_completed"];
      stats.reactivations +=
          nodeStats["sequencer_reactivations_for_metadata_update"];
    }
    return stats;
  };

  // Find node with most STOREs and disable it. This should  regenerate the
  // nodeset for the written log.
  std::unordered_map<node_index_t, size_t> stores;
  node_index_t to_disable = 1;
  for (node_index_t n = 2; n <= 6; ++n) {
    stores[n] = cluster->getNode(n).stats()["message_received.STORE"];
    if (stores[n] > stores[to_disable]) {
      to_disable = n;
    }
  }

  ld_info("Set the weight of N%hu to 0.", to_disable);
  auto start = RecordTimestamp::now().toSeconds();
  cluster->updateNodeAttributes(
      to_disable, configuration::StorageState::DISABLED, 1);
  cluster->waitForConfigUpdate();
  cluster->waitForRecovery();

  // Wait until the scheduled activations are completed
  wait_until("background activations completed", [&]() {
    auto stats = get_stats();
    EXPECT_GT(stats.scheduled, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return stats.statsMatch();
  });

  // We are done processing all the scheduled activations so those that need to
  // be postponed must also be scheduled by now.
  wait_until("Delayed activations completed", [&]() {
    auto stats = get_stats();
    EXPECT_GT(stats.delayed, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return stats.statsMatch();
  });

  // The delayed reactivatins must result in actual reactivations
  auto stats = get_stats();
  uint64_t activations_after_state_change =
      stats.reactivations - initial_activations;
  EXPECT_GE(activations_after_state_change, stats.delayCompleted);

  // Check that reactivations completed after waiting for the specified delay in
  // the settings.
  auto delay = RecordTimestamp::now().toSeconds() - start;
  EXPECT_GE(delay.count(), 6);
}

// Sequencer activation may be performed immediately or postponed
// based on the information that is changing. Changes to important
// information like a logs config parameters need to be updated
// immediately while a change in the nodeset due to changes to
// the storage state can be applied after a delay.
//
// Test that the sequencer reactivation is delayed at least by
// sequencer-reactivation-delay-secs if the nodeset is changed
// due to an expand or shrink operation.
TEST_F(SequencerIntegrationTest, ExpandShrinkReactivationDelayTest) {
  Configuration::Nodes nodes;
  size_t num_nodes = 10;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  shard_size_t num_shards = 1;
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  log_attrs.set_nodeSetSize(5);
  uint32_t numLogs = 32;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setLogGroupName("test_range")
                     .setLogAttributes(log_attrs)
                     .setParam("--sequencer-reactivation-delay-secs", "6s..12s")
                     .setParam("--nodeset-adjustment-period", "0s")
                     .setParam("--nodeset-adjustment-min-window", "1s")
                     .setParam("--nodeset-size-adjustment-min-factor", "0")
                     .setNumDBShards(num_shards)
                     .setNumLogs(numLogs)
                     .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();
  ld_info("starting test...");

  // Append some records
  // for log 1..numLogs, send a write to activate the sequencer
  for (logid_t::raw_type logid_r = 1; logid_r <= numLogs; ++logid_r) {
    lsn_t lsn;
    do {
      lsn = client->appendSync(logid_t(logid_r), "dummy");
    } while (lsn == LSN_INVALID);
  }

  auto get_activations = [&]() {
    size_t activations = 0;
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      activations += stats["sequencer_activations"];
    }
    return activations;
  };

  const size_t initial_activations = get_activations();
  EXPECT_GE(initial_activations, numLogs);

  auto get_stats = [&]() {
    SeqReactivationStats stats;
    for (size_t nodeId = 0; nodeId < num_nodes; nodeId++) {
      auto nodeStats = cluster->getNode(nodeId).stats();
      stats.scheduled +=
          nodeStats["background_sequencer_reactivation_checks_scheduled"];
      stats.completed +=
          nodeStats["background_sequencer_reactivation_checks_completed"];

      stats.delayed += nodeStats["sequencer_reactivations_delayed"];
      stats.delayCompleted +=
          nodeStats["sequencer_reactivations_delay_completed"];
      stats.reactivations +=
          nodeStats["sequencer_reactivations_for_metadata_update"];
    }
    return stats;
  };

  // Wait until all the scheduled activations are completed
  wait_until("background activations completed", [&]() {
    auto stats = get_stats();
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return stats.statsMatch();
  });

  auto stats = get_stats();
  EXPECT_EQ(stats.delayed, 0);
  // Randomly chose to expand or shrink the cluster
  auto start = RecordTimestamp::now().toSeconds();
  size_t numExpandNodes = 5;
  size_t numShrinkNodes = 4;
  if ((folly::Random::rand32() % 2) == 0) {
    ld_info("test expanding cluster");
    cluster->expand(numExpandNodes, true);
    num_nodes += numExpandNodes;
  } else {
    ld_info("test shrinking cluster");
    cluster->shrink(numShrinkNodes);
    num_nodes -= numShrinkNodes;
  }

  // Wait until the scheduled activations are completed
  wait_until("background activations completed", [&]() {
    auto latest_stats = get_stats();
    EXPECT_GT(latest_stats.scheduled, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return latest_stats.statsMatch();
  });

  // We are done processing all the scheduled activations so those that need to
  // be postponed must also be scheduled by now.
  wait_until("Delayed activations completed", [&]() {
    auto latest_stats = get_stats();
    EXPECT_GT(latest_stats.delayed, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return latest_stats.statsMatch();
  });

  // The delayed reactivations must result in actual reactivations
  stats = get_stats();
  uint64_t activations_after_size_change =
      stats.reactivations - initial_activations;
  EXPECT_GE(activations_after_size_change, stats.delayCompleted);

  // Check that reactivations completed after waiting for the specified delay in
  // the settings.
  auto delay = RecordTimestamp::now().toSeconds() - start;
  EXPECT_GE(delay.count(), 6);
}

// Sequencer activation may be performed immediately or postponed
// based on the information that is changing. Changes to important
// information like a logs config parameters need to be updated
// immediately while a change in the nodeset due to changes to
// the storage state can be applied after a delay.
//
// Test that the sequencer reactivation occurs immediately if one
// of the sequencer options like the window size, or config params
// like the replication factor, change in combination with the storage
// state. Just the storage state change related reactivation could be
// delayed but not in combination with other important information changing.
TEST_F(SequencerIntegrationTest, ConfigParamsAndStorageStateChangeDelayTest) {
  Configuration::Nodes nodes;
  size_t num_nodes = 7;
  // 2 sequencer nodes and 5 storage nodes
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i < 2) {
      node.addSequencerRole();
    } else {
      node.addStorageRole();
    }
  }

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  // window size is initially 200 and repl factor is initally 2.
  log_attrs.set_maxWritesInFlight(200);
  log_attrs.set_replicationFactor(2);
  log_attrs.set_nodeSetSize(3);

  uint32_t numLogs = 1;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .setLogGroupName("test_range")
          .setLogAttributes(log_attrs)
          .setParam("--sequencer-reactivation-delay-secs", "1200s..2000s")
          .setParam("--nodeset-adjustment-period", "0s")
          .setParam("--nodeset-adjustment-min-window", "1s")
          .setParam("--nodeset-size-adjustment-min-factor", "0")
          .setNumLogs(numLogs)
          .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();
  ld_info("starting test...");

  // Append some records
  // for log 1..numLogs, send a write to activate the sequencer
  for (logid_t::raw_type logid_r = 1; logid_r <= numLogs; ++logid_r) {
    lsn_t lsn;
    do {
      lsn = client->appendSync(logid_t(logid_r), "dummy");
    } while (lsn == LSN_INVALID);
  }

  auto get_activations = [&]() {
    size_t activations = 0;
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      activations += stats["sequencer_activations"];
    }
    return activations;
  };

  const size_t initial_activations = get_activations();
  EXPECT_GE(initial_activations, numLogs);

  // Find node with most STOREs. Don't disable it yet but when we do it should
  // regenerate the nodeset for the written log.
  std::unordered_map<node_index_t, size_t> stores;
  node_index_t to_disable = 1;
  for (node_index_t n = 2; n <= 6; ++n) {
    stores[n] = cluster->getNode(n).stats()["message_received.STORE"];
    if (stores[n] > stores[to_disable]) {
      to_disable = n;
    }
  }

  auto full_config = cluster->getConfig()->get();
  ld_check(full_config);
  auto logs_config_changed = full_config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());

  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;

  // With some probability, update the config by changing the replication
  // factor. This test depends on at least one of those changing so make sure
  // that happens.
  bool config_changed = false;
  while (!config_changed) {
    if ((folly::Random::rand32() % 2) == 0) {
      ld_info("changing replication factor from 2 to 1");
      auto& attrs = log_in_directory.log_group->attrs();
      ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
                ReplicationProperty::fromLogAttributes(attrs).toString());
      auto new_attrs = attrs.with_replicationFactor(1);
      ASSERT_TRUE(local_logs_config_changed->replaceLogGroup(
          log_in_directory.getFullyQualifiedName(),
          log_in_directory.log_group->withLogAttributes(new_attrs)));
      config_changed = true;
    }

    // With some probability, change the window size
    if ((folly::Random::rand32() % 2) == 0) {
      // changing window size to 300 for the whole log group
      ld_info("changing window size from 200 to 300");
      bool rv = local_logs_config_changed->replaceLogGroup(
          log_in_directory.getFullyQualifiedName(),
          log_in_directory.log_group->withLogAttributes(
              log_in_directory.log_group->attrs().with_maxWritesInFlight(300)));
      ASSERT_TRUE(rv);
      config_changed = true;
    }
  }

  // Now disable the node then write out the new config that contains changes
  // about the change to storage state as well as the window size and or
  // replication factor.
  Configuration::Nodes tmp_nodes = full_config->serverConfig()->getNodes();
  const auto& node_disable = nodes[to_disable];
  ld_check(node_disable.storage_attributes != nullptr);
  node_disable.storage_attributes->state =
      configuration::StorageState::DISABLED;

  auto start = RecordTimestamp::now().toSeconds();
  Configuration::NodesConfig new_nodes_config(std::move(tmp_nodes));
  std::shared_ptr<ServerConfig> new_server_config =
      full_config->serverConfig()->withNodes(std::move(new_nodes_config));
  cluster->writeConfig(new_server_config.get(), logs_config_changed.get());

  cluster->waitForConfigUpdate();
  cluster->waitForRecovery();
  ld_info("Set the weight of N%hu to 0.", to_disable);

  auto get_stats = [&]() {
    SeqReactivationStats stats;
    for (const auto& it : nodes) {
      auto nodeStats = cluster->getNode(it.first).stats();
      stats.scheduled +=
          nodeStats["background_sequencer_reactivation_checks_scheduled"];
      stats.completed +=
          nodeStats["background_sequencer_reactivation_checks_completed"];

      stats.delayed += nodeStats["sequencer_reactivations_delayed"];
      stats.delayCompleted +=
          nodeStats["sequencer_reactivations_delay_completed"];
      stats.reactivations +=
          nodeStats["sequencer_reactivations_for_metadata_update"];
    }
    return stats;
  };

  // Wait until the scheduled activations are completed
  wait_until("background activations completed", [&]() {
    auto stats = get_stats();
    EXPECT_GT(stats.scheduled, 0);
    ld_info("reactivations: scheduled: %lu, completed: %lu, "
            "delayed: %lu, delayCompleted: %lu, reactivations: %lu",
            stats.scheduled,
            stats.completed,
            stats.delayed,
            stats.delayCompleted,
            stats.reactivations);
    return stats.statsMatch();
  });

  // We are done processing all the scheduled activations so those that need to
  // be postponed must also be scheduled by now. Check that reactivations
  // completed without waiting for the specified delay in the settings.
  auto delay = RecordTimestamp::now().toSeconds() - start;
  EXPECT_LT(delay.count(), 1200);

  auto stats = get_stats();
  EXPECT_EQ(stats.delayed, 0);
  EXPECT_EQ(stats.delayCompleted, 0);

  size_t num_activations_after_config_change = get_activations();
  EXPECT_GE(num_activations_after_config_change - initial_activations, numLogs);
}

TEST_F(SequencerIntegrationTest, SequencerIsolation) {
  const int NNODES = 5;

  // figure out which node is running a sequencer for log 1 and partition it on
  // the minority side later
  auto node_idx = hashing::weighted_ch(1, {1.0, 1.0, 1.0, 1.0});
  std::set<int> partition1;
  std::set<int> partition2;
  partition1.insert(node_idx);
  for (int i = 0; i < NNODES; ++i) {
    if (i == node_idx) {
      continue;
    }
    if (partition1.size() < 2) {
      partition1.insert(i);
    } else {
      partition2.insert(i);
    }
  }
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    NodeLocation location;
    if (partition1.find(i) != partition1.end()) {
      location.fromDomainString("region1.dc1.cl1.ro1.rk1");
    } else {
      location.fromDomainString("region1.dc1.cl1.ro1.rk2");
    }
    nodes[i].location = location;
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .setParam("--isolated-sequencer-ttl", "5s")
          .useHashBasedSequencerAssignment()
          .enableMessageErrorInjection()
          .oneConfigPerNode()
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(NNODES);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->getNode(idx).waitUntilAvailable();
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  ASSERT_EQ(
      "ACTIVE", cluster->getNode(node_idx).sequencerInfo(logid_t(1))["State"]);

  cluster->partition({partition1, partition2});
  wait_until("node isolated. waiting for sequencers to be disabled.", [&]() {
    return cluster->getNode(node_idx).sequencerInfo(logid_t(1))["State"] ==
        "UNAVAILABLE";
  });

  auto reply = cluster->getNode(node_idx).upDown(logid_t(1));
  ASSERT_TRUE(
      reply.find("Node is isolated. Sequencer activation is suspended\r\n") ==
      0);
}

// Creates a cluster consisting of multiple sequencer nodes. Kills a node
// that is running a sequencer for log 1 and verifies that appends still
// succeed and cause a new node to bring up a sequencer.
TEST_F(SequencerIntegrationTest, SequencerReactivationOnFailure) {
  const int NNODES = 4;
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);
  }
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(NNODES);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->getNode(idx).waitUntilAvailable();
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  // figure out which node is running a sequencer for log 1 and kill it
  auto node_idx = hashing::weighted_ch(1, {1.0, 1.0, 1.0, 1.0});
  ASSERT_EQ(
      "ACTIVE", cluster->getNode(node_idx).sequencerInfo(logid_t(1))["State"]);
  cluster->getNode(node_idx).kill();

  // wait until all other nodes detect that `node_idx' is dead
  cluster->waitUntilGossip(/* alive */ false, node_idx);

  // write into log 1 and verify that the LSN belongs to a new epoch
  lsn_t lsn2 = client->appendSync(logid_t(1), "bar");
  ASSERT_NE(LSN_INVALID, lsn2);
  ASSERT_GE(lsn_to_epoch(lsn2), lsn_to_epoch(lsn1));

  // bring node_idx up and do another write
  cluster->getNode(node_idx).start();
  lsn_t lsn3;
  wait_until("append succeeds after sequencer reactivation", [&] {
    lsn3 = client->appendSync(logid_t(1), "baz");
    if (lsn3 == LSN_INVALID) {
      EXPECT_EQ(E::ISOLATED, err);
    }
    return lsn3 != LSN_INVALID;
  });
  ASSERT_GT(lsn3, lsn2);
}

// Creates a cluster consisting of multiple sequencer nodes.
// Simulates situation where a node is preempted by a node that is now dead.
// scenario:
//    1- client sends append for log 1
//       ==> this brings up sequencer on primary
//    2- kill primary
//    3- client sends append for log 1
//       ==> this activates sequencer on secondary
//    4- start primary
//    5- client sends append for log 1
//      ==> this activates sequencer on primary and preempt secondary
//    6- kill primary again
//    7- client sends GET_SEQ_STATE for log 1
//      ==> this should reactivate sequencer on secondary
//      (prior to fix D3033233, secondary would send a preempted redirect
//       to dead primary)
TEST_F(SequencerIntegrationTest, SequencerReactivationPreemptorDead) {
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < 4; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);
  }
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(nodes.size());

  for (const auto& it : nodes) {
    cluster->waitUntilGossip(/* alive */ true, it.first);
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  // figure out which node is running a sequencer for log 1 (primary)
  // and kill it
  std::vector<double> weights = {1.0, 1.0, 1.0, 1.0};
  auto primary = hashing::weighted_ch(1, weights);
  weights[primary] = 0;
  auto secondary = hashing::weighted_ch(1, weights);
  ASSERT_NE(primary, secondary);
  ASSERT_EQ(
      "ACTIVE", cluster->getNode(primary).sequencerInfo(logid_t(1))["State"]);
  cluster->getNode(primary).kill();

  // wait until all other nodes detect that `primary' is dead
  cluster->waitUntilGossip(false, primary);

  // write into log 1 and verify that the LSN belongs to a new epoch
  // a new sequencer should be brought up
  lsn_t lsn2 = client->appendSync(logid_t(1), "bar");
  ASSERT_NE(LSN_INVALID, lsn2);
  ASSERT_GE(lsn_to_epoch(lsn2), lsn_to_epoch(lsn1));

  // bring primary up and do another write
  cluster->getNode(primary).start();
  lsn_t lsn3;
  wait_until("append succeeds after sequencer reactivation", [&] {
    lsn3 = client->appendSync(logid_t(1), "baz");
    if (lsn3 == LSN_INVALID) {
      EXPECT_EQ(E::ISOLATED, err);
    }
    return lsn3 != LSN_INVALID;
  });
  ASSERT_GT(lsn3, lsn2);

  // now kill 'primary' again
  cluster->getNode(primary).kill();
  cluster->waitUntilGossip(/* alive */ false, primary);

  wait_until(
      "GET_SEQ_STATE confirms that the secondary is the sequencer node", [&] {
        IntegrationTestUtils::SequencerState seq_state;
        auto st = IntegrationTestUtils::getSeqState(
            client.get(), logid_t(1), seq_state, true);
        return st == E::OK && seq_state.node.index() == secondary;
      });
}

// It's a variation of SequnecerReactivationPreemptorDead where instead of GCS
// we send an append. It used to cause append failure with NOSEQUENCER, but now
// with the REDIRECT_NOT_ALIVE flag, it shopuld force reactivation on the
// secondary.
//
// Creates a cluster consisting of multiple sequencer nodes.
// Simulates situation where a node is preempted by a node that is now dead.
// scenario:
//    1- client sends append for log 1
//       ==> this brings up sequencer on primary
//    2- kill primary
//    3- client sends append for log 1
//       ==> this activates sequencer on secondary
//    4- start primary
//    5- client sends append for log 1
//      ==> this activates sequencer on primary and preempt secondary
//    6- kill primary again
//    7- client sends append for log 1
//      ==> this should reactivate sequencer on secondary
TEST_F(SequencerIntegrationTest, SequencerReactivationRedirectedNotAlive) {
  Configuration::Nodes nodes;
  int num_nodes = 6;
  // 4 storage-only nodes
  std::vector<double> weights(6);
  for (node_index_t i = 0; i < 4; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addStorageRole(/*num_shards*/ 2);
    weights[i] = node.getSequencerWeight();
    ASSERT_EQ(weights[i], 0);
  }
  // 2 sequencer-only nodes
  for (node_index_t i = 4; i < 6; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    weights[i] = node.getSequencerWeight();
    ASSERT_EQ(weights[i], 1.0);
  }

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .setParam("--sticky-copysets-block-size", "1")
          .setParam("--store-timeout", "1s..10s")
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(nodes.size());

  for (const auto& it : nodes) {
    cluster->waitUntilGossip(/* alive */ true, it.first);
  }

  auto client = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"connect-throttle", "0s..0s"}}));
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  // figure out which node is running a sequencer for log 1 (primary)
  // and kill it
  auto primary = hashing::weighted_ch(1, weights);
  weights[primary] = 0;
  auto secondary = hashing::weighted_ch(1, weights);
  ld_info("primary is N%lu and secondary is N%lu", primary, secondary);
  ASSERT_NE(primary, secondary);
  ASSERT_EQ(
      "ACTIVE", cluster->getNode(primary).sequencerInfo(logid_t(1))["State"]);
  cluster->getNode(primary).kill();

  // wait until all other nodes detect that `primary' is dead
  cluster->waitUntilGossip(/* alive */ false, primary);

  // write into log 1 and verify that the LSN belongs to a new epoch
  // a new sequencer should be brought up
  lsn_t lsn2;
  epoch_t expected_epoch = epoch_t(lsn_to_epoch(lsn1).val() + 1);
  for (int i = 0; i < 10; i++) {
    lsn2 = client->appendSync(logid_t(1), "bar");
    ASSERT_NE(LSN_INVALID, lsn2);
    ASSERT_EQ(expected_epoch, lsn_to_epoch(lsn2));
  }

  // bring primary up and do another write
  cluster->getNode(primary).start();
  cluster->waitUntilGossip(/* alive */ true, primary);
  lsn_t lsn3;
  // create another client, to make sure the epoch_seen is not updated and gets
  // in the way of this test causing automatic re-activation
  auto client2 = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"connect-throttle", "0s..0s"}}));
  wait_until("append succeeds after primary sequencer reactivation", [&] {
    lsn3 = client2->appendSync(logid_t(1), "baz");
    if (lsn3 == LSN_INVALID) {
      EXPECT_EQ(E::ISOLATED, err);
    }
    return lsn3 != LSN_INVALID;
  });
  ASSERT_GT(lsn3, lsn2);

  // now write more records to make sure we hit all the storage nodes
  for (int i = 0; i < 10; i++) {
    lsn_t l = client2->appendSync(logid_t(1), "baz");
    ASSERT_NE(LSN_INVALID, l);
  }

  // now kill 'primary' again
  cluster->getNode(primary).kill();
  cluster->waitUntilGossip(/* alive */ false, primary);

  Stats stats_before =
      checked_downcast<ClientImpl&>(*client).stats()->aggregate();
  lsn_t lsn4;
  wait_until(
      "append succeeds after forced secondary sequencer reactivation", [&] {
        lsn4 = client->appendSync(logid_t(1), "bat");
        if (lsn4 == LSN_INVALID) {
          EXPECT_EQ(E::ISOLATED, err);
        }
        return lsn4 != LSN_INVALID;
      });
  Stats stats_after =
      checked_downcast<ClientImpl&>(*client).stats()->aggregate();

  ASSERT_GT(lsn4, lsn3);
  ASSERT_GE(lsn_to_epoch(lsn4), lsn_to_epoch(lsn3));
  ASSERT_EQ(stats_before.client.append_redirected_not_alive_success + 1,
            stats_after.client.append_redirected_not_alive_success);
}

// Checks that the sequencer gets reactivated if it receives an APPEND message
// with seen epoch higher than its own.
TEST_F(SequencerIntegrationTest, SeenEpochReactivation) {
  // AppendRequest that allows us to inject a value for seen_epoch to the APPEND
  // message.
  class MockAppendRequest : public AppendRequest {
   public:
    MockAppendRequest(std::string payload,
                      epoch_t seen_epoch,
                      append_callback_t callback,
                      std::chrono::milliseconds timeout)
        : AppendRequest(nullptr,
                        logid_t(1),
                        AppendAttributes(),
                        std::move(payload),
                        timeout,
                        std::move(callback)),
          seen_epoch_(seen_epoch) {
      bypassWriteTokenCheck();
    }
    epoch_t getSeenEpoch(logid_t /*log*/) const override {
      return seen_epoch_;
    }

   private:
    epoch_t seen_epoch_;
  };

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .create(1);
  cluster->getNode(0).waitUntilAvailable();

  Settings settings = create_default_settings<Settings>();
  auto processor =
      Processor::create(cluster->getConfig(),
                        std::make_shared<NoopTraceLogger>(cluster->getConfig()),
                        UpdateableSettings<Settings>(settings),
                        nullptr, /* stats*/

                        make_test_plugin_registry());

  epoch_t first_epoch;

  // send the first append (without seen_epoch set)
  std::unique_ptr<Request> rq1 = std::make_unique<MockAppendRequest>(
      "You fight like a dairy farmer!",
      EPOCH_INVALID,
      [&](Status st, const DataRecord& r) {
        EXPECT_EQ(E::OK, st);
        first_epoch = lsn_to_epoch(r.attrs.lsn);
      },
      testTimeout());
  ASSERT_EQ(0, processor->blockingRequest(rq1));

  // the second append includes a higher seen_epoch, triggering sequencer
  // reactivation
  const epoch_t seen_epoch = epoch_t(first_epoch.val_ + 1);
  std::unique_ptr<Request> rq2 = std::make_unique<MockAppendRequest>(
      "How appropriate, you fight like a cow!",
      seen_epoch,
      [&](Status st, const DataRecord& r) {
        EXPECT_EQ(E::OK, st);
        EXPECT_LT(first_epoch, lsn_to_epoch(r.attrs.lsn));
      },
      testTimeout());
  ASSERT_EQ(0, processor->blockingRequest(rq2));
}

// Simulates situation where a node is preempted by a node that is no longer in
// the config
//    1- client sends append for log 1
//       ==> this brings up sequencer on primary
//    2- stop primary
//    3- client sends append for log 1
//       ==> this activates sequencer on secondary
//    4- resume primary
//    5- bump generation number of secondary and wait for config update
//    6- client sends append for log 1
//      ==> this reaches the primary, which responds it was preempted by
//          secondary. however the generation number doesn't match the config,
//          so the clients fails with NOTINCONFIG and retries sending to the
//          primary forcing reactivation, which succeeds.
TEST_F(SequencerIntegrationTest, SequencerReactivationPreemptorNotInConfig) {
  dbg::parseLoglevelOption("debug");
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < 4; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);
  }
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setNodes(nodes)
          .useHashBasedSequencerAssignment(100, "10s")
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(nodes.size());

  for (size_t idx = 0; idx < nodes.size(); ++idx) {
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  // figure out which node is running a sequencer for log 1 (primary)
  // and kill it
  std::vector<double> weights = {1.0, 1.0, 1.0, 1.0};
  auto primary = hashing::weighted_ch(1, weights);
  weights[primary] = 0;
  auto secondary = hashing::weighted_ch(1, weights);
  ASSERT_NE(primary, secondary);
  ASSERT_EQ(
      "ACTIVE", cluster->getNode(primary).sequencerInfo(logid_t(1))["State"]);
  cluster->getNode(primary).suspend();

  // wait until all other nodes detect that `primary' is dead
  cluster->waitUntilGossip(/* alive */ false, primary);

  // write into log 1 and verify that the LSN belongs to a new epoch
  // a new sequencer should be brought up
  auto client2 = cluster->createClient();
  lsn_t lsn2 = client2->appendSync(logid_t(1), "bar");
  ASSERT_NE(LSN_INVALID, lsn2);
  ASSERT_GE(lsn_to_epoch(lsn2), lsn_to_epoch(lsn1));

  // bring primary up
  cluster->getNode(primary).resume();
  cluster->waitUntilGossip(/* alive */ true, primary);

  // replace secondary node to up its generation number
  cluster->bumpGeneration(secondary);
  cluster->waitForConfigUpdate();

  // write into the log again - this should succeed after internally retrying
  // due to: primary PREEMPTED --> secondary NOTINCONFIG -->
  // primary reactivate_if_preempted
  auto client3 = cluster->createClient();
  lsn_t lsn4 = client3->appendSync(logid_t(1), "baz");
  ASSERT_NE(LSN_INVALID, lsn4);
  ASSERT_GE(lsn4, lsn2);
}

// Simulates situation where a node which was sequencer and previously
// preempted the primary, now is no longer a sequencer based on the config.
// this is similar test as above.
//    1- client sends append for log 1
//       ==> this brings up sequencer on primary
//    2- stop primary
//    3- client sends append for log 1
//       ==> this activates sequencer on secondary
//    4- resume primary
//    5- update weights of secondary and wait for config update
//    6- client sends append for log 1
//      ==> this reaches the primary, which responds it was preempted by
//          secondary. however the secondary shouldn't run sequencers based on
//          its config, so it returns NOTREADY. so the clients retries sending
//          to the primary forcing reactivation, which succeeds.
TEST_F(SequencerIntegrationTest, SequencerReactivationPreemptorZeroWeight) {
  dbg::parseLoglevelOption("debug");

  // figure out which nodes will be running a sequencer for log 1
  std::vector<double> weights = {1.0, 1.0, 1.0, 1.0};
  auto primary = hashing::weighted_ch(1, weights);
  weights[primary] = 0;
  auto secondary = hashing::weighted_ch(1, weights);
  ASSERT_NE(primary, secondary);

  Configuration::Nodes nodes, metadata_nodes;
  for (node_index_t i = 0; i < 4; ++i) {
    Configuration::Node node;
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);

    if (i != secondary) {
      // Don't put secondary sequencer node in metadata logs nodeset because
      // we're going to turn it into a non-storage node.
      metadata_nodes[i] = configuration::Node(node);
    }
    nodes[i] = std::move(node);
  }

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(ServerConfig::NodesConfig(metadata_nodes),
                               metadata_nodes.size() - 1,
                               /*replication=*/3,
                               NodeLocationScope::NODE);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .setNodes(nodes)
          .setMetaDataLogsConfig(meta_config)
          .useHashBasedSequencerAssignment(100, "10s")
          .create(nodes.size());

  for (size_t idx = 0; idx < nodes.size(); ++idx) {
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), "foo");
  ASSERT_NE(LSN_INVALID, lsn1);

  // Kill the node running a sequencer for log 1 (primary)
  ASSERT_EQ(
      "ACTIVE", cluster->getNode(primary).sequencerInfo(logid_t(1))["State"]);
  cluster->getNode(primary).suspend();

  // wait until all other nodes detect that `primary' is dead
  cluster->waitUntilGossip(/* alive */ false, primary);

  // write into log 1 and verify that the LSN belongs to a new epoch
  // a new sequencer should be brought up
  auto client2 = cluster->createClient();
  lsn_t lsn2 = client2->appendSync(logid_t(1), "bar");
  ASSERT_NE(LSN_INVALID, lsn2);
  ASSERT_GE(lsn_to_epoch(lsn2), lsn_to_epoch(lsn1));

  // bring primary up
  cluster->getNode(primary).resume();

  // wait for everyone to see it's up
  cluster->waitUntilGossip(/* alive */ true, primary);

  // update the weights so the secondary is no longer a sequencer
  cluster->updateNodeAttributes(
      secondary, configuration::StorageState::DISABLED, 0);
  cluster->waitForConfigUpdate();

  // write into the log again - this should succeed after internally retrying
  // due to: primary PREEMPTED --> secondary NOTREADY -->
  // primary reactivate_if_preempted
  auto client3 = cluster->createClient();
  lsn_t lsn4 = client3->appendSync(logid_t(1), "baz");
  ASSERT_NE(LSN_INVALID, lsn4);
  ASSERT_GE(lsn4, lsn2);
}

// Start a cluster with lazy sequencer placement on all nodes.
std::unique_ptr<IntegrationTestUtils::Cluster> createClusterFromNodes(
    Configuration::Nodes& nodes,
    size_t num_nodes,
    std::vector<IntegrationTestUtils::ParamSpec> extra_params) {
  for (int i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 2);
  }

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  log_attrs.set_replicationFactor(num_nodes - 1);
  log_attrs.set_extraCopies(1);

  auto factory = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .setLogAttributes(log_attrs)
                     .doPreProvisionEpochMetaData() // to ensure all records
                                                    // fall into expected epochs
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection();

  for (const auto& param : extra_params) {
    factory = factory.setParam(param);
  }

  // This starts the nodes and waits until they're all available.
  auto cluster = factory.create(num_nodes);

  // Now wait until all nodes know that each other are available.
  //
  // We don't consider the gossip info valid until all nodes have
  // established gossip connections with each other and received at least
  // one gossip message on each connection.
  for (const auto& indexAndNode : cluster->getNodes()) {
    ld_info("Waiting for node %d to realize all other nodes are alive",
            indexAndNode.first);
    int rv = wait_until([&]() {
      for (const auto& nodeAndStatus : indexAndNode.second->gossipCount()) {
        if (nodeAndStatus.second.first != "ALIVE" ||
            nodeAndStatus.second.second > 1000000) {
          return false;
        }
      }
      return true;
    });

    if (rv == 0) {
      ld_info("Node %d believes all other nodes are alive", indexAndNode.first);
    } else {
      ld_info("Timed out waiting for node %d to believe all others are alive",
              indexAndNode.first);
      abort();
    }
  }

  return cluster;
}

uint64_t sequencerNode(IntegrationTestUtils::Cluster& cluster,
                       const std::vector<uint64_t>& down_nodes,
                       uint64_t logid = 1,
                       bool verify_by_querying_node = true) {
  const Configuration::Nodes& nodes =
      cluster.getConfig()->get()->serverConfig()->getNodes();

  std::vector<double> weights(nodes.size());

  for (const auto& n : nodes) {
    weights[n.first] = n.second.getSequencerWeight();
  }
  for (uint64_t idx : down_nodes) {
    weights[idx] = 0.0;
  }
  auto node_idx = hashing::weighted_ch(logid, weights);

  if (verify_by_querying_node) {
    auto info = cluster.getNode(node_idx).sequencerInfo(logid_t(logid));
    if (info.empty()) {
      ld_error("No sequencer on node %lu!", node_idx);
      EXPECT_FALSE(info.empty());
      ld_check(!info.empty());
    } else {
      EXPECT_EQ("ACTIVE", info["State"]);
    }
  }

  return node_idx;
}

// simulate a partition by:
// 1- blocking any new connection
// 2- blacklisting all the nodes from the failure detector, effectively causing
//    it to not send any gossip to any node, which makes all the nodes believe
//    it is dead.
void isolateNode(node_index_t node_id, IntegrationTestUtils::Cluster& cluster) {
  ld_info("Starting simulated partition for N%d.", node_id);
  auto& node = cluster.getNode(node_id);
  node.newConnections(false);
  for (const auto& it : cluster.getNodes()) {
    node_index_t idx = it.first;
    if (idx == node_id) {
      continue;
    }
    node.gossipBlacklist(idx);
  }

  ld_info("Waiting for all nodes to detect that N%d is now dead.", node_id);
  cluster.waitUntilGossip(/* alive */ false, node_id);
}

// undo isolation simulation
void rejoinNode(node_index_t node_id, IntegrationTestUtils::Cluster& cluster) {
  ld_info("Ending simulated partition for N%d.", node_id);
  auto& node = cluster.getNode(node_id);
  node.newConnections(true);
  for (const auto& it : cluster.getNodes()) {
    node_index_t idx = it.first;
    if (idx == node_id) {
      continue;
    }
    node.gossipWhitelist(idx);
  }

  ld_info("Waiting for all nodes to detect that N%d is now alive.", node_id);
  cluster.waitUntilGossip(/* alive */ true, node_id);
}

TEST_F(SequencerIntegrationTest, SoftSeal) {
  // Start a cluster with lazy sequencer placement on all nodes.

  Configuration::Nodes nodes;

  // Tell all nodes to not do recovery (including sealing).
  auto cluster = createClusterFromNodes(nodes, 3, {{"--test-bypass-recovery"}});

  auto client = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  // Append some records.
  epoch_t e;

  for (int i = 0; i < 10; ++i) {
    lsn_t lsn = client->appendSync(logid_t(1), Payload("hi", 2));
    if (i == 0) {
      ASSERT_NE(LSN_INVALID, lsn);
      e = lsn_to_epoch(lsn);
    } else {
      EXPECT_EQ(e, lsn_to_epoch(lsn));
    }
  }

  // Figure out which node is running a sequencer for log 1 and pause it.
  auto node_idx = sequencerNode(*cluster, {});
  cluster->getNode(node_idx).suspend();

  // Wait until all other nodes detect that `node_idx' is dead.
  cluster->waitUntilGossip(/* alive */ false, node_idx);

  // Append some more records expecting them to go to some other node.
  client = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  ++e.val_;
  for (int i = 0; i < 10; ++i) {
    lsn_t lsn = client->appendSync(logid_t(1), Payload("hello", 5));
    EXPECT_EQ(e, lsn_to_epoch(lsn));
  }

  auto node_idx2 = sequencerNode(*cluster, {node_idx});
  EXPECT_NE(node_idx2, node_idx);

  // Resume `node_idx`.
  cluster->getNode(node_idx).resume();

  // Wait until all other nodes detect that `node_idx' is alive again.
  cluster->waitUntilGossip(/* alive */ true, node_idx);

  // Recovery need to complete before the following appends can succeed, since
  // they'll be redirected with PREEMPTED, and the sequencer has to hold them
  // until it finds out whether or not the original append was recovered.
  cluster->getNode(node_idx).startRecovery(logid_t{1});
  cluster->getNode(node_idx).waitForRecovery(logid_t{1});

  // Append some records to `node_idx` again. It should be redirected to
  // `node_idx2`, then to `node_idx` again, where reactivation should be forced
  // because of a cycle.

  client = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  ++e.val_;
  for (int i = 0; i < 10; ++i) {
    lsn_t lsn = client->appendSync(logid_t(1), Payload("bonjour", 7));
    EXPECT_EQ(e, lsn_to_epoch(lsn)); // most notably, not e-2
  }
}

TEST_F(SequencerIntegrationTest, AutoLogProvisioning) {
  // Test that the sequencer provisions its own metadata

  Configuration::Nodes nodes;
  size_t num_nodes = 3;
  for (int i = 0; i < 3; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(/*num_shards*/ 1);
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_nodeSetSize(10);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setLogGroupName("test_range")
                     .setLogAttributes(log_attrs)
                     .create(3);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();

  // Append a record

  lsn_t lsn;
  do {
    ld_info("Appending record");
    lsn = client->appendSync(logid_t(1), Payload("hi", 2));
    if (lsn == LSN_INVALID) {
      ASSERT_EQ(E::CANCELLED, err);
    }
  } while (lsn == LSN_INVALID);

  ld_info("Waiting for metadata log writes to complete");
  cluster->waitForMetaDataLogWrites();

  auto getSequencerEpoch = [&]() {
    epoch_t res = EPOCH_INVALID;
    for (int i = 0; i < 3; ++i) {
      auto seq_info = cluster->getNode(i).sequencerInfo(logid_t(1));
      if (!seq_info.empty()) {
        res = std::max(epoch_t(atoi(seq_info["Epoch"].c_str())), res);
      }
    }
    ld_info("Current epoch %u", res.val());
    return res;
  };

  wait_until(
      "sequencer reactivates", [&]() { return getSequencerEpoch().val() > 1; });

  // Get current sequencer epoch
  epoch_t cur_epoch = getSequencerEpoch();
  ASSERT_NE(EPOCH_INVALID, cur_epoch);

  // Read all metadata, verify it's correct
  Status result = E::OK;
  class ReadMetaDataRequest : public Request {
   public:
    using Callback =
        std::function<void(Status, size_t, std::unique_ptr<EpochMetaData>)>;
    ReadMetaDataRequest(Callback cb)
        : Request(RequestType::TEST_DUMP_METADATA_GET_ALL_METADATA_REQUEST),
          cb_(std::move(cb)) {}

    Request::Execution execute() override {
      reader_ = std::make_unique<MetaDataLogReader>(
          logid_t(1),
          EPOCH_MIN,
          EPOCH_MAX,
          [this](Status st, MetaDataLogReader::Result result) {
            ++records_read_;
            if (st != E::OK) {
              ld_error(
                  "Could not read metadata log: %s", error_description(st));
              complete(st);
            } else {
              ld_info("Read metadata log entry at lsn %s: %s",
                      lsn_to_string(result.lsn).c_str(),
                      result.metadata->toString().c_str());
              ld_check(result.metadata->isValid());
              last_metadata_ = std::move(result.metadata);
              if (result.source == MetaDataLogReader::RecordSource::LAST) {
                complete(E::OK);
              }
            }
          });
      reader_->start();
      return Execution::CONTINUE;
    }

    void complete(Status st) {
      Worker::onThisThread()->disposeOfMetaReader(std::move(reader_));
      cb_(st, records_read_, std::move(last_metadata_));
      delete this;
    }

   private:
    size_t records_read_{0};
    std::unique_ptr<EpochMetaData> last_metadata_;
    Callback cb_;
    std::unique_ptr<MetaDataLogReader> reader_;
  };

  folly::Baton<> baton;
  Status res = E::OK;
  size_t num_records = 0;
  std::unique_ptr<EpochMetaData> metadata_read;
  std::unique_ptr<Request> request = std::make_unique<ReadMetaDataRequest>(
      [&](Status st,
          size_t records_read,
          std::unique_ptr<EpochMetaData> metadata) {
        res = st;
        num_records = records_read;
        metadata_read = std::move(metadata);
        baton.post();
      });

  ClientImpl* impl = checked_downcast<ClientImpl*>(client.get());
  ssize_t rv = impl->getProcessor().postWithRetrying(request);
  ASSERT_EQ(0, rv);
  baton.wait();
  ASSERT_EQ(E::OK, res);
  ASSERT_EQ(1, num_records);
  ASSERT_TRUE(metadata_read);
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
            metadata_read->replication.toString());
  ASSERT_EQ(epoch_t(1), metadata_read->h.effective_since);

  // Changing the replication factor in the config
  auto full_config = cluster->getConfig()->get();
  ld_check(full_config);
  auto logs_config_changed = full_config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());

  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;
  auto& attrs = log_in_directory.log_group->attrs();
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
            ReplicationProperty::fromLogAttributes(attrs).toString());
  auto new_attrs = attrs.with_replicationFactor(1);
  ASSERT_TRUE(local_logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withLogAttributes(new_attrs)));

  ld_info("Changing config");
  cluster->writeConfig(
      full_config->serverConfig().get(), logs_config_changed.get());

  wait_until("sequencer activates", [&]() {
    lsn_t tmp_lsn = client->appendSync(logid_t(1), Payload("hi", 2));
    if (tmp_lsn == LSN_INVALID) {
      return false;
    }
    ld_info("Appended %s", lsn_to_string(tmp_lsn).c_str());
    return getSequencerEpoch() > cur_epoch;
  });

  auto get_stats = [&]() {
    std::pair<uint64_t, uint64_t> res{0, 0};
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      res.first += stats["background_sequencer_reactivation_checks_scheduled"];
      res.second += stats["background_sequencer_reactivation_checks_completed"];
    }
    return res;
  };
  wait_until("stats match up", [&]() {
    auto stats = get_stats();
    ld_check(stats.first > 0);
    ld_info("Scheduled reactivations: %lu, completed: %lu",
            stats.first,
            stats.second);
    return stats.first == stats.second;
  });

  ld_info("Sequencer reactivated");
  cluster->waitForMetaDataLogWrites();

  folly::Baton<> baton2;
  request = std::make_unique<ReadMetaDataRequest>(
      [&](Status st,
          size_t records_read,
          std::unique_ptr<EpochMetaData> metadata) {
        res = st;
        num_records = records_read;
        metadata_read = std::move(metadata);
        baton2.post();
      });

  rv = impl->getProcessor().postWithRetrying(request);
  ASSERT_EQ(0, rv);
  baton2.wait();
  ASSERT_EQ(E::OK, res);
  ASSERT_EQ(2, num_records);
  ASSERT_TRUE(metadata_read);
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 1}}).toString(),
            metadata_read->replication.toString());
  ASSERT_GT(metadata_read->h.effective_since.val(), 1);

  ld_info("Reactivating sequencers by restarting the cluster");
  cluster->stop();
  cluster->start();
  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }
  cluster->waitForMetaDataLogWrites();

  folly::Baton<> baton3;
  request = std::make_unique<ReadMetaDataRequest>(
      [&](Status st,
          size_t records_read,
          std::unique_ptr<EpochMetaData> metadata) {
        res = st;
        num_records = records_read;
        metadata_read = std::move(metadata);
        baton3.post();
      });

  rv = impl->getProcessor().postWithRetrying(request);
  ASSERT_EQ(0, rv);
  baton3.wait();

  // verifying, that no new metadata has been provisioned
  ASSERT_EQ(E::OK, res);
  ASSERT_EQ(2, num_records);
}

TEST_F(SequencerIntegrationTest, AutoLogProvisioningEpochStorePreemption) {
  // Test that the sequencer gets preempted when it detects that a newer
  // sequencer is running in the tier

  Configuration::Nodes nodes;
  size_t num_nodes = 5;
  // 2 sequencer nodes and 3 storage nodes
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i < 2) {
      node.addSequencerRole();
    } else {
      node.addStorageRole();
    }
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_nodeSetSize(10);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setLogGroupName("test_range")
                     .setLogAttributes(log_attrs)
                     .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  auto client = cluster->createClient();

  // Append a record

  lsn_t lsn;
  do {
    ld_info("Appending record");
    lsn = client->appendSync(logid_t(1), Payload("hi", 2));
    if (lsn == LSN_INVALID) {
      ASSERT_EQ(E::CANCELLED, err);
    }
  } while (lsn == LSN_INVALID);

  ld_info("Waiting for metadata log writes to complete");
  cluster->waitForMetaDataLogWrites();
  cluster->waitForRecovery();

  // metadata is written, now stop N0
  ld_info("Stopping N0");
  cluster->getNode(0).suspend();

  ld_info("Waiting for all nodes to detect latest cluster state.");
  cluster->waitUntilGossip(/* alive */ false, 0);
  // Appending another record to make sure a sequencer is active on N1

  auto client2 = cluster->createClient();
  do {
    ld_info("Appending record");
    lsn = client2->appendSync(logid_t(1), Payload("hi", 2));
    if (lsn == LSN_INVALID) {
      ASSERT_EQ(E::CANCELLED, err);
    }
  } while (lsn == LSN_INVALID);

  cluster->waitForRecovery();

  // Resume N0, stop N1
  ld_info("Stopping N1");
  cluster->getNode(1).suspend();
  ld_info("Resuming N0");
  cluster->getNode(0).resume();

  // Wait until all nodes detect the latest cluster state
  ld_info("Waiting for all nodes to detect latest cluster state.");
  cluster->waitUntilGossip(/* alive */ true, 0, {1});
  cluster->waitUntilGossip(/* alive */ false, 1);

  // Appending another record to make sure a sequencer is active on N0
  auto client3 = cluster->createClient();
  do {
    ld_info("Appending record");
    lsn = client3->appendSync(logid_t(1), Payload("hi", 2));
    if (lsn == LSN_INVALID) {
      ASSERT_EQ(E::CANCELLED, err);
    }
  } while (lsn == LSN_INVALID);
  cluster->waitForRecovery();

  ld_info("Resuming N1");
  cluster->getNode(1).resume();

  // Wait until all nodes detect the latest cluster state
  ld_info("Waiting for all nodes to detect latest cluster state.");
  cluster->waitUntilGossip(/* alive */ true, 1);

  // Verifying that there are 2 active sequencers

  auto getNumActiveSequencers = [&]() {
    int res = 0;
    for (int i = 0; i < num_nodes; ++i) {
      auto seq_info = cluster->getNode(i).sequencerInfo(logid_t(1));
      if (!seq_info.empty() && seq_info["State"] == "ACTIVE") {
        ++res;
      }
    }
    ld_info("%d active sequencers", res);
    return res;
  };
  ASSERT_EQ(2, getNumActiveSequencers());

  auto getSequencerEpoch = [&]() {
    epoch_t res = EPOCH_INVALID;
    for (int i = 0; i < num_nodes; ++i) {
      auto seq_info = cluster->getNode(i).sequencerInfo(logid_t(1));
      if (!seq_info.empty()) {
        res = std::max(epoch_t(atoi(seq_info["Epoch"].c_str())), res);
      }
    }
    ld_info("Current epoch %u", res.val());
    return res;
  };
  epoch_t cur_epoch = getSequencerEpoch();

  // Updating the config - changing the replication factor
  auto full_config = cluster->getConfig()->get();
  ld_check(full_config);
  auto logs_config_changed = full_config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());

  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;
  auto& attrs = log_in_directory.log_group->attrs();
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
            ReplicationProperty::fromLogAttributes(attrs).toString());
  auto new_attrs = attrs.with_replicationFactor(1);
  ASSERT_TRUE(local_logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withLogAttributes(new_attrs)));

  ld_info("Changing config");
  cluster->writeConfig(
      full_config->serverConfig().get(), logs_config_changed.get());

  wait_until("sequencer reactivates",
             [&]() { return getSequencerEpoch() > cur_epoch; });

  ld_info("Sequencer reactivated");

  // Verifying that one of the sequencers has went into a PREEMPTED state
  wait_until("sequencer gets preempted",
             [&]() { return 1 == getNumActiveSequencers(); });
}

// TODO (T25848602): this test is broken
TEST_F(SequencerIntegrationTest, DISABLED_SilentDuplicatesBasic) {
  Configuration::Nodes nodes;
  // Start a cluster with lazy sequencer placement on all nodes.
  // Tell all nodes to not do recovery (including sealing).
  // Note that the cluster must be small to make sure copysets intersect,
  // causing preemption through soft seals.
  auto cluster =
      createClusterFromNodes(nodes,
                             3,
                             {{"--test-bypass-recovery"},
                              {"--disable-check-seals"},
                              {"--hold-store-replies", "true"},
                              {"--disable-chain-sending",
                               IntegrationTestUtils::ParamScope::SEQUENCER}});

  auto client = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  // Append a record, to create a non-empty epoch.
  epoch_t first_epoch;

  // Sending e2n1
  ld_info("Appending record that creates first sequencer.");
  lsn_t lsn1 = client->appendSync(logid_t(1), Payload("hi", 2));
  ld_info("   Record that creates first sequencer: %s",
          lsn_to_string(lsn1).c_str());
  ASSERT_NE(LSN_INVALID, lsn1);
  first_epoch = lsn_to_epoch(lsn1);

  // Figure out which node is running a sequencer for log 1, and simulate a
  // partial netowrk partition for it.
  auto sequencer_idx = sequencerNode(*cluster, {});
  isolateNode(sequencer_idx, *cluster);

  // Old client is still talking to the old sequencer.  Create a new client that
  // will talk to the new sequencer.

  auto client2 = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  // Append a record to activate the new sequencer.
  // Sending e3n1. The copyset will contain all nodes except old sequencer node.
  ld_info("Appending record that creates second sequencer.");
  lsn_t lsn2 = client2->appendSync(logid_t(1), Payload("hello", 5));
  ld_info("  record that creates second sequencer: %s",
          lsn_to_string(lsn2).c_str());
  epoch_t second_epoch{lsn_to_epoch(lsn2)};

  EXPECT_EQ(first_epoch.val_ + 1, second_epoch.val_);

  auto sequencer_idx2 = sequencerNode(*cluster, {sequencer_idx});
  EXPECT_NE(sequencer_idx2, sequencer_idx);

  // The original connection should still be open, and when we try to use it,
  // the old sequencer should create LSNs and send STORE messages, then get
  // pre-empted.

  std::atomic_bool done{false};
  // Record how many append_bytes each sequencer has received, so we can
  // tell when the append reaches each sequencer.
  auto new_sequencer_prev_bytes =
      cluster->getNode(sequencer_idx2).stats()["append_payload_bytes"];
  auto old_sequencer_prev_bytes =
      cluster->getNode(sequencer_idx).stats()["append_payload_bytes"];

  // Tell the old sequencer to avoid picking the new sequencer node in copysets.
  // This will ensure that the it'll pick itself into the copyset, thus hitting
  // a non-soft-sealed node, preventing APPENDED_Message::NOT_REPLICATED.
  std::string response =
      cluster->getNode(sequencer_idx)
          .sendCommand(("set --ttl=3600s test-do-not-pick-in-copysets " +
                        std::to_string(sequencer_idx2))
                           .c_str());
  EXPECT_EQ("END\r\n",
            response.substr(response.size() - std::min(response.size(), 5ul)));
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");

  uint64_t lsn3{LSN_INVALID};
  // This is the message that will get both PREEMPTED and OK (on different
  // nodes), and thus could cause silent duplicates. The preemption will be
  // caused by a soft seal.
  ld_info("Appending record that should get preempted. This will wait for "
          "recovery.");
  int rc = client->append(logid_t(1),
                          Payload("bonjour", 7),
                          [&done, &lsn3](Status st, const DataRecord& rec) {
                            EXPECT_EQ(Status::OK, st);
                            lsn3 = rec.attrs.lsn;
                            done.store(true);
                          });
  ASSERT_EQ(0, rc);

  // Wait for it to be PREEMPTED, then sent to new sequencer.
  ld_info("Waiting for append to reach new sequencer.");
  wait_until([&] {
    auto new_sequencer_current_bytes =
        cluster->getNode(sequencer_idx2).stats()["append_payload_bytes"];
    if (new_sequencer_current_bytes > new_sequencer_prev_bytes) {
      EXPECT_EQ(new_sequencer_prev_bytes + 7, new_sequencer_current_bytes);
      return true;
    }
    return false;
  });
  ld_info("Append reached new sequencer.");

  // Make sure it also went to the old sequencer.
  auto old_sequencer_current_bytes =
      cluster->getNode(sequencer_idx).stats()["append_payload_bytes"];
  EXPECT_EQ(old_sequencer_prev_bytes + 7, old_sequencer_current_bytes);

  // The request should be held until we complete recovery, and since we paused
  // recovery, it shouldn't be done yet.
  EXPECT_FALSE(done.load());

  // End the simulated partition before doing recovery.
  rejoinNode(sequencer_idx, *cluster);

  ld_info("Starting recovery.");
  cluster->getNode(sequencer_idx2).startRecovery(logid_t{1});
  ld_info("Waiting for recovery to complete.");
  cluster->getNode(sequencer_idx2).waitForRecovery(logid_t{1});
  ld_info("Recovery complete, waiting for preempted append to be done.");

  // Wait for PREEMPTED append to be done.
  wait_until([&] { return done.load(); });
  ld_info("Preempted append is done!");

  // The original STORE should be recovered, so should be in the original
  // epoch:
  EXPECT_NE(LSN_INVALID, lsn3);
  EXPECT_EQ(first_epoch, lsn_to_epoch(lsn3));
  EXPECT_EQ(lsn1 + 1, lsn3);

  // Write one more record, to serve as an "end of stream" record, then read up
  // to that and see how many records we get.
  lsn_t lsn_end = client2->appendSync(logid_t(1), Payload("goodbye", 7));

  // Now that the partition is healed, the active sequencer has moved back to
  // it's original location, causing another epoch.  Since we disabled automatic
  // recovery, we need to manually trigger it before we can read.
  cluster->getNode(sequencer_idx).startRecovery(logid_t{1});

  // Now read from the log, and make sure we don't have any duplicates.
  std::unique_ptr<Reader> reader(client->createReader(1));
  reader->startReading(logid_t{1}, lsn1);

  // We've done 4 writes, so we expect 4 records.  If there are any more, we
  // have silent duplicates!

  std::vector<lsn_t> expected{lsn1,
                              lsn3,
                              static_cast<lsn_t>(GapType::BRIDGE),
                              static_cast<lsn_t>(GapType::BRIDGE),
                              lsn2,
                              static_cast<lsn_t>(GapType::BRIDGE),
                              static_cast<lsn_t>(GapType::BRIDGE),
                              lsn_end};

  std::vector<lsn_t> actual;
  for (size_t i = 0; i < expected.size(); ++i) {
    std::vector<std::unique_ptr<DataRecord>> data_out;
    GapRecord gap_out;

    auto num_read = reader->read(1, &data_out, &gap_out);
    if (num_read == -1) {
      actual.push_back(static_cast<lsn_t>(gap_out.type));
    } else {
      ASSERT_EQ(1, num_read);
      actual.push_back(data_out.front()->attrs.lsn);
    }
  }

  EXPECT_EQ(expected, actual);
}

// this is essentially the same test as above except that the preempted
// epoch is the previous previous epoch (2 epochs older than current)
// to validate that recovery of older epoch also allows preventing
// silent duplicates
// TODO (T25848602): this test is broken
TEST_F(SequencerIntegrationTest, DISABLED_SilentDuplicatesTwoEpochsRecovered) {
  Configuration::Nodes nodes;
  // Start a cluster with lazy sequencer placement on all nodes.
  // Tell all nodes to not do recovery (including sealing).
  auto cluster =
      createClusterFromNodes(nodes,
                             3,
                             {{"--test-bypass-recovery"},
                              {"--disable-check-seals"},
                              {"--hold-store-replies", "true"},
                              {"--disable-chain-sending",
                               IntegrationTestUtils::ParamScope::SEQUENCER}});

  auto client = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  // Append a record, to create a non-empty epoch.
  epoch_t first_epoch;

  // Sending e2n1
  ld_info("Appending record that creates first sequencer.");
  lsn_t lsn1 = client->appendSync(logid_t(1), Payload("hi", 2));
  ld_info("   Record that creates first sequencer: %s",
          lsn_to_string(lsn1).c_str());
  ASSERT_NE(LSN_INVALID, lsn1);
  first_epoch = lsn_to_epoch(lsn1);

  // Figure out which node is running a sequencer for log 1, and simulate a
  // partial netowrk partition for it.
  auto sequencer_idx = sequencerNode(*cluster, {});
  isolateNode(sequencer_idx, *cluster);

  // Old client is still talking to the old sequencer.  Create a new client that
  // will talk to the new sequencer.

  auto client2 = cluster->createClient(
      DEFAULT_TEST_TIMEOUT,
      createClientSettings({{"enable-initial-get-cluster-state", "false"}}));

  // Append a record to activate the new sequencer.
  // Sending e3n1
  ld_info("Appending record that creates second sequencer.");
  lsn_t lsn2 = client2->appendSync(logid_t(1), Payload("hello", 5));
  ld_info("  record that creates second sequencer: %s",
          lsn_to_string(lsn2).c_str());
  epoch_t second_epoch{lsn_to_epoch(lsn2)};

  EXPECT_EQ(first_epoch.val_ + 1, second_epoch.val_);

  auto sequencer_idx2 = sequencerNode(*cluster, {sequencer_idx});
  EXPECT_NE(sequencer_idx2, sequencer_idx);

  // now restart the secondary (to triggger another sequencer reactivation
  // upon the next append)
  cluster->getNode(sequencer_idx2).restart(false, true);
  cluster->waitUntilGossip(/* alive */ true, sequencer_idx2);
  cluster->waitUntilGossip(/* alive */ false, sequencer_idx);

  // Append another record to activate again the seconday
  // Sending e4n1
  ld_info("Appending record that re-creates second sequencer.");
  lsn_t lsn3 = client2->appendSync(logid_t(1), Payload("hola", 4));
  ld_info("  record that re-creates second sequencer: %s",
          lsn_to_string(lsn3).c_str());
  epoch_t third_epoch{lsn_to_epoch(lsn3)};

  EXPECT_EQ(first_epoch.val_ + 2, third_epoch.val_);

  // The original connection should still be open, and when we try to use it,
  // the old sequencer should create LSNs and send STORE messages, then get
  // pre-empted.

  std::atomic_bool done{false};
  // Record how many append_bytes each sequencer has received, so we can
  // tell when the append reaches each sequencer.
  auto new_sequencer_prev_bytes =
      cluster->getNode(sequencer_idx2).stats()["append_payload_bytes"];
  auto old_sequencer_prev_bytes =
      cluster->getNode(sequencer_idx).stats()["append_payload_bytes"];

  uint64_t lsn4{LSN_INVALID};
  // This is the message that will get both STORED and PREEMPTED (on different
  // nodes), and thus could cause silent duplicates.
  ld_info("Appending record that should get preempted. This will wait for "
          "recovery.");
  int rc = client->append(logid_t(1),
                          Payload("bonjour", 7),
                          [&done, &lsn4](Status st, const DataRecord& rec) {
                            EXPECT_EQ(Status::OK, st);
                            lsn4 = rec.attrs.lsn;
                            done.store(true);
                          });
  ASSERT_EQ(0, rc);

  // Wait for it to be PREEMPTED, then sent to new sequencer.
  ld_info("Waiting for append to reach new sequencer.");
  wait_until([&] {
    auto new_sequencer_current_bytes =
        cluster->getNode(sequencer_idx2).stats()["append_payload_bytes"];
    if (new_sequencer_current_bytes > new_sequencer_prev_bytes) {
      EXPECT_EQ(new_sequencer_prev_bytes + 7, new_sequencer_current_bytes);
      return true;
    }
    return false;
  });
  ld_info("Append reached new sequencer.");

  // Make sure it also went to the old sequencer.
  auto old_sequencer_current_bytes =
      cluster->getNode(sequencer_idx).stats()["append_payload_bytes"];
  EXPECT_EQ(old_sequencer_prev_bytes + 7, old_sequencer_current_bytes);

  // The request should be held until we complete recovery, and since we paused
  // recovery, it shouldn't be done yet.
  EXPECT_FALSE(done.load());

  // End the simulated partition before doing recovery.
  rejoinNode(sequencer_idx, *cluster);

  ld_info("Starting recovery.");
  cluster->getNode(sequencer_idx2).startRecovery(logid_t{1});
  ld_info("Waiting for recovery to complete.");
  cluster->getNode(sequencer_idx2).waitForRecovery(logid_t{1});
  ld_info("Recovery complete, waiting for preempted append to be done.");

  // Wait for PREEMPTED append to be done.
  wait_until([&] { return done.load(); });
  ld_info("Preempted append is done!");

  // The original STORE should be recovered, so should be in the original
  // epoch:
  EXPECT_NE(LSN_INVALID, lsn4);
  EXPECT_EQ(first_epoch, lsn_to_epoch(lsn4));
  EXPECT_EQ(lsn1 + 1, lsn4);

  // Write one more record, to serve as an "end of stream" record, then read up
  // to that and see how many records we get.
  lsn_t lsn_end = client2->appendSync(logid_t(1), Payload("goodbye", 7));

  // Now that the partition is healed, the active sequencer has moved back to
  // it's original location, causing another epoch.  Since we disabled automatic
  // recovery, we need to manually trigger it before we can read.
  cluster->getNode(sequencer_idx).startRecovery(logid_t{1});

  // Now read from the log, and make sure we don't have any duplicates.
  std::unique_ptr<Reader> reader(client->createReader(1));
  reader->startReading(logid_t{1}, lsn1);

  // We've done 4 writes, so we expect 4 records.  If there are any more, we
  // have silent duplicates!

  std::vector<lsn_t> expected{lsn1,
                              lsn4,
                              static_cast<lsn_t>(GapType::BRIDGE),
                              static_cast<lsn_t>(GapType::BRIDGE),
                              lsn2,
                              static_cast<lsn_t>(GapType::BRIDGE),
                              static_cast<lsn_t>(GapType::BRIDGE),
                              lsn3,
                              static_cast<lsn_t>(GapType::BRIDGE),
                              static_cast<lsn_t>(GapType::BRIDGE),
                              lsn_end};

  std::vector<lsn_t> actual;
  for (size_t i = 0; i < expected.size(); ++i) {
    std::vector<std::unique_ptr<DataRecord>> data_out;
    GapRecord gap_out;

    auto num_read = reader->read(1, &data_out, &gap_out);
    if (num_read == -1) {
      actual.push_back(static_cast<lsn_t>(gap_out.type));
    } else {
      ASSERT_EQ(1, num_read);
      actual.push_back(data_out.front()->attrs.lsn);
    }
  }

  EXPECT_EQ(expected, actual);
}

// Test that an Appender will return CANCELLED to client if it believes
// a silent duplicate may have been created.
// The scenario involves having two overlapping copysets, so an appends gets
// a preempted error from one storage node and gets successfully stored on
// another one.
// TODO (#26486221): This test is broken.
TEST_F(SequencerIntegrationTest, DISABLED_SilentDuplicatesCancelled) {
  Configuration::Nodes nodes;
  int num_nodes = 5;
  // 3 storage-only nodes
  node_index_t idx = 0;
  for (; idx < 3; idx++) {
    auto& node = nodes[idx];
    node.generation = 1;
    node.addStorageRole(/*num_shards*/ 2);
  }
  // 2 sequencer-only nodes
  for (; idx < num_nodes; idx++) {
    auto& node = nodes[idx];
    node.generation = 1;
    node.addSequencerRole();
  }

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  // This test relies on a replication factor of 2. Set it explicitly here.
  log_attrs.set_replicationFactor(2);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .setLogAttributes(log_attrs)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .setParam("--sticky-copysets-block-size", "1")
          .setParam("--store-timeout", "1s..10s")
          .setParam("--disable-check-seals", "true")
          .setParam("--disable-chain-sending", "true")
          .setParam("--test-bypass-recovery", "true")
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(nodes.size());

  // append several records to activate sequencer on primary
  // and write on all store nodes
  auto client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, createClientSettings({}));
  // the following code ensures that at least one record is written on all the
  // storage nodes.
  cluster->getNode(0).suspend();
  cluster->waitUntilGossip(/* alive */ false, 0);
  lsn_t lsn1 = client->appendSync(logid_t(1), "hi");
  ASSERT_NE(LSN_INVALID, lsn1);
  cluster->getNode(0).resume();
  cluster->waitUntilGossip(/* alive */ true, 0);
  cluster->getNode(1).suspend();
  cluster->waitUntilGossip(/* alive */ false, 1);
  lsn1 = client->appendSync(logid_t(1), "hi");
  ASSERT_NE(LSN_INVALID, lsn1);
  cluster->getNode(1).resume();
  cluster->waitUntilGossip(/* alive */ true, 1);
  // at this point, all the sotrage nodes have seen records of epoch 1

  // figure out which node is running a sequencer for log 1 (primary)
  auto primary = sequencerNode(*cluster, {}, 1);

  // stop a storage node, to prevent stores from reaching it. the copyset of
  // next appends will be {1,2}
  cluster->getNode(0).suspend();
  cluster->waitUntilGossip(/* alive */ false, 0);
  // stop primary temporarily
  cluster->getNode(primary).suspend();
  cluster->waitUntilGossip(/* alive */ false, primary);

  // append a record with a different client to activate sequencer on secondary
  auto client2 =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, createClientSettings({}));
  lsn_t lsn2 = client2->appendSync(logid_t(1), "hello");
  ASSERT_NE(LSN_INVALID, lsn2);

  auto secondary = sequencerNode(*cluster, {primary}, 1);
  ASSERT_NE(primary, secondary);

  // resume primary
  cluster->getNode(primary).resume();
  cluster->waitUntilGossip(/* alive */ true, primary);
  // resume N0 to let it accept stores on the next append, and stop a different
  // storage node. the idea here is to have a different copyset (that
  // intersects with previous one), so that later we get both a preempted and
  // successful store. the copyset of next appends will be {0,2}. So N0 should
  // accept the STORE, but N2 should fail with PREEMPTED since it already
  // received a record in higher epoch.
  cluster->getNode(0).resume();
  cluster->waitUntilGossip(/* alive */ true, 0);
  cluster->getNode(1).suspend();
  cluster->waitUntilGossip(/* alive */ false, 1);

  // Record how many append_bytes each sequencer has received, so we can
  // tell when the append reaches each sequencer.
  auto primary_prev_bytes =
      cluster->getNode(primary).stats()["append_payload_bytes"];
  auto secondary_prev_bytes =
      cluster->getNode(secondary).stats()["append_payload_bytes"];

  Semaphore sem;
  // then send another append with first client, it should
  // - get preempted on primary back to secondary (with one successful store)
  // - get redirected from secondary back to primary
  // - fail on primary after reactivation because the LSN has already been
  // assigned in previous epoch and recovery was unable to re-replicate it.
  int rc = client->append(logid_t(1),
                          Payload("bonjour", 7),
                          [&sem](Status st, const DataRecord& rec) {
                            // this append is expected to fail
                            EXPECT_EQ(Status::CANCELLED, st);
                            sem.post();
                          });
  ASSERT_EQ(0, rc);

  // Wait for it to be PREEMPTED, then sent to new sequencer.
  ld_info("Waiting for append to reach new sequencer.");
  wait_until([&] {
    // wait until append went to the old sequencer (twice).
    auto primary_current_bytes =
        cluster->getNode(primary).stats()["append_payload_bytes"];
    return (primary_prev_bytes + 7 + 7) == primary_current_bytes;
  });
  ld_info("Append reached new sequencer and came back.");

  // make sure we also reached the secondary once
  auto secondary_current_bytes =
      cluster->getNode(secondary).stats()["append_payload_bytes"];
  EXPECT_EQ(secondary_prev_bytes + 7, secondary_current_bytes);

  // The request should be held until we complete recovery, and since we paused
  // recovery, it shouldn't be done yet.
  EXPECT_EQ(0, sem.value());

  // go back to previous setup so that recovery cannot see the stored copy.
  cluster->getNode(1).resume();
  cluster->waitUntilGossip(/* alive */ true, 1);
  cluster->getNode(0).suspend();
  cluster->waitUntilGossip(/* alive */ false, 0);

  ld_info("Starting recovery.");
  cluster->getNode(primary).startRecovery(logid_t{1});
  ld_info("Waiting for recovery to complete.");
  cluster->getNode(primary).waitForRecovery(logid_t{1});
  ld_info("Recovery complete, waiting for preempted append to be done.");

  // Wait for PREEMPTED append to be done and append failed with CANCELLED.
  sem.wait();
  ld_info("Preempted append is done!");
}

// simulate a case where all the stores return preempted and so there is no
// risk of silent duplicate. the client should succeed storing the record in
// the new eopch.
TEST_F(SequencerIntegrationTest, SilentDuplicatesBypassed) {
  Configuration::Nodes nodes;
  int num_nodes = 4;
  // 2 storage-only nodes
  node_index_t idx = 0;
  for (; idx < 2; idx++) {
    auto& node = nodes[idx];
    node.generation = 1;
    node.addStorageRole(/*num_shards*/ 2);
  }
  // 2 sequencer-only nodes
  for (; idx < num_nodes; idx++) {
    auto& node = nodes[idx];
    node.generation = 1;
    node.addSequencerRole();
  }

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  // This test relies on a replication factor of 2. Set it explicitly here.
  log_attrs.set_replicationFactor(2);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNodes(nodes)
          .setLogAttributes(log_attrs)
          .useHashBasedSequencerAssignment(100, "10s")
          .enableMessageErrorInjection()
          .setParam("--sticky-copysets-block-size", "1")
          .setParam("--store-timeout", "1s..10s")
          .setParam("--disable-check-seals", "true")
          .setParam("--disable-chain-sending", "true")
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .create(nodes.size());

  // append several records to activate sequencer on primary
  // and write on all store nodes
  auto client =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, createClientSettings({}));
  lsn_t lsn1 = client->appendSync(logid_t(1), "hi");
  ASSERT_NE(LSN_INVALID, lsn1);

  // figure out which node is running a sequencer for log 1 (primary)
  auto primary = sequencerNode(*cluster, {}, 1);

  cluster->getNode(primary).waitForRecovery(logid_t(1));

  // stop primary temporarily
  cluster->getNode(primary).suspend();
  cluster->waitUntilGossip(/* alive */ false, primary);

  // append a record with a different client to activate sequencer on secondary
  auto client2 =
      cluster->createClient(DEFAULT_TEST_TIMEOUT, createClientSettings({}));
  lsn_t lsn2 = client2->appendSync(logid_t(1), "hello");
  ASSERT_NE(LSN_INVALID, lsn2);

  // resume primary
  cluster->getNode(primary).resume();
  cluster->waitUntilGossip(/* alive */ true, primary);

  Semaphore sem;
  // then send another append with first client, it should
  // - get preempted on primary back to secondary (with no successful store
  //   and thus NOT_REPLICATED flag set)
  // - get redirected from secondary back to primary
  // - succeed on primary after reactivation
  int rc = client->append(logid_t(1),
                          Payload("bonjour", 7),
                          [&sem](Status st, const DataRecord& rec) {
                            EXPECT_EQ(Status::OK, st);
                            sem.post();
                          });
  ASSERT_EQ(0, rc);
  sem.wait();
}

TEST_F(SequencerIntegrationTest, AbortEpoch) {
  // N0 sequencer, N1, N2, N3, N4 storage nodes
  const size_t num_nodes = 5;
  const size_t window_size = 128;
  // starting epoch after initial provisioning
  const epoch_t starting_epoch = epoch_t(2);

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  log_attrs.set_replicationFactor(3);
  log_attrs.set_maxWritesInFlight(window_size);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setLogAttributes(log_attrs)
                     .setParam("--epoch-draining-timeout",
                               "3s",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .create(num_nodes);
  std::shared_ptr<Client> client = cluster->createClient();
  const logid_t logid(1);
  wait_until([&]() {
    return std::to_string(starting_epoch.val_) ==
        cluster->getNode(0).sequencerInfo(logid)["Epoch"];
  });

  // stop N2, N3
  for (auto node_idx : NodeSetIndices{3, 4}) {
    cluster->getNode(node_idx).suspend();
  }
  std::unordered_map<Status, size_t, HashEnum<Status>> result;
  Semaphore sem;
  for (int i = 0; i <= window_size; ++i) {
    std::string data("data" + std::to_string(i));
    int rv = client->append(
        logid, std::move(data), [&result, &sem](Status st, const DataRecord&) {
          ++result[st];
          sem.post();
        });
    ASSERT_EQ(0, rv);
  }

  ld_info("waiting for sequencer window to be filled up.");
  // sequencer window must be filled up, and one append should already
  // be rejected
  wait_until([&]() {
    return cluster->getNode(0).sequencerInfo(logid)["In flight"] ==
        std::to_string(window_size) &&
        cluster->getNode(0).stats()["append_rejected_window_full"] == 1;
  });

  // reactivate logid by sending the `up' admin command
  std::string reply = cluster->getNode(0).sendCommand("up --logid 1");
  ASSERT_NE(
      std::string::npos, reply.find("Started sequencer activation for log 1"));

  // must started draining previous epoch
  ld_info("waiting for sequencer to drain epoch %u.", starting_epoch.val_);
  wait_until([&]() {
    return std::to_string(starting_epoch.val_) ==
        cluster->getNode(0).sequencerInfo(logid)["Draining epoch"];
  });

  // wait until all results comeback
  ld_info("waiting for appends to finish.");
  for (int i = 0; i <= window_size; ++i) {
    sem.wait();
  }

  ASSERT_EQ(2, result.size());
  ASSERT_EQ(1, result[E::SEQNOBUFS]);
  ASSERT_EQ(window_size, result[E::CANCELLED]);

  auto seq_info = cluster->getNode(0).sequencerInfo(logid);
  // there should be no in-flight appends
  ASSERT_EQ(std::to_string(0), seq_info["In flight"]);
  // must advanced to a new epoch
  ASSERT_EQ(std::to_string(starting_epoch.val_ + 1), seq_info["Epoch"]);
  ASSERT_EQ(window_size, cluster->getNode(0).stats()["appender_aborted_epoch"]);
  ASSERT_EQ(
      window_size, cluster->getNode(0).stats()["append_rejected_cancelled"]);
}

// test changing weight to 0 for a storage node would make sequencer stops
// sending writes to the node
TEST_F(SequencerIntegrationTest, WeightChangeToZero) {
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(2);
  log_attrs.set_maxWritesInFlight(1024);
  log_attrs.set_replicationFactor(1);
  log_attrs.set_extraCopies(0);

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(/*nodeset=*/{1}, /*replication=*/1);

  // N0 sequencer, N1, N2 storage node
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          // disable recovery so there will be no mutation stores
          .setParam(
              "--skip-recovery", IntegrationTestUtils::ParamScope::SEQUENCER)
          .setParam("--sticky-copysets-block-size", "1")
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .doPreProvisionEpochMetaData()
          .setNumLogs(1)
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .setConfigLogAttributes(log_attrs)
          .setMetaDataLogsConfig(meta_config)
          .create(3);
  std::shared_ptr<Client> client = cluster->createClient();

  const size_t NAPPENDS{100};
  auto do_write = [&] {
    for (int i = 0; i < NAPPENDS; ++i) {
      lsn_t lsn = client->appendSync(logid_t(1), Payload("dummy", 5));
      EXPECT_NE(LSN_INVALID, lsn);
    }
  };

  do_write();
  // each node should have received some STOREs
  std::unordered_map<node_index_t, size_t> stores;
  node_index_t to_disable = 1;
  for (node_index_t n = 1; n <= 2; ++n) {
    stores[n] = cluster->getNode(n).stats()["message_received.STORE"];
    if (stores[n] > stores[to_disable]) {
      to_disable = n;
    }
  }
  // there might be more waves
  EXPECT_GE(stores[1] + stores[2], NAPPENDS);
  ld_info("STORES receives: N1: %lu, N2: %lu.", stores[1], stores[2]);

  // set the weight of the node that received most stores to 0
  cluster->updateNodeAttributes(
      to_disable, configuration::StorageState::READ_ONLY, 0);
  cluster->waitForConfigUpdate();
  ld_info("Set the weight of N%hu to 0.", to_disable);

  // Get most recent stat for the node being disabled, in case auto log
  // provisioning was still doing some before we set the weight to 0
  stores[to_disable] =
      cluster->getNode(to_disable).stats()["message_received.STORE"];

  // write 100 more records
  do_write();
  for (node_index_t n = 1; n <= 2; ++n) {
    if (n == to_disable) {
      // for the disabled nodes, it shouldn't receive any more STOREs
      ASSERT_EQ(
          stores[n], cluster->getNode(n).stats()["message_received.STORE"]);
    } else {
      stores[n] = cluster->getNode(n).stats()["message_received.STORE"];
    }
  }
  EXPECT_GE(stores[1] + stores[2], NAPPENDS * 2);
  ld_info(
      "STORES receives after disable: N1: %lu, N2: %lu.", stores[1], stores[2]);
}

// Reproduce a crash that occurs when a sequencer is trying to provision new
// metadata and write to the metadata logs, while the config changes to disable
// that sequencer.
TEST_F(SequencerIntegrationTest, SequencerMetaDataManagerNullptrCrash) {
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(2);
  log_attrs.set_maxWritesInFlight(1024);
  log_attrs.set_replicationFactor(1);
  log_attrs.set_extraCopies(0);

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(/*nodeset=*/{1}, /*replication=*/1);

  const int num_logs = 1000;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--enable-sticky-copysets",
                    "false",
                    IntegrationTestUtils::ParamScope::ALL)
          .doPreProvisionEpochMetaData()
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setNumLogs(num_logs)
          .useHashBasedSequencerAssignment(100, "10s")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .setConfigLogAttributes(log_attrs)
          .setMetaDataLogsConfig(meta_config)
          .create(3);
  std::shared_ptr<Client> client = cluster->createClient();

  // first, write one record to every log
  for (int i = 1; i < num_logs; ++i) {
    lsn_t lsn = client->appendSync(logid_t(i), Payload("dummy", 5));
    EXPECT_NE(LSN_INVALID, lsn);
  }

  const size_t NAPPENDS{100};
  auto do_write = [&] {
    for (int i = 0; i < NAPPENDS; ++i) {
      lsn_t lsn = client->appendSync(logid_t(1), Payload("dummy", 5));
      if (lsn == LSN_INVALID && err != E::CANCELLED) {
        ld_info("Failed to write to log 1: %s", error_description(err));
        EXPECT_NE(LSN_INVALID, lsn);
      }
    }
  };

  do_write();

  // set the weight of N2 to -1 (in order to trigger new nodeset generation,
  // and sequencer reactivation).
  cluster->updateNodeAttributes(2, configuration::StorageState::DISABLED, 1);
  // set sequencer weight of N0 to 0 (in order to reproduce the condition of
  // the crash)
  cluster->updateNodeAttributes(0, configuration::StorageState::READ_WRITE, 0);
  cluster->waitForConfigUpdate();

  do_write();

  // make sure no node crashed
  for (node_index_t n = 0; n <= 2; ++n) {
    ASSERT_TRUE(cluster->getNode(n).isRunning());
  }
}

TEST_F(SequencerIntegrationTest, LogRemovalStressTest) {
  Configuration::Nodes nodes;
  size_t num_nodes = 5;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_nodeSetSize(3);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setLogGroupName("test_range")
                     .setLogAttributes(log_attrs)
                     .create(num_nodes);
  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->getNode(idx).waitUntilAvailable();
  }
  auto client = cluster->createClient();

  // spawn two threads, one for keeping appending records, the other
  // is for removing/adding the log to the config in a loop
  std::set<lsn_t> appended_lsns;
  std::atomic<size_t> append_failures{0};
  std::atomic<size_t> config_changes{0};
  std::chrono::seconds test_time{15};
  std::atomic<bool> should_stop{false};
  const logid_t dummy_log(993248043);
  const logid_t LOG_ID(1);
  const auto original_config = cluster->getConfig()->get();

  ld_info("starting test...");

  auto config_thread = std::thread([&] {
    while (!should_stop.load()) {
      auto full_config = cluster->getConfig()->get();
      ld_check(full_config);
      auto logs_config_changed = full_config->localLogsConfig()->copy();
      auto local_logs_config_changed =
          checked_downcast<configuration::LocalLogsConfig*>(
              logs_config_changed.get());
      auto& tree = const_cast<logsconfig::LogsConfigTree&>(
          local_logs_config_changed->getLogsConfigTree());
      tree.setVersion(tree.version() + 1);

      auto& logs = const_cast<logsconfig::LogMap&>(
          checked_downcast<configuration::LocalLogsConfig*>(
              logs_config_changed.get())
              ->getLogMap());
      auto log_in_directory = logs.begin()->second;
      bool rv = local_logs_config_changed->replaceLogGroup(
          log_in_directory.getFullyQualifiedName(),
          log_in_directory.log_group->withRange(
              logid_range_t({dummy_log, dummy_log})));
      EXPECT_TRUE(rv);

      cluster->writeConfig(
          full_config->serverConfig().get(), logs_config_changed.get());
      ++config_changes;
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      // change it back
      cluster->writeConfig(*original_config);
      ++config_changes;
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }
  });

  auto append_thread = std::thread([&] {
    size_t i = 0;
    while (!should_stop.load()) {
      lsn_t lsn = client->appendSync(LOG_ID, std::to_string(++i));
      if (lsn != LSN_INVALID) {
        auto result = appended_lsns.insert(lsn);
        EXPECT_TRUE(result.second);
        ld_spew("Appended %s", lsn_to_string(lsn).c_str());
      } else {
        ++append_failures;
      }
    }
  });

  /* sleep override */
  std::this_thread::sleep_for(test_time);
  should_stop.store(true);
  append_thread.join();
  config_thread.join();

  // collect sequencer stats
  size_t activations = 0;
  size_t removes = 0;
  for (const auto& it : nodes) {
    auto stats = cluster->getNode(it.first).stats();
    activations += stats["sequencer_activations"];
    removes += stats["sequencer_unavailable_log_removed_from_config"];
  }

  ld_info("Successful appends: %lu, failed appends: %lu, config changes %lu, "
          "sequencer activations: %lu, seq unavailable log removals: %lu.",
          appended_lsns.size(),
          append_failures.load(),
          config_changes.load(),
          activations,
          removes);

  cluster->writeConfig(*original_config);
  cluster->waitForConfigUpdate();

  std::set<lsn_t> read_lsns;
  // read the newly written data
  Semaphore sem;
  std::unique_ptr<AsyncReader> async_reader = client->createAsyncReader(10);
  const lsn_t last_lsn = *appended_lsns.rbegin();
  async_reader->setRecordCallback([&](std::unique_ptr<DataRecord>& r) {
    auto result = read_lsns.insert(r->attrs.lsn);
    EXPECT_TRUE(result.second);
    if (r->attrs.lsn == last_lsn) {
      sem.post();
    }
    return true;
  });
  async_reader->startReading(LOG_ID, LSN_OLDEST, last_lsn);
  sem.wait();

  // every single appended lsn must be read (the reverse is not true because of
  // recovery may re-replicate unconfirmed records)
  for (auto lsn : appended_lsns) {
    ASSERT_GT(read_lsns.count(lsn), 0);
  }
}

// test that sequencers should get activated on window size changes in config
TEST_F(SequencerIntegrationTest, DynamicallyChangingWindowSize) {
  Configuration::Nodes nodes;
  size_t num_nodes = 5;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
          num_nodes);
  // window size is initially 200
  log_attrs.set_maxWritesInFlight(200);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes)
                     .useHashBasedSequencerAssignment(100, "10s")
                     .enableMessageErrorInjection()
                     .setLogAttributes(log_attrs)
                     .setNumLogs(299)
                     .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }
  auto client = cluster->createClient();
  ld_info("starting test...");

  // for log 1..128, send a write to activate the sequencer
  for (logid_t::raw_type logid_r = 1; logid_r <= 128; ++logid_r) {
    lsn_t lsn;
    do {
      lsn = client->appendSync(logid_t(logid_r), "dummy");
    } while (lsn == LSN_INVALID);
  }

  auto get_activations = [&]() {
    size_t activations = 0;
    for (const auto& it : nodes) {
      auto stats = cluster->getNode(it.first).stats();
      activations += stats["sequencer_activations"];
    }
    return activations;
  };

  const size_t num_activations = get_activations();
  EXPECT_GE(num_activations, 128);
  // change the sequencer window size in config
  auto full_config = cluster->getConfig()->get();
  ld_check(full_config);
  auto logs_config_changed = full_config->localLogsConfig()->copy();
  auto local_logs_config_changed =
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get());

  auto& logs = const_cast<logsconfig::LogMap&>(
      checked_downcast<configuration::LocalLogsConfig*>(
          logs_config_changed.get())
          ->getLogMap());
  auto log_in_directory = logs.begin()->second;
  // changing window size to 300 for the whole log group
  bool rv = local_logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withLogAttributes(
          log_in_directory.log_group->attrs().with_maxWritesInFlight(300)));
  ASSERT_TRUE(rv);

  cluster->writeConfig(
      full_config->serverConfig().get(), logs_config_changed.get());

  cluster->waitForConfigUpdate();
  // waiting for all sequencer activations to complete
  cluster->waitForRecovery();
  const size_t num_activations_after_config_change = get_activations();
  // we should expect at least one reactivation per active sequencer
  ASSERT_GE(num_activations_after_config_change - num_activations, 128);
}

TEST_F(SequencerIntegrationTest, SequencerZeroWeightWhileAppendsPending) {
  std::vector<double> weights = {1.0, 1.0, 1.0, 1.0};
  node_index_t primary = hashing::weighted_ch(1, weights);

  Configuration::Nodes nodes, metadata_nodes;
  for (node_index_t i = 0; i < 4; ++i) {
    Configuration::Node node;
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);

    auto meta_node(node);
    nodes[i] = std::move(node);
    metadata_nodes[i] = std::move(meta_node);
  }

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(ServerConfig::NodesConfig(metadata_nodes),
                               metadata_nodes.size() - 1,
                               /*max_replication=*/3,
                               NodeLocationScope::NODE);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_maxWritesInFlight(256);
  log_attrs.set_replicationFactor(3);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // prevents spurious sequencer
                                         // reactivations due to metadata log
                                         // writes
          .setNodes(nodes)
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setMetaDataLogsConfig(meta_config)
          .setLogGroupName("ns/test_logs")
          .setLogAttributes(log_attrs)
          .useHashBasedSequencerAssignment(100, "10s")
          .deferStart()
          .create(nodes.size());

  // starting just 2 nodes - the primary sequencer for the log and another node.
  // This is so no writes can actually go through, since we don't have 3 nodes
  // to place a copy into
  std::vector<node_index_t> nodes_to_start = {
      primary, node_index_t(primary == 0 ? 1 : 0)};
  cluster->start(nodes_to_start);

  // queue an append that should get "stuck" in the sequencer window

  auto client = cluster->createClient();
  Semaphore sem;
  auto cb = [&](Status st, const DataRecord& r) {
    ASSERT_EQ(E::CANCELLED, st);
    ASSERT_EQ(r.attrs.lsn, LSN_INVALID);
    sem.post();
  };
  int rv = client->append(logid_t(1), "foo", cb);
  ASSERT_EQ(0, rv);

  // Check that the sequencer is active
  wait_until("sequencer activates", [&]() {
    return cluster->getNode(primary).sequencerInfo(logid_t(1))["State"] ==
        "ACTIVE";
  });

  // Make it not a sequencer node
  cluster->updateNodeAttributes(
      primary, configuration::StorageState::READ_WRITE, 0);
  cluster->waitForConfigUpdate();

  // Check that the sequencer moved into UNAVAILABLE state on the primary
  wait_until("sequencer is unavailable", [&]() {
    return cluster->getNode(primary).sequencerInfo(logid_t(1))["State"] ==
        "UNAVAILABLE";
  });

  ld_info("Waiting for the append to complete");
  sem.wait();
}

// test that existing metadata sequencer can react to weight changes in
// metadata nodeset for completing metadata appends eventually
TEST_F(SequencerIntegrationTest, MetaDataLogSequencerReactToWeightChanges) {
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(2);
  log_attrs.set_maxWritesInFlight(1024);
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);

  const int NNODES = 5;
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i == 0) {
      node.addSequencerRole();
    } else {
      node.addStorageRole(/*num_shards*/ 2);
    }
  }

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(/*nodeset=*/{1, 2, 3, 4}, /*replication=*/3);

  // N0 sequencer, N1, N2, N3, N4 storage node and also metadata nodes
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          // use a smaller store timeout so that Appender can react quickly
          .setParam("--store-timeout",
                    "50ms..100ms",
                    IntegrationTestUtils::ParamScope::ALL)
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setNodes(nodes)
          .setNumLogs(1)
          .useHashBasedSequencerAssignment(100, "10s")
          .setLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .setConfigLogAttributes(log_attrs)
          .setMetaDataLogsConfig(meta_config)
          .create(NNODES);
  std::shared_ptr<Client> client = cluster->createClient();

  ld_info("disabling N1");
  for (auto n : {1}) {
    cluster->updateNodeAttributes(n, configuration::StorageState::READ_ONLY, 0);
  }
  cluster->waitForConfigUpdate();

  // suspend N3
  cluster->getNode(3).suspend();

  // make sure there is no sequencer running on N0
  auto seq = cluster->getNode(0).sequencerInfo(logid_t{1});
  ASSERT_TRUE(seq.empty());

  // send a write to trigger auto log provisioning
  lsn_t lsn = client->appendSync(logid_t(1), Payload("dummy", 5));
  EXPECT_NE(LSN_INVALID, lsn);

  // metadata writes should never succeed
  int rv = cluster->waitForMetaDataLogWrites(std::chrono::steady_clock::now() +
                                             std::chrono::seconds(1));
  // the wait should timed out
  EXPECT_EQ(-1, rv);

  // try to read the metadata log it should also timed out since
  // the record shouldn't be fully written
  std::unique_ptr<Reader> reader(client->createReader(1));
  reader->setTimeout(std::chrono::milliseconds(500));
  reader->startReading(MetaDataLog::metaDataLogID(logid_t{1}),
                       compose_lsn(EPOCH_MIN, ESN_MIN),
                       LSN_MAX);
  std::vector<std::unique_ptr<DataRecord>> data_out;
  GapRecord gap_out;
  auto res = reader->read(1, &data_out, &gap_out);
  EXPECT_EQ(0, res);
  rv = reader->stopReading(MetaDataLog::metaDataLogID(logid_t{1}));
  ASSERT_EQ(0, rv);

  // re-enable N1, changing the weight back to 1
  ld_info("re-enabling N1");
  for (auto n : {1}) {
    cluster->updateNodeAttributes(
        n, configuration::StorageState::READ_WRITE, 0);
  }

  ld_info("Waiting for metadata log writes to complete");
  // this time metadata log record should get written
  rv = cluster->waitForMetaDataLogWrites();
  // the wait shouldn't timed out
  ASSERT_EQ(0, rv);

  // try to read the metadata log it should get one record
  reader->startReading(MetaDataLog::metaDataLogID(logid_t{1}),
                       compose_lsn(EPOCH_MIN, ESN_MIN),
                       LSN_MAX);
  res = reader->read(1, &data_out, &gap_out);
  EXPECT_EQ(1, res);
  rv = reader->stopReading(MetaDataLog::metaDataLogID(logid_t{1}));
  ASSERT_EQ(0, rv);

  // try to read the data log it should succeed as well
  ld_info("reading lsn %s for log 1.", lsn_to_string(lsn).c_str());
  reader->startReading(logid_t{1}, lsn, lsn);
  res = reader->read(1, &data_out, &gap_out);
  EXPECT_EQ(1, res);
}

// Test that sequencer can get the trim point from storage nodes
TEST_F(SequencerIntegrationTest, SequencerReadTrimPointTest) {
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(2);
  log_attrs.set_maxWritesInFlight(1024);
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_nodeSetSize(4);

  const int NNODES = 6;
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i == 0) {
      node.addSequencerRole();
    }
    node.addStorageRole(/*num_shards*/ 2);
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     // sequencer polling for trim point more frequently
                     .setParam("--get-trimpoint-interval",
                               "1s",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .setNodes(nodes)
                     .setNumLogs(1)
                     .useHashBasedSequencerAssignment()
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(log_attrs)
                     .setConfigLogAttributes(log_attrs)
                     .create(NNODES);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t logid{1};

  auto write_records = [&](int n) {
    lsn_t lsn = LSN_INVALID;
    for (int i = 0; i < n; ++i) {
      lsn = client->appendSync(logid, Payload("dummy", 5));
      EXPECT_NE(LSN_INVALID, lsn);
    }
    return lsn;
  };

  lsn_t last_written_lsn = write_records(71);
  ld_info("Last written record %s.", lsn_to_string(last_written_lsn).c_str());

  // trim point should be LSN_INVALID
  EXPECT_EQ(std::to_string(LSN_INVALID),
            cluster->getNode(0).sequencerInfo(logid)["Trim point"]);

  // trim the log to last_written_lsn
  int rv = client->trimSync(logid, last_written_lsn);
  EXPECT_EQ(0, rv);

  // sequencer should eventually observe the trim point movement
  wait_until("Sequencer to advance its trim point", [&]() {
    return std::to_string(last_written_lsn) ==
        cluster->getNode(0).sequencerInfo(logid)["Trim point"];
  });

  // reactivate sequencer, do it again and see if trim point can
  // move in the new epoch

  // reactivate logid by sending the `up' admin command
  std::string reply = cluster->getNode(0).sendCommand(
      "up --logid " + std::to_string(logid.val_));
  ASSERT_NE(
      std::string::npos, reply.find("Started sequencer activation for log"));

  // trim point should stay the same during activation
  wait_until("Sequencer to advance its trim point", [&]() {
    return std::to_string(last_written_lsn) ==
        cluster->getNode(0).sequencerInfo(logid)["Trim point"];
  });

  last_written_lsn = write_records(23);
  ld_info("Last written record %s.", lsn_to_string(last_written_lsn).c_str());

  // trim the log to last_written_lsn
  rv = client->trimSync(logid, last_written_lsn);
  EXPECT_EQ(0, rv);
  // sequencer should eventually observe the trim point movement
  wait_until("Sequencer to advance its trim point", [&]() {
    return std::to_string(last_written_lsn) ==
        cluster->getNode(0).sequencerInfo(logid)["Trim point"];
  });
}

// test that preemption while writing to metadata logs, doesn't cause ping-pong
TEST_F(SequencerIntegrationTest, MetaDataWritePreempted) {
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(2);
  log_attrs.set_maxWritesInFlight(1024);
  log_attrs.set_replicationFactor(1);
  log_attrs.set_extraCopies(0);

  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(/*nodeset=*/{1}, /*replication=*/1);

  // N0 sequencer, N1, N2 storage node
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNumLogs(1)
                     .setLogAttributes(log_attrs)
                     .deferStart()
                     .setNumDBShards(1)
                     .setParam("--reactivation-limit", "1000/1s")
                     .setEventLogAttributes(log_attrs)
                     .setConfigLogAttributes(log_attrs)
                     .setMetaDataLogsConfig(meta_config)
                     .create(3);

  cluster->setStartingEpoch(logid_t(1), epoch_t(2));
  const NodeID sealed_by(2, 1);
  const epoch_t epoch(8);
  {
    auto store = cluster->getNode(1).createLocalLogStore();
    SealMetadata meta(Seal(epoch, sealed_by));
    ASSERT_EQ(0,
              store->getByIndex(0)->writeLogMetadata(
                  MetaDataLog::metaDataLogID(logid_t(1)),
                  meta,
                  LocalLogStore::WriteOptions()));
  }

  cluster->start();
  cluster->waitForRecovery();

  std::shared_ptr<Client> client = cluster->createClient();

  lsn_t lsn = client->appendSync(logid_t(1), Payload("test", 4));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(err, E::NOSEQUENCER);

  ld_info("Getting stats and sequencer info.");
  auto stats = cluster->getNode(0).stats();
  auto seq_info = cluster->getNode(0).sequencerInfo(logid_t(1));

  ASSERT_EQ("PREEMPTED", seq_info["State"]);
  ASSERT_EQ(std::to_string(sealed_by.index()), seq_info["Preempted by"]);
  int preempted_epoch = folly::to<int>(seq_info["Preempted epoch"]);
  ASSERT_GE(8, preempted_epoch);
}

TEST_F(SequencerIntegrationTest, NodeSetAdjustment) {
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(1);
  log_attrs.set_backlogDuration(std::chrono::seconds(40));
  log_attrs.set_nodeSetSize(1);
  const uint32_t num_user_logs = 1;

  // N0 sequencer, N1, N2 storage nodes
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(num_user_logs)
          .setLogAttributes(log_attrs)
          .setNumDBShards(1)
          .setParam("--nodeset-adjustment-period", "2s")
          .setParam("--nodeset-adjustment-min-window", "1s")
          .setParam("--nodeset-size-adjustment-min-factor", "0")
          .setParam("--nodeset-adjustment-target-bytes-per-shard", "500K")
          .create(3);

  // Make sure a sequencer is active.
  ld_info("Appending first record");
  std::shared_ptr<Client> client = cluster->createClient();
  lsn_t lsn1 = client->appendSync(logid_t(1), std::string("pikachu"));
  ASSERT_NE(LSN_INVALID, lsn1);
  ld_info("Got lsn %s", lsn_to_string(lsn1).c_str());

  ld_info("Sleeping");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::map<std::string, int64_t> stats;

  auto print_stats = [&] {
    ld_info("Adjustments done: %ld, skipped: %ld, randomizations: %ld, "
            "reactivations: %ld, "
            "non-reactivations: %ld",
            stats.at("nodeset_adjustments_done"),
            stats.at("nodeset_adjustments_skipped"),
            stats.at("nodeset_randomizations_done"),
            stats.at("sequencer_reactivations_for_metadata_update"),
            stats.at("metadata_updates_without_sequencer_reactivation"));
  };

  // Nodesets of log 1 and internal logs should be randomized every 2 second.
  const uint32_t num_internal_logs =
      configuration::InternalLogs::numInternalLogs();
  uint32_t min_reactivations = (num_internal_logs + num_user_logs) *
      (10 / 2) /*sleep time / adjustment period*/;
  ld_info("Checking stats");
  stats = cluster->getNode(0).stats();

  // Conservatively since this is a time based test
  EXPECT_GE(stats.at("nodeset_randomizations_done"), min_reactivations / 1.5);
  EXPECT_EQ(0, stats.at("nodeset_adjustments_done"));
  print_stats();

  // Stop randomizing nodesets, only resize based on throughput now.
  ld_info("Disabling randomization");
  cluster->getNode(0).updateSetting(
      "nodeset-size-adjustment-min-factor", "1.1");
  stats = cluster->getNode(0).stats();
  int64_t randomizations_before = stats.at("nodeset_randomizations_done");
  print_stats();

  ld_info("Sleeping");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(10));

  // No logs have enough throughput to increase nodeset size from 1 to 2.
  min_reactivations = (2 * min_reactivations);
  ld_info("Checking stats");
  stats = cluster->getNode(0).stats();
  EXPECT_EQ(randomizations_before, stats.at("nodeset_randomizations_done"));
  EXPECT_EQ(0, stats.at("nodeset_adjustments_done"));

  // Conservatively since this is a time based test
  EXPECT_GE(stats.at("nodeset_adjustments_skipped"), min_reactivations / 1.5);
  print_stats();

  // Append to log 1 at 100 KB/s for 20 seconds.
  // This should trigger an increase in nodeset size and at least one nodeset
  // randomization: 100 KB/s * 40 s = 500 KB * 2 nodes * 4 randomizations.
  auto t = std::chrono::steady_clock::now();
  lsn_t lsn2 = LSN_INVALID;
  for (int i = 0; i < 200; ++i) {
    if (i % 10 == 0) {
      ld_info("Doing appends");
    }
    sleep_until_safe(t);
    lsn_t lsn = client->appendSync(logid_t(1), std::string(10000, '.'));
    EXPECT_NE(LSN_INVALID, lsn);

    if (lsn != LSN_INVALID) {
      // Should all be in same epoch.
      if (lsn2 == LSN_INVALID) {
        ld_info("First lsn: %s", lsn_to_string(lsn).c_str());
      }
      lsn2 = lsn;
    }

    t += std::chrono::milliseconds(100);
  }
  ld_info("Last lsn: %s", lsn_to_string(lsn2).c_str());

  // Nodeset size of log 1 should have been increased from 1 to 2.
  // There can be more than one adjustment as throughput estimate changes
  // over time, and nodeset is allowed to be bigger than 2.
  // E.g. target nodeset size can change 1 -> 3 -> 7 -> 8, the last two
  // changes done without sequencer reactivation and only affecting
  // rate of randomization.
  ld_info("Checking stats");
  stats = cluster->getNode(0).stats();
  EXPECT_GE(stats.at("nodeset_adjustments_done"), 1);
  EXPECT_LE(stats.at("nodeset_adjustments_done"), 10);
  int64_t adjustments_before = stats.at("nodeset_adjustments_done");
  EXPECT_GE(stats.at("nodeset_randomizations_done") - randomizations_before, 1);
  EXPECT_LE(stats.at("nodeset_randomizations_done") - randomizations_before, 5);
  print_stats();

  ld_info("Sleeping");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(6));

  // After some time of zero throughput nodeset size is decreased back to 1.
  ld_info("Checking stats");
  stats = cluster->getNode(0).stats();
  EXPECT_GE(stats.at("nodeset_adjustments_done") - adjustments_before, 1);
  EXPECT_LE(stats.at("nodeset_adjustments_done") - adjustments_before, 5);
  adjustments_before = stats.at("nodeset_adjustments_done");
  EXPECT_GE(stats.at("nodeset_randomizations_done") - randomizations_before, 1);
  EXPECT_LE(stats.at("nodeset_randomizations_done") - randomizations_before, 6);
  randomizations_before = stats.at("nodeset_randomizations_done");
  print_stats();

  // Sleep a bit more and make sure nodeset updates stopped.
  ld_info("Sleeping");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(5));
  ld_info("Checking stats");
  stats = cluster->getNode(0).stats();
  EXPECT_EQ(adjustments_before, stats.at("nodeset_adjustments_done"));
  EXPECT_EQ(randomizations_before, stats.at("nodeset_randomizations_done"));
  print_stats();

  // Check that the nodeset resizings bumped epoch.
  ld_info("Appending last record");
  lsn_t lsn3 = client->appendSync(logid_t(1), "bye");
  EXPECT_NE(LSN_INVALID, lsn3);
  EXPECT_GT(lsn_to_epoch(lsn2).val(), lsn_to_epoch(lsn1).val());
  EXPECT_GT(lsn_to_epoch(lsn3).val(), lsn_to_epoch(lsn2).val());
  ld_info("Got lsn %s", lsn_to_string(lsn3).c_str());
}
