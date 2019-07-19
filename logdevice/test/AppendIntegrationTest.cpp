/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <thread>

#include <folly/json.h>
#include <gtest/gtest.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class AppendIntegrationTest : public IntegrationTestBase {};

/**
 * An Echo test. Posts an AppendRequest to a Worker, expects a reply.
 */
TEST_F(AppendIntegrationTest, AppendRequestEcho) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);

  Semaphore reply_sem;

  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto processor =
      Processor::create(cluster->getConfig(),
                        std::make_shared<NoopTraceLogger>(cluster->getConfig()),
                        UpdateableSettings<Settings>(settings),
                        nullptr, /*stats*/

                        make_test_plugin_registry());

  char data[128]; // send the contents of this array as payload

  std::unique_ptr<AppendRequest> appendrq(
      new AppendRequest(nullptr,
                        logid_t(1),
                        AppendAttributes(),
                        Payload(data, sizeof(data)),
                        std::chrono::milliseconds::max(),
                        [&reply_sem](Status st, const DataRecord& r) {
                          EXPECT_EQ(E::OK, st);
                          EXPECT_EQ(esn_t(1), lsn_to_esn(r.attrs.lsn));
                          reply_sem.post();
                        }));

  appendrq->bypassWriteTokenCheck();
  std::unique_ptr<Request> rq(std::move(appendrq));
  EXPECT_EQ(0, processor->postRequest(rq));

  reply_sem.wait();
}

/**
 * Creates a logdevice::Client and appends four records through it. The
 * first two are sent via non-blocking append() call, the last two using
 * a blocking appendSync(). Verifies that sequence numbers are assigned
 * in order.
 *
 * Suspends the sequencer logdeviced, makes an appendSync() request
 * with 100ms timeout.  Expects it to fail with E::TIMEDOUT. This
 * excercises the code path in the messaging layer where the
 * connection to server is already established.
 */
TEST_F(AppendIntegrationTest, AppendEchoClient) {
  int rv;

  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  // Make sure that sequencer won't reactivate.
  cluster->waitForMetaDataLogWrites();

  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE((bool)client);

  char data[8 * 1024]; // send the contents of this array as payload
  const int small_payload_size = 128;
  const int large_payload_size = sizeof(data);

  static_assert(
      small_payload_size <= sizeof(data), "small_payload_size is too large");

  // For synchronisation with async appends
  Semaphore async_append_sem;

  Payload payload1(data, small_payload_size);

  rv = client->append(
      logid_t(2),
      payload1,
      [payload1, &async_append_sem](Status st, const DataRecord& r) {
        if (st != E::OK) {
          ld_error("Async append failed with status %s", error_description(st));
        }
        EXPECT_EQ(E::OK, st);
        EXPECT_EQ(logid_t(2), r.logid);
        EXPECT_EQ(esn_t(1), lsn_to_esn(r.attrs.lsn));
        EXPECT_EQ(payload1.data(), r.payload.data());
        EXPECT_EQ(payload1.size(), r.payload.size());
        async_append_sem.post();
      });
  EXPECT_EQ(0, rv);

  Payload payload2(data + 10, small_payload_size - 10);

  rv = client->append(
      logid_t(2),
      payload2,
      [payload2, &async_append_sem](Status st, const DataRecord& r) {
        if (st != E::OK) {
          ld_error("Async append failed with status %s", error_description(st));
        }
        EXPECT_EQ(E::OK, st);
        EXPECT_EQ(logid_t(2), r.logid);
        EXPECT_EQ(esn_t(2), lsn_to_esn(r.attrs.lsn));
        EXPECT_EQ(payload2.data(), r.payload.data());
        EXPECT_EQ(payload2.size(), r.payload.size());
        async_append_sem.post();
      });
  EXPECT_EQ(0, rv);

  Payload payload3(data, large_payload_size);

  lsn_t lsn = client->appendSync(logid_t(2), payload3);

  EXPECT_EQ(esn_t(3), lsn_to_esn(lsn));

  lsn = client->appendSync(logid_t(2), payload2);

  EXPECT_EQ(esn_t(4), lsn_to_esn(lsn));

  async_append_sem.wait();
  async_append_sem.wait();

  // Stop the sequencer, make an append() call, expect a
  // timeout
  client->setTimeout(std::chrono::milliseconds(100));
  cluster->getSequencerNode().suspend();
  lsn = client->appendSync(logid_t(2), payload3);

  ASSERT_EQ(LSN_INVALID, lsn);
  EXPECT_EQ(E::TIMEDOUT, err);
}

/**
 * Sends 1000 appends to 1000 logs.
 * Checks throttling in SyncStorageThread.
 */
TEST_F(AppendIntegrationTest, SyncStorageThreadThrottling) {
  int rv;

  const int NUM_LOGS = 1000;

  auto cluster =
      IntegrationTestUtils::ClusterFactory().setNumLogs(NUM_LOGS).create(1);
  // Make sure that sequencer won't reactivate.
  cluster->waitForMetaDataLogWrites();

  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE((bool)client);

  // For synchronisation with async appends
  Semaphore async_append_sem;

  for (int i = 0; i < NUM_LOGS; ++i) {
    rv = client->append(
        logid_t(i + 1),
        "payload",
        [&async_append_sem](Status st, const DataRecord& /* unused */) {
          if (st != E::OK) {
            EXPECT_EQ(E::OK, st);
            ld_error(
                "Async append failed with status %s", error_description(st));
          }
          async_append_sem.post();
        });
    EXPECT_EQ(0, rv);
  }

  for (int i = 0; i < NUM_LOGS; ++i) {
    async_append_sem.wait();
  }

  std::map<std::string, int64_t> stats = cluster->getNode(0).stats();
  // expectation values depend on chosen NUM_LOGS
  // most syncs here come from sequencer activation
  // note opt mode also affects these numbers i.e. buck with @mode/opt
  int fdatasync = stats["fdatasyncs"];
  int wal_syncs = stats["wal_syncs"];
  EXPECT_GT(fdatasync, 10);
  EXPECT_LT(fdatasync, 250);
  EXPECT_GT(wal_syncs, 10);
  EXPECT_LT(wal_syncs, 250);
}

/**
 * Tests that sequencers that are caught flipping bits in payloads/checksums
 * are aborted, except if 35+ % of the cluster is dead.
 */
TEST_F(AppendIntegrationTest, AbortCorruptedSequencers) {
  // n=6, r=2 so that we can take down 35% of the cluster (2 nodes) yet write
  // n=4, r=1 would mean that stats bump fails if write was to the same node
  const int NUM_NODES = 6;
  const int REPLICATION_FACTOR = 2;
  const logid_t LOG_ID = logid_t(2);
  std::array<char, 128> data{}; // send the contents of this array as payload
  data.fill('x');
  lsn_t lsn;
  int payload_corruption_tot;
  int payload_corruption_ignored_tot;
  int graylist_shard_added_tot;
  int sequencer_node_id;

  auto cluster_factory = IntegrationTestUtils::ClusterFactory();
  cluster_factory.useHashBasedSequencerAssignment(
      /*gossip_interval_ms=*/20, "10s");
  cluster_factory.setParam("--rocksdb-verify-checksum-during-store", "true");
  logsconfig::LogAttributes log_attrs =
      cluster_factory.createDefaultLogAttributes(NUM_NODES);
  log_attrs.set_replicationFactor(REPLICATION_FACTOR);
  cluster_factory.setLogAttributes(log_attrs);
  auto cluster = cluster_factory.create(NUM_NODES);

  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE((bool)client);

  // Appends should work fine
  lsn = client->appendSync(LOG_ID, Payload(data.data(), 96));
  ASSERT_NE(ESN_INVALID, lsn_to_esn(lsn));

  // Tell sequencer to corrupt incoming stores
  sequencer_node_id =
      cluster->getHashAssignedSequencerNodeId(LOG_ID, client.get());
  ASSERT_NE(sequencer_node_id, -1);
  ASSERT_TRUE(cluster->getNode(sequencer_node_id).isRunning());
  cluster->getNode(sequencer_node_id)
      .updateSetting("test-sequencer-corrupt-stores", "true");
  client->setTimeout(std::chrono::milliseconds(1000));
  lsn = client->appendSync(LOG_ID, Payload(data.data(), 58));
  ASSERT_EQ(ESN_INVALID, lsn_to_esn(lsn));
  if (err != E::PEER_CLOSED && err != E::TIMEDOUT) {
    ld_info("err: %s", error_description(err));
    ASSERT_TRUE(false);
  }

  // Verify that bad sequencer aborts (exit code 6)
  ASSERT_EQ(cluster->getNode(sequencer_node_id).waitUntilExited(), 128 + 6 - 1);
  int dead_node = sequencer_node_id;

  // Some node should have bumped the payload_corruption stat, but none should
  // have bumped payload_corruption_ignored
  auto check_stats = [&]() {
    payload_corruption_tot = 0;
    payload_corruption_ignored_tot = 0;
    graylist_shard_added_tot = 0;
    for (auto& it : cluster->getNodes()) {
      auto stats = it.second->stats();
      payload_corruption_tot += stats["payload_corruption"];
      payload_corruption_ignored_tot += stats["payload_corruption_ignored_tot"];
    }
    ASSERT_NE(payload_corruption_tot, 0);
    ASSERT_EQ(payload_corruption_ignored_tot, 0);
    ASSERT_EQ(graylist_shard_added_tot, 0);
  };
  check_stats();

  // Failover should bring up a new sequencer for LOG_ID on some other node.
  // Find out which one.
  wait_until(
      "Sequencer failover should bring up a new sequencer on some other node",
      [&] {
        sequencer_node_id =
            cluster->getHashAssignedSequencerNodeId(LOG_ID, client.get());
        return sequencer_node_id != -1;
      });

  // Kill two more nodes, avoid sequencer just to not have to wait for failover
  int node_to_kill = dead_node;
  for (int i = 0; i < 2; i++) {
    node_to_kill = (node_to_kill + 1) % NUM_NODES;
    node_to_kill = node_to_kill == sequencer_node_id
        ? (node_to_kill + 1) % NUM_NODES
        : node_to_kill;
    cluster->getNode(node_to_kill).kill();

    // Wait a bit so sequencer node notices that a new node is dead
    cluster->getNode(sequencer_node_id).waitUntilKnownDead(node_to_kill);
  }
  client->setTimeout(getDefaultTestTimeout());
  // We should still be able to write
  lsn = client->appendSync(LOG_ID, Payload(data.data(), 128));
  EXPECT_NE(ESN_INVALID, lsn_to_esn(lsn));

  // Since we don't permit bad sequencers to abort after 35% of the cluster is
  // dead, and 3 nodes are dead at this point, verify that a bad sequencer
  // doesn't abort
  auto seq_stats = cluster->getNode(0).stats();
  cluster->getNode(sequencer_node_id)
      .updateSetting("test-sequencer-corrupt-stores", "true");
  ASSERT_EQ(seq_stats["payload_corruption_ignored"], 0);
  client->setTimeout(std::chrono::milliseconds(1000));
  lsn = client->appendSync(LOG_ID, Payload(data.data(), 112));
  ASSERT_EQ(lsn_to_esn(lsn), ESN_INVALID);
  ASSERT_EQ(E::TIMEDOUT, err);
  bool aborted = false;
  wait_until(
      "Sequencer should notice that it corrupted some records and bump a stat "
      "indicating that it won't abort, since too many nodes are dead",
      [&] {
        aborted = !cluster->getNode(sequencer_node_id).isRunning();
        seq_stats = cluster->getNode(sequencer_node_id).stats();
        return aborted || seq_stats["payload_corruption_ignored"] > 0;
      });
  ASSERT_FALSE(aborted);
  ASSERT_TRUE(cluster->getNode(sequencer_node_id).isRunning());
  check_stats();
}

/**
 * Checks that, with --rocksdb-verify-checksum-during-store enabled, a storage
 * node that is caught corrupting payloads/checksums is graylisted.
 */
TEST_F(AppendIntegrationTest, GraylistCorruptedStorageNodes) {
  const int NUM_NODES = 4;
  const int REPLICATION_FACTOR = 3;
  const int LOG_ID = 2;
  std::array<char, 128> data; // send the contents of this array as payload
  data.fill('x');
  lsn_t lsn;

  // Stats names
  std::string corr_fld = "payload_corruption";
  std::string graylist_fld = "graylist_shard_added";
  std::string stored_msg_fld = "message_sent.STORED";
  std::string corr_ignored_fld = "payload_corruption_ignored";

  auto cluster_factory = IntegrationTestUtils::ClusterFactory();
  cluster_factory.setParam("--rocksdb-verify-checksum-during-store", "true");
  cluster_factory.setParam("--disable-graylisting", "false");
  cluster_factory.useHashBasedSequencerAssignment(/*gossip_interval_ms=*/20);
  logsconfig::LogAttributes log_attrs =
      cluster_factory.createDefaultLogAttributes(NUM_NODES);
  log_attrs.set_replicationFactor(REPLICATION_FACTOR);
  cluster_factory.setLogAttributes(log_attrs);
  auto cluster = cluster_factory.create(NUM_NODES);

  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE((bool)client);

  // Appends should work fine
  lsn = client->appendSync(logid_t(LOG_ID), Payload(data.data(), 128));
  ASSERT_NE(lsn_to_esn(lsn), ESN_INVALID);
  ASSERT_EQ(lsn_to_esn(lsn), esn_t(1));

  /*
  We have 4 nodes and a replication factor of 3, so every store will hit at
  least one of N0 and N1. Make them both corrupt every store, and then send an
  append. It will time out, and the sequencer will be left repeatedly clearing
  the graylist and attempting new waves. Then make N1 stop corrupting stores,
  which gets the cluster to a state where N0 should be graylisted but N1 should
  be getting stores. Also confirm that the appropriate stats are bumped along
  the way since that's how we'll get alarms about this.
  */
  int sequencer_node_id =
      cluster->getHashAssignedSequencerNodeId(logid_t(LOG_ID), client.get());
  ASSERT_NE(sequencer_node_id, -1);

  // There should be no known cases of corruption beforehand
  auto sequencer_stats_before = cluster->getNode(sequencer_node_id).stats();
  auto stats_before_n0 = cluster->getNode(0).stats();
  auto stats_before_n1 = cluster->getNode(1).stats();
  ASSERT_EQ(stats_before_n0[corr_fld], 0);
  ASSERT_EQ(stats_before_n1[corr_fld], 0);
  ASSERT_EQ(sequencer_stats_before[corr_ignored_fld], 0);

  cluster->getNode(0).updateSetting("rocksdb-test-corrupt-stores", "true");
  cluster->getNode(1).updateSetting("rocksdb-test-corrupt-stores", "true");

  // This will not be successful, as 2/4 nodes keep reporting corruption
  client->setTimeout(std::chrono::milliseconds(1000));
  lsn = client->appendSync(logid_t(LOG_ID), Payload(data.data(), 28));
  ASSERT_EQ(lsn_to_esn(lsn), ESN_INVALID);
  ASSERT_EQ(E::TIMEDOUT, err);

  // Make N1 stop corrupting stores, leading to only N0 remaining graylisted
  cluster->getNode(1).updateSetting("rocksdb-test-corrupt-stores", "false");

  // Append should work
  client->setTimeout(std::chrono::milliseconds(60000));
  lsn = client->appendSync(logid_t(LOG_ID), Payload(data.data(), 7));
  ASSERT_NE(ESN_INVALID, lsn_to_esn(lsn));

  std::map<std::string, int64_t> sequencer_stats_after;
  int rv = wait_until("Waiting for appenders to be done", [&]() {
    sequencer_stats_after = cluster->getNode(sequencer_node_id).stats();
    return sequencer_stats_after["num_appenders"] == 0;
  });
  ld_check(rv == 0);

  auto stats_after_n0 = cluster->getNode(0).stats();
  auto stats_after_n1 = cluster->getNode(1).stats();

  // Check that appropriate stats were bumped at failed append
  ASSERT_NE(stats_after_n0[corr_fld], 0);
  ASSERT_NE(stats_after_n1[corr_fld], 0);
  ASSERT_NE(sequencer_stats_after[graylist_fld],
            sequencer_stats_before[graylist_fld]);
  ASSERT_EQ(sequencer_stats_after[corr_ignored_fld], 0);

  // Write again; should not hit graylisted node N0
  lsn = client->appendSync(logid_t(LOG_ID), Payload(data.data(), 64));
  ASSERT_NE(lsn_to_esn(lsn), ESN_INVALID);
  ASSERT_NE(ESN_INVALID, lsn_to_esn(lsn));
  stats_before_n0 = stats_after_n0;
  stats_before_n1 = stats_after_n1;
  stats_after_n0 = cluster->getNode(0).stats();
  stats_after_n1 = cluster->getNode(1).stats();
  sequencer_stats_before = sequencer_stats_after;
  sequencer_stats_after = cluster->getNode(sequencer_node_id).stats();
  // N1 should have received new stores; N0 should not
  ASSERT_EQ(stats_before_n0[stored_msg_fld], stats_after_n0[stored_msg_fld]);
  ASSERT_NE(stats_before_n1[stored_msg_fld], stats_after_n1[stored_msg_fld]);
  // No new corruption or graylisting should be registered
  ASSERT_EQ(stats_before_n0[corr_fld], stats_after_n0[corr_fld]);
  ASSERT_EQ(stats_before_n1[corr_fld], stats_after_n1[corr_fld]);
  ASSERT_EQ(sequencer_stats_before[graylist_fld],
            sequencer_stats_after[graylist_fld]);
  ASSERT_EQ(sequencer_stats_after[corr_ignored_fld], 0);
}

/**
 * Makes an appendSync() request for a log that the cluster is not running a
 * sequencer for. Expects the server to reply that it does not have a sequencer
 * for that log.
 */
TEST_F(AppendIntegrationTest, NoSequencer) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);

  const logid_t EXTRA_LOG_ID(949494);

  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();

  // Create a config with an extra log that the servers don't know about.
  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      checked_downcast<std::unique_ptr<configuration::LocalLogsConfig>>(
          cluster_config->localLogsConfig()->copy());
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  logs_config->insert(EXTRA_LOG_ID.val_, "test_log_log", log_attrs);

  auto client_config = std::make_shared<UpdateableConfig>();
  client_config->updateableServerConfig()->update(
      cluster_config->serverConfig());
  client_config->updateableLogsConfig()->update(logs_config);
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      this->testTimeout(),
      std::unique_ptr<ClientSettings>(),
      plugin_registry);
  ASSERT_TRUE((bool)client);

  // make an appendSync() call for the new log. Expect "no sequencer for log"
  char data[20];
  lsn_t lsn = client->appendSync(EXTRA_LOG_ID, Payload(data, sizeof data));

  EXPECT_EQ(LSN_INVALID, lsn);
  EXPECT_EQ(E::NOSEQUENCER, err);
}

/**
 * Similar to the test above, but this time LogDevice is running w/ on-demand
 * sequencer bring-up. Expect that the server will fail the append and reply
 * E::NOTINSERVERCONFIG to the client
 */
TEST_F(AppendIntegrationTest, LogIdNotInServerConfig) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .enableMessageErrorInjection()
                     .create(1);

  const logid_t EXTRA_LOG_ID(949494);

  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();

  // Create a config with an extra log that the servers don't know about.
  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      checked_downcast<std::unique_ptr<configuration::LocalLogsConfig>>(
          cluster_config->localLogsConfig()->copy());
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  logs_config->insert(EXTRA_LOG_ID.val_, "test_log_log", log_attrs);

  auto client_config = std::make_shared<UpdateableConfig>();
  client_config->updateableServerConfig()->update(
      cluster_config->serverConfig());
  client_config->updateableLogsConfig()->update(logs_config);
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      this->testTimeout(),
      std::unique_ptr<ClientSettings>(),
      plugin_registry);
  ASSERT_TRUE((bool)client);

  // make an appendSync() call for the new log.
  // Expect "not in server config error"
  char data[20];
  lsn_t lsn = client->appendSync(EXTRA_LOG_ID, Payload(data, sizeof data));
  EXPECT_EQ(LSN_INVALID, lsn);
  EXPECT_EQ(E::NOTINSERVERCONFIG, err);
}

/**
 * Suspends the sequencer logdeviced, makes an appendSync() request
 * with 100ms timeout.  Expects it to fail with E::TIMEDOUT. This
 * exercises the code path in the messaging layer where the
 * connection to server does not yet exist.
 *
 * Resumes the sequencer node, sends an append with a 0 timeout. Expects
 * either success or (more likely) E::TIMEDOUT.
 */
TEST_F(AppendIntegrationTest, AppendTimeout) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);

  cluster->getSequencerNode().suspend();

  std::shared_ptr<Client> client =
      cluster->createClient(std::chrono::milliseconds(100));
  ASSERT_TRUE((bool)client);

  char data[128]; // send the contents of this array as payload
  Payload payload(data, sizeof(data));

  lsn_t lsn = client->appendSync(logid_t(2), payload);

  ASSERT_EQ(LSN_INVALID, lsn);
  EXPECT_EQ(E::TIMEDOUT, err);

  cluster->getSequencerNode().resume();

  client->setTimeout(std::chrono::milliseconds::zero());

  lsn = client->appendSync(logid_t(2), payload);

  EXPECT_TRUE(lsn != LSN_INVALID || err == E::TIMEDOUT);
}

TEST_F(AppendIntegrationTest, ClientDestroyed) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);

  cluster->getSequencerNode().suspend();

  std::shared_ptr<Client> client = cluster->createClient(std::chrono::hours(1));
  ASSERT_TRUE((bool)client);

  char data[128]; // send the contents of this array as payload
  Payload payload(data, sizeof(data));

  std::atomic<bool> cb_called(false);
  auto check_shutdown_cb = [&](Status st, const DataRecord& /*r*/) {
    cb_called.store(true);
    EXPECT_EQ(E::SHUTDOWN, st);
    cluster->getSequencerNode().resume();
  };

  client->append(logid_t(2), payload, check_shutdown_cb);
  client.reset(); // this blocks until all Worker threads shut down
  ASSERT_TRUE(cb_called.load());
}

// Tests that appends for the same log made from the same
// client thread are always processed by the same worker thread.
TEST_F(AppendIntegrationTest, ThreadMapping) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);

  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 32;
  auto processor =
      Processor::create(cluster->getConfig(),
                        std::make_shared<NoopTraceLogger>(cluster->getConfig()),
                        UpdateableSettings<Settings>(settings),
                        nullptr, /*Stats*/

                        make_test_plugin_registry());

  const std::string payload = "foo";
  auto append = [&](logid_t log_id) -> std::thread::id {
    Semaphore sem;
    std::thread::id thread_id;

    std::unique_ptr<AppendRequest> appendrq(
        new AppendRequest(nullptr,
                          log_id,
                          AppendAttributes(),
                          Payload(payload.data(), payload.size()),
                          std::chrono::milliseconds::max(),
                          [&](Status /*st*/, const DataRecord& /*r*/) {
                            thread_id = std::this_thread::get_id();
                            sem.post();
                          }));
    appendrq->bypassWriteTokenCheck();
    std::unique_ptr<Request> rq(std::move(appendrq));
    EXPECT_EQ(0, processor->postRequest(rq));

    sem.wait();
    return thread_id;
  };

  // two appends for log 1 from this thread must be handled by the same
  // worker thread
  std::thread::id id1 = append(logid_t(1));
  std::thread::id id2 = append(logid_t(1));
  EXPECT_EQ(id1, id2);

  // issue appends for several logs for a single client thread; make sure
  // they're not all executed by the same worker
  std::set<std::thread::id> thread_ids;
  for (logid_t log_id = logid_t(1); log_id.val_ <= 1000; ++log_id.val_) {
    thread_ids.insert(append(log_id));
  }
  EXPECT_GT(thread_ids.size(), 1);

  // spawn several client threads, each writing into log 2, and check that
  // those writes are spread out across different workers
  std::vector<std::thread> threads;
  std::mutex mutex;
  thread_ids.clear();
  for (int i = 0; i < 64; ++i) {
    threads.emplace_back([&]() {
      std::lock_guard<std::mutex> lock(mutex);
      thread_ids.insert(append(logid_t(2)));
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_GT(thread_ids.size(), 1);
}

// Appending to a log that doesn't exist must not crash the client (t8978453)
TEST_F(AppendIntegrationTest, AppendToNonexistentLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  auto client = cluster->createClient();
  const logid_t LOG_ID(0x581b51288a55); // random
  EXPECT_EQ(LSN_INVALID, client->appendSync(LOG_ID, "boom"));
  EXPECT_EQ(E::NOTFOUND, err);
}

TEST_F(AppendIntegrationTest, InternalLogAppendersMemoryLimitTest) {
  const int nodes = 4;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--max-total-appenders-size-hard", "1")
                     .useHashBasedSequencerAssignment()
                     .create(nodes);

  for (const auto& it : cluster->getNodes()) {
    it.second->waitUntilAvailable();
  }

  auto client = cluster->createClient();
  const logid_t LOG_ID(1);
  EXPECT_EQ(LSN_INVALID, client->appendSync(LOG_ID, "boom"));
  EXPECT_EQ(E::SEQNOBUFS, err);

  // Trigger write to event log.
  // It should success because the limit for internal logs is disabled.
  EXPECT_NE(
      LSN_INVALID, IntegrationTestUtils::markShardUndrained(*client, 0, 0));
}

TEST_F(AppendIntegrationTest, HandleNOSPCData) {
  // creating a storage node that cannot accept writes
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--free-disk-space-threshold",
                               "0.999999",
                               IntegrationTestUtils::ParamScope::STORAGE_NODE)
                     .setParam("--nospace-retry-interval",
                               "120s",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .doPreProvisionEpochMetaData() // to avoid counting NOSPC
                                                    // replies to the sequencer
                     .create(1);
  std::shared_ptr<Client> client =
      cluster->createClient(std::chrono::seconds(1));

  const logid_t logid(2);
  const size_t num_records = 40;

  // sleep for 1 second to ensure the monitor thread disallows writes
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));

  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));

    EXPECT_EQ(LSN_INVALID, lsn);
    if (i == 0) {
      // we expect the first record to fail because of timeout
      EXPECT_EQ(E::TIMEDOUT, err);
    } else {
      // we expect other records to fail immediately with E::NOSPC, possibly
      // along with some timeouts if the test system is slow
      EXPECT_TRUE(err == E::NOSPC || err == E::TIMEDOUT);
    }
  }

  // verify the stats
  std::map<std::string, int64_t> stats = cluster->getNode(0).stats();
  EXPECT_GT(stats["append_rejected_nospace"], 0);
  EXPECT_EQ(1, stats["node_out_of_space_received"]);
}

TEST_F(AppendIntegrationTest, HandleNOSPCMetaAndInternal) {
  // creating a storage node that cannot accept writes
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--free-disk-space-threshold",
                               "0.999999",
                               IntegrationTestUtils::ParamScope::STORAGE_NODE)
                     .setParam("--nospace-retry-interval",
                               "120s",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .doPreProvisionEpochMetaData() // to avoid counting NOSPC
                                                    // replies to the sequencer
                     .create(1);
  std::shared_ptr<Client> client_base =
      cluster->createClient(std::chrono::seconds(1));

  auto client = std::dynamic_pointer_cast<ClientImpl>(client_base);
  client->allowWriteMetaDataLog();
  client->allowWriteInternalLog();

  const logid_t logid(1);
  const logid_t metadata_logid = MetaDataLog::metaDataLogID(logid);
  const size_t num_records = 5;

  EpochMetaData buf(StorageSet{ShardID(1, 0)},
                    ReplicationProperty({{NodeLocationScope::NODE, 1}}),
                    epoch_t(3),
                    epoch_t(2));
  ASSERT_TRUE(buf.isValid());
  std::string metadata = buf.toStringPayload();

  auto do_write = [&]() {
    lsn_t record_lsn;
    // retry until the write can complete
    wait_until([&]() {
      lsn_t lsn = client->appendSync(
          metadata_logid, Payload(metadata.data(), metadata.size()));
      if (lsn == LSN_INVALID) {
        // expect E::SEQNOBUFS when there is a write in-flight
        EXPECT_EQ(E::SEQNOBUFS, err);
        return false;
      }
      record_lsn = lsn;
      return true;
    });
    return record_lsn;
  };

  // sleep for 1 second to ensure the monitor thread disallows writes
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));

  for (int i = 0; i < num_records; ++i) {
    // First write to the metadata log which should succeed.
    lsn_t lsn = do_write();
    ASSERT_NE(LSN_INVALID, lsn);

    // And then to the internal log which should also succeed.
    std::string internalData("internal data" + std::to_string(i));
    lsn = client->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                             Payload(internalData.data(), internalData.size()));
    EXPECT_NE(LSN_INVALID, lsn);
  }

  // verify the stats
  std::map<std::string, int64_t> stats = cluster->getNode(0).stats();
  EXPECT_EQ(stats["append_rejected_nospace"], 0);
  EXPECT_EQ(stats["node_out_of_space_received"], 0);
}

// When a node is replaced, we should restore write availability eventually
TEST_F(AppendIntegrationTest, WriteAfterReplace) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  // We expect this to be a 1 seqencer + 1 storage node setup
  ld_check(cluster->getConfig()
               ->get()
               ->serverConfig()
               ->getNode(0)
               ->isSequencingEnabled());
  ld_check(cluster->getConfig()
               ->get()
               ->serverConfig()
               ->getNode(1)
               ->isReadableStorageNode());

  const logid_t LOG_ID(1);
  Payload payload("123", 3);
  ASSERT_NE(LSN_INVALID, client->appendSync(LOG_ID, payload));

  // Replace N1
  ASSERT_EQ(0, cluster->replace(1));

  ASSERT_NE(LSN_INVALID, client->appendSync(LOG_ID, payload));
}

// Logs configured with a write token should reject writes if the correct
// string is not supplied
TEST_F(AppendIntegrationTest, WriteToken) {
  const logid_t LOG_ID(1);

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(1);
  log_attrs.set_writeToken(std::string("hunter2"));

  auto cluster =
      IntegrationTestUtils::ClusterFactory().setLogAttributes(log_attrs).create(
          1);

  auto client = cluster->createClient();

  // Writing without supplying the right token should fail
  facebook::logdevice::err = E::OK;
  ASSERT_EQ(LSN_INVALID, client->appendSync(LOG_ID, "foo"));
  ASSERT_EQ(E::ACCESS, facebook::logdevice::err);
  facebook::logdevice::err = E::OK;
  ASSERT_EQ(-1, client->trimSync(LOG_ID, LSN_OLDEST));
  ASSERT_EQ(E::ACCESS, facebook::logdevice::err);

  // Also fail if the wrong token is supplied
  client->addWriteToken("hunter3");
  facebook::logdevice::err = E::OK;
  ASSERT_EQ(LSN_INVALID, client->appendSync(LOG_ID, "foo"));
  ASSERT_EQ(E::ACCESS, facebook::logdevice::err);
  facebook::logdevice::err = E::OK;
  ASSERT_EQ(-1, client->trimSync(LOG_ID, LSN_OLDEST));
  ASSERT_EQ(E::ACCESS, facebook::logdevice::err);

  // After supplying the right token, writes should go through
  client->addWriteToken("hunter2");
  ASSERT_NE(LSN_INVALID, client->appendSync(LOG_ID, "foo"));
  ASSERT_EQ(0, client->trimSync(LOG_ID, LSN_OLDEST));
}

TEST_F(AppendIntegrationTest, CheckNodeHealthTest) {
  // creating a storage node that cannot accept writes
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--free-disk-space-threshold",
                    "0.999999",
                    IntegrationTestUtils::ParamScope::STORAGE_NODE)
          .setParam("--nospace-retry-interval",
                    "1s",
                    IntegrationTestUtils::ParamScope::SEQUENCER)
          // Ensure sequencer doesn't retry, which would throw off the
          // store_received stat we check
          .setParam("--store-timeout",
                    "9999s",
                    IntegrationTestUtils::ParamScope::SEQUENCER)
          // skip recovery so that store_received won't be affected by
          // mutations
          .setParam("--skip-recovery",
                    IntegrationTestUtils::ParamScope::SEQUENCER)
          .doPreProvisionEpochMetaData() // to avoid the stats being off due to
                                         // sequencer stores
          .create(1);
  std::shared_ptr<Client> client =
      cluster->createClient(std::chrono::seconds(1));

  const logid_t logid(2);

  // sleep for 1 second to ensure the monitor thread disallows writes
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));

  int num_records = 0;
  wait_until(
      "sequencer sends a few probes to the storage node to check if it has "
      "freed up space",
      [&] {
        ++num_records;
        lsn_t lsn = client->appendSync(logid, "data");
        EXPECT_EQ(LSN_INVALID, lsn);
        std::map<std::string, int64_t> stats = cluster->getNode(0).stats();
        return stats["message_received.CHECK_NODE_HEALTH"] >= 2;
      });
  std::map<std::string, int64_t> stats = cluster->getNode(0).stats();
  // No more than 1 store should have been sent
  EXPECT_EQ(1, stats["store_received"]);
}

// test write lsn after local trim point
TEST_F(AppendIntegrationTest, WriteLsnAfterTrimPoint) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  auto client = cluster->createClient();

  std::string data(128, 'x');

  auto stats = cluster->getNode(0).stats();
  auto skipped_records = stats["skipped_record_lsn_before_trim_point"];

  // it is highly unlikely that this doesn't succeed in 10 tries.
  auto tries = 0;
  auto max_tries = 10;
  int rv;
  std::string cmd, response;
  lsn_t new_lsn, old_lsn;
  for (tries = 0; tries < max_tries; tries++) {
    // write two records
    old_lsn = client->appendSync(logid_t(2), data);
    ASSERT_NE(LSN_INVALID, old_lsn);
    old_lsn = client->appendSync(logid_t(2), data);
    ASSERT_NE(LSN_INVALID, old_lsn);

    cmd = folly::sformat(
        "info record {} {} {} --json  --table", 2, old_lsn, old_lsn);
    response = cluster->getNode(0).sendCommand(cmd, false);
    // even with --json, there are some trailing chars that need to be clipped
    EXPECT_GT(response.size(), 7);
    auto json = folly::parseJson(response.substr(0, response.size() - 7));
    // sanity check (this is a dict with rows and headers keys)
    EXPECT_EQ(2, json.size());

    stats = cluster->getNode(0).stats();
    skipped_records = stats["skipped_record_lsn_before_trim_point"];

    // trim way past this lsn
    rv = client->settings().set("disable-trim-past-tail-check", "true");
    EXPECT_EQ(0, rv);
    // 1024 because, why not!
    rv = client->trimSync(logid_t(2), old_lsn + 1024);
    ASSERT_EQ(0, rv);

    // write another record. we expect the lsn to be less than the trim point
    new_lsn = client->appendSync(logid_t(2), data);
    ASSERT_NE(LSN_INVALID, new_lsn);
    // if epoch incremented, we cannot proceed with the test. retry.
    if (lsn_to_epoch(new_lsn) == lsn_to_epoch(old_lsn)) {
      ASSERT_GT(old_lsn + 1024, new_lsn);
      break;
    }
  }

  if (tries == max_tries) {
    // this is highly unlikely but this is NOT a test failure
    ld_info("Test did not exercise WriteLsnAfterTrimPoint");
    return;
  }

  // the write for the record with new_lsn should succeed but the record
  // shouldn't actually get written to RocksDB. verify with stats
  stats = cluster->getNode(0).stats();
  ASSERT_GT(stats["skipped_record_lsn_before_trim_point"], skipped_records);

  // double check with admin command that the record wasn't written to RocksDB
  cmd = folly::sformat("info record {} {} {}", 2, new_lsn, new_lsn);
  response = cluster->getNode(0).sendCommand(cmd, false);
  EXPECT_EQ("END\r\n", response);

  // we shouldn't be able to read anything back (everything is trimmed)
  auto reader = client->createReader(1);
  reader->setTimeout(std::chrono::seconds(5));
  rv = reader->startReading(logid_t(2), old_lsn);
  ASSERT_EQ(0, rv);

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;
  auto nread = reader->read(2, &records, &gap);
  ASSERT_EQ(-1, nread);
}

// Returns the number of successful writes
static int hammer_client_with_writes(Client& client,
                                     const int NAPPENDS,
                                     const int PAYLOAD_SIZE,
                                     const int NTHREADS) {
  std::atomic<int> sent{0}, success{0};
  // Have writer threads hammer the sequencer with writes in rapid succession.
  // Due to the small size of the sequencer window, many of them should fail.
  auto writer_fn = [&](int nwrites) {
    std::string payload(PAYLOAD_SIZE, '!');
    for (int i = 0; i < nwrites; ++i) {
      lsn_t rv = client.appendSync(logid_t(1), payload);
      ++sent;
      if (rv != LSN_INVALID) {
        ++success;
      }
    }
  };

  std::vector<std::thread> writers;
  for (int i = 0; i < NTHREADS; ++i) {
    int nwrites = NAPPENDS / NTHREADS + (i < NAPPENDS % NTHREADS);
    writers.emplace_back(writer_fn, nwrites);
  }
  for (auto& th : writers) {
    th.join();
  }

  ld_check(NAPPENDS == sent.load());
  return success.load();
}

// Test that server stats like `append_success' and `append_failed' are
// accurate even in the presence of append probes or sequencer batching, which
// present tricky interactions.
static void AppendIntegrationTest_Stats_impl(bool sequencer_batching_on) {
  const int NAPPENDS = 5000;
  const int PAYLOAD_SIZE = 300;
  const int NTHREADS = 16;
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(1);
  log_attrs.set_maxWritesInFlight(2);
  auto factory = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .enableMessageErrorInjection()
                     .setLogAttributes(log_attrs);
  if (sequencer_batching_on) {
    factory.setParam("--sequencer-batching")
        .setParam("--sequencer-batching-size-trigger",
                  std::to_string(2 * PAYLOAD_SIZE))
        // The bulk of appends should quickly hit the size trigger but also
        // lower the time trigger from the default 1s so that a single slow
        // writer thread does not repeatedly wait for the time trigger (the
        // worst-case runtime for the test is NAPPENDS / NTHREADS *
        // time_trigger).
        .setParam("--sequencer-batching-time-trigger", "20ms");
  }
  auto cluster = factory.create(1);
  std::shared_ptr<Client> client = cluster->createClient();
  int success =
      hammer_client_with_writes(*client, NAPPENDS, PAYLOAD_SIZE, NTHREADS);
  ld_info("%d appends, %d succededed, %d failed",
          NAPPENDS,
          success,
          NAPPENDS - success);
  auto stats = cluster->getNode(0).stats();
  EXPECT_EQ(NAPPENDS, stats["append_received"]);
  EXPECT_EQ(success, stats["append_success"]);
  EXPECT_EQ(NAPPENDS - success, stats["append_failed"]);
}

TEST_F(AppendIntegrationTest, Stats) {
  AppendIntegrationTest_Stats_impl(false);
}

TEST_F(AppendIntegrationTest, StatsWithSequencerBatching) {
  AppendIntegrationTest_Stats_impl(true);
}
