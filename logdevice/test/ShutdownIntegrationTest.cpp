/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class ShutdownIntegrationTest : public IntegrationTestBase {};

TEST_F(ShutdownIntegrationTest, BasicShutdownTest) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(5);
  auto& nodes = cluster->getNodes();
  for (auto& node : nodes) {
    node.second->signal(SIGTERM);
    auto exitCode = node.second->waitUntilExited();
    ASSERT_EQ(0, exitCode) << "Process exited non-success status" << exitCode;
  }
}

// In this test we try to force append pending appenders during node shutdown.
// We create a append and through test parameter do not allow the appender to
// finish. We try to shutdown the nodes and expect that all appenders should be
// force aborted.
TEST_F(ShutdownIntegrationTest, ForceAbortPendingRequest) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--test-appender-skip-stores",
                               "true",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .setParam("--time-delay-before-force-abort",
                               "50",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .create(5);

  // Append data so that we are sure that appender is created on the sequencer.
  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE(client != nullptr);

  std::array<char, 8 * 1024> data; // send the contents of this array as payload
  const int small_payload_size = 128;

  Payload payload1(data.data(), small_payload_size);

  // Initiate an append, but the append should not finish as the appender is not
  // allowed complete.
  int rv =
      client->append(logid_t(2),
                     payload1,
                     [payload1](Status st, const DataRecord& /* unused */) {
                       EXPECT_EQ(E::SHUTDOWN, st);
                     });
  EXPECT_EQ(0, rv);

  // Sleep for few seconds

  std::this_thread::sleep_for(std::chrono::seconds(5));

  auto& node = cluster->getSequencerNode();
  node.signal(SIGTERM);
  auto exitCode = node.waitUntilExited();
  ASSERT_EQ(0, exitCode) << "Process exited success status" << exitCode;
}

// In this test we try to shutdown the node and test the abort sequence. If the
// system does not quiesce in shutdown time we abort the
// system. In this test we set a short shutdown time so that force abort logic
// does not kick in and we are sure that the system is aborted.
TEST_F(ShutdownIntegrationTest, CrashAfterTimeout) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--test-appender-skip-stores",
                               "true",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     // Set a very small shutdown timeout so that process does
                     // not get a chance to force abort.
                     .setParam("--shutdown-timeout",
                               "15s",
                               IntegrationTestUtils::ParamScope::SEQUENCER)
                     .create(5);

  auto& node = cluster->getSequencerNode();
  node.signal(SIGTERM);
  auto exitCode = node.waitUntilExited();
  ASSERT_NE(0, exitCode) << "Process exited success status" << exitCode;
}

// Slow down sockets on sequencer node by reducing the send buffer size. Send
// around 2000 append and shut down the node. The data in the socket should not
// be drained in the given timeout for force abort as it's very small. It should
// trigger force close of sockets. The test cannot verify that the forceabort
// was actually triggered because sometimes data can be drained quickly
// something to work on.
TEST_F(ShutdownIntegrationTest, ForceCloseSockets) {
  // Initialize necessary params.
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--time-delay-before-force-abort",
                    "1",
                    IntegrationTestUtils::ParamScope::SEQUENCER)
          .setParam(
              "--sendbuf-kb", "1", IntegrationTestUtils::ParamScope::SEQUENCER)
          .create(5);

  auto& node = cluster->getSequencerNode();

  // Append data.
  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE(client != nullptr);

  std::array<char, 8 * 1024> data; // send the contents of this array as payload
  for (auto i = 0; i < 2000; ++i) {
    const int small_payload_size = 8192;

    Payload payload1(data.data(), small_payload_size);

    int rv = client->append(
        logid_t(2),
        payload1,
        [payload1](Status /* unused */, const DataRecord& /* unused */) {});

    EXPECT_EQ(0, rv);
  }

  // Initiate shutdown.
  node.signal(SIGTERM);
  auto exitCode = node.waitUntilExited();
  // Expected that the process finishes cleanly.
  ASSERT_EQ(0, exitCode) << "Process exited success status" << exitCode;
}
