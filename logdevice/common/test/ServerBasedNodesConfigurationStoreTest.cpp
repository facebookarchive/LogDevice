/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/ServerBasedNodesConfigurationStore.h"

#include <gtest/gtest.h>

#include "logdevice/common/ConfigurationFetchRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/test/TestUtil.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::configuration::nodes;

TEST(ServerBasedNodesConfigurationStoreTest, SuccessScenario) {
  auto updatable_config =
      std::make_shared<UpdateableConfig>(createSimpleConfig(3, 0));
  updatable_config->getServerConfig()->setMyNodeID(NodeID(0, 1));
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 3;
  auto processor = make_test_processor(settings, std::move(updatable_config));

  folly::Baton<> b;

  auto cb = [&b](Status status, std::string config) {
    EXPECT_EQ(Status::OK, status);
    EXPECT_EQ("{config}", config);
    b.post();
  };

  fulfill_on_worker<folly::Unit>(processor.get(),
                                 worker_id_t(1),
                                 WorkerType::GENERAL,
                                 [&cb](folly::Promise<folly::Unit> p) {
                                   ServerBasedNodesConfigurationStore store;
                                   store.getConfig(std::move(cb));
                                   p.setValue();
                                 })
      .get();

  // Wait a bit until the request is executed
  /* sleep override */ std::this_thread::sleep_for(
      std::chrono::milliseconds(100));

  // Simulate a CONFIG_CHANGED received
  auto futures = fulfill_on_worker_pool<folly::Unit>(
      processor.get(), WorkerType::GENERAL, [](folly::Promise<folly::Unit> p) {
        auto worker = Worker::onThisThread();
        auto& map = worker->runningConfigurationFetches().map;
        if (map.size() == 0) {
          // That's not the worker on which the request ran.
          return;
        }
        ASSERT_EQ(1, map.size());
        auto rid = map.begin()->first;

        CONFIG_CHANGED_Header hdr{
            Status::OK,
            rid,
            1234,
            1,
            NodeID(2, 1),
            CONFIG_CHANGED_Header::ConfigType::NODES_CONFIGURATION,
            CONFIG_CHANGED_Header::Action::CALLBACK};
        std::unique_ptr<facebook::logdevice::Message> msg =
            std::make_unique<CONFIG_CHANGED_Message>(hdr, "{config}");
        worker->message_dispatch_->onReceived(msg.get(), Address(NodeID(2, 1)));
        p.setValue();
      });
  folly::collectAll(futures).get();

  // Wait until the getConfig callback is called which has the test
  // expectations.
  b.wait();
}
