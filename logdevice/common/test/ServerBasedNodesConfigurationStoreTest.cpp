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
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
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
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 5;
  settings.bootstrapping = true;
  auto processor = make_test_processor(
      settings, std::move(updatable_config), nullptr, NodeID(0, 1));
  auto nc = processor->getNodesConfiguration();
  const std::string nc_str = NodesConfigurationCodec::serialize(*nc, {false});
  auto nc_bumped = nc->withIncrementedVersionAndTimestamp();
  const std::string nc_str_bumped =
      NodesConfigurationCodec::serialize(*nc_bumped, {false});

  folly::Baton<> b;

  auto cb = [&b, &nc_str_bumped](Status status, std::string config) {
    EXPECT_EQ(Status::OK, status);
    // should return the config with higher config version
    EXPECT_EQ(nc_str_bumped, config);
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

  // Simulate two CONFIG_CHANGED received based on an atomic counter
  // 0: send nc_str. 1: send nc_bumped. 2+: no-op
  std::atomic<int> counter{0};
  auto futures = fulfill_on_worker_pool<folly::Unit>(
      processor.get(),
      WorkerType::GENERAL,
      [&nc_str, &nc_str_bumped, &counter](folly::Promise<folly::Unit> p) {
        auto worker = Worker::onThisThread();
        auto& map = worker->runningConfigurationFetches().map;
        for (const auto& kv : map) {
          auto c = counter.fetch_add(1);
          if (c <= 1) {
            auto rid = kv.first;
            CONFIG_CHANGED_Header hdr{
                Status::OK,
                rid,
                1234,
                1,
                NodeID(2, 1),
                CONFIG_CHANGED_Header::ConfigType::NODES_CONFIGURATION,
                CONFIG_CHANGED_Header::Action::CALLBACK};
            std::unique_ptr<facebook::logdevice::Message> msg =
                std::make_unique<CONFIG_CHANGED_Message>(
                    hdr, c == 0 ? nc_str : nc_str_bumped);
            worker->message_dispatch_->onReceived(
                msg.get(), Address(NodeID(2, 1)), PrincipalIdentity());
          }
        }
        p.setValue();
      });
  folly::collectAll(futures).get();

  // there should only be 2 config fetched requests
  EXPECT_EQ(2, counter.load());

  // Wait until the getConfig callback is called which has the test
  // expectations.
  b.wait();
}
