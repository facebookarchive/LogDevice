/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"

#include <chrono>

#include <folly/Conv.h>
#include <folly/json.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ZookeeperNodesConfigurationStore.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/test/InMemNodesConfigurationStore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/test/ZookeeperClientInMemory.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::configuration::nodes::ncm;
using namespace facebook::logdevice::membership;
using namespace std::chrono_literals;

struct TestDeps : public Dependencies {
  using Dependencies::Dependencies;
  using Dependencies::kConfigKey;
  ~TestDeps() override {}
};

TEST(NodesConfigurationManagerTest, basic) {
  constexpr MembershipVersion::Type kVersion{102};
  NodesConfiguration initial_config;
  initial_config.setVersion(kVersion);
  EXPECT_TRUE(initial_config.validate());
  auto z = std::make_shared<ZookeeperClientInMemory>(
      "unused quorum",
      ZookeeperClientInMemory::state_map_t{
          {TestDeps::kConfigKey,
           {NodesConfigurationCodecFlatBuffers::serialize(initial_config),
            zk::Stat{.version_ = 4}}}});
  auto store = std::make_unique<ZookeeperNodesConfigurationStore>(
      NodesConfigurationCodecFlatBuffers::extractConfigVersion, z);

  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 3;
  auto processor = make_test_processor(settings);

  auto deps = std::make_unique<TestDeps>(processor.get(), std::move(store));
  auto m = NodesConfigurationManager::create(
      NodesConfigurationManager::OperationMode::forTooling(), std::move(deps));
  m->init();

  auto new_version = MembershipVersion::Type{kVersion.val() + 1};

  NodesConfiguration new_config;
  new_config.setVersion(new_version);
  EXPECT_TRUE(new_config.validate());

  // fire and forget
  z->setData(TestDeps::kConfigKey,
             NodesConfigurationCodecFlatBuffers::serialize(new_config),
             /* cb = */ {});

  // TODO: better testing after offering a subscription API
  while (m->getConfig() == nullptr ||
         m->getConfig()->getVersion() == kVersion) {
    /* sleep override */ std::this_thread::sleep_for(200ms);
  }
  auto p = m->getConfig();
  EXPECT_EQ(new_version, p->getVersion());

  // verify each worker has the up-to-date config
  auto verify_version = [new_version](folly::Promise<folly::Unit> p) {
    auto nc = Worker::onThisThread()
                  ->getUpdateableConfig()
                  ->updateableNodesConfiguration();
    EXPECT_TRUE(nc);
    EXPECT_EQ(new_version, nc->get()->getVersion());
    p.setValue();
  };
  auto futures =
      fulfill_on_all_workers<folly::Unit>(processor.get(), verify_version);
  folly::collectAllSemiFuture(futures).get();
}
