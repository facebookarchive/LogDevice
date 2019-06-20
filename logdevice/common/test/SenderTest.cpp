/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sender.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/EventBase.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/ShapingConfig.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/test/MockConnectionFactory.h"
#include "logdevice/common/test/MockNodeServiceDiscovery.h"
#include "logdevice/common/test/MockNodesConfiguration.h"
#include "logdevice/common/test/MockSettings.h"
#include "logdevice/common/test/MockShapingConfig.h"
#include "logdevice/include/NodeLocationScope.h"

namespace facebook { namespace logdevice {

using namespace testing;
using namespace configuration::nodes;

struct FakeMessage : public Message {
  FakeMessage(MessageType type, TrafficClass tc) : Message(type, tc) {}
  void serialize(ProtocolWriter& writer) const override {
    constexpr static folly::StringPiece hello = "hello";
    writer.write(hello.data(), hello.size());
  }

  Disposition onReceived(const Address&) override {
    return Disposition::NORMAL;
  }
};

TEST(SenderTest, StartStop) {
  folly::IOThreadPoolExecutor thread_pool(1);
  configuration::MockShapingConfig sc;
  ClientIdxAllocator client_idx_allocator;
  std::shared_ptr<NodesConfiguration> nodes =
      std::make_shared<MockNodesConfiguration>();
  NodeLocation loc;
  loc.fromDomainString("ash.2.08.k.z");
  std::shared_ptr<Settings> settings = std::make_shared<MockSettings>();

  auto connections = std::make_unique<MockConnectionFactory>();

  Sender sender(settings,
                thread_pool.getEventBase()->getLibeventBase(),
                sc,
                &client_idx_allocator,
                false,
                nodes,
                node_index_t{0},
                loc,
                std::move(connections),
                nullptr);
  // TODO: I haven't been able to mock socket right now,
  // because of very tricky construction.
}
}} // namespace facebook::logdevice
