/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/compat/ThriftSender.h"

#include <memory>
#include <thread>

#include <folly/Optional.h>
#include <folly/SocketAddress.h>
#include <folly/executors/ManualExecutor.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/async/ServerStream.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/if/gen-cpp2/ApiModel_types.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/ConnectionTest_fixtures.h"
#include "logdevice/common/test/MockNetworkDependencies.h"
#include "logdevice/common/test/MockThriftApi.h"
#include "logdevice/common/test/TestUtil.h"

using namespace ::testing;
using facebook::logdevice::configuration::nodes::NodesConfiguration;

using apache::thrift::CompactSerializer;
using apache::thrift::ServerStream;
using apache::thrift::ServerStreamPublisher;

namespace facebook { namespace logdevice {

class ThriftSenderTest : public ::testing::Test {
 public:
  std::unique_ptr<NetworkDependencies> makeDeps() {
    auto deps = std::make_unique<NiceMock<MockNetworkDependencies>>();
    ON_CALL(*deps, getSettings()).WillByDefault(ReturnRef(*settings));
    ON_CALL(*deps, getServerConfig()).WillByDefault(Return(server_config));
    ON_CALL(*deps, getNodesConfiguration()).WillByDefault(Return(nodes));
    ON_CALL(*deps, getCSID()).WillByDefault(ReturnRef(csid));
    ON_CALL(*deps, getClientBuildInfo()).WillByDefault(Return(build_info));
    ON_CALL(*deps, getExecutor()).WillByDefault(Return(&cb_executor));
    return deps;
  }

  ServerSession* findServerSession(ThriftSender& sender, node_index_t node) {
    return sender.findServerSession(node);
  }

  ServerStreamPublisher<thrift::Message> injectSubscripion(ThriftSender& sender,
                                                           node_index_t node) {
    auto* session = findServerSession(sender, node);
    EXPECT_TRUE(session);
    auto [stream, publisher] = ServerStream<thrift::Message>::createPublisher();
    session->registerStream(std::move(stream).toClientStream());
    return std::move(publisher);
  }

  MockApiClient* setupClient(const Sockaddr& address) {
    auto client = std::make_unique<MockApiClient>();
    auto* client_ref = client.get();
    EXPECT_CALL(router, getApiAddress()).WillOnce(Return(Sockaddr(address)));
    EXPECT_CALL(router, createApiClient())
        .WillOnce(Return(ByMove(std::move(client))));
    return client_ref;
  }

  thrift::Message serialize(const Message& msg) {
    auto source = serializer.toThrift(msg, settings->max_protocol);
    auto serialized = CompactSerializer::serialize<std::string>(*source);
    return CompactSerializer::deserialize<thrift::Message>(serialized);
  }

  thrift::Message createAck() {
    ACK_Header ackhdr{0, request_id_t(1), 1, settings->max_protocol, E::OK};
    ACK_Message ack(ackhdr);
    return serialize(ack);
  }

  ThriftMessageSerializer serializer{"test"};
  MockThriftRouter router;
  std::shared_ptr<Settings> settings =
      std::make_shared<Settings>(create_default_settings<Settings>());
  std::shared_ptr<NodesConfiguration> nodes =
      std::make_shared<NodesConfiguration>();
  std::shared_ptr<ServerConfig> server_config =
      ServerConfig::fromDataTest(__FILE__);
  folly::ManualExecutor cb_executor;
  std::string csid = "csid";
  std::string build_info = "{}";
};

// Checks general lifecycle of server session: creation, handshake and
// destruction
TEST_F(ThriftSenderTest, ServerSessionLifecycle) {
  Sockaddr address("some/path");
  auto client = setupClient(address);
  ThriftSender sender(SocketType::DATA, folly::none, router, makeDeps());
  node_index_t node = 1;
  Address node_address = Address(node);

  // 1. Connect and check connection is alive and has right attributes
  EXPECT_NE(sender.checkServerConnection(node), 0);
  // Not connected yet
  EXPECT_CALL(*client, createSession(_, _));
  int rv = sender.connect(node);
  EXPECT_EQ(rv, 0);
  EXPECT_FALSE(sender.isClosed(node_address));
  const auto* info = sender.getConnectionInfo(node_address);
  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->peer_name, node_address);
  EXPECT_EQ(info->peer_address, address);
  EXPECT_TRUE(info->is_active->load());

  // 2. Simulate handshake responses: stream and message in this stream
  auto publisher = injectSubscripion(sender, node);
  publisher.next(createAck());

  // 3. Wait till it is processed, since callback is called from Thrift thread
  wait_until("ACK delivered", [&]() {
    cb_executor.drain();
    return findServerSession(sender, node)->getState() ==
        ThriftSession::State::ESTABLISHED;
  });

  // 4. Check fields are filled during handshake
  EXPECT_EQ(sender.checkServerConnection(node), 0);
  info = sender.getConnectionInfo(node_address);
  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->protocol.value(), settings->max_protocol);
  EXPECT_EQ(info->our_name_at_peer.value(), ClientID(1));

  // 5. Disconnect and ensure connection has disappeared from API
  rv = sender.closeConnection(node_address, E::SHUTDOWN);
  cb_executor.drain();
  EXPECT_EQ(rv, 0);
  EXPECT_TRUE(sender.isClosed(node_address));
  EXPECT_NE(sender.checkServerConnection(node), 0);
  info = sender.getConnectionInfo(node_address);
  EXPECT_EQ(info, nullptr);
  std::move(publisher).complete();
}

}} // namespace facebook::logdevice
