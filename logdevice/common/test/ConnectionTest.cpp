/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Connection.h"

#include <folly/io/async/AsyncSocket.h>
#include <gtest/gtest.h>

#include "logdevice/common/ProtocolHandler.h"
#include "logdevice/common/libevent/test/EvBaseMock.h"
#include "logdevice/common/network/MessageReader.h"
#include "logdevice/common/test/MockSocketAdapter.h"
#include "logdevice/common/test/SocketTest_fixtures.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::WithArg;

using namespace facebook::logdevice;

class ConnectionTest : public SocketTest {
 public:
  ConnectionTest() {
    cluster_name_ = "Socket_test_cluster";
    credentials_ = "Socket_test_credentials";
    csid_ = "client_uuid";
    client_build_info_ = "{}";
    conn_ = std::make_unique<Connection>(
        server_name_,
        SocketType::DATA,
        ConnectionType::PLAIN,
        flow_group_,
        std::make_unique<TestSocketDependencies>(this));
  }
  ~ConnectionTest() {}
  std::unique_ptr<Connection> conn_;
};

TEST_F(ConnectionTest, ConnectTest) {
  auto sock = std::make_unique<MockSocketAdapter>();
  EXPECT_CALL(*sock, connect_(_, server_addr_.getSocketAddress(), _, _, _))
      .Times(1)
      .WillOnce(
          WithArg<0>(Invoke([](folly::AsyncSocket::ConnectCallback* conn_cb) {
            conn_cb->connectSuccess();
          })));
  conn_->setSocketAdapter(std::move(sock));
  EXPECT_EQ(conn_->connect(), 0);
}

TEST_F(ConnectionTest, DISABLED_SendBuffers) {
  auto sock = std::make_unique<MockSocketAdapter>();
  EXPECT_CALL(*sock, connect_(_, server_addr_.getSocketAddress(), _, _, _))
      .Times(1)
      .WillOnce(
          WithArg<0>(Invoke([](folly::AsyncSocket::ConnectCallback* conn_cb) {
            conn_cb->connectSuccess();
          })));
  EXPECT_CALL(*sock, writeChain(NotNull(), NotNull(), folly::WriteFlags::NONE))
      .Times(1);
  ON_CALL(*sock, good()).WillByDefault(Return(true));
  conn_->setSocketAdapter(std::move(sock));

  EXPECT_EQ(conn_->connect(), 0);

  auto iobuf = folly::IOBuf::create(10);
  iobuf->append(10);
  conn_->sendBuffer(std::move(iobuf));
}

TEST_F(ConnectionTest, ReceiveBuffers) {
  EvBaseMock ev_base_mock(EvBase::MOCK_EVENTBASE);
  std::unique_ptr<ProtocolHandler> proto_handler =
      std::make_unique<ProtocolHandler>(conn_.get(), "", &ev_base_mock);
  MessageReader read_cb(*proto_handler, 1);
}
