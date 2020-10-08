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

#include "logdevice/common/ConnectionKind.h"
#include "logdevice/common/ProtocolHandler.h"
#include "logdevice/common/libevent/test/EvBaseMock.h"
#include "logdevice/common/network/MessageReader.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"
#include "logdevice/common/test/ConnectionTest_fixtures.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

using ::testing::_;
using ::testing::Args;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::WithArg;

using namespace facebook::logdevice;

class ServerConnectionTest : public ConnectionTest {
 public:
  ServerConnectionTest() {
    settings_.server = true;
    source_node_id_ = server_name_;
    ev_base_folly_.selectEvBase(EvBase::FOLLY_EVENTBASE);
    deps_ = new TestNetworkDependencies(this);
  }

  void SetUp() override {
    auto sock = std::make_unique<testing::NiceMock<MockSocketAdapter>>();
    sock_ = sock.get();
    ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
    ON_CALL(*sock_, good()).WillByDefault(Return(!socket_closed_));
    conn_ = std::make_unique<Connection>(
        42 /* fd */,
        ClientID(client_id_) /* client_name */,
        Sockaddr(get_localhost_address_str(), 4440) /* client_addr */,
        ResourceBudget::Token() /* accounting token, not used */,
        SocketType::DATA /* socket type */,
        ConnectionType::PLAIN,
        flow_group_,
        std::unique_ptr<SocketNetworkDependencies>(deps_),
        std::move(sock),
        ConnectionKind::DATA);
    // A server socket is connected from the beginning.
    EXPECT_TRUE(connected());
    EXPECT_FALSE(handshaken());
  }

  ~ServerConnectionTest() override {
    conn_.reset();
    EXPECT_EQ(bytes_pending_, 0);
  }

  template <typename T>
  friend void receiveMessage(T& socket,
                             const facebook::logdevice::Message* msg,
                             uint16_t proto);
};

TEST_F(ClientConnectionTest, ConnectTest) {
  EXPECT_CALL(*sock_, connect_(_, server_addr_.getSocketAddress(), _, _, _))
      .Times(1)
      .WillOnce(
          WithArg<0>(Invoke([](folly::AsyncSocket::ConnectCallback* conn_cb) {
            conn_cb->connectSuccess();
          })));
  EXPECT_EQ(conn_->connect(), 0);
  ev_base_folly_.loopOnce();
}

TEST_F(ClientConnectionTest, CompleteConnectionSuccessfully) {
  std::chrono::milliseconds timeout = settings_.connect_timeout;
  size_t max_retries = settings_.connection_retries;
  auto connect_timeout_retry_multiplier =
      settings_.connect_timeout_retry_multiplier;

  for (size_t retry_count = 1; retry_count < max_retries; ++retry_count) {
    timeout += std::chrono::duration_cast<std::chrono::milliseconds>(
        settings_.connect_timeout *
        pow(connect_timeout_retry_multiplier, retry_count));
  }
  EXPECT_CALL(
      *sock_,
      connect_(_, server_addr_.getSocketAddress(), timeout.count(), _, _))
      .Times(1);
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
}

template <typename T>
void receiveMessage(T& socket,
                    const facebook::logdevice::Message* msg,
                    uint16_t proto) {
  size_t msg_size = msg->size(proto);
  ASSERT_GT(msg_size, 0);
  size_t hdr_size = ProtocolHeader::bytesNeeded(msg->type_, proto);
  auto io_buf = folly::IOBuf::create(msg_size);
  ProtocolHeader* hdr = (ProtocolHeader*)io_buf->writableTail();
  hdr->len = msg_size;
  hdr->type = msg->type_;
  hdr->cksum = 0;
  io_buf->advance(hdr_size);

  ProtocolWriter writer(msg->type_, io_buf.get(), proto);
  msg->serialize(writer);
  ssize_t bodylen = writer.result();
  ASSERT_GT(bodylen, 0);

  if (!isHandshakeMessage(msg->type_)) {
    hdr->cksum = writer.computeChecksum();
    if (socket.tamper_checksum_) {
      hdr->cksum += 1;
    }
  }

  io_buf->prepend(hdr_size);

  ld_info(
      "Received Message size %lu serialized body len %lu", msg_size, bodylen);
  void* buffer;
  size_t len;
  socket.rd_callback_->getReadBuffer(&buffer, &len);
  ld_info("len recv %lu, io_buf->length() %lu", len, io_buf->length());
  ASSERT_LE(hdr_size, len);
  memcpy(buffer, io_buf->data(), std::min(io_buf->length(), len));
  socket.rd_callback_->readDataAvailable(std::min(io_buf->length(), len));
  io_buf->trimStart(std::min(io_buf->length(), len));

  if (io_buf->length() == 0) {
    return;
  }
  socket.rd_callback_->getReadBuffer(&buffer, &len);
  ld_info("len recv %lu, io_buf->length() %lu", len, io_buf->length());
  // Protocol reader detected error
  if (len == 0) {
    return;
  }
  ASSERT_LE(io_buf->length(), len);
  memcpy(buffer, io_buf->data(), io_buf->length());
  socket.rd_callback_->readDataAvailable(io_buf->length());
  io_buf->trimStart(io_buf->length());
}

void ClientConnectionTest::receiveAckMessage(
    Status st,
    facebook::logdevice::Message::Disposition disp,
    uint16_t proto) {
  SCOPE_EXIT {
    on_received_hook_ = nullptr;
  };
  ACK_Header ackhdr{0, request_id_t(0), client_id_, proto, st};
  on_received_hook_ = [&ackhdr, &disp, &st](facebook::logdevice::Message* msg,
                                            const Address&,
                                            std::shared_ptr<PrincipalIdentity>,
                                            ResourceBudget::Token) {
    EXPECT_EQ(msg->type_, MessageType::ACK);
    ACK_Message* ack = dynamic_cast<ACK_Message*>(msg);
    EXPECT_EQ(memcmp(&ackhdr, &ack->getHeader(), sizeof(ackhdr)), 0);
    err = st;
    return disp;
  };
  ACK_Message msg(ackhdr);
  receiveMessage(*this, &msg, proto);
}

TEST_F(ClientConnectionTest, Handshake) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, good()).WillByDefault(Return(true));
  EXPECT_CALL(*sock_, writeChain_(_, _, _))
      .WillOnce(Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                          folly::IOBuf* buf,
                                          folly::WriteFlags) {
        wr_callback_ = cb;
        hello_buf.reset(buf);
      }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
  ev_base_folly_.loopOnce();
  writeSuccess();
  receiveAckMessage();
  EXPECT_TRUE(handshaken());
}
static Envelope* create_message(Connection& s) {
  GET_SEQ_STATE_flags_t flags = 0;
  auto msg = std::make_unique<GET_SEQ_STATE_Message>(
      logid_t(42),
      request_id_t(1),
      flags,
      GetSeqStateRequest::Context::UNKNOWN);
  auto envelope = s.registerMessage(std::move(msg));
  return envelope;
}

TEST_F(ClientConnectionTest, SerializationStages) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  receiveAckMessage();
  EXPECT_TRUE(handshaken());
  CHECK_SERIALIZEQ();
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::OK);
  CHECK_SERIALIZEQ();
}

// Verify that handshake works for a server Socket.
TEST_F(ServerConnectionTest, Handshake) {
  // Simulate HELLO to be received by the server.
  HELLO_Header hdr{
      uint16_t(max_proto_), uint16_t(max_proto_), 0, request_id_t(0), {}};
  bool called = false;
  on_received_hook_ = [&called](facebook::logdevice::Message* msg,
                                const Address&,
                                std::shared_ptr<PrincipalIdentity>,
                                ResourceBudget::Token) {
    EXPECT_FALSE(called);
    EXPECT_EQ(msg->type_, MessageType::HELLO);
    err = E::OK;
    called = true;
    return facebook::logdevice::Message::Disposition::NORMAL;
  };
  receiveMessage(
      *this, new HELLO_Message(hdr), Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_TRUE(called);
  // Simulate the server replying ACK.
  ACK_Header ackhdr{
      0, request_id_t(0), client_id_, uint16_t(max_proto_), E::OK};
  std::unique_ptr<Message> msg = std::make_unique<ACK_Message>(ackhdr);
  auto envelope = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*envelope);
  // We should be handshaken now.
  EXPECT_TRUE(handshaken());
}

// Check that last used time gets updated on key events
TEST_F(ClientConnectionTest, LastUsedUpdatedOnMessages) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, good()).WillByDefault(Return(true));
  EXPECT_CALL(*sock_, writeChain_(_, _, _))
      .WillOnce(Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                          folly::IOBuf* buf,
                                          folly::WriteFlags) {
        wr_callback_ = cb;
        hello_buf.reset(buf);
      }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(usedSinceLastCheck());
  ev_base_folly_.loopOnce();
  writeSuccess();
  receiveAckMessage();
  EXPECT_TRUE(usedSinceLastCheck());
  // Send a message.
  Envelope* envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);
  EXPECT_TRUE(usedSinceLastCheck());
  // No activity on connection since last check
  EXPECT_FALSE(usedSinceLastCheck());
}

// Check that last used time gets updated on key events
TEST_F(ClientConnectionTest, ListenerKeepConnectionActive) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, good()).WillByDefault(Return(true));
  EXPECT_CALL(*sock_, writeChain_(_, _, _))
      .WillOnce(Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                          folly::IOBuf* buf,
                                          folly::WriteFlags) {
        wr_callback_ = cb;
        hello_buf.reset(buf);
      }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(usedSinceLastCheck());
  ev_base_folly_.loopOnce();
  writeSuccess();
  receiveAckMessage();
  EXPECT_TRUE(usedSinceLastCheck());
  {
    EmptySocketCallback cb;
    conn_->pushOnCloseCallback(cb);
    EXPECT_TRUE(usedSinceLastCheck());
  }
  EXPECT_FALSE(usedSinceLastCheck());
}

TEST_F(ServerConnectionTest, IncomingMessageBytesLimitHandshake) {
  incoming_message_bytes_limit_.setLimit(0);
  // Simulate HELLO to be received by the server.
  HELLO_Header hdr{
      uint16_t(max_proto_), uint16_t(max_proto_), 0, request_id_t(0), {}};
  bool called = false;
  on_received_hook_ = [&called](facebook::logdevice::Message* msg,
                                const Address&,
                                std::shared_ptr<PrincipalIdentity>,
                                ResourceBudget::Token) {
    EXPECT_FALSE(called);
    EXPECT_EQ(msg->type_, MessageType::HELLO);
    err = E::OK;
    called = true;
    return facebook::logdevice::Message::Disposition::NORMAL;
  };
  receiveMessage(
      *this, new HELLO_Message(hdr), Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_TRUE(called);
  // Simulate the server replying ACK.
  ACK_Header ackhdr{
      0, request_id_t(0), client_id_, uint16_t(max_proto_), E::OK};
  std::unique_ptr<Message> msg = std::make_unique<ACK_Message>(ackhdr);
  auto envelope = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*envelope);
  // We should be handshaken now.
  EXPECT_TRUE(handshaken());
}

TEST_F(ServerConnectionTest, IncomingMessageBytesLimit) {
  incoming_message_bytes_limit_.setLimit(0);
  // Simulate HELLO to be received by the server.
  HELLO_Header hdr{
      uint16_t(max_proto_), uint16_t(max_proto_), 0, request_id_t(0), {}};
  on_received_hook_ = [](facebook::logdevice::Message* msg,
                         const Address&,
                         std::shared_ptr<PrincipalIdentity>,
                         ResourceBudget::Token) {
    EXPECT_EQ(msg->type_, MessageType::HELLO);
    err = E::OK;
    return facebook::logdevice::Message::Disposition::NORMAL;
  };
  receiveMessage(
      *this, new HELLO_Message(hdr), Compatibility::MAX_PROTOCOL_SUPPORTED);
  // Simulate the server replying ACK.
  ACK_Header ackhdr{
      0, request_id_t(0), client_id_, uint16_t(max_proto_), E::OK};
  auto envelope = conn_->registerMessage(std::make_unique<ACK_Message>(ackhdr));
  conn_->releaseMessage(*envelope);
  // We should be handshaken now.
  EXPECT_TRUE(handshaken());

  // With limit zero ResourceBudget allows a single message
  // even though we go beyond allowed limit.
  ResourceBudget::Token token;
  on_received_hook_ = [&](Message* msg,
                          const Address&,
                          std::shared_ptr<PrincipalIdentity>,
                          ResourceBudget::Token t) {
    EXPECT_TRUE(t.valid());
    EXPECT_FALSE(incoming_message_bytes_limit_.acquire(msg->size()));
    token = std::move(t);
    return Message::Disposition::NORMAL;
  };

  auto check_node_hdr = CHECK_NODE_HEALTH_Header{request_id_t(1), 1, 0};
  auto msg =
      new TestFixedSizeMessage<CHECK_NODE_HEALTH_Header,
                               MessageType::CHECK_NODE_HEALTH,
                               TrafficClass::FAILURE_DETECTOR>(check_node_hdr);
  receiveMessage(*this, msg, Compatibility::MAX_PROTOCOL_SUPPORTED);

  // Try sending another message, this message should fail to send with
  // ENOBUFS.
  auto new_msg =
      new TestFixedSizeMessage<CHECK_NODE_HEALTH_Header,
                               MessageType::CHECK_NODE_HEALTH,
                               TrafficClass::FAILURE_DETECTOR>(check_node_hdr);
  bool called = false;
  on_received_hook_ = [&](Message*,
                          const Address&,
                          std::shared_ptr<PrincipalIdentity>,
                          ResourceBudget::Token) {
    called = true;
    return Message::Disposition::NORMAL;
  };
  // We have captured the last token hence this message should not be received.
  receiveMessage(*this, new_msg, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_FALSE(called);
  EXPECT_TRUE(rd_callback_ == nullptr);
  EXPECT_TRUE(conn_->msgRetryTimerArmed());

  // Try again without releasing token and the state should remain the same.
  ev_base_folly_.loopOnce();
  EXPECT_FALSE(called);
  EXPECT_TRUE(rd_callback_ == nullptr);
  EXPECT_TRUE(conn_->msgRetryTimerArmed());
  token.release();
  // Once the eventloop is run we should be able to accept the message as the
  // socket token is released.
  ev_base_folly_.loopOnce();
  EXPECT_TRUE(called);
  EXPECT_TRUE(rd_callback_ != nullptr);
  EXPECT_FALSE(conn_->msgRetryTimerArmed());
  // Reset limit and make sure we get the callback.
  incoming_message_bytes_limit_.setLimit(std::numeric_limits<uint64_t>::max());
  msg =
      new TestFixedSizeMessage<CHECK_NODE_HEALTH_Header,
                               MessageType::CHECK_NODE_HEALTH,
                               TrafficClass::FAILURE_DETECTOR>(check_node_hdr);
  called = false;
  on_received_hook_ = [&](Message*,
                          const Address&,
                          std::shared_ptr<PrincipalIdentity>,
                          ResourceBudget::Token) {
    called = true;
    return Message::Disposition::NORMAL;
  };
  receiveMessage(*this, msg, Compatibility::MAX_PROTOCOL_SUPPORTED);
  EXPECT_TRUE(called);
  EXPECT_TRUE(rd_callback_ != nullptr);
  EXPECT_FALSE(conn_->msgRetryTimerArmed());
}

// Bad protocol error.
TEST_F(ClientConnectionTest, InvalidAckMessage) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  receiveAckMessage(
      E::PROTONOSUPPORT, facebook::logdevice::Message::Disposition::ERROR);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PROTONOSUPPORT);
}

TEST_F(ClientConnectionTest, GetDscp) {
  auto net_socket = folly::netops::socket(AF_INET, SOCK_STREAM, 0);
  ON_CALL(*sock_, getNetworkSocket()).WillByDefault(Return(net_socket));
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  int rv = conn_->connect();
  ASSERT_EQ(0, rv);

  conn_->setDSCP(4);
  EXPECT_EQ(4 << 2, getDscp());
}

TEST_F(ClientConnectionTest, PeerShutdown) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  int rv = conn_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);

  // Simulate the socket closing.
  // onSent(st=E::SHUTDOWN) should be called for GET_SEQ_STATE.
  conn_->setPeerShuttingDown();
  rd_callback_->readErr(folly::AsyncSocketException(
      folly::AsyncSocketException::END_OF_FILE, ""));
  ev_base_folly_.loopOnce();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::SHUTDOWN);
}

// A message is enqueued in the Socket but finally it is rejected once handshake
// completes with a protocol that's incompatible with that message.
TEST_F(ClientConnectionTest, MessageRejectedAfterHandshakeInvalidProtocol) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  // Send a message that requires a protocol >= MIN_PROTOCOL_SUPPORTED+1
  std::unique_ptr<facebook::logdevice::Message> msg =
      std::make_unique<VarLengthTestMessage>(
          Compatibility::MIN_PROTOCOL_SUPPORTED + 1 /* min_proto+1 */,
          1 /* size */);
  auto envelope = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::TEST);

  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::TEST);
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::TEST);

  receiveAckMessage(E::OK,
                    facebook::logdevice::Message::Disposition::NORMAL,
                    Compatibility::MIN_PROTOCOL_SUPPORTED);
  EXPECT_TRUE(handshaken());
  // The message required protocol >= 6, so it is finally rejected (removed
  // from serializeq_) before it could be serialized.
  CHECK_ON_SENT(MessageType::TEST, E::PROTONOSUPPORT);
  CHECK_SERIALIZEQ();
}

// A message is enqueued in the Socket. At that time socket adjusts
// bytes_pending_ according to the size it would have if the handshaken protocol
// is the maximum supported protocol. However, the server requires the client to
// talk with a lower protocol version. The message is still compatible with that
// version so it can be serialized. The only difference is that with this
// protocol version the message is smaller.
// This test verifies that under this scenario the bytes_pending_ accounting is
// accurate.
// See T6281298.
TEST_F(ClientConnectionTest, MessageChangesSizeAfterHandshake) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  // Send a message that requires a protocol >= 3.
  // Its size is 21 for protocols in range [3, 5] and 42 in range [6, MAX].
  auto raw_msg = new VarLengthTestMessage(3 /* min_proto */, 42 /* size */);
  raw_msg->setSize(5, 21);
  std::unique_ptr<facebook::logdevice::Message> msg(raw_msg);
  auto envelope = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::TEST);

  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::TEST);

  // HELLO is sent.
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::TEST);

  // The server sends back ACK with min protocol
  receiveAckMessage(E::OK,
                    facebook::logdevice::Message::Disposition::NORMAL,
                    Compatibility::MIN_PROTOCOL_SUPPORTED);

  // We completed handshake. TEST is serialized.

  CHECK_SERIALIZEQ();
}

// Verify the behavior when up to --connection-retries connect attempts failed.
// Enqueued messages should have their onSent(st=E::TIMEDOUT) called.
TEST_F(ClientConnectionTest, ConnectionTimeout) {
  settings_.connection_retries = 2;
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  // Send a message.
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::TIMED_OUT, "Request timed out.");
  // Attempt 1 fails.

  conn_callback_->connectErr(ex);
  ev_base_folly_.loopOnce();
  // onSent(st=E::TIMEDOUT) should be called for HELLO and GET_SEQ_STATE.
  CHECK_ON_SENT(MessageType::HELLO, E::TIMEDOUT);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::TIMEDOUT);
}

// Verify the behavior when handshake timeout happens (the socket successfully
// connected but ACK could not be received in time).
// Enqueued messages should have their onSent(st=E::TIMEDOUT) called.
TEST_F(ClientConnectionTest, HandshakeTimeout) {
  settings_.connection_retries = 2;
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  // Send a message.
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  // Socket is connected, HELLO can be serialized.
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);

  // simulate the handshake timeout timer to trigger.
  triggerHandshakeTimeout();
  // onSent(st=E::TIMEDOUT) should be called for GET_SEQ_STATE.
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::TIMEDOUT);
}

// A message is sent but the socket is closed.
TEST_F(ClientConnectionTest, ConnectionResetByPeer) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);

  // Send a message.
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  // Socket is connected, HELLO can be serialized.
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  // Simulate the socket closing.
  // onSent(st=E::PEER_CLOSED) should be called for GET_SEQ_STATE.
  folly::AsyncSocketException ex(
      folly::AsyncSocketException::END_OF_FILE, "Peer closed socket.");
  rd_callback_->readErr(ex);
  ev_base_folly_.loopOnce();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PEER_CLOSED);
}

// The socket cannot connect. Enqueued messages should have
// onSent(st=E::CONNFAILED) called.
TEST_F(ClientConnectionTest, ConnFailed) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);

  // Send a message.
  auto envelope = create_message(*conn_);
  ASSERT_NE(envelope, nullptr);
  conn_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  folly::AsyncSocketException ex(
      folly::AsyncSocketException::NETWORK_ERROR, "Network error.");
  conn_callback_->connectErr(ex);
  ev_base_folly_.loopOnce();
  CHECK_ON_SENT(MessageType::HELLO, E::CONNFAILED);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::CONNFAILED);
}

TEST_F(ClientConnectionTest, DownRevEvbufferAccounting) {
  // Verify that sending down-protocol messages does not break
  // evbuffer space accounting.
  const uint16_t test_proto_ver = max_proto_ - 1;
  const size_t msg_test_proto_size = 5;
  const size_t msg_max_proto_size = 50;

  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);

  // Complete the handshake part.
  conn_callback_->connectSuccess();
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  receiveAckMessage(
      E::OK, facebook::logdevice::Message::Disposition::NORMAL, test_proto_ver);
  ASSERT_EQ(0, bytes_pending_);

  auto msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, msg_max_proto_size);
  msg->setSize(test_proto_ver, msg_test_proto_size);
  // max_proto_ should be MAX_PROTOCOL_SUPPORTED.
  msg->setSize(max_proto_, msg_max_proto_size);

  size_t protohdr_size_for_test_proto =
      ProtocolHeader::bytesNeeded(msg->type_, test_proto_ver);
  auto* envelope = conn_->registerMessage(std::move(msg));

  // Queued messages are accounted assuming MAX_PROTOCOL_SUPPORTED.
  // Therefore, we expect full ProtocolHeader
  ASSERT_EQ(bytes_pending_, msg_max_proto_size + sizeof(ProtocolHeader));

  // Serialize to the asyncsocket. This will add the serialization cost
  // to the queued cost.
  conn_->releaseMessage(*envelope);

  ASSERT_EQ(bytes_pending_,
            msg_max_proto_size + sizeof(ProtocolHeader) +
                protohdr_size_for_test_proto + msg_test_proto_size);

  // Send the message. Both the bytes consumed in the asyncsocket and the
  // "queued cost" should be released.
  ev_base_folly_.loopOnce();
  writeSuccess();
  ASSERT_EQ(0, bytes_pending_);
}

// Verify if the client rejects any messages that exceeds
// Message::MAX_LEN + sizeof(PayloadHeader)
TEST_F(ClientConnectionTest, MaxLenRejected) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  ON_CALL(*sock_, closeNow()).WillByDefault(Invoke([this]() {
    socket_closed_ = true;
  }));
  EXPECT_EQ(conn_->connect(), 0);

  CHECK_SERIALIZEQ(MessageType::HELLO);
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ();
  //
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ();
  //
  receiveAckMessage();
  CHECK_SERIALIZEQ();
  //
  SCOPE_EXIT {
    on_received_hook_ = nullptr;
  };
  bool called = false;
  EXPECT_CALL(*sock_, closeNow()).Times(1);
  size_t max_size = facebook::logdevice::Message::MAX_LEN;
  on_received_hook_ = [&called](facebook::logdevice::Message* msg,
                                const Address&,
                                std::shared_ptr<PrincipalIdentity>,
                                ResourceBudget::Token) {
    EXPECT_FALSE(called);
    EXPECT_EQ(msg->type_, MessageType::TEST);
    err = E::OK;
    called = true;
    return facebook::logdevice::Message::Disposition::NORMAL;
  };
  // Receive a message that's small enough to be received
  auto msg =
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED, max_size);
  receiveMessage(*this, msg, Compatibility::MAX_PROTOCOL_SUPPORTED);
  delete msg;
  EXPECT_TRUE(called);
  // Messages too big cause the socket to be closed
  msg = new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, max_size + 1);
  receiveMessage(*this, msg, Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
}

TEST_F(ClientConnectionTest, CloseConnectionOnProtocolChecksumMismatch) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);

  CHECK_SERIALIZEQ(MessageType::HELLO);
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ();

  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ();

  receiveAckMessage();
  CHECK_SERIALIZEQ();

  EXPECT_CALL(*sock_, closeNow()).Times(1);
  int called = 3;
  on_received_hook_ = [&called](facebook::logdevice::Message* msg,
                                const Address&,
                                std::shared_ptr<PrincipalIdentity>,
                                ResourceBudget::Token) {
    EXPECT_EQ(msg->type_, MessageType::TEST);
    err = E::OK;
    EXPECT_LT(0, called);
    --called;
    return facebook::logdevice::Message::Disposition::NORMAL;
  };

  // 1. Receiving a non-tampered message when checksumming is disabled,
  //    socket shouldn't be closed
  settings_.checksumming_enabled = false;
  tamper_checksum_ = false;
  receiveMessage(
      *this,
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
  // 2. Receiving a tampered message won't close socket since checksumming
  //    is disabled
  tamper_checksum_ = true;
  receiveMessage(
      *this,
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
  // 3. Receiving a non-tampered message, when checksumming is enabled,
  //    socket shouldn't be closed
  settings_.checksumming_enabled = true;
  tamper_checksum_ = false;
  receiveMessage(
      *this,
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
  // 4. Receive a tampered message, when checksumming is enabled,
  //    verify that socket gets closed
  tamper_checksum_ = true;
  receiveMessage(
      *this,
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
}

TEST_F(ClientConnectionTest, RunSocketHealthCheck) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, good()).WillByDefault(Return(true));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
  ev_base_folly_.loopOnce();
  writeSuccess();
  receiveAckMessage();
  EXPECT_TRUE(handshaken());

  // Test cases that consider just socket-idle-threshold to qualify a active
  // socket. min_socket_idle_threshold_percent is zero for these cases.
  settings_.socket_health_check_period = std::chrono::milliseconds(1000);
  settings_.min_socket_idle_threshold_percent = 0;
  settings_.socket_idle_threshold = 100;
  settings_.min_bytes_to_drain_per_second = 1000;
  // Message well below socket-idle-threshold should not be considered in
  // slowness detection.
  auto msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, 10);
  auto e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += settings_.socket_health_check_period;
  socket_flow_stats_.busy_time += settings_.socket_health_check_period;
  ev_base_folly_.loopOnce();
  writeSuccess();
  EXPECT_EQ(SocketDrainStatusType::IDLE, conn_->checkSocketHealth());

  // Message is above socket-idle-threshold and also the throughput is way low
  // compared min_bytes_to_drain_per_second
  msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, 200);
  e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += settings_.socket_health_check_period;
  socket_flow_stats_.busy_time += settings_.socket_health_check_period;
  ev_base_folly_.loopOnce();
  writeSuccess();
  EXPECT_EQ(SocketDrainStatusType::NET_SLOW, conn_->checkSocketHealth());

  // Message is above socket-idle-threshold and also the throughput is
  // above min_bytes_to_drain_per_second.
  msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, 1000);
  e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += settings_.socket_health_check_period;
  socket_flow_stats_.busy_time += settings_.socket_health_check_period;
  ev_base_folly_.loopOnce();
  writeSuccess();
  EXPECT_EQ(SocketDrainStatusType::ACTIVE, conn_->checkSocketHealth());

  // Message above socket-idle-threshold and also the throughput is way low
  // compared min_bytes_to_drain_per_second. But the socket is receiver
  // limited.
  msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, 200);
  e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += settings_.socket_health_check_period;
  socket_flow_stats_.busy_time += settings_.socket_health_check_period;
  socket_flow_stats_.rwnd_limited_time += std::chrono::milliseconds(
      51 * settings_.socket_health_check_period.count() / 100);
  ev_base_folly_.loopOnce();
  writeSuccess();
  EXPECT_EQ(SocketDrainStatusType::RECV_SLOW, conn_->checkSocketHealth());

  // Tests cases with non-zero min_socket_idle_threshold_percent where is a
  // socket is not active for enough time it is not considered active and is
  // not included in slow detection.
  settings_.min_socket_idle_threshold_percent = 40;

  // Pass time to 61% of socket_health_check_period this means socket can be
  // active only for 39% of time in this period, below the threshold limit.
  cur_time_ += std::chrono::milliseconds(
      settings_.socket_health_check_period.count() * 61 / 100);
  msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, 200);
  e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += std::chrono::milliseconds(
      settings_.socket_health_check_period.count() * 39 / 100);
  socket_flow_stats_.busy_time += settings_.socket_health_check_period;
  ev_base_folly_.loopOnce();
  writeSuccess();
  EXPECT_EQ(SocketDrainStatusType::IDLE, conn_->checkSocketHealth());
  // Pass time to 55% of ocket_health_check_period this means socket can be
  // active for more than 40% time.
  cur_time_ += std::chrono::milliseconds(
      settings_.socket_health_check_period.count() * 55 / 100);
  msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, 200);
  e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += std::chrono::milliseconds(
      settings_.socket_health_check_period.count() * 45 / 100);
  socket_flow_stats_.busy_time += settings_.socket_health_check_period;
  ev_base_folly_.loopOnce();
  writeSuccess();
  EXPECT_EQ(SocketDrainStatusType::NET_SLOW, conn_->checkSocketHealth());
}
TEST_F(ClientConnectionTest, SocketThroughput) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, good()).WillByDefault(Return(true));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
  ev_base_folly_.loopOnce();
  writeSuccess();
  receiveAckMessage();
  EXPECT_TRUE(handshaken());

  // Test cases that don't care about idle percent and just make sure correct
  // throughput is calculated.
  settings_.socket_health_check_period = std::chrono::milliseconds(1000);
  settings_.min_socket_idle_threshold_percent = 0;
  settings_.socket_idle_threshold = 0;
  // To reset health stats invoke checkSocketHealth
  conn_->checkSocketHealth();

  auto payload_size = 301;
  auto msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, payload_size);
  auto total_msg_size = msg->size();
  auto e = conn_->registerMessage(std::move(msg));
  conn_->releaseMessage(*e);
  cur_time_ += settings_.socket_health_check_period;
  ev_base_folly_.loopOnce();
  writeSuccess();
  conn_->checkSocketHealth();
  EXPECT_EQ(
      int(total_msg_size * 1e3 / settings_.socket_health_check_period.count()),
      int(conn_->getSocketThroughput() * 1e3));

  // Send 6 messages 3 messages above idle-threshold 3 messages below
  // idle-threshold and make sure write throughput is used.
  payload_size = 305 / 3;
  settings_.socket_idle_threshold = payload_size - 1;
  auto time_slice = settings_.socket_health_check_period.count() / 6;
  total_msg_size = 0;
  for (int i = 1; i <= 3; ++i) {
    msg = std::make_unique<VarLengthTestMessage>(
        Compatibility::MIN_PROTOCOL_SUPPORTED, payload_size);
    total_msg_size += msg->size();
    e = conn_->registerMessage(std::move(msg));
    conn_->releaseMessage(*e);
    cur_time_ += std::chrono::milliseconds(time_slice);
    ev_base_folly_.loopOnce();
    writeSuccess();
  }
  for (int i = 1; i <= 3; ++i) {
    msg = std::make_unique<VarLengthTestMessage>(
        Compatibility::MIN_PROTOCOL_SUPPORTED, 1);
    total_msg_size += msg->size();
    e = conn_->registerMessage(std::move(msg));
    conn_->releaseMessage(*e);
    cur_time_ += std::chrono::milliseconds(time_slice);
    ev_base_folly_.loopOnce();
    writeSuccess();
  }
  conn_->checkSocketHealth();
  EXPECT_GT(int(conn_->getSocketThroughput() * 1e3), 0);
  EXPECT_EQ(
      int(total_msg_size * 1e3 / settings_.socket_health_check_period.count()),
      int(conn_->getSocketThroughput() * 1e3));
  // Send 6 messages with alternating busy and idle times and make sure
  // calculated throughput is correct.
  // Using same values for payload_size, time_slice and total_msg_size.
  for (int i = 1; i <= 3; ++i) {
    msg = std::make_unique<VarLengthTestMessage>(
        Compatibility::MIN_PROTOCOL_SUPPORTED, payload_size);
    e = conn_->registerMessage(std::move(msg));
    conn_->releaseMessage(*e);
    cur_time_ += std::chrono::milliseconds(time_slice);
    ev_base_folly_.loopOnce();
    writeSuccess();
    msg = std::make_unique<VarLengthTestMessage>(
        Compatibility::MIN_PROTOCOL_SUPPORTED, 1);
    e = conn_->registerMessage(std::move(msg));
    conn_->releaseMessage(*e);
    cur_time_ += std::chrono::milliseconds(time_slice);
    ev_base_folly_.loopOnce();
    writeSuccess();
  }
  conn_->checkSocketHealth();
  EXPECT_GT(int(conn_->getSocketThroughput() * 1e3), 0);
  EXPECT_EQ(
      int(total_msg_size * 1e3 / settings_.socket_health_check_period.count()),
      int(conn_->getSocketThroughput() * 1e3));
}

TEST_F(ClientConnectionTest, SenderBytesPendingTest) {
  std::unique_ptr<folly::IOBuf> hello_buf;
  ON_CALL(*sock_, connect_(_, _, _, _, _))
      .WillByDefault(SaveArg<0>(&conn_callback_));
  ON_CALL(*sock_, writeChain_(_, _, _))
      .WillByDefault(
          Invoke([this, &hello_buf](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
            wr_callback_ = cb;
            hello_buf.reset(buf);
          }));
  ON_CALL(*sock_, setReadCB(_)).WillByDefault(SaveArg<0>(&rd_callback_));
  EXPECT_EQ(conn_->connect(), 0);

  CHECK_SERIALIZEQ(MessageType::HELLO);
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ();
  ev_base_folly_.loopOnce();
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ();

  receiveAckMessage();
  CHECK_SERIALIZEQ();
  EXPECT_TRUE(handshaken());

  // Send a message that requires a protocol >= 3.
  auto raw_msg = new VarLengthTestMessage(3 /* min_proto */, 42 /* size */);
  std::unique_ptr<facebook::logdevice::Message> msg(raw_msg);
  auto msg_size_max_proto = msg->size();
  auto msg_size_at_proto = msg->size(conn_->getInfo().protocol.value());
  auto envelope = conn_->registerMessage(std::move(msg));
  // Message cost at max compatibility is added at registerMessage.
  EXPECT_EQ(bytes_pending_, msg_size_max_proto);
  conn_->releaseMessage(*envelope);
  // Now message is added into the sendq and Connection::sendChain_ which
  // will lead to double counting.
  EXPECT_EQ(bytes_pending_, msg_size_max_proto + msg_size_at_proto);
  ev_base_folly_.loopOnce();
  // Message is now added into the asyncsocket and removed from sendq.
  EXPECT_EQ(bytes_pending_, msg_size_at_proto);
  writeSuccess();
  // Message written into tcp socket.
  EXPECT_EQ(bytes_pending_, 0);
}

TEST_F(ClientConnectionTest, ShouldDetectChangesInAddressForConnection) {
  using namespace configuration::nodes;

  const Sockaddr kTestAddress1 = conn_->getInfo().peer_address;
  const Sockaddr kTestAddress2 = Sockaddr{get_localhost_address_str(), 4441};
  const int server_nidx = server_name_.index();
  const int other_server_nidx = server_nidx + 1;
  const auto generation = server_name_.generation();
  const auto metadata_replication_property =
      ReplicationProperty{{NodeLocationScope::NODE, 1}};

  // setup nodes configuration to contain server_name_
  Configuration::Nodes nodes;
  for (auto nidx : {server_nidx, other_server_nidx}) {
    nodes[nidx].generation = generation;
    nodes[nidx].addSequencerRole();
    nodes[nidx].addStorageRole(2);
  }
  nodes_configuration_ = NodesConfigurationTestUtil::provisionNodes(
      nodes, metadata_replication_property);

  EXPECT_CALL(*deps_, getNodeSockaddr(_, _, _))
      .WillRepeatedly(testing::ReturnRef(kTestAddress1));

  EXPECT_FALSE(conn_->isNodeConnectionAddressOrGenerationOutdated())
      << "Initial config should have correct address";

  EXPECT_CALL(*deps_, getNodeSockaddr(_, _, _))
      .WillRepeatedly(testing::ReturnRef(kTestAddress2));
  EXPECT_TRUE(conn_->isNodeConnectionAddressOrGenerationOutdated())
      << "Changing node address in config should result in outdated address";

  EXPECT_CALL(*deps_, getNodeSockaddr(_, _, _))
      .WillRepeatedly(testing::ReturnRef(kTestAddress1));
  EXPECT_FALSE(conn_->isNodeConnectionAddressOrGenerationOutdated())
      << "Initial config should have correct address";

  nodes[server_nidx].generation = generation + 1;
  nodes_configuration_ = NodesConfigurationTestUtil::provisionNodes(
      nodes, metadata_replication_property);
  EXPECT_TRUE(conn_->isNodeConnectionAddressOrGenerationOutdated())
      << "Change in generation should be detected";

  // replace config by one without server
  nodes.erase(server_nidx);
  nodes_configuration_ = NodesConfigurationTestUtil::provisionNodes(
      nodes, metadata_replication_property);

  EXPECT_TRUE(conn_->isNodeConnectionAddressOrGenerationOutdated())
      << "Removing node from config should make connection have outdated "
         "address.";
}
