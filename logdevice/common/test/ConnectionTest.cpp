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
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"
#include "logdevice/common/test/MockSocketAdapter.h"
#include "logdevice/common/test/SocketTest_fixtures.h"

using ::testing::_;
using ::testing::Args;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::WithArg;

using namespace facebook::logdevice;

class ClientConnectionTest : public SocketTest {
 public:
  ClientConnectionTest() {
    deps_ = new TestSocketDependencies(this);
    auto sock = std::make_unique<testing::NiceMock<MockSocketAdapter>>();
    sock_ = sock.get();
    use_mock_evbase_ = false;
    ev_base_folly_.selectEvBase(EvBase::FOLLY_EVENTBASE);
    conn_ =
        std::make_unique<Connection>(server_name_,
                                     SocketType::DATA,
                                     ConnectionType::PLAIN,
                                     flow_group_,
                                     std::unique_ptr<SocketDependencies>(deps_),
                                     std::move(sock));
    socket_ = std::unique_ptr<Socket, SocketDeleter>(
        conn_.get(), SocketDeleter(true /* skip */));
    csid_ = "client_uuid";
    EXPECT_FALSE(connected());
    EXPECT_FALSE(handshaken());
  }

  void SetUp() override {
    ON_CALL(*sock_, good()).WillByDefault(Return(!socket_closed_));
  }

  void writeSuccess() {
    wr_callback_->writeSuccess();
    ev_base_folly_.loopOnce();
  }

  void receiveMessage(const facebook::logdevice::Message* msg,
                      uint16_t proto = Compatibility::MAX_PROTOCOL_SUPPORTED);

  void
  receiveAckMessage(Status st = E::OK,
                    facebook::logdevice::Message::Disposition disp =
                        facebook::logdevice::Message::Disposition::NORMAL,
                    uint16_t proto = Compatibility::MAX_PROTOCOL_SUPPORTED);
  ~ClientConnectionTest() override {}
  SocketDependencies* deps_;
  std::unique_ptr<Connection> conn_;
  testing::NiceMock<MockSocketAdapter>* sock_;
  folly::AsyncSocket::ConnectCallback* conn_callback_;
  folly::AsyncSocket::WriteCallback* wr_callback_;
  folly::AsyncSocket::ReadCallback* rd_callback_;
  bool tamper_checksum_;
  bool socket_closed_{false};
};

class ServerConnectionTest : public SocketTest {
 public:
  ServerConnectionTest() {
    settings_.server = true;
    source_node_id_ = server_name_;
    conn_ = std::make_unique<Connection>(
        42 /* fd */,
        ClientID(client_id_) /* client_name */,
        Sockaddr(get_localhost_address_str(), 4440) /* client_addr */,
        ResourceBudget::Token() /* accounting token, not used */,
        SocketType::DATA /* socket type */,
        ConnectionType::PLAIN,
        flow_group_,
        std::make_unique<TestSocketDependencies>(this));
  }
  ~ServerConnectionTest() {}
  std::unique_ptr<Connection> conn_;
};

TEST_F(ClientConnectionTest, ConnectTest) {
  EXPECT_CALL(*sock_, connect_(_, server_addr_.getSocketAddress(), _, _, _))
      .Times(1)
      .WillOnce(
          WithArg<0>(Invoke([](folly::AsyncSocket::ConnectCallback* conn_cb) {
            conn_cb->connectSuccess();
          })));
  EXPECT_CALL(*sock_, writeChain_(_, _, _))
      .WillOnce(Invoke([](folly::AsyncSocket::WriteCallback*,
                          folly::IOBuf* buf,
                          folly::WriteFlags) { delete buf; }));
  EXPECT_EQ(conn_->connect(), 0);
}

TEST_F(ClientConnectionTest, SendBuffers) {
  EXPECT_CALL(*sock_, connect_(_, server_addr_.getSocketAddress(), _, _, _))
      .Times(1)
      .WillOnce(
          WithArg<0>(Invoke([](folly::AsyncSocket::ConnectCallback* conn_cb) {
            conn_cb->connectSuccess();
          })));
  EXPECT_CALL(
      *sock_, writeChain_(NotNull(), NotNull(), folly::WriteFlags::NONE))
      .Times(2)
      .WillRepeatedly(Invoke([this](folly::AsyncSocket::WriteCallback* cb,
                                    folly::IOBuf* buf,
                                    folly::WriteFlags) {
        wr_callback_ = cb;
        delete buf;
      }));
  ON_CALL(*sock_, good()).WillByDefault(Return(true));

  EXPECT_EQ(conn_->connect(), 0);
  writeSuccess();
  auto iobuf = folly::IOBuf::create(10);
  iobuf->append(10);
  conn_->sendBuffer(std::move(iobuf));
  bytes_pending_ += 10;
  writeSuccess();
}

TEST_F(ClientConnectionTest, ReceiveBuffers) {
  EvBaseMock ev_base_mock(EvBase::MOCK_EVENTBASE);
  std::unique_ptr<ProtocolHandler> proto_handler =
      std::make_unique<ProtocolHandler>(conn_.get(), "", &ev_base_mock);
  MessageReader read_cb(*proto_handler, 1);
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
  EXPECT_CALL(*sock_, writeChain_(_, _, _))
      .WillOnce(Invoke([](folly::AsyncSocket::WriteCallback*,
                          folly::IOBuf* buf,
                          folly::WriteFlags) { delete buf; }));
  EXPECT_EQ(conn_->connect(), 0);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
}

void ClientConnectionTest::receiveMessage(
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
    if (tamper_checksum_) {
      hdr->cksum += 1;
    }
  }

  io_buf->prepend(hdr_size);

  ld_info(
      "Received Message size %lu serialized body len %lu", msg_size, bodylen);
  void* buffer;
  size_t len;
  rd_callback_->getReadBuffer(&buffer, &len);
  ld_info("len recv %lu, io_buf->length() %lu", len, io_buf->length());
  ASSERT_LE(hdr_size, len);
  memcpy(buffer, io_buf->data(), std::min(io_buf->length(), len));
  rd_callback_->readDataAvailable(std::min(io_buf->length(), len));
  io_buf->trimStart(std::min(io_buf->length(), len));

  if (io_buf->length() == 0) {
    return;
  }
  rd_callback_->getReadBuffer(&buffer, &len);
  ld_info("len recv %lu, io_buf->length() %lu", len, io_buf->length());
  // Protocol reader detected error
  if (len == 0) {
    return;
  }
  ASSERT_LE(io_buf->length(), len);
  memcpy(buffer, io_buf->data(), io_buf->length());
  rd_callback_->readDataAvailable(io_buf->length());
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
  receiveMessage(&msg, proto);
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
  writeSuccess();
  receiveAckMessage();
  EXPECT_TRUE(handshaken());
}
static Envelope* create_message(Socket& s) {
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
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  conn_callback_->connectSuccess();
  EXPECT_TRUE(connected());
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  receiveAckMessage();
  EXPECT_TRUE(handshaken());
  CHECK_SERIALIZEQ();

  writeSuccess();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::OK);
  CHECK_SERIALIZEQ();
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
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  receiveAckMessage(
      E::PROTONOSUPPORT, facebook::logdevice::Message::Disposition::ERROR);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PROTONOSUPPORT);
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
  auto envelope = socket_->registerMessage(std::move(msg));
  socket_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  receiveAckMessage(E::OK,
                    facebook::logdevice::Message::Disposition::NORMAL,
                    Compatibility::MIN_PROTOCOL_SUPPORTED);
  EXPECT_TRUE(handshaken());
  // The message required protocol >= 6, so it is finally rejected (removed
  // from serializeq_) before it could be serialized.
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PROTONOSUPPORT);
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
  auto envelope = socket_->registerMessage(std::move(msg));
  socket_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  // HELLO is sent.
  writeSuccess();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

  // The server sends back ACK with min protocol
  receiveAckMessage(E::OK,
                    facebook::logdevice::Message::Disposition::NORMAL,
                    Compatibility::MIN_PROTOCOL_SUPPORTED);

  // We completed handshake. GET_SEQ_STATE is serialized.

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
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

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
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  // Socket is connected, HELLO can be serialized.
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

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
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);

  // Socket is connected, HELLO can be serialized.
  conn_callback_->connectSuccess();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);

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
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

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
      ProtocolHeader::bytesNeeded(msg->getType(), test_proto_ver);
  auto* envelope = socket_->registerMessage(std::move(msg));

  // Queued messages are accounted assuming MAX_PROTOCOL_SUPPORTED.
  // Therefore, we expect full ProtocolHeader
  ASSERT_EQ(bytes_pending_, msg_max_proto_size + sizeof(ProtocolHeader));

  // Serialize to the evbuffer. This will add the serialization cost
  // to the queued cost.
  socket_->releaseMessage(*envelope);

  ASSERT_EQ(bytes_pending_, protohdr_size_for_test_proto + msg_test_proto_size);

  // Send the message. Both the bytes consumed in the evbuffer and the
  // "queued cost" should be released.
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
    EXPECT_EQ(msg->type_, MessageType::GET_SEQ_STATE);
    err = E::OK;
    called = true;
    return facebook::logdevice::Message::Disposition::NORMAL;
  };
  // Receive a message that's small enough to be received
  auto msg =
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED, max_size);
  receiveMessage(msg, Compatibility::MAX_PROTOCOL_SUPPORTED);
  delete msg;
  EXPECT_TRUE(called);
  // Messages too big cause the socket to be closed
  msg = new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, max_size + 1);
  receiveMessage(msg, Compatibility::MAX_PROTOCOL_SUPPORTED);
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
    EXPECT_EQ(msg->type_, MessageType::GET_SEQ_STATE);
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
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
  // 2. Receiving a tampered message won't close socket since checksumming
  //    is disabled
  tamper_checksum_ = true;
  receiveMessage(
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
  // 3. Receiving a non-tampered message, when checksumming is enabled,
  //    socket shouldn't be closed
  settings_.checksumming_enabled = true;
  tamper_checksum_ = false;
  receiveMessage(
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
  // 4. Receive a tampered message, when checksumming is enabled,
  //    verify that socket gets closed
  tamper_checksum_ = true;
  receiveMessage(
      new VarLengthTestMessage(Compatibility::MIN_PROTOCOL_SUPPORTED,
                               facebook::logdevice::Message::MAX_LEN),
      Compatibility::MAX_PROTOCOL_SUPPORTED);
  ev_base_folly_.loopOnce();
}
