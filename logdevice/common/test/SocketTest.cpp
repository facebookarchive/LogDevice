/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/protocol/CHECK_NODE_HEALTH_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"
#include "logdevice/common/test/SocketTest_fixtures.h"

namespace facebook { namespace logdevice {

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

// Verify that HELLO gets serialized as soon as we connect and other messages as
// soon as we handshake.
TEST_F(ClientSocketTest, SerializationStages) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);
  CHECK_SENDQ();
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();
  ACK_Header ackhdr{0, request_id_t(0), client_id_, max_proto_, E::OK};
  receiveMsg(new TestACK_Message(ackhdr));
  CHECK_SERIALIZEQ();
  CHECK_SENDQ(MessageType::GET_SEQ_STATE);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::OK);
  CHECK_SERIALIZEQ();
  CHECK_SENDQ();
}

// A client sends HELLO, we respond ACK with PROTONOSUPPORT. Verify that
// enqueued messages are rejected with E::PROTONOSUPPORT.
TEST_F(ClientSocketTest, BadProto) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();
  ACK_Header ackhdr{0, request_id_t(0), client_id_, 0, E::OK};
  receiveMsg(new TestACK_MessageProtoNoSupport(ackhdr));
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PROTONOSUPPORT);
  CHECK_SENDQ();
}

namespace {
class TestHELLO_Message : public HELLO_Message {
 public:
  using HELLO_Message::HELLO_Message;
  // SocketTest::receiveMsg() has a hack that will invoke this deserializer
  static MessageReadResult deserialize(ProtocolReader& reader) {
    HELLO_Header hdr;
    reader.read(&hdr);
    EXPECT_TRUE(reader.ok());
    return reader.result([&] { return new TestHELLO_Message(hdr); });
  }
  Disposition onReceived(const Address& /*from*/) override {
    return Disposition::NORMAL;
  }
};
} // namespace

// Verify that handshake works for a server Socket.
TEST_F(ServerSocketTest, Handshake) {
  // Simulate HELLO to be received by the server.
  HELLO_Header hdr{
      uint16_t(max_proto_), uint16_t(max_proto_), 0, request_id_t(0), {}};
  receiveMsg(new TestHELLO_Message(hdr));
  // Simulate the server replying ACK.
  ACK_Header ackhdr{
      0, request_id_t(0), client_id_, uint16_t(max_proto_), E::OK};
  std::unique_ptr<Message> msg = std::make_unique<ACK_Message>(ackhdr);
  auto envelope = socket_->registerMessage(std::move(msg));
  socket_->releaseMessage(*envelope);
  // We should be handshaken now.
  EXPECT_TRUE(handshaken());
}

TEST_F(ClientSocketTest, GetDscp) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  socket_->setDSCP(4);
  EXPECT_EQ(4 << 2, SocketTest::getDscp());
}

// Verify that PEER CLOSED on server socket is translated to
// E::SHUTDOWN when peer_shuttingdown flag is set.
TEST_F(ClientSocketTest, PeerShutdown) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Socket is connected, HELLO can be serialized.
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);

  // Simulate the socket closing.
  // onSent(st=E::SHUTDOWN) should be called for GET_SEQ_STATE.
  socket_->setPeerShuttingDown();
  triggerEventEOF();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::SHUTDOWN);
}

// A message is enqueued in the Socket but finally it is rejected once handshake
// completes with a protocol that's incompatible with that message.
TEST_F(ClientSocketTest, MessageRejectedAfterHandshakeInvalidProtocol) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message that requires a protocol >= MIN_PROTOCOL_SUPPORTED+1
  std::unique_ptr<Message> msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED + 1 /* min_proto+1 */,
      1 /* size */);
  auto envelope = socket_->registerMessage(std::move(msg));
  socket_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // The socket connects, HELLO is serialized.
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);

  // HELLO is sent.
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // The server sends back ACK with min protocol supported
  ACK_Header ackhdr{0,
                    request_id_t(0),
                    client_id_,
                    Compatibility::MIN_PROTOCOL_SUPPORTED,
                    E::OK};
  receiveMsg(new TestACK_Message(ackhdr));

  // The message required protocol >= 6, so it is finally rejected (removed from
  // serializeq_) before it could be serialized.
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PROTONOSUPPORT);
  CHECK_SERIALIZEQ();
  CHECK_SENDQ();
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
TEST_F(ClientSocketTest, MessageChangesSizeAfterHanshake) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message that requires a protocol >= 3.
  // Its size is 21 for protocols in range [3, 5] and 42 in range [6, MAX].
  auto raw_msg = new VarLengthTestMessage(3 /* min_proto */, 42 /* size */);
  raw_msg->setSize(5, 21);
  std::unique_ptr<Message> msg(raw_msg);
  auto envelope = socket_->registerMessage(std::move(msg));
  socket_->releaseMessage(*envelope);
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // The socket connects, HELLO is serialized.
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);

  // HELLO is sent.
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // The server sends back ACK with min protocol
  ACK_Header ackhdr{0,
                    request_id_t(0),
                    client_id_,
                    Compatibility::MIN_PROTOCOL_SUPPORTED,
                    E::OK};
  receiveMsg(new TestACK_Message(ackhdr));

  // We completed handshake. GET_SEQ_STATE is serialized.
  CHECK_SENDQ(MessageType::GET_SEQ_STATE);
  CHECK_SERIALIZEQ();
}

// Verify that a client socket retries connect attempts. Any message that was
// sent by the user and enqueued in serializeq_ in the mean time should be sent
// once we succeed.
TEST_F(ClientSocketTest, ConnectionRetries) {
  settings_.connection_retries = 2;
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Attempt 1 fails.
  EXPECT_EQ(1, connection_attempts_);
  triggerConnectAttemptTimeout();
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Attempt 2 fails.
  EXPECT_EQ(2, connection_attempts_);
  triggerConnectAttemptTimeout();
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Attempt 3 succeeds. HELLO should be serialized.
  EXPECT_EQ(3, connection_attempts_);
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);
}

// Verify the behavior when up to --connection-retries connect attempts failed.
// Enqueued messages should have their onSent(st=E::TIMEDOUT) called.
TEST_F(ClientSocketTest, ConnectionTimeout) {
  settings_.connection_retries = 2;
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Attempt 1 fails.
  EXPECT_EQ(1, connection_attempts_);
  triggerConnectAttemptTimeout();
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Attempt 2 fails.
  EXPECT_EQ(2, connection_attempts_);
  triggerConnectAttemptTimeout();
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Attempt 3 fails.
  EXPECT_EQ(3, connection_attempts_);
  triggerConnectAttemptTimeout();
  // onSent(st=E::TIMEDOUT) should be called for HELLO and GET_SEQ_STATE.
  CHECK_ON_SENT(MessageType::HELLO, E::TIMEDOUT);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::TIMEDOUT);
}

// Verify the behavior when handshake timeout happens (the socket successfully
// connected but ACK could not be received in time).
// Enqueued messages should have their onSent(st=E::TIMEDOUT) called.
TEST_F(ClientSocketTest, HandshakeTimeout) {
  settings_.connection_retries = 2;
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Socket is connected, HELLO can be serialized.
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);

  // simulate the handshake timeout timer to trigger.
  triggerHandshakeTimeout();
  // onSent(st=E::TIMEDOUT) should be called for GET_SEQ_STATE.
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::TIMEDOUT);
}

// A message is sent but the Socket cannot connect because the connection is
// unroutable.
TEST_F(ClientSocketTest, Unroutable) {
  // If buffereventSocketConnect returns -1 with err set to ENETUNREACH,
  // connect() should fail immediately with E::UNROUTABLE.
  setNextConnectAttempsStatus(ENETUNREACH);
  int rv = socket_->connect();
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::UNROUTABLE, err);
}

// very unlikely situation:
// Similar to the previous test, but this time this happens after a connection
// retry and a message was enqueued in serializeq_.
// onSent(st=E::UNROUTABLE) should be called on that message.
// TODO(T6307427): fix this and re-enable this test.
TEST_F(ClientSocketTest, DISABLED_UnroutableAfterOneTry) {
  settings_.connection_retries = 2;
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // Attempt 1 fails. Socket should try again but this time we get ENETUNREACH.
  EXPECT_EQ(1, connection_attempts_);
  setNextConnectAttempsStatus(ENETUNREACH);
  triggerConnectAttemptTimeout();

  // The enqueued message should get E::UNROUTABLE.
  CHECK_ON_SENT(MessageType::HELLO, E::UNROUTABLE);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::UNROUTABLE);
}

// A message is sent but the socket is closed.
TEST_F(ClientSocketTest, ConnectionResetByPeer) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Socket is connected, HELLO can be serialized.
  triggerEventConnected();
  CHECK_SERIALIZEQ(MessageType::GET_SEQ_STATE);
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);

  // Simulate the socket closing.
  // onSent(st=E::PEER_CLOSED) should be called for GET_SEQ_STATE.
  triggerEventEOF();
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::PEER_CLOSED);
}

// The socket cannot connect. Enqueued messages should have
// onSent(st=E::CONNFAILED) called.
TEST_F(ClientSocketTest, ConnFailed) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send a message.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // HELLO will be serialized once we are connected.
  CHECK_SERIALIZEQ(MessageType::HELLO, MessageType::GET_SEQ_STATE);
  CHECK_SENDQ();

  // Trigger an error. The messages should have onSent(st=E::CONNFAILED) called.
  triggerEventError(BEV_EVENT_READING);
  CHECK_ON_SENT(MessageType::HELLO, E::CONNFAILED);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::CONNFAILED);
}

// Basic test to verify that onSent() for a serialized message is called exactly
// when all of its bytes have been removed from the Socket's output evbuffer.
TEST_F(ClientSocketTest, DrainPosAccounting) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Send GET_SEQ_STATE.
  auto envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  size_t get_seq_state_sz = envelope->message().size(max_proto_) -
      (ProtocolHeader::needChecksumInHeader(
           MessageType::GET_SEQ_STATE, max_proto_)
           ? 0
           : sizeof(ProtocolHeader::cksum));
  socket_->releaseMessage(*envelope);

  // Send another one.
  envelope = create_message(*socket_);
  ASSERT_NE(envelope, nullptr);
  socket_->releaseMessage(*envelope);

  // Complete the handshake part.
  triggerEventConnected();
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  ACK_Header ackhdr{0, request_id_t(0), client_id_, max_proto_, E::OK};
  receiveMsg(new TestACK_Message(ackhdr));

  // Now two GET_SEQ_STATE messages need to be serialized.
  CHECK_SERIALIZEQ();
  CHECK_SENDQ(MessageType::GET_SEQ_STATE, MessageType::GET_SEQ_STATE);

  // Flush all of the last message minus 1 byte.
  dequeueBytesFromOutputEvbuffer(get_seq_state_sz - 1);
  CHECK_NO_MESSAGE_SENT();
  // Flush 1 more byte. GET_SEQ_STATE should be sent.
  dequeueBytesFromOutputEvbuffer(1);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::OK);
  // Flush one more byte.
  dequeueBytesFromOutputEvbuffer(1);
  CHECK_NO_MESSAGE_SENT();
  // Flush the rest of the bytes.
  dequeueBytesFromOutputEvbuffer(get_seq_state_sz - 1);
  CHECK_ON_SENT(MessageType::GET_SEQ_STATE, E::OK);
}

TEST_F(ClientSocketTest, DownRevEvbufferAccounting) {
  // Verify that sending down-protocol messages does not break
  // evbuffer space accounting.
  const uint16_t test_proto_ver = max_proto_ - 1;
  const size_t msg_test_proto_size = 5;
  const size_t msg_max_proto_size = 50;

  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  // Complete the handshake part.
  triggerEventConnected();
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  ACK_Header ackhdr{0, request_id_t(0), client_id_, test_proto_ver, E::OK};
  receiveMsg(new TestACK_Message(ackhdr));
  ASSERT_EQ(0, bytes_pending_);

  auto msg = std::make_unique<VarLengthTestMessage>(
      Compatibility::MIN_PROTOCOL_SUPPORTED, msg_max_proto_size);
  msg->setSize(test_proto_ver, msg_test_proto_size);
  // max_proto_ should be MAX_PROTOCOL_SUPPORTED.
  msg->setSize(max_proto_, msg_max_proto_size);

  size_t protohdr_size_for_max_proto =
      ProtocolHeader::bytesNeeded(msg->getType(), max_proto_);
  size_t protohdr_size_for_test_proto =
      ProtocolHeader::bytesNeeded(msg->getType(), test_proto_ver);
  auto* envelope = socket_->registerMessage(std::move(msg));

  // Queued messages are accounted assuming MAX_PROTOCOL_SUPPORTED.
  // Therefore, we expect full ProtocolHeader
  ASSERT_EQ(bytes_pending_, msg_max_proto_size + sizeof(ProtocolHeader));

  // Serialize to the evbuffer. This will add the serialization cost
  // to the queued cost.
  socket_->releaseMessage(*envelope);
  ASSERT_EQ(bytes_pending_,
            msg_max_proto_size + msg_test_proto_size +
                protohdr_size_for_max_proto + protohdr_size_for_test_proto);

  // Send the message. Both the bytes consumed in the evbuffer and the
  // "queued cost" should be released.
  flushOutputEvBuffer();
  ASSERT_EQ(0, bytes_pending_);
}

// Verify if the client rejects any messages that exceeds
// Message::MAX_LEN + sizeof(PayloadHeader)
TEST_F(ClientSocketTest, MaxLenRejected) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  CHECK_SENDQ();
  CHECK_SERIALIZEQ(MessageType::HELLO);
  triggerEventConnected();
  CHECK_SERIALIZEQ();
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ();
  CHECK_SENDQ();
  ACK_Header ackhdr{0, request_id_t(0), client_id_, max_proto_, E::OK};
  receiveMsg(new TestACK_Message(ackhdr));
  CHECK_SERIALIZEQ();
  CHECK_SENDQ();

  // Receive a message that's small enough to be received
  receiveMsg(new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, Message::MAX_LEN));
  ld_check(!socket_->isClosed());
  // Messages too big cause the socket to be closed
  receiveMsg(new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, Message::MAX_LEN + 1));
  ld_check(socket_->isClosed());
}

TEST_F(ClientSocketTest, CloseConnectionOnProtocolChecksumMismatch) {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  CHECK_SENDQ();
  CHECK_SERIALIZEQ(MessageType::HELLO);
  triggerEventConnected();
  CHECK_SERIALIZEQ();
  CHECK_SENDQ(MessageType::HELLO);
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  CHECK_SERIALIZEQ();
  CHECK_SENDQ();
  ACK_Header ackhdr{0, request_id_t(0), client_id_, max_proto_, E::OK};
  receiveMsg(new TestACK_Message(ackhdr));
  CHECK_SERIALIZEQ();
  CHECK_SENDQ();

  // 1. Receiving a non-tampered message when checksumming is disabled,
  //    socket shouldn't be closed
  settings_.checksumming_enabled = false;
  socket_->enableChecksumTampering(false);
  receiveMsg(new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, Message::MAX_LEN));
  ld_check(!socket_->isClosed());

  // 2. Receiving a tampered message won't close socket since checksumming
  //    is disabled
  socket_->enableChecksumTampering(true);
  receiveMsg(new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, Message::MAX_LEN));
  ld_check(!socket_->isClosed());

  // 3. Receiving a non-tampered message, when checksumming is enabled,
  //    socket shouldn't be closed
  settings_.checksumming_enabled = true;
  socket_->enableChecksumTampering(false);
  receiveMsg(new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, Message::MAX_LEN));
  ld_check(!socket_->isClosed());

  // 4. Receive a tampered message, when checksumming is enabled,
  //    verify that socket gets closed
  socket_->enableChecksumTampering(true);
  receiveMsg(new VarLengthTestMessage(
      Compatibility::MIN_PROTOCOL_SUPPORTED, Message::MAX_LEN));
  ld_check(socket_->isClosed());
}
// Test that we can reconnect after error
TEST_F(ClientSocketTest, ReconnectPossible) {
  // If buffereventSocketConnect returns -1 with err set to ENETUNREACH,
  // connect() should fail immediately with E::UNROUTABLE.
  setNextConnectAttempsStatus(ENETUNREACH);
  int rv = socket_->connect();
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::UNROUTABLE, err);
  setNextConnectAttempsStatus(0);
  rv = socket_->connect();
  ASSERT_EQ(E::DISABLED, err);
  ASSERT_EQ(-1, rv);
  socket_->resetConnectThrottle();
  rv = socket_->connect();
  ASSERT_EQ(0, rv);
}

TEST_F(ServerSocketTest, IncomingMessageBytesLimitHandshake) {
  incoming_message_bytes_limit_.setLimit(0);
  // Simulate HELLO to be received by the server.
  HELLO_Header hdr{
      uint16_t(max_proto_), uint16_t(max_proto_), 0, request_id_t(0), {}};
  receiveMsg(new TestHELLO_Message(hdr));
  // Simulate the server replying ACK.
  ACK_Header ackhdr{
      0, request_id_t(0), client_id_, uint16_t(max_proto_), E::OK};
  std::unique_ptr<Message> msg = std::make_unique<ACK_Message>(ackhdr);
  auto envelope = socket_->registerMessage(std::move(msg));
  socket_->releaseMessage(*envelope);
  // We should be handshaken now.
  EXPECT_TRUE(handshaken());
}

TEST_F(ServerSocketTest, IncomingMessageBytesLimit) {
  incoming_message_bytes_limit_.setLimit(0);
  // Simulate HELLO to be received by the server.
  HELLO_Header hdr{
      uint16_t(max_proto_), uint16_t(max_proto_), 0, request_id_t(0), {}};
  receiveMsg(new TestHELLO_Message(hdr));
  // Simulate the server replying ACK.
  ACK_Header ackhdr{
      0, request_id_t(0), client_id_, uint16_t(max_proto_), E::OK};
  auto envelope =
      socket_->registerMessage(std::make_unique<ACK_Message>(ackhdr));
  socket_->releaseMessage(*envelope);
  // We should be handshaken now.
  EXPECT_TRUE(handshaken());

  // With limit zero and prev use as zero ResourceBudget allows a single message
  // even though we go beyond allowed limit.
  on_received_hook_ = [&](Message* msg,
                          const Address&,
                          std::shared_ptr<PrincipalIdentity>,
                          ResourceBudget::Token token) {
    ASSERT_TRUE(token.valid());
    ASSERT_FALSE(incoming_message_bytes_limit_.acquire(msg->size()));
    auto check_node_hdr = CHECK_NODE_HEALTH_Header{request_id_t(1), 1, 0};
    // Try sending another message, this message should fail to send with
    // ENOBUFS.
    auto new_msg = new TestFixedSizeMessage<CHECK_NODE_HEALTH_Header,
                                            MessageType::CHECK_NODE_HEALTH,
                                            TrafficClass::FAILURE_DETECTOR>(
        check_node_hdr);
    ev_timer_add_hook_ = [&](struct event* /* ev */) {
      if (err != E::OK) {
        ASSERT_EQ(err, E::NOBUFS);
      }
    };
    receiveMsg(new_msg);
  };

  auto check_node_hdr = CHECK_NODE_HEALTH_Header{request_id_t(1), 1, 0};
  auto msg =
      new TestFixedSizeMessage<CHECK_NODE_HEALTH_Header,
                               MessageType::CHECK_NODE_HEALTH,
                               TrafficClass::FAILURE_DETECTOR>(check_node_hdr);
  receiveMsg(msg);

  // Reset limit and make sure we do not get the callback.
  incoming_message_bytes_limit_.setLimit(std::numeric_limits<uint64_t>::max());
  msg =
      new TestFixedSizeMessage<CHECK_NODE_HEALTH_Header,
                               MessageType::CHECK_NODE_HEALTH,
                               TrafficClass::FAILURE_DETECTOR>(check_node_hdr);
  on_received_hook_ = nullptr;
  ev_timer_add_hook_ = [&](struct event* /* ev */) { ASSERT_EQ(err, E::OK); };
  receiveMsg(msg);
}

}} // namespace facebook::logdevice
