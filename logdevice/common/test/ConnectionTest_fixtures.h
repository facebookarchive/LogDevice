/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <map>
#include <queue>

#include <gtest/gtest.h>

#include "logdevice/common/Connection.h"
#include "logdevice/common/Envelope.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/test/EvBaseMock.h"
#include "logdevice/common/protocol/ACK_Message.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/protocol/MessageDeserializers.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/MockSocketAdapter.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {
class ConnectionTest;

////////////////////////////////////////////////////////////////////////////////
// TestSocketDependencies

class TestSocketDependencies : public SocketDependencies {
 public:
  explicit TestSocketDependencies(ConnectionTest* owner)
      : SocketDependencies(nullptr, nullptr), owner_(owner) {}

  virtual const Settings& getSettings() const override;
  virtual StatsHolder* getStats() const override;
  virtual void noteBytesQueued(size_t nbytes,
                               PeerType peer_type,
                               folly::Optional<MessageType>) override;
  virtual void noteBytesDrained(size_t nbytes,
                                PeerType peer_type,
                                folly::Optional<MessageType>) override;
  virtual size_t getBytesPending() const override;
  virtual std::shared_ptr<folly::SSLContext> getSSLContext() const override;
  virtual bool shuttingDown() const override;
  virtual std::string dumpQueuedMessages(Address addr) const override;
  virtual const Sockaddr&
  getNodeSockaddr(NodeID node_id,
                  SocketType socket_type,
                  ConnectionType connection_type) override;
  EvBase* getEvBase() override;
  virtual SteadyTimestamp getCurrentTimestamp() override;
  virtual void onSent(std::unique_ptr<Message> msg,
                      const Address& to,
                      Status st,
                      const SteadyTimestamp enqueue_time,
                      Message::CompletionMethod cm) override;
  Message::Disposition
  onReceived(Message* msg,
             const Address& from,
             std::shared_ptr<PrincipalIdentity> principal,
             ResourceBudget::Token resource_token) override;
  virtual void processDeferredMessageCompletions() override;
  virtual NodeID getMyNodeID() override;
  virtual ResourceBudget& getConnBudgetExternal() override;
  virtual std::string getClusterName() override;
  virtual const std::string& getHELLOCredentials() override;
  virtual const std::string& getCSID() override;
  virtual bool includeHELLOCredentials() override;
  virtual std::string getClientBuildInfo() override;
  virtual bool authenticationEnabled() override;
  virtual void onStartedRunning(RunContext context) override;
  virtual void onStoppedRunning(RunContext prev_context) override;
  ResourceBudget::Token getResourceToken(size_t payload_size) override;
  virtual int setSoMark(int fd, uint32_t so_mark) override;
  virtual int getTCPInfo(TCPInfo*, int fd) override;

  NodeID getDestinationNodeID();

  virtual folly::Executor* getExecutor() const override;

  virtual ~TestSocketDependencies() {}

  ConnectionTest* owner_;
};

////////////////////////////////////////////////////////////////////////////////
// Test fixtures
class ConnectionTest : public ::testing::Test {
 public:
  ConnectionTest()
      : settings_(create_default_settings<Settings>()),
        server_name_(0, 1),
        server_addr_(get_localhost_address_str(), 4440),
        destination_node_id_(client_id_, 1),
        flow_group_(
            std::make_unique<NwShapingFlowGroupDeps>(nullptr, nullptr)) {}

  ~ConnectionTest() {
    EXPECT_EQ(bytes_pending_, 0);
  }

  using SentMsg = std::tuple<MessageType, Status>;

  void triggerHandshakeTimeout() {
    conn_->onHandshakeTimeout();
  }

  const Connection::EnvelopeQueue& getSerializeq() const {
    return conn_->serializeq_;
  }
  const Connection::EnvelopeQueue& getSendq() const {
    return conn_->sendq_;
  }

  bool connected() const {
    return conn_->connected_;
  }
  bool handshaken() const {
    return conn_->handshaken_;
  }

  int getDscp();

  bool usedSinceLastCheck() {
    bool used = !conn_->isIdleAfter(last_usage_check_time_);
    last_usage_check_time_ = SteadyTimestamp::now();
    return used;
  }

  const int client_id_{1};
  const uint16_t max_proto_ = Compatibility::MAX_PROTOCOL_SUPPORTED;

  Settings settings_;
  NodeID server_name_;
  Sockaddr server_addr_;  // stays invalid on a client.
  NodeID source_node_id_; // stays invalid on a client.
  NodeID destination_node_id_;
  std::string cluster_name_{"ConnectionTest_cluster"};
  std::string credentials_{"ConnectionTest_credentials"};
  std::string csid_;
  std::string client_build_info_{"{}"};
  FlowGroup flow_group_;
  EvBase ev_base_folly_;
  SteadyTimestamp cur_time_{SteadyTimestamp::now()};

  // Updated when Connection calls noteBytesQueued/noteBytesDrained.
  size_t bytes_pending_{0};
  // Keep track of all calls to onSent.
  std::queue<SentMsg> sent_;

  // Keep track of socket flow stats.
  TCPInfo socket_flow_stats_;

  ResourceBudget conn_budget_external_{std::numeric_limits<uint64_t>::max()};
  ResourceBudget incoming_message_bytes_limit_{
      std::numeric_limits<uint64_t>::max()};
  // On returning ERROR or NORMAL, the message onReceived is not invoked but the
  // status is returned to caller for appropriate processing. On returning KEEP
  // the message is forwarded to msg->onReceived for further processing.
  std::function<Message::Disposition(Message*,
                                     const Address&,
                                     std::shared_ptr<PrincipalIdentity>,
                                     ResourceBudget::Token)>
      on_received_hook_;

  std::unique_ptr<Connection> conn_;
  SocketDependencies* deps_;

  testing::NiceMock<MockSocketAdapter>* sock_;
  folly::AsyncSocket::WriteCallback* wr_callback_;
  folly::AsyncSocket::ReadCallback* rd_callback_;
  bool tamper_checksum_;
  bool socket_closed_{false};
  SteadyTimestamp last_usage_check_time_{SteadyTimestamp::min()};
};

class ClientConnectionTest : public ConnectionTest {
 public:
  ClientConnectionTest() : connect_throttle_({1, 1000}) {
    deps_ = new TestSocketDependencies(this);
    auto sock = std::make_unique<testing::NiceMock<MockSocketAdapter>>();
    sock_ = sock.get();
    ev_base_folly_.selectEvBase(EvBase::FOLLY_EVENTBASE);
    conn_ =
        std::make_unique<Connection>(server_name_,
                                     SocketType::DATA,
                                     ConnectionType::PLAIN,
                                     flow_group_,
                                     std::unique_ptr<SocketDependencies>(deps_),
                                     std::move(sock));
    csid_ = "client_uuid";
    EXPECT_FALSE(connected());
    EXPECT_FALSE(handshaken());
    conn_->setConnectThrottle(&connect_throttle_);
  }

  void SetUp() override {
    ON_CALL(*sock_, good()).WillByDefault(::testing::Return(!socket_closed_));
  }

  void writeSuccess() {
    wr_callback_->writeSuccess();
    ev_base_folly_.loopOnce();
  }

  void
  receiveAckMessage(Status st = E::OK,
                    facebook::logdevice::Message::Disposition disp =
                        facebook::logdevice::Message::Disposition::NORMAL,
                    uint16_t proto = Compatibility::MAX_PROTOCOL_SUPPORTED);
  ~ClientConnectionTest() override {
    conn_.reset();
    EXPECT_EQ(bytes_pending_, 0);
  }

  folly::AsyncSocket::ConnectCallback* conn_callback_;
  ConnectThrottle connect_throttle_;
};

////////////////////////////////////////////////////////////////////////////////
// Test messages.

/**
 * Useful class for being able to send a fixed size message of an existing type
 * (ACK for instance), but we mock the onReceived() callback to provide our own
 * version which returns an error code defined as a template parameter.
 */
template <class Header,
          MessageType MESSAGE_TYPE,
          TrafficClass TRAFFIC_CLASS,
          Message::Disposition DISP = Message::Disposition::NORMAL,
          Status STATUS = E::OK>
class TestFixedSizeMessage
    : public FixedSizeMessage<Header, MESSAGE_TYPE, TRAFFIC_CLASS> {
 public:
  explicit TestFixedSizeMessage(const Header& header)
      : FixedSizeMessage<Header, MESSAGE_TYPE, TRAFFIC_CLASS>(header) {}

  static MessageReadResult deserialize(ProtocolReader& reader) {
    std::unique_ptr<TestFixedSizeMessage> m(
        new TestFixedSizeMessage<Header,
                                 MESSAGE_TYPE,
                                 TRAFFIC_CLASS,
                                 DISP,
                                 STATUS>());
    reader.read(const_cast<Header*>(&m->header_));
    return reader.resultMsg(std::move(m));
  }

  Message::Disposition onReceived(const Address& /*from*/) override {
    err = STATUS;
    return DISP;
  }

 private:
  TestFixedSizeMessage() {}
};

struct EmptySocketCallback : public SocketCallback {
  void operator()(Status /*unused*/, const Address& /*unused*/) override {}
};

// Various message types we can use for ACK and HELLO messages.

using TestACK_Message =
    TestFixedSizeMessage<ACK_Header, MessageType::ACK, TrafficClass::HANDSHAKE>;

// An ACK message whose onReceive() method fails with E::PROTONOSUPPORT.
using TestACK_MessageProtoNoSupport =
    TestFixedSizeMessage<ACK_Header,
                         MessageType::ACK,
                         TrafficClass::HANDSHAKE,
                         Message::Disposition::ERROR,
                         E::PROTONOSUPPORT>;

/**
 * Use this class to create a message that has a different length depending on
 * the protocol passed to serialize.
 */
class VarLengthTestMessage : public Message {
 public:
  // Pretend to be TEST_Message. This will make the recipient deserialize and
  // process a TEST_Message instead of a VarLengthTestMessage.
  VarLengthTestMessage(uint16_t min_proto,
                       size_t size,
                       TrafficClass tc = TrafficClass::RECOVERY)
      : Message(MessageType::TEST, tc), min_proto_(min_proto) {
    size_map_[Compatibility::MAX_PROTOCOL_SUPPORTED] = size;
  }

  /**
   * serialize will use the size passed in the constructor by default.
   * Tests can use this function to define a protocol number up to which the
   * size of the message should be different.
   *
   * Example:
   * auto msg = new VarLengthTestMessage(2, 42);
   * msg->setSize(3, 30);
   * msg->setSize(4, 40);
   *
   * - For protocols in range [0, 2], connection will reject the message;
   * - In range [2, 3], the message will have size 30;
   * - In range [4, 4], the message will have size 40;
   * - In range [5, MAX_PROTOCOL_SUPPORTED], it will have size 42.
   */
  VarLengthTestMessage* setSize(uint16_t proto, size_t sz) {
    size_map_[proto] = sz;
    return this;
  }

  void serialize(ProtocolWriter& writer) const override {
    uint16_t proto = writer.proto();
    EXPECT_LE(proto, Compatibility::MAX_PROTOCOL_SUPPORTED);
    // Determine the size this message should have.
    size_t sz = 0;
    for (auto& p : size_map_) {
      if (p.first > proto) {
        break;
      }
      sz = p.second;
    }

    // Send a dummy payload.
    std::string payload(sz, ' ');
    writer.writeVector(payload);
  }

  static MessageReadResult deserialize(ProtocolReader& reader) {
    const size_t size = reader.bytesRemaining();
    std::unique_ptr<char[]> buf(new char[size]);
    reader.read(buf.get(), size);
    return reader.result([&] { return new VarLengthTestMessage(0, size); });
  }

  uint16_t getMinProtocolVersion() const override {
    return min_proto_;
  }

  void setOnSent(folly::Function<void(Status, const Address&) const> f) {
    message_onSent_ = std::move(f);
  }

  void onSent(Status st, const Address& to) const override {
    if (message_onSent_) {
      message_onSent_(st, to);
    }
  }

  Message::Disposition onReceived(const Address& /*from*/) override {
    return Message::Disposition::NORMAL;
  }

 private:
  folly::Function<void(Status, const Address&) const> message_onSent_;
  uint16_t min_proto_;
  std::map<uint8_t, size_t> size_map_;
};

////////////////////////////////////////////////////////////////////////////////
// Macros

/**
 * Check that the content of an Envelope queue on a Connection.
 */
#define CHECK_CONNQ(queue, ...)                                          \
  {                                                                      \
    std::vector<MessageType> expected_q = {__VA_ARGS__};                 \
    std::vector<MessageType> actual_q((queue).size());                   \
    std::transform((queue).begin(),                                      \
                   (queue).end(),                                        \
                   actual_q.begin(),                                     \
                   [](const Envelope& e) { return e.message().type_; }); \
    ASSERT_EQ(expected_q, actual_q);                                     \
  }

#define CHECK_PENDINGQ(...) CHECK_CONNQ(getPendingq(), __VA_ARGS__)
#define CHECK_SERIALIZEQ(...) CHECK_CONNQ(getSerializeq(), __VA_ARGS__)
#define CHECK_SENDQ(...) CHECK_CONNQ(getSendq(), __VA_ARGS__)

#define CHECK_ON_SENT(type, status)              \
  {                                              \
    ASSERT_TRUE(!sent_.empty());                 \
    const auto expected = SentMsg{type, status}; \
    EXPECT_EQ(expected, sent_.front());          \
    sent_.pop();                                 \
  }

#define CHECK_NO_MESSAGE_SENT() ASSERT_TRUE(sent_.empty())

}} // namespace facebook::logdevice
