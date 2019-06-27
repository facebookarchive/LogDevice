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

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event2/bufferevent_struct.h"
#include "event2/event.h"
#include "logdevice/common/Envelope.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ACK_Message.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/protocol/MessageDeserializers.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {
class SocketTest;

////////////////////////////////////////////////////////////////////////////////
// TestSocketDependencies

class TestSocketDependencies : public SocketDependencies {
 public:
  explicit TestSocketDependencies(SocketTest* owner)
      : SocketDependencies(nullptr, nullptr, Address()), owner_(owner) {}
  virtual const Settings& getSettings() const override;
  virtual StatsHolder* getStats() const override;
  virtual void noteBytesQueued(size_t nbytes) override;
  virtual void noteBytesDrained(size_t nbytes) override;
  virtual size_t getBytesPending() const override;
  virtual std::shared_ptr<folly::SSLContext>
  getSSLContext(bufferevent_ssl_state, bool) const override;
  virtual bool shuttingDown() const override;
  virtual std::string dumpQueuedMessages(Address addr) const override;
  virtual const Sockaddr& getNodeSockaddr(NodeID nid,
                                          SocketType type,
                                          ConnectionType conntype) override;
  virtual int eventAssign(struct event* ev,
                          void (*cb)(evutil_socket_t, short what, void* arg),
                          void* arg) override;
  virtual void eventActive(struct event* ev, int what, short ncalls) override;
  virtual void eventDel(struct event* ev) override;
  virtual int eventPrioritySet(struct event* ev, int priority) override;
  virtual int evtimerAssign(struct event* ev,
                            void (*cb)(evutil_socket_t, short what, void* arg),
                            void* arg) override;
  virtual void evtimerDel(struct event* ev) override;
  virtual int evtimerPending(struct event* ev, struct timeval* tv) override;
  virtual const struct timeval*
  getCommonTimeout(std::chrono::milliseconds t) override;
  virtual const timeval*
  getTimevalFromMilliseconds(std::chrono::milliseconds t) override;
  virtual const struct timeval* getZeroTimeout() override;
  virtual int evtimerAdd(struct event* ev,
                         const struct timeval* timeout) override;
  virtual struct bufferevent*
  buffereventSocketNew(int sfd,
                       int opts,
                       bool secure,
                       bufferevent_ssl_state ssl_state,
                       folly::SSLContext*) override;
  struct evbuffer* getOutput(struct bufferevent* bev) override;
  virtual struct evbuffer* getInput(struct bufferevent* bev) override;
  virtual int buffereventSocketConnect(struct bufferevent* bev,
                                       struct sockaddr* ss,
                                       int len) override;
  virtual void buffereventSetWatermark(struct bufferevent* bev,
                                       short events,
                                       size_t lowmark,
                                       size_t highmark) override;
  virtual void buffereventSetCb(struct bufferevent* bev,
                                bufferevent_data_cb readcb,
                                bufferevent_data_cb writecb,
                                bufferevent_event_cb eventcb,
                                void* cbarg) override;
  virtual void buffereventShutDownSSL(struct bufferevent* bev) override;
  virtual void buffereventFree(struct bufferevent* bev) override;
  virtual int evUtilMakeSocketNonBlocking(int sfd) override;
  virtual int buffereventSetMaxSingleWrite(struct bufferevent* bev,
                                           size_t size) override;
  virtual int buffereventSetMaxSingleRead(struct bufferevent* bev,
                                          size_t size) override;
  virtual int buffereventEnable(struct bufferevent* bev, short event) override;
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
  virtual void configureSocket(bool is_tcp,
                               int fd,
                               int* snd_out,
                               int* rcv_out,
                               sa_family_t sa_family,
                               const uint8_t default_dscp) override;
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

  NodeID getDestinationNodeID();

  virtual ~TestSocketDependencies() {}

  SocketTest* owner_;
};

////////////////////////////////////////////////////////////////////////////////
// Test fixtures

class SocketTest : public ::testing::Test {
 public:
  SocketTest()
      : settings_(create_default_settings<Settings>()),
        server_name_(0, 1),
        server_addr_(get_localhost_address_str(), 4440),
        destination_node_id_(client_id_, 1),
        flow_group_(std::make_unique<NwShapingFlowGroupDeps>(nullptr)) {
    socket_ = std::make_unique<Socket>(
        server_name_,
        SocketType::DATA,
        ConnectionType::PLAIN,
        flow_group_,
        std::make_unique<TestSocketDependencies>(this));
    input_ = LD_EV(evbuffer_new)();
    output_ = LD_EV(evbuffer_new)();
  }

  ~SocketTest() {
    socket_.reset(); // before freeing the evbuffers
    LD_EV(evbuffer_free)(input_);
    LD_EV(evbuffer_free)(output_);
  }

  using SentMsg = std::tuple<MessageType, Status>;

  void triggerEventConnected() {
    ASSERT_NE(nullptr, event_cb_);
    event_cb_(&bev_, BEV_EVENT_CONNECTED, (void*)socket_.get());
  }

  void triggerEventEOF() {
    ASSERT_NE(nullptr, event_cb_);
    event_cb_(&bev_, BEV_EVENT_EOF, (void*)socket_.get());
  }

  void triggerEventError(short dir) {
    ASSERT_NE(nullptr, event_cb_);
    event_cb_(&bev_, BEV_EVENT_ERROR | dir, (void*)socket_.get());
  }

  void triggerOnDataAvailable() {
    ASSERT_NE(nullptr, read_cb_);
    read_cb_(&bev_, (void*)socket_.get());
  }

  void triggerConnectAttemptTimeout() {
    socket_->onConnectAttemptTimeout();
  }

  void triggerHandshakeTimeout() {
    socket_->onHandshakeTimeout();
  }

  /**
   * Use this to make future calls to buffereventSocketConnect fail with the
   * given status code. Pass 0 if the future calls should succeed.
   */
  void setNextConnectAttempsStatus(int err) {
    next_connect_attempts_errno_ = err;
  }

  /**
   * Simulate the Socket receiving a message through its input evbuffer.
   */
  template <typename MSG>
  void receiveMsg(MSG* raw_msg) {
    // Hack:
    // Update the deserializer of the messages that we mock because we want to
    // ensure that Socket calls MSG::deserialize.
    messageDeserializers.set(raw_msg->type_, &MSG::deserialize);

    std::unique_ptr<Message> msg(raw_msg);
    // Calling msg->serialize() with a null output evbuffer to get the size of
    // the message without the protocol header
    ProtocolWriter wtmp(msg->type_, nullptr, socket_->proto_);
    msg->serialize(wtmp);
    auto bodylen = wtmp.result();
    EXPECT_TRUE(bodylen > 0);

    size_t protohdr_bytes =
        ProtocolHeader::bytesNeeded(msg->type_, socket_->proto_);

    // using checksum version because some tests like
    // ClientSocketTest.CloseConnectionOnProtocolChecksumMismatch
    // need to always run irrespective of the
    // "Settings::checksumming_enabled" value
    socket_->serializeMessageWithChecksum(
        *msg, protohdr_bytes + bodylen, input_);

    triggerOnDataAvailable();
  }

  /**
   * Dequeue some bytes from the Socket's output evbuffer.
   *
   * @param nbytes number of bytes to remove. This function verifies that this
   *               is smaller or equal than the number of bytes in the socket's
   *               output evbuffer.
   */
  void dequeueBytesFromOutputEvbuffer(size_t nbytes) {
    ASSERT_TRUE(nbytes <= 1024);
    static char buf[1024];
    // The test expects at least this amount of bytes to be in the output
    // evbuffer of the socket. Verify that.
    ASSERT_TRUE(LD_EV(evbuffer_get_length)(output_) >= nbytes);
    // Remove the bytes. Socket should be notified automatically through
    // `bytesSentCallback`.
    LD_EV(evbuffer_remove)(output_, buf, nbytes);
  }

  /**
   * Dequeue all the bytes from the Socket's output evbuffer, simulating that
   * all the messages that were serialized get sent, thus all these messages
   * should have their onSent() callback called.
   */
  void flushOutputEvBuffer() {
    static char buf[1024];
    const size_t nbytes = LD_EV(evbuffer_get_length)(output_);
    ASSERT_TRUE(nbytes <= 1024);
    LD_EV(evbuffer_remove)(output_, buf, nbytes);
  }

  size_t getTotalOutbufLength() {
    return socket_->getTotalOutbufLength();
  }

  virtual void eventActive(struct event* ev, int /*what*/, short /*ncalls*/) {
    ld_check(ev == &socket_->end_stream_rewind_event_);
    socket_->endStreamRewind();
  }

  // Check if the timer is for buffered output. If it is - flush the buffer
  int evtimerAdd(struct event* ev, const struct timeval* /*timeout*/) {
    if (ev == &socket_->buffered_output_flush_event_) {
      socket_->flushBufferedOutput();
    } else if (ev == &socket_->deferred_event_queue_event_) {
      socket_->processDeferredEventQueue();
    }
    if (ev_timer_add_hook_) {
      ev_timer_add_hook_(ev);
    }
    return 0;
  }

  const Socket::EnvelopeQueue& getSerializeq() const {
    return socket_->serializeq_;
  }
  const Socket::EnvelopeQueue& getSendq() const {
    return socket_->sendq_;
  }

  bool connected() const {
    return socket_->connected_;
  }
  bool handshaken() const {
    return socket_->handshaken_;
  }

  int getDscp();

  const int client_id_{1};
  const uint16_t max_proto_ = Compatibility::MAX_PROTOCOL_SUPPORTED;

  Settings settings_;
  NodeID server_name_;
  Sockaddr server_addr_;  // stays invalid on a client.
  NodeID source_node_id_; // stays invalid on a client.
  NodeID destination_node_id_;
  std::string cluster_name_;
  std::string credentials_;
  std::string csid_;
  std::string client_build_info_;
  FlowGroup flow_group_;

  // IMPORTANT: this remains uninitialized and a pointer to this is returned by
  // buffereventSocketConnect(). This remains untouched because Socket only
  // accesses this through TestSocketDependencies.
  struct bufferevent bev_;
  // evbuffers that will be used by socket_ to write and read data.
  struct evbuffer* input_;
  struct evbuffer* output_;

  // Updated when Socket calls noteBytesQueued/noteBytesDrained.
  size_t bytes_pending_{0};
  // Incremented when Socket calls buffereventSocketConnect.
  size_t connection_attempts_{0};
  // Future calls to buffereventSocketConnect will fail with this value if !=
  // E::OK:
  int next_connect_attempts_errno_{0};
  // Keep track of all calls to onSent.
  std::queue<SentMsg> sent_;

  // These are set when Socket calls buffereventSetCb.
  bufferevent_data_cb read_cb_{nullptr};
  bufferevent_data_cb write_cb_{nullptr};
  bufferevent_event_cb event_cb_{nullptr};

  ResourceBudget conn_budget_external_{std::numeric_limits<uint64_t>::max()};
  ResourceBudget incoming_message_bytes_limit_{
      std::numeric_limits<uint64_t>::max()};
  std::function<void(Message*,
                     const Address&,
                     std::shared_ptr<PrincipalIdentity>,
                     ResourceBudget::Token)>
      on_received_hook_;

  std::function<void(struct event*)> ev_timer_add_hook_;

  std::unique_ptr<Socket> socket_;
};

class ClientSocketTest : public SocketTest {
 public:
  ClientSocketTest() {
    // Create a client socket.
    socket_ = std::make_unique<Socket>(
        server_name_,
        SocketType::DATA,
        ConnectionType::PLAIN,
        flow_group_,
        std::make_unique<TestSocketDependencies>(this));
    cluster_name_ = "Socket_test_cluster";
    credentials_ = "Socket_test_credentials";
    csid_ = "client_uuid";
    client_build_info_ = "{}";
    EXPECT_FALSE(connected());
    EXPECT_FALSE(handshaken());
  }
};

class ServerSocketTest : public SocketTest {
 public:
  ServerSocketTest() {
    settings_.server = true;
    source_node_id_ = server_name_;
    cluster_name_ = "Socket_test_cluster";
    credentials_ = "Socket_test_credentials";
    csid_ = "";

    // Create a server socket.
    // Note that we can pass whatever we want here for fd and client_addr
    // because Socket will not use them directly, it will always pass them to
    // methods of TestSocketDependencies so these will remain untouched.
    socket_ = std::make_unique<Socket>(
        42 /* fd */,
        ClientID(client_id_) /* client_name */,
        Sockaddr(get_localhost_address_str(), 4440) /* client_addr */,
        ResourceBudget::Token() /* accounting token, not used */,
        SocketType::DATA /* socket type */,
        ConnectionType::PLAIN,
        flow_group_,
        std::make_unique<TestSocketDependencies>(this));
    // A server socket is connected from the beginning.
    EXPECT_TRUE(connected());
    EXPECT_FALSE(handshaken());
  }
};

// FlowGroups can be applied to any Socket type. Using the client socket
// mocks simplifies the test code (e.g.no handshake required before
// registering messages with the socket).
class FlowGroupTest : public ClientSocketTest {
 public:
  FlowGroupTest();

  // Create an up and handshaked client connection.
  void setupConnection();

  bool drain(Envelope& e, Priority p) {
    if (e.message().tc_ == TrafficClass::HANDSHAKE) {
      return true;
    }
    return flow_group->drain(e.cost(), p);
  }

  void push(Envelope& e, Priority p) {
    flow_group->push(e, p);
  }

  bool run() {
    SteadyTimestamp run_deadline(SteadyTimestamp::now() +
                                 settings_.flow_groups_run_yield_interval);
    return flow_group->run(flow_meter_mutex, run_deadline);
  }

  void resetMeter(int32_t level) {
    for (auto& e : flow_group->meter_.entries) {
      e.reset(level);
    }
  }

  std::mutex flow_meter_mutex;
  std::unique_ptr<FlowGroup> flow_group;
  FlowGroupsUpdate::GroupEntry update;
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
  VarLengthTestMessage(uint16_t min_proto,
                       size_t size,
                       TrafficClass tc = TrafficClass::RECOVERY)
      : Message(MessageType::GET_SEQ_STATE, tc), min_proto_(min_proto) {
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
   * - For protocols in range [0, 2], socket will reject the message;
   * - In range [2, 3], the message will have size 30;
   * - In range [4, 4], the message will have size 40;
   * - In range [5, MAX_PROTOCOL_SUPPORTED], it will have size 42.
   */
  VarLengthTestMessage* setSize(uint16_t proto, size_t sz) {
    size_map_[proto] = sz;
    return this;
  }

  MessageType getType() {
    return MessageType::GET_SEQ_STATE;
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

  virtual Message::Disposition onReceived(const Address& /*from*/) override {
    return Message::Disposition::NORMAL;
  }

 private:
  uint16_t min_proto_;
  std::map<uint8_t, size_t> size_map_;
};

////////////////////////////////////////////////////////////////////////////////
// Macros

/**
 * Check that the content of an Envelope queue on a Socket.
 */
#define CHECK_SOCKETQ(queue, ...)                                        \
  {                                                                      \
    std::vector<MessageType> expected_q = {__VA_ARGS__};                 \
    std::vector<MessageType> actual_q((queue).size());                   \
    std::transform((queue).begin(),                                      \
                   (queue).end(),                                        \
                   actual_q.begin(),                                     \
                   [](const Envelope& e) { return e.message().type_; }); \
    ASSERT_EQ(expected_q, actual_q);                                     \
  }

#define CHECK_PENDINGQ(...) CHECK_SOCKETQ(getPendingq(), __VA_ARGS__)
#define CHECK_SERIALIZEQ(...) CHECK_SOCKETQ(getSerializeq(), __VA_ARGS__)
#define CHECK_SENDQ(...) CHECK_SOCKETQ(getSendq(), __VA_ARGS__)

#define CHECK_ON_SENT(type, status)              \
  {                                              \
    ASSERT_TRUE(!sent_.empty());                 \
    const auto expected = SentMsg{type, status}; \
    EXPECT_EQ(expected, sent_.front());          \
    sent_.pop();                                 \
  }

#define CHECK_NO_MESSAGE_SENT() ASSERT_TRUE(sent_.empty())

}} // namespace facebook::logdevice
