/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/test/SocketTest_fixtures.h"

namespace facebook { namespace logdevice {

const Settings& TestSocketDependencies::getSettings() const {
  return owner_->settings_;
}

StatsHolder* TestSocketDependencies::getStats() {
  return nullptr;
}

void TestSocketDependencies::noteBytesQueued(size_t nbytes) {
  owner_->bytes_pending_ += nbytes;
}

void TestSocketDependencies::noteBytesDrained(size_t nbytes) {
  ASSERT_TRUE(owner_->bytes_pending_ >= nbytes);
  owner_->bytes_pending_ -= nbytes;
}

size_t TestSocketDependencies::getBytesPending() const {
  return owner_->bytes_pending_;
}

bool TestSocketDependencies::bytesPendingLimitReached() const {
  return owner_->bytes_pending_ >
      owner_->settings_.outbufs_mb_max_per_thread * 1024 * 1024;
}

worker_id_t TestSocketDependencies::getWorkerId() const {
  return worker_id_t(0);
}

std::shared_ptr<folly::SSLContext>
TestSocketDependencies::getSSLContext(bufferevent_ssl_state /*ssl_state*/,
                                      bool /*null_ciphers_only*/) const {
  return nullptr;
}

bool TestSocketDependencies::shuttingDown() const {
  return false;
}

std::string TestSocketDependencies::dumpQueuedMessages(Address /*addr*/) const {
  return "";
}

const Sockaddr& TestSocketDependencies::getNodeSockaddr(NodeID nid,
                                                        SocketType /*type*/) {
  // Socket should only call this function on owner_->server_name_.
  EXPECT_EQ(owner_->server_name_, nid);
  EXPECT_TRUE(owner_->server_addr_.valid());
  return owner_->server_addr_;
}

int TestSocketDependencies::eventAssign(struct event* /*ev*/,
                                        void (*/*cb*/)(evutil_socket_t,
                                                       short what,
                                                       void* arg),
                                        void* /*arg*/) {
  return 0;
}

void TestSocketDependencies::eventActive(struct event* ev,
                                         int what,
                                         short ncalls) {
  owner_->eventActive(ev, what, ncalls);
}

void TestSocketDependencies::eventDel(struct event* /*ev*/) {}
int TestSocketDependencies::eventPrioritySet(struct event* /*ev*/,
                                             int /*priority*/) {
  return 0;
}

int TestSocketDependencies::evtimerAssign(struct event* /*ev*/,
                                          void (*/*cb*/)(evutil_socket_t,
                                                         short what,
                                                         void* arg),
                                          void* /*arg*/) {
  return 0;
}

void TestSocketDependencies::evtimerDel(struct event* /*ev*/) {}

int TestSocketDependencies::evtimerPending(struct event*, struct timeval*) {
  return 0;
}

const struct timeval*
TestSocketDependencies::getCommonTimeout(std::chrono::milliseconds /*t*/) {
  // This is passed to evtimerAdd and ignored.
  return nullptr;
}

const struct timeval* TestSocketDependencies::getZeroTimeout() {
  // This is passed to evtimerAdd and ignored.
  return nullptr;
}

int TestSocketDependencies::evtimerAdd(struct event* ev,
                                       const struct timeval* timeout) {
  return owner_->evtimerAdd(ev, timeout);
}

struct bufferevent* TestSocketDependencies::buffereventSocketNew(
    int /*sfd*/,
    int /*opts*/,
    bool /*secure*/,
    bufferevent_ssl_state /*ssl_state*/,
    folly::SSLContext* /*ssl_ctx*/) {
  return &owner_->bev_;
}

struct evbuffer*
TestSocketDependencies::getOutput(struct bufferevent* /*bev*/) {
  return owner_->output_;
}

struct evbuffer* TestSocketDependencies::getInput(struct bufferevent* /*bev*/) {
  return owner_->input_;
}

int TestSocketDependencies::buffereventSocketConnect(
    struct bufferevent* /*bev*/,
    struct sockaddr* /*ss*/,
    int /*len*/) {
  // Keep track of how many times this function was called. Useful for tests
  // that verify connection retries.
  ++owner_->connection_attempts_;

  // Simulate this function failing if a test required so.
  if (owner_->next_connect_attempts_errno_ != 0) {
    errno = owner_->next_connect_attempts_errno_;
    return -1;
  }

  return 0;
}

void TestSocketDependencies::buffereventSetWatermark(
    struct bufferevent* /*bev*/,
    short /*events*/,
    size_t /*lowmark*/,
    size_t /*highmark*/) {
  // Ignored.
}

void TestSocketDependencies::buffereventSetCb(struct bufferevent* /*bev*/,
                                              bufferevent_data_cb readcb,
                                              bufferevent_data_cb writecb,
                                              bufferevent_event_cb eventcb,
                                              void* /*cbarg*/) {
  owner_->read_cb_ = readcb;
  owner_->write_cb_ = writecb;
  owner_->event_cb_ = eventcb;
}

void TestSocketDependencies::buffereventShutDownSSL(
    struct bufferevent* /*bev*/) {
  // Ignored.
}

void TestSocketDependencies::buffereventFree(struct bufferevent* /*bev*/) {
  // Ignored.
}

int TestSocketDependencies::evUtilMakeSocketNonBlocking(int /*sfd*/) {
  return 0;
}

int TestSocketDependencies::buffereventSetMaxSingleWrite(
    struct bufferevent* /*bev*/,
    size_t /*size*/) {
  // There are no tests that simulate this function returning != 0 yet.
  return 0;
}

int TestSocketDependencies::buffereventSetMaxSingleRead(
    struct bufferevent* /*bev*/,
    size_t /*size*/) {
  // There are no tests that simulate this function returning != 0 yet.
  return 0;
}

int TestSocketDependencies::buffereventEnable(struct bufferevent* /*bev*/,
                                              short /*event*/) {
  // There are no tests that simulate this function returning != 0 yet.
  return 0;
}

std::string
TestSocketDependencies::describeConnection(const Address& /*addr*/) {
  return "";
}

void TestSocketDependencies::onSent(std::unique_ptr<Message> msg,
                                    const Address& /*to*/,
                                    Status st,
                                    const SteadyTimestamp /*enqueue_time*/,
                                    Message::CompletionMethod) {
  owner_->sent_.push(ClientSocketTest::SentMsg{msg->type_, st});
}

Message::Disposition TestSocketDependencies::onReceived(Message* msg,
                                                        const Address& from) {
  return msg->onReceived(from);
}

void TestSocketDependencies::processDeferredMessageCompletions() {}

NodeID TestSocketDependencies::getMyNodeID() {
  return owner_->source_node_id_;
}

NodeID TestSocketDependencies::getDestinationNodeID() {
  return owner_->destination_node_id_;
}

void TestSocketDependencies::configureSocket(bool /*is_tcp*/,
                                             int /*fd*/,
                                             int* /*snd_out*/,
                                             int* /*rcv_out*/) {}

ResourceBudget& TestSocketDependencies::getConnBudgetExternal() {
  return owner_->conn_budget_external_;
}

std::string TestSocketDependencies::getClusterName() {
  return owner_->cluster_name_;
}

const std::string& TestSocketDependencies::getHELLOCredentials() {
  return owner_->credentials_;
}

const std::string& TestSocketDependencies::getCSID() {
  return owner_->csid_;
}

std::string TestSocketDependencies::getClientBuildInfo() {
  return owner_->client_build_info_;
}

bool TestSocketDependencies::includeHELLOCredentials() {
  return false;
}

bool TestSocketDependencies::authenticationEnabled() {
  return false;
}

void TestSocketDependencies::onStartedRunning(RunState /*state*/) {}

void TestSocketDependencies::onStoppedRunning(RunState /*prev_state*/) {}

FlowGroupTest::FlowGroupTest() {
  flow_group.setScope(nullptr, NodeLocationScope::ROOT);

  setupConnection();

  // Setup a default update from the TrafficShaper
  FlowGroupPolicy policy;
  policy.setEnabled(true);
  // Set burst capacity small to increase the likelyhood of experiencing
  // a message deferral during a test run.
  policy.set(Priority::MAX, /*burst*/ 10000, /*Bps*/ 1000000);
  policy.set(Priority::CLIENT_HIGH, /*burst*/ 10000, /*Bps*/ 1000000);
  policy.set(Priority::CLIENT_NORMAL, /*burst*/ 10000, /*Bps*/ 1000000);
  policy.set(Priority::CLIENT_LOW, /*burst*/ 10000, /*Bps*/ 1000000);
  policy.set(Priority::BACKGROUND, /*burst*/ 10000, /*Bps*/ 1000000);
  policy.set(Priority::IDLE, /*burst*/ 10000, /*Bps*/ 1000000);
  policy.set(FlowGroup::PRIORITYQ_PRIORITY, /*burst*/ 10000, /*Bps*/ 1000000);

  update.policy =
      policy.normalize(/*workers*/ 16, std::chrono::microseconds(1000));
}

// Create an up and handshaked client connection.
void FlowGroupTest::setupConnection() {
  int rv = socket_->connect();
  ASSERT_EQ(0, rv);

  triggerEventConnected();
  flushOutputEvBuffer();
  CHECK_ON_SENT(MessageType::HELLO, E::OK);
  ACK_Header ackhdr{0, request_id_t(0), client_id_, max_proto_, E::OK};
  receiveMsg(new TestACK_Message(ackhdr));
}

}} // namespace facebook::logdevice
