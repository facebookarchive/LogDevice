/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/test/ConnectionTest_fixtures.h"

#include <folly/executors/InlineExecutor.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace facebook { namespace logdevice {
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SaveArg;

TestSocketDependencies::TestSocketDependencies(ConnectionTest* owner)
    : SocketDependencies(nullptr, nullptr), owner_(owner) {
  ON_CALL(*this, getNodeSockaddr(owner_->server_name_, _, _))
      .WillByDefault(testing::ReturnRef(owner_->server_addr_));
}

const Settings& TestSocketDependencies::getSettings() const {
  return owner_->settings_;
}

StatsHolder* TestSocketDependencies::getStats() const {
  return nullptr;
}

void TestSocketDependencies::noteBytesQueued(size_t nbytes,
                                             PeerType /* Peer type */,
                                             folly::Optional<MessageType>) {
  owner_->bytes_pending_ += nbytes;
}

void TestSocketDependencies::noteBytesDrained(size_t nbytes,
                                              PeerType /* Peer type */,
                                              folly::Optional<MessageType>) {
  ASSERT_GE(owner_->bytes_pending_, nbytes);
  owner_->bytes_pending_ -= nbytes;
}

size_t TestSocketDependencies::getBytesPending() const {
  return owner_->bytes_pending_;
}

std::shared_ptr<folly::SSLContext>
TestSocketDependencies::getSSLContext() const {
  return nullptr;
}

bool TestSocketDependencies::shuttingDown() const {
  return false;
}

std::string TestSocketDependencies::dumpQueuedMessages(Address /*addr*/) const {
  return "";
}

EvBase* TestSocketDependencies::getEvBase() {
  return &owner_->ev_base_folly_;
}

SteadyTimestamp TestSocketDependencies::getCurrentTimestamp() {
  return owner_->cur_time_;
}

void TestSocketDependencies::onSent(std::unique_ptr<Message> msg,
                                    const Address& /*to*/,
                                    Status st,
                                    const SteadyTimestamp /*enqueue_time*/,
                                    Message::CompletionMethod) {
  owner_->sent_.push(ConnectionTest::SentMsg{msg->type_, st});
}

Message::Disposition
TestSocketDependencies::onReceived(Message* msg,
                                   const Address& from,
                                   std::shared_ptr<PrincipalIdentity> identity,
                                   ResourceBudget::Token token) {
  if (owner_->on_received_hook_) {
    auto retval =
        owner_->on_received_hook_(msg, from, identity, std::move(token));
    if (retval == Message::Disposition::NORMAL ||
        retval == Message::Disposition::ERROR) {
      return retval;
    }
  }
  return msg->onReceived(from);
}

void TestSocketDependencies::processDeferredMessageCompletions() {}

NodeID TestSocketDependencies::getMyNodeID() {
  return owner_->source_node_id_;
}

NodeID TestSocketDependencies::getDestinationNodeID() {
  return owner_->destination_node_id_;
}

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

void TestSocketDependencies::onStartedRunning(RunContext /*context*/) {}

void TestSocketDependencies::onStoppedRunning(RunContext /*prev_context*/) {}

ResourceBudget::Token
TestSocketDependencies::getResourceToken(size_t payload_size) {
  return owner_->incoming_message_bytes_limit_.acquireToken(payload_size);
}

int TestSocketDependencies::setSoMark(int /*fd*/, uint32_t /*so_mark*/) {
  return 0;
}

folly::Executor* TestSocketDependencies::getExecutor() const {
  return &folly::InlineExecutor::instance();
}

int TestSocketDependencies::getTCPInfo(TCPInfo* info, int /*fd*/) {
  *info = owner_->socket_flow_stats_;
  return 0;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
TestSocketDependencies::getNodesConfiguration() const {
  return owner_->nodes_configuration_;
}

//

int ConnectionTest::getDscp() {
  int dscp = 0;
  unsigned int dscplen = sizeof(dscp);
  int rc = 0;
  switch (conn_->getInfo().peer_address.family()) {
    case AF_INET: {
      rc = getsockopt(conn_->fd_, IPPROTO_IP, IP_TOS, &dscp, &dscplen);
      break;
    }
    case AF_INET6: {
      rc = getsockopt(conn_->fd_, IPPROTO_IPV6, IPV6_TCLASS, &dscp, &dscplen);
      break;
    }
    default:
      EXPECT_FALSE(
          "Please implement this if you add some other socket family support");
  }
  EXPECT_EQ(0, rc);
  return dscp;
}

}} // namespace facebook::logdevice
