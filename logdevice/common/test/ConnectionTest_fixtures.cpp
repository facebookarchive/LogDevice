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

TestNetworkDependencies::TestNetworkDependencies(ConnectionTest* owner)
    : SocketNetworkDependencies(nullptr, nullptr), owner_(owner) {
  ON_CALL(*this, getNodeSockaddr(owner_->server_name_, _, _))
      .WillByDefault(testing::ReturnRef(owner_->server_addr_));
}

const Settings& TestNetworkDependencies::getSettings() const {
  return owner_->settings_;
}

StatsHolder* TestNetworkDependencies::getStats() const {
  return nullptr;
}

void TestNetworkDependencies::noteBytesQueued(size_t nbytes,
                                              PeerType /* Peer type */,
                                              folly::Optional<MessageType>) {
  owner_->bytes_pending_ += nbytes;
}

void TestNetworkDependencies::noteBytesDrained(size_t nbytes,
                                               PeerType /* Peer type */,
                                               folly::Optional<MessageType>) {
  ASSERT_GE(owner_->bytes_pending_, nbytes);
  owner_->bytes_pending_ -= nbytes;
}

size_t TestNetworkDependencies::getBytesPending() const {
  return owner_->bytes_pending_;
}

std::shared_ptr<folly::SSLContext>
TestNetworkDependencies::getSSLContext() const {
  return nullptr;
}

bool TestNetworkDependencies::shuttingDown() const {
  return false;
}

std::string
TestNetworkDependencies::dumpQueuedMessages(Address /*addr*/) const {
  return "";
}

EvBase* TestNetworkDependencies::getEvBase() {
  return &owner_->ev_base_folly_;
}

SteadyTimestamp TestNetworkDependencies::getCurrentTimestamp() {
  return owner_->cur_time_;
}

void TestNetworkDependencies::onSent(std::unique_ptr<Message> msg,
                                     const Address& /*to*/,
                                     Status st,
                                     const SteadyTimestamp /*enqueue_time*/,
                                     Message::CompletionMethod) {
  owner_->sent_.push(ConnectionTest::SentMsg{msg->type_, st});
}

Message::Disposition
TestNetworkDependencies::onReceived(Message* msg,
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

void TestNetworkDependencies::processDeferredMessageCompletions() {}

NodeID TestNetworkDependencies::getMyNodeID() {
  return owner_->source_node_id_;
}

NodeID TestNetworkDependencies::getDestinationNodeID() {
  return owner_->destination_node_id_;
}

ResourceBudget& TestNetworkDependencies::getConnBudgetExternal() {
  return owner_->conn_budget_external_;
}

std::string TestNetworkDependencies::getClusterName() {
  return owner_->cluster_name_;
}

const std::string& TestNetworkDependencies::getHELLOCredentials() {
  return owner_->credentials_;
}

const std::string& TestNetworkDependencies::getCSID() {
  return owner_->csid_;
}

std::string TestNetworkDependencies::getClientBuildInfo() {
  return owner_->client_build_info_;
}

bool TestNetworkDependencies::includeHELLOCredentials() {
  return false;
}

bool TestNetworkDependencies::authenticationEnabled() {
  return false;
}

void TestNetworkDependencies::onStartedRunning(RunContext /*context*/) {}

void TestNetworkDependencies::onStoppedRunning(RunContext /*prev_context*/) {}

ResourceBudget::Token
TestNetworkDependencies::getResourceToken(size_t payload_size) {
  return owner_->incoming_message_bytes_limit_.acquireToken(payload_size);
}

int TestNetworkDependencies::setSoMark(int /*fd*/, uint32_t /*so_mark*/) {
  return 0;
}

folly::Executor* TestNetworkDependencies::getExecutor() const {
  return &folly::InlineExecutor::instance();
}

int TestNetworkDependencies::getTCPInfo(TCPInfo* info, int /*fd*/) {
  *info = owner_->socket_flow_stats_;
  return 0;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
TestNetworkDependencies::getNodesConfiguration() const {
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
