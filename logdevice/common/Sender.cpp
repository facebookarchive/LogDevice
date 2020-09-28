/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sender.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/network/IConnectionFactory.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

namespace admin_command_table {
template <>
std::string Converter<ClientID>::operator()(ClientID client,
                                            bool /* prettify */) {
  return Sender::describeConnection(Address(client));
}

template <>
std::string Converter<Address>::operator()(Address address,
                                           bool /* prettify */) {
  return Sender::describeConnection(address);
}
} // namespace admin_command_table

bool SenderProxy::canSendToImpl(const Address& addr,
                                TrafficClass tc,
                                BWAvailableCallback& on_bw_avail) {
  auto w = Worker::onThisThread();
  return w->sender().canSendTo(addr, tc, on_bw_avail);
}

int SenderProxy::sendMessageImpl(std::unique_ptr<Message>&& msg,
                                 const Address& addr,
                                 BWAvailableCallback* on_bw_avail,
                                 SocketCallback* onclose) {
  auto w = Worker::onThisThread();
  return w->sender().sendMessage(std::move(msg), addr, on_bw_avail, onclose);
}

void SenderBase::MessageCompletion::send() {
  auto prev_context = Worker::packRunContext();
  RunContext run_context(msg_->type_);
  const auto w = Worker::onThisThread();
  w->onStartedRunning(run_context);
  w->message_dispatch_->onSent(*msg_, status_, destination_, enqueue_time_);
  msg_.reset(); // count destructor as part of message's execution time
  w->onStoppedRunning(run_context);
  Worker::unpackRunContext(prev_context);
}

folly::Optional<uint16_t>
Sender::getSocketProtocolVersion(node_index_t idx) const {
  const auto* info = getConnectionInfo(Address(idx));
  return info ? info->protocol : folly::none;
}

ClientID Sender::getOurNameAtPeer(node_index_t node_index) const {
  const auto* info = getConnectionInfo(Address(node_index));
  folly::Optional<ClientID> name = info ? info->our_name_at_peer : folly::none;
  return name.value_or(ClientID::INVALID);
}

Sockaddr Sender::getSockaddr(const Address& addr) const {
  const auto* info = getConnectionInfo(addr);
  return info ? info->peer_address : Sockaddr::INVALID;
}

const PrincipalIdentity* Sender::getPrincipal(const Address& address) const {
  const auto* info = getConnectionInfo(address);
  return info ? info->principal.get() : nullptr;
}

folly::Optional<std::string> Sender::getCSID(const Address& addr) const {
  const auto* info = getConnectionInfo(addr);
  return info ? info->csid : folly::none;
}

std::string Sender::getClientLocation(const ClientID& cid) const {
  const auto* info = getConnectionInfo(Address(cid));
  folly::Optional<std::string> location =
      info ? info->client_location : folly::none;
  return location.value_or("");
}

/* static */
Sockaddr Sender::sockaddrOrInvalid(const Address& addr) {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    return Sockaddr();
  }
  return w->sender().getSockaddr(addr);
}

folly::Optional<node_index_t> Sender::getNodeIdx(const Address& addr) const {
  if (!addr.isClientAddress()) {
    return addr.id_.node_.index();
  }
  const auto* info = getConnectionInfo(addr);
  return info ? info->peer_node_idx : folly::none;
}

/* static */
std::string Sender::describeConnection(const Address& addr) {
  if (!ThreadID::isWorker()) {
    return addr.toString();
  }
  Worker* w = Worker::onThisThread();
  ld_check(w);

  if (w->shuttingDown()) {
    return addr.toString();
  }

  if (!addr.valid()) {
    return addr.toString();
  }

  // index of worker to which a connection from or to peer identified by
  // this Address is assigned
  ClientIdxAllocator& alloc = w->processor_->clientIdxAllocator();
  std::pair<WorkerType, worker_id_t> assignee = addr.isClientAddress()
      ? alloc.getWorkerId(addr.id_.client_)
      : std::make_pair(w->worker_type_, w->idx_);

  std::string res;
  res.reserve(64);

  if (assignee.second.val() == -1) {
    res += "(disconnected)";
  } else {
    res += Worker::getName(assignee.first, assignee.second);
  }
  res += ":";
  res += addr.toString();
  if (!addr.isClientAddress() ||
      (assignee.first == w->worker_type_ && assignee.second == w->idx_)) {
    const Sockaddr& sa = w->sender().getSockaddr(addr);
    res += " (";
    res += sa.valid() ? sa.toString() : std::string("UNKNOWN");
    res += ")";
  }

  return res;
}

std::shared_ptr<const std::atomic<bool>>
Sender::getConnectionToken(const ClientID cid) const {
  ld_check(cid.valid());
  const auto* info = getConnectionInfo(Address(cid));
  return info && info->is_active->load() ? info->is_active : nullptr;
}
}} // namespace facebook::logdevice
