// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/SocketNetworkDependencies.h"

#include <folly/io/async/SSLContext.h>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/SSLPrincipalParser.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketSender.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/ServerAddressRouter.h"
#include "logdevice/common/libevent/LibEventCompatibility.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/protocol/ACK_Message.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/protocol/MessageDeserializers.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/protocol/SHUTDOWN_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

SocketNetworkDependencies::SocketNetworkDependencies(Processor* processor,
                                                     SocketSender* sender)
    : NetworkDependencies(processor), sender_(sender) {}

void SocketNetworkDependencies::noteBytesQueued(
    size_t nbytes,
    PeerType peer_type,
    folly::Optional<MessageType> message_type) {
  sender_->noteBytesQueued(nbytes, peer_type, message_type);
}

void SocketNetworkDependencies::noteBytesDrained(
    size_t nbytes,
    PeerType peer_type,
    folly::Optional<MessageType> message_type) {
  sender_->noteBytesDrained(nbytes, peer_type, message_type);
}

size_t SocketNetworkDependencies::getBytesPending() const {
  return sender_->getBytesPending();
}

std::string SocketNetworkDependencies::dumpQueuedMessages(Address addr) const {
  return sender_->dumpQueuedMessages(addr);
}

EvBase* SocketNetworkDependencies::getEvBase() {
  return &EventLoop::onThisThread()->getEvBase();
}

void SocketNetworkDependencies::onSent(std::unique_ptr<Message> msg,
                                       const Address& to,
                                       Status st,
                                       SteadyTimestamp t,
                                       Message::CompletionMethod cm) {
  switch (cm) {
    // TODO(T73926311): Deprecate deferred completion as soon as D22393001 lands
    case Message::CompletionMethod::DEFERRED:
      sender_->queueMessageCompletion(std::move(msg), to, st, t);
      break;
    case Message::CompletionMethod::IMMEDIATE: {
      NetworkDependencies::onSent(std::move(msg), to, st, t, cm);
      break;
    }
  }
}

void SocketNetworkDependencies::processDeferredMessageCompletions() {
  sender_->deliverCompletedMessages();
}

int SocketNetworkDependencies::setDSCP(int fd,
                                       sa_family_t sa_family,
                                       const uint8_t default_dscp) {
  int rv = 0;
  switch (sa_family) {
    case AF_INET: {
      int ip_tos = default_dscp << 2;
      rv = setsockopt(fd, IPPROTO_IP, IP_TOS, &ip_tos, sizeof(ip_tos));
      break;
    }
    case AF_INET6: {
      int tclass = default_dscp << 2;
      rv = setsockopt(fd, IPPROTO_IPV6, IPV6_TCLASS, &tclass, sizeof(tclass));
      break;
    }
    default:
      rv = 0;
      break;
  }
  return rv;
}

int SocketNetworkDependencies::setSoMark(int fd, uint32_t so_mark) {
  return setsockopt(fd, SOL_SOCKET, SO_MARK, &so_mark, sizeof(so_mark));
}

int SocketNetworkDependencies::getTCPInfo(TCPInfo* info, int fd) {
  LinuxNetUtils util;
  return util.getTCPInfo(info, fd);
}
}} // namespace facebook::logdevice
