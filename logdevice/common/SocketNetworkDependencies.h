// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include "logdevice/common/NetworkDependencies.h"
#include "logdevice/common/libevent/LibEventCompatibility.h"
#include "logdevice/common/network/LinuxNetUtils.h"

namespace facebook { namespace logdevice {
namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class Processor;
class Configuration;
class SSLPrincipalParser;
class ServerConfig;
class StatsHolder;
struct Settings;
class Sockaddr;
class SocketSender;
class SSLSessionCache;
class Worker;

/**
 * Unit tests implement a derived class, @see common/test/SocketTest.cpp.
 */
class SocketNetworkDependencies : public NetworkDependencies {
 public:
  SocketNetworkDependencies(Processor* processor, SocketSender* sender);

  virtual void noteBytesQueued(size_t nbytes,
                               PeerType peer_type,
                               folly::Optional<MessageType>);
  virtual void noteBytesDrained(size_t nbytes,
                                PeerType peer_type,
                                folly::Optional<MessageType>);
  virtual size_t getBytesPending() const;

  virtual std::string dumpQueuedMessages(Address addr) const;

  virtual EvBase* getEvBase();

  void onSent(std::unique_ptr<Message> msg,
              const Address& to,
              Status st,
              SteadyTimestamp enqueue_time,
              Message::CompletionMethod) override;

  virtual void processDeferredMessageCompletions();
  virtual int setDSCP(int fd, sa_family_t sa_family, uint8_t default_dscp);
  virtual int setSoMark(int fd, uint32_t so_mark);

  virtual int getTCPInfo(TCPInfo* info, int fd);

 private:
  SocketSender* sender_;
};
}} // namespace facebook::logdevice
