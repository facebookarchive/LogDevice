// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <folly/io/async/SSLContext.h>

#include "logdevice/common/ConnectionInfo.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/RunContext.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/libevent/LibEventCompatibility.h"
#include "logdevice/common/network/LinuxNetUtils.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {
namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class Processor;
class Sender;
class Configuration;
class SSLPrincipalParser;
class ServerConfig;
class StatsHolder;
struct Settings;
class Sockaddr;
class SSLSessionCache;
class Worker;

/**
 * Unit tests implement a derived class, @see common/test/SocketTest.cpp.
 */
class SocketDependencies {
 public:
  using SSLCtxPtr = std::shared_ptr<folly::SSLContext>;

  SocketDependencies(Processor* processor, Sender* sender);
  virtual const Settings& getSettings() const;
  virtual StatsHolder* getStats() const;
  virtual std::shared_ptr<Configuration> getConfig() const;
  virtual std::shared_ptr<ServerConfig> getServerConfig() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;
  virtual void noteBytesQueued(size_t nbytes,
                               PeerType peer_type,
                               folly::Optional<MessageType>);
  virtual void noteBytesDrained(size_t nbytes,
                                PeerType peer_type,
                                folly::Optional<MessageType>);
  virtual size_t getBytesPending() const;

  virtual SSLCtxPtr getSSLContext() const;
  virtual SSLSessionCache& getSSLSessionCache() const;
  virtual std::shared_ptr<SSLPrincipalParser> getPrincipalParser() const;
  virtual bool shuttingDown() const;
  virtual std::string dumpQueuedMessages(Address addr) const;
  virtual const Sockaddr& getNodeSockaddr(NodeID node_id,
                                          SocketType socket_type,
                                          ConnectionType connection_type);
  virtual EvBase* getEvBase();

  virtual void onSent(std::unique_ptr<Message> msg,
                      const Address& to,
                      Status st,
                      const SteadyTimestamp enqueue_time,
                      Message::CompletionMethod);
  virtual Message::Disposition
  onReceived(Message* msg,
             const Address& from,
             std::shared_ptr<PrincipalIdentity> principal,
             ResourceBudget::Token resource_token);
  virtual void processDeferredMessageCompletions();
  virtual NodeID getMyNodeID();
  virtual int setDSCP(int fd,
                      sa_family_t sa_family,
                      const uint8_t default_dscp);
  virtual int setSoMark(int fd, uint32_t so_mark);
  virtual ResourceBudget& getConnBudgetExternal();
  virtual std::string getClusterName();
  virtual ServerInstanceId getServerInstanceId();
  virtual const std::string& getHELLOCredentials();
  virtual const std::string& getCSID();
  virtual std::string getClientBuildInfo();
  virtual SteadyTimestamp getCurrentTimestamp();
  virtual bool authenticationEnabled();
  virtual bool allowUnauthenticated();
  virtual bool includeHELLOCredentials();
  virtual void onStartedRunning(RunContext context);
  virtual void onStoppedRunning(RunContext prev_context);
  virtual ResourceBudget::Token getResourceToken(size_t payload_size);
  virtual std::unique_ptr<Message> createHelloMessage(NodeID destNodeID);
  virtual std::unique_ptr<Message>
  createShutdownMessage(uint32_t serverInstanceID);
  virtual void processHelloMessage(const Message& msg, ConnectionInfo& info);
  virtual void processACKMessage(const Message& msg, ConnectionInfo& info);
  virtual std::unique_ptr<Message> deserialize(const ProtocolHeader& ph,
                                               ProtocolReader& reader);
  virtual std::string describeConnection(const Address& addr);
  /**
   * Setups context to execute SocketCallbacks and other events.
   * Returns a callback which destroys context once the callback execution
   * finishes.
   * The callback can be invoked explicitly when context needs to be destroyed
   * or can be used to create guard using folly::makeGuard. Check Connection.cpp
   * for usage.
   */
  virtual folly::Func setupContextGuard();
  virtual folly::Executor* getExecutor() const;
  virtual int getTCPInfo(TCPInfo* info, int fd);
  virtual ~SocketDependencies() {}

 private:
  Processor* const processor_;
  Sender* sender_;
  Worker* const worker_;
};
}} // namespace facebook::logdevice
