// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <folly/io/async/SSLContext.h>

#include "logdevice/common/ConnectionInfo.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/RunContext.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {
namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class Processor;
class Configuration;
class ThriftRouter;
class SSLPrincipalParser;
class ServerConfig;
class StatsHolder;
struct Settings;
class Sockaddr;
class SSLSessionCache;
class TimerInterface;
class Worker;

/**
 * Unit tests implement a derived class, @see common/test/SocketTest.cpp.
 */
class NetworkDependencies {
 public:
  using SSLCtxPtr = std::shared_ptr<folly::SSLContext>;

  explicit NetworkDependencies(Processor* processor);

  virtual const Settings& getSettings() const;
  virtual StatsHolder* getStats() const;
  virtual std::shared_ptr<Configuration> getConfig() const;
  virtual std::shared_ptr<ServerConfig> getServerConfig() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual SSLCtxPtr getSSLContext() const;
  virtual SSLSessionCache& getSSLSessionCache() const;
  virtual std::shared_ptr<SSLPrincipalParser> getPrincipalParser() const;
  virtual bool shuttingDown() const;
  virtual const Sockaddr& getNodeSockaddr(NodeID node_id,
                                          SocketType socket_type,
                                          ConnectionType connection_type);

  virtual void onSent(std::unique_ptr<Message> msg,
                      const Address& to,
                      Status st,
                      SteadyTimestamp enqueue_time,
                      Message::CompletionMethod);
  virtual Message::Disposition
  onReceived(Message* msg,
             const Address& from,
             std::shared_ptr<PrincipalIdentity> principal,
             ResourceBudget::Token resource_token);
  virtual NodeID getMyNodeID();
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
  virtual std::unique_ptr<TimerInterface>
  createTimer(std::function<void()>&&, std::chrono::microseconds delay);

  virtual ~NetworkDependencies() = default;

 protected:
  Processor* const processor_;
  Worker* const worker_;
};
}} // namespace facebook::logdevice
