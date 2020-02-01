// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <fizz/server/FizzServerContext.h>
#include <folly/io/async/SSLContext.h>

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event2/bufferevent_ssl.h"
#include "event2/event.h"
#include "event2/event_struct.h"
#include "logdevice/common/Address.h"
#include "logdevice/common/NodeID.h"
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
class ServerConfig;
class StatsHolder;
struct Settings;
class Sockaddr;
class Worker;

/**
 * Unit tests implement a derived class, @see common/test/SocketTest.cpp.
 */
class SocketDependencies {
 public:
  using SSLCtxPtr = std::shared_ptr<folly::SSLContext>;
  using FizzClientCtxPair =
      std::pair<std::shared_ptr<const fizz::client::FizzClientContext>,
                std::shared_ptr<const fizz::CertificateVerifier>>;

  SocketDependencies(Processor* processor, Sender* sender);
  /*
   * Indicates whether the event base on this socket is attached to legacy
   * type or not.
   */
  virtual bool attachedToLegacyEventBase() const;
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
  virtual std::shared_ptr<const fizz::server::FizzServerContext>
  getFizzServerContext() const;
  virtual FizzClientCtxPair getFizzClientContext() const;
  virtual bool shuttingDown() const;
  virtual std::string dumpQueuedMessages(Address addr) const;
  virtual const Sockaddr& getNodeSockaddr(NodeID node_id,
                                          SocketType socket_type,
                                          ConnectionType connection_type,
                                          PeerType peer_type);
  virtual EvBase* getEvBase();
  virtual const struct timeval* getCommonTimeout(std::chrono::milliseconds t);
  virtual const timeval*
  getTimevalFromMilliseconds(std::chrono::milliseconds t);
  virtual const struct timeval* getZeroTimeout();
  virtual struct bufferevent* buffereventSocketNew(int sfd,
                                                   int opts,
                                                   bool secure,
                                                   bufferevent_ssl_state,
                                                   folly::SSLContext*);

  virtual struct evbuffer* getOutput(struct bufferevent* bev);
  virtual struct evbuffer* getInput(struct bufferevent* bev);
  virtual int buffereventSocketConnect(struct bufferevent* bev,
                                       struct sockaddr* ss,
                                       int len);
  virtual void buffereventSetWatermark(struct bufferevent* bev,
                                       short events,
                                       size_t lowmark,
                                       size_t highmark);
  virtual void buffereventSetCb(struct bufferevent* bev,
                                bufferevent_data_cb readcb,
                                bufferevent_data_cb writecb,
                                bufferevent_event_cb eventcb,
                                void* cbarg);
  virtual void buffereventShutDownSSL(struct bufferevent* bev);
  virtual void buffereventFree(struct bufferevent* bev);
  virtual int evUtilMakeSocketNonBlocking(int sfd);
  virtual int buffereventSetMaxSingleWrite(struct bufferevent* bev,
                                           size_t size);
  virtual int buffereventSetMaxSingleRead(struct bufferevent* bev, size_t size);
  virtual int buffereventEnable(struct bufferevent* bev, short event);
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
  virtual void configureSocket(bool is_tcp,
                               int fd,
                               int* snd_out,
                               int* rcv_out,
                               sa_family_t sa_family,
                               const uint8_t default_dscp);
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
  virtual uint16_t processHelloMessage(const Message* msg);
  virtual void processACKMessage(const Message* msg,
                                 ClientID* our_name_at_peer,
                                 uint16_t* destProto);
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
