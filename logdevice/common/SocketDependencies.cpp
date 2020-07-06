// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/SocketDependencies.h"

#include <folly/io/async/SSLContext.h>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/SSLPrincipalParser.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
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
using folly::SSLContext;
using namespace std::placeholders;
// SocketDependencies is created during socket creation from worker context.
// Save off all the fields that need explicit worker access.
SocketDependencies::SocketDependencies(Processor* processor, Sender* sender)
    : processor_(processor),
      sender_(sender),
      worker_(Worker::onThisThread(false /*enforce_worker*/)) {}

const Settings& SocketDependencies::getSettings() const {
  return *processor_->settings();
}

StatsHolder* SocketDependencies::getStats() const {
  return processor_->stats_;
}

std::shared_ptr<Configuration> SocketDependencies::getConfig() const {
  return processor_->getConfig();
}

std::shared_ptr<ServerConfig> SocketDependencies::getServerConfig() const {
  return getConfig()->serverConfig();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
SocketDependencies::getNodesConfiguration() const {
  return processor_->getNodesConfiguration();
}

void SocketDependencies::noteBytesQueued(
    size_t nbytes,
    PeerType peer_type,
    folly::Optional<MessageType> message_type) {
  sender_->noteBytesQueued(nbytes, peer_type, message_type);
}

void SocketDependencies::noteBytesDrained(
    size_t nbytes,
    PeerType peer_type,
    folly::Optional<MessageType> message_type) {
  sender_->noteBytesDrained(nbytes, peer_type, message_type);
}

size_t SocketDependencies::getBytesPending() const {
  return sender_->getBytesPending();
}

std::shared_ptr<SSLContext> SocketDependencies::getSSLContext() const {
  return Worker::onThisThread()->sslFetcher().getSSLContext();
}

SSLSessionCache& SocketDependencies::getSSLSessionCache() const {
  return Worker::onThisThread()->processor_->sslSessionCache();
}

std::shared_ptr<SSLPrincipalParser>
SocketDependencies::getPrincipalParser() const {
  return Worker::onThisThread()
      ->processor_->security_info_->get()
      ->principal_parser;
}

bool SocketDependencies::shuttingDown() const {
  return Worker::onThisThread()->shuttingDown();
}

std::string SocketDependencies::dumpQueuedMessages(Address addr) const {
  return sender_->dumpQueuedMessages(addr);
}

const Sockaddr&
SocketDependencies::getNodeSockaddr(NodeID node_id,
                                    SocketType socket_type,
                                    ConnectionType connection_type) {
  auto nodes_configuration = getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);

  // note: we don't check for generation here, if the generation has changed in
  // the future, Sender will reset the connection
  const auto* node_service_discovery =
      nodes_configuration->getNodeServiceDiscovery(node_id.index());

  const Settings& settings = getSettings();
  bool is_server = settings.server;
  bool use_s2s_addr = settings.use_dedicated_server_to_server_address;

  if (node_service_discovery) {
    if (socket_type == SocketType::GOSSIP &&
        !getSettings().send_to_gossip_port) {
      return node_service_discovery->getSockaddr(
          SocketType::DATA, connection_type, is_server, use_s2s_addr);
    }

    return node_service_discovery->getSockaddr(
        socket_type, connection_type, is_server, use_s2s_addr);
  }

  return Sockaddr::INVALID;
}

EvBase* SocketDependencies::getEvBase() {
  return &EventLoop::onThisThread()->getEvBase();
}

void SocketDependencies::onSent(std::unique_ptr<Message> msg,
                                const Address& to,
                                Status st,
                                SteadyTimestamp t,
                                Message::CompletionMethod cm) {
  switch (cm) {
    case Message::CompletionMethod::IMMEDIATE: {
      // Note: instead of creating a RunContext with message type, we could
      // use RunContext of whoever sent the message (grabbed in
      // Sender::send()), similar to how timers do it.
      RunContext run_context(msg->type_);
      auto prev_context = Worker::packRunContext();
      onStartedRunning(run_context);
      Worker::onThisThread()->message_dispatch_->onSent(*msg, st, to, t);
      msg.reset(); // count destructor as part of message's execution time
      onStoppedRunning(run_context);
      Worker::unpackRunContext(prev_context);
      break;
    }
    default:
      ld_check(false);
      FOLLY_FALLTHROUGH;
    case Message::CompletionMethod::DEFERRED:
      sender_->queueMessageCompletion(std::move(msg), to, st, t);
      break;
  }
}

namespace {
template <typename T, typename M>
void executeOnWorker(Worker* worker,
                     T cb,
                     M message,
                     const Address& from,
                     std::shared_ptr<PrincipalIdentity> identity,
                     int8_t pri,
                     ResourceBudget::Token resource_token) {
  worker->addWithPriority(
      [msg = std::unique_ptr<typename std::remove_pointer<M>::type>(message),
       from,
       identity = std::move(identity),
       cb = std::move(cb),
       resource_token = std::move(resource_token)]() mutable {
        auto worker = Worker::onThisThread();
        RunContext run_context(msg->type_);
        worker->onStartedRunning(run_context);
        SCOPE_EXIT {
          worker->onStoppedRunning(run_context);
        };

        auto rv = cb(msg.get(), from, *identity);
        switch (rv) {
          case Message::Disposition::ERROR:
            worker->sender().closeSocket(from, err);
            break;
          case Message::Disposition::KEEP:
            // Ownership transferred.
            msg.release();
            break;
          case Message::Disposition::NORMAL:
            break;
        }
      },
      pri);
}
} // namespace

Message::Disposition
SocketDependencies::onReceived(Message* msg,
                               const Address& from,
                               std::shared_ptr<PrincipalIdentity> principal,
                               ResourceBudget::Token resource_token) {
  ld_assert(principal);
  auto worker = Worker::onThisThread();
  if (getSettings().inline_message_execution || shouldBeInlined(msg->type_)) {
    // This must be synchronous, because we are processing this message
    // during message disposition.
    return worker->message_dispatch_->onReceived(msg, from, *principal);
  }

  executeOnWorker(worker,
                  std::bind(&MessageDispatch::onReceived,
                            worker->message_dispatch_.get(),
                            _1,
                            _2,
                            _3),
                  msg,
                  from,
                  principal,
                  msg->getExecutorPriority(),
                  std::move(resource_token));
  return Message::Disposition::KEEP;
}

void SocketDependencies::processDeferredMessageCompletions() {
  sender_->deliverCompletedMessages();
}

NodeID SocketDependencies::getMyNodeID() {
  return processor_->getMyNodeID();
}

int SocketDependencies::setDSCP(int fd,
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

int SocketDependencies::setSoMark(int fd, uint32_t so_mark) {
  return setsockopt(fd, SOL_SOCKET, SO_MARK, &so_mark, sizeof(so_mark));
}

ResourceBudget& SocketDependencies::getConnBudgetExternal() {
  return processor_->conn_budget_external_;
}

std::string SocketDependencies::getClusterName() {
  return getServerConfig()->getClusterName();
}

ServerInstanceId SocketDependencies::getServerInstanceId() {
  return processor_->getServerInstanceId();
}

const std::string& SocketDependencies::getHELLOCredentials() {
  return processor_->HELLOCredentials_;
}

const std::string& SocketDependencies::getCSID() {
  return processor_->csid_;
}

std::string SocketDependencies::getClientBuildInfo() {
  auto build_info = processor_->getPluginRegistry()->getSinglePlugin<BuildInfo>(
      PluginType::BUILD_INFO);
  ld_check(build_info);
  return build_info->getBuildInfoJson();
}

SteadyTimestamp SocketDependencies::getCurrentTimestamp() {
  return SteadyTimestamp::now();
}

bool SocketDependencies::authenticationEnabled() {
  return processor_->security_info_ &&
      processor_->security_info_->get()->isAuthenticationEnabled();
}

bool SocketDependencies::allowUnauthenticated() {
  return getServerConfig()->allowUnauthenticated();
}

bool SocketDependencies::includeHELLOCredentials() {
  // Only include HELLOCredentials in HELLO_Message when the PrincipalParser
  // will use the data.
  const auto auth_type = processor_->security_info_->get()->auth_type;
  return auth_type == AuthenticationType::SELF_IDENTIFICATION;
}

void SocketDependencies::onStartedRunning(RunContext context) {
  if (worker_) {
    worker_->onStartedRunning(context);
  } else {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Worker not set. New context: %s.",
                    context.describe().c_str());
    ld_check(false);
  }
}

void SocketDependencies::onStoppedRunning(RunContext prev_context) {
  if (worker_) {
    worker_->onStoppedRunning(prev_context);
  } else {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Worker not set. New context: %s.",
                    prev_context.describe().c_str());
    ld_check(false);
  }
}

ResourceBudget::Token
SocketDependencies::getResourceToken(size_t payload_size) {
  return processor_->getIncomingMessageToken(payload_size);
}

std::unique_ptr<Message>
SocketDependencies::createHelloMessage(NodeID destNodeID) {
  uint16_t max_protocol = getSettings().max_protocol;
  ld_check(max_protocol >= Compatibility::MIN_PROTOCOL_SUPPORTED);
  ld_check(max_protocol <= Compatibility::MAX_PROTOCOL_SUPPORTED);
  HELLO_Header hdr{Compatibility::MIN_PROTOCOL_SUPPORTED,
                   max_protocol,
                   0,
                   request_id_t(0),
                   {}};
  NodeID source_node_id, destination_node_id;
  std::string cluster_name, extra_build_info, client_location;

  // If this is a LogDevice server, include its NodeID in the HELLO
  // message.
  if (getSettings().server) {
    hdr.flags |= HELLO_Header::SOURCE_NODE;
    source_node_id = getMyNodeID();
  } else {
    // if this is a client, include build information
    hdr.flags |= HELLO_Header::BUILD_INFO;
    extra_build_info = getClientBuildInfo();
  }

  // If include_destination_on_handshake is set, then include the
  // destination's NodeID in the HELLO message
  if (getSettings().include_destination_on_handshake) {
    hdr.flags |= HELLO_Header::DESTINATION_NODE;
    destination_node_id = destNodeID;
  }

  // Only include credentials when needed
  if (authenticationEnabled() && includeHELLOCredentials()) {
    const std::string& credentials = getHELLOCredentials();
    ld_check(credentials.size() < HELLO_Header::CREDS_SIZE_V1);

    // +1 for null terminator
    std::memcpy(hdr.credentials, credentials.c_str(), credentials.size() + 1);
  }

  // If include_cluster_name_on_handshake is set then include the cluster
  // name in the HELLOv2 message
  if (getSettings().include_cluster_name_on_handshake) {
    hdr.flags |= HELLO_Header::CLUSTER_NAME;
    cluster_name = getClusterName();
  }

  // If the client location is specified in settings, include it in the HELLOv2
  // message.
  auto& client_location_opt = getSettings().client_location;
  if (client_location_opt.has_value()) {
    client_location = client_location_opt.value().toString();
    hdr.flags |= HELLO_Header::CLIENT_LOCATION;
  }

  const std::string& csid = getCSID();
  ld_check(csid.size() < MAX_CSID_SIZE);
  if (!csid.empty()) {
    hdr.flags |= HELLO_Header::CSID;
  }

  std::unique_ptr<Message> hello = std::make_unique<HELLO_Message>(hdr);
  auto hello_v2 = static_cast<HELLO_Message*>(hello.get());
  hello_v2->source_node_id_ = source_node_id;
  hello_v2->destination_node_id_ = destination_node_id;
  hello_v2->cluster_name_ = cluster_name;
  hello_v2->csid_ = csid;
  hello_v2->build_info_ = extra_build_info;
  hello_v2->client_location_ = client_location;

  return hello;
}

std::unique_ptr<Message>
SocketDependencies::createShutdownMessage(uint32_t serverInstanceId) {
  SHUTDOWN_Header hdr{E::SHUTDOWN, serverInstanceId};
  return std::make_unique<SHUTDOWN_Message>(hdr);
}

uint16_t SocketDependencies::processHelloMessage(const Message* msg) {
  return std::min(static_cast<const HELLO_Message*>(msg)->header_.proto_max,
                  getSettings().max_protocol);
}

void SocketDependencies::processACKMessage(const Message* msg,
                                           ClientID* our_name_at_peer,
                                           uint16_t* destProto) {
  const ACK_Message* ack = static_cast<const ACK_Message*>(msg);
  *our_name_at_peer = ClientID(ack->getHeader().client_idx);
  *destProto = ack->getHeader().proto;
}

std::unique_ptr<Message>
SocketDependencies::deserialize(const ProtocolHeader& ph,
                                ProtocolReader& reader) {
  Message::deserializer_t* deserializer = messageDeserializers[ph.type];
  if (deserializer == nullptr) {
    ld_error("PROTOCOL ERROR: got an unknown message type '%c' (%d) from "
             "peer",
             int(ph.type),
             int(ph.type));
    err = E::BADMSG;
    return nullptr;
  }
  return deserializer(reader).msg;
}

std::string SocketDependencies::describeConnection(const Address& addr) {
  return Sender::describeConnection(addr);
}

folly::Func SocketDependencies::setupContextGuard() {
  // context can be setup multiple times in a recursive call to
  // setupContextGuard. unset_context logic makes sure that context is set just
  // once and unset in the same stack frame that it was set.
  bool unset_context =
      worker_ && worker_ != Worker::onThisThread(false /*enforce_worker*/);
  if (unset_context) {
    Worker::onSet(worker_);
  }

  return [unset_context] {
    if (unset_context) {
      Worker::onUnset();
    }
  };
}

folly::Executor* SocketDependencies::getExecutor() const {
  return worker_->getExecutor();
}

int SocketDependencies::getTCPInfo(TCPInfo* info, int fd) {
  LinuxNetUtils util;
  return util.getTCPInfo(info, fd);
}
}} // namespace facebook::logdevice
