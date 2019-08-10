// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/SocketDependencies.h"

#include <folly/io/async/SSLContext.h>

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/libevent/compat.h"
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
    folly::Optional<MessageType> message_type) {
  sender_->noteBytesQueued(nbytes, message_type);
}

void SocketDependencies::noteBytesDrained(
    size_t nbytes,
    folly::Optional<MessageType> message_type) {
  sender_->noteBytesDrained(nbytes, message_type);
}

size_t SocketDependencies::getBytesPending() const {
  return sender_->getBytesPending();
}

std::shared_ptr<SSLContext>
SocketDependencies::getSSLContext(bufferevent_ssl_state ssl_state) const {
  // Servers are required to have a certificate so that the client can verify
  // them. If clients specify that they want to include their certificate, then
  // the server will also authenticate the client certificates.
  bool loadCert = getSettings().server || getSettings().ssl_load_client_cert;
  bool ssl_accepting = ssl_state == BUFFEREVENT_SSL_ACCEPTING;

  return Worker::onThisThread()->sslFetcher().getSSLContext(
      loadCert, ssl_accepting);
}

bool SocketDependencies::shuttingDown() const {
  return Worker::onThisThread()->shuttingDown();
}

std::string SocketDependencies::dumpQueuedMessages(Address addr) const {
  return sender_->dumpQueuedMessages(addr);
}

const Sockaddr& SocketDependencies::getNodeSockaddr(NodeID nid,
                                                    SocketType type,
                                                    ConnectionType conntype) {
  auto nodes_configuration = getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);

  // note: we don't check for generation here, if the generation has changed in
  // the future, Sender will reset the connection
  const auto* node_service_discovery =
      nodes_configuration->getNodeServiceDiscovery(nid.index());

  if (node_service_discovery) {
    if (type == SocketType::GOSSIP && !getSettings().send_to_gossip_port) {
      return node_service_discovery->getSockaddr(SocketType::DATA, conntype);
    } else {
      return node_service_discovery->getSockaddr(type, conntype);
    }
  }

  return Sockaddr::INVALID;
}

int SocketDependencies::eventAssign(struct event* ev,
                                    void (*cb)(evutil_socket_t,
                                               short what,
                                               void* arg),
                                    void* arg) {
  return LD_EV(event_assign)(ev,
                             EventLoop::onThisThread()->getEventBase(),
                             -1,
                             EV_WRITE | EV_PERSIST,
                             cb,
                             arg);
}

void SocketDependencies::eventActive(struct event* ev, int what, short ncalls) {
  LD_EV(event_active)(ev, what, ncalls);
}

void SocketDependencies::eventDel(struct event* ev) {
  LD_EV(event_del)(ev);
}

int SocketDependencies::eventPrioritySet(struct event* ev, int priority) {
  return LD_EV(event_priority_set)(ev, priority);
}

int SocketDependencies::evtimerAssign(struct event* ev,
                                      void (*cb)(evutil_socket_t,
                                                 short what,
                                                 void* arg),
                                      void* arg) {
  return evtimer_assign(ev, EventLoop::onThisThread()->getEventBase(), cb, arg);
}

void SocketDependencies::evtimerDel(struct event* ev) {
  evtimer_del(ev);
}

const struct timeval*
SocketDependencies::getCommonTimeout(std::chrono::milliseconds timeout) {
  return EventLoop::onThisThread()->getCommonTimeout(timeout);
}
const timeval*
SocketDependencies::getTimevalFromMilliseconds(std::chrono::milliseconds t) {
  static thread_local timeval tv_buf{0, 0};
  tv_buf.tv_sec = t.count() / 1000000;
  tv_buf.tv_usec = t.count() % 1000000;
  return &tv_buf;
}

const struct timeval* SocketDependencies::getZeroTimeout() {
  return EventLoop::onThisThread()->getZeroTimeout();
}

int SocketDependencies::evtimerAdd(struct event* ev,
                                   const struct timeval* timeout) {
  return evtimer_add(ev, timeout);
}

int SocketDependencies::evtimerPending(struct event* ev, struct timeval* tv) {
  return evtimer_pending(ev, tv);
}

struct bufferevent* FOLLY_NULLABLE
SocketDependencies::buffereventSocketNew(int sfd,
                                         int opts,
                                         bool secure,
                                         bufferevent_ssl_state ssl_state,
                                         SSLContext* ssl_ctx) {
  if (secure) {
    if (!ssl_ctx) {
      ld_error("Invalid SSLContext, can't create SSL socket");
      return nullptr;
    }

    SSL* ssl = ssl_ctx->createSSL();
    if (!ssl) {
      ld_error("Null SSL* returned, can't create SSL socket");
      return nullptr;
    }

    struct bufferevent* bev = bufferevent_openssl_socket_new(
        EventLoop::onThisThread()->getEventBase(), sfd, ssl, ssl_state, opts);
    if (!bev) {
      return nullptr;
    }

    ld_check(bufferevent_get_openssl_error(bev) == 0);
#if LIBEVENT_VERSION_NUMBER >= 0x02010100
    bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
#endif
    return bev;

  } else {
    return LD_EV(bufferevent_socket_new)(
        EventLoop::onThisThread()->getEventBase(), sfd, opts);
  }
}

struct evbuffer* SocketDependencies::getOutput(struct bufferevent* bev) {
  return LD_EV(bufferevent_get_output)(bev);
}

struct evbuffer* SocketDependencies::getInput(struct bufferevent* bev) {
  return LD_EV(bufferevent_get_input)(bev);
}

int SocketDependencies::buffereventSocketConnect(struct bufferevent* bev,
                                                 struct sockaddr* ss,
                                                 int len) {
  return LD_EV(bufferevent_socket_connect)(bev, ss, len);
}

void SocketDependencies::buffereventSetWatermark(struct bufferevent* bev,
                                                 short events,
                                                 size_t lowmark,
                                                 size_t highmark) {
  LD_EV(bufferevent_setwatermark)(bev, events, lowmark, highmark);
}

void SocketDependencies::buffereventSetCb(struct bufferevent* bev,
                                          bufferevent_data_cb readcb,
                                          bufferevent_data_cb writecb,
                                          bufferevent_event_cb eventcb,
                                          void* cbarg) {
  LD_EV(bufferevent_setcb)(bev, readcb, writecb, eventcb, cbarg);
}

void SocketDependencies::buffereventShutDownSSL(struct bufferevent* bev) {
  SSL* ctx = bufferevent_openssl_get_ssl(bev);
  ld_check(ctx);
  SSL_set_shutdown(ctx, SSL_RECEIVED_SHUTDOWN);
  SSL_shutdown(ctx);
  while (ERR_get_error()) {
    // flushing all SSL errors so they don't get misattributed to another
    // socket.
  }
}

void SocketDependencies::buffereventFree(struct bufferevent* bev) {
  LD_EV(bufferevent_free)(bev);
}

int SocketDependencies::evUtilMakeSocketNonBlocking(int sfd) {
  return LD_EV(evutil_make_socket_nonblocking)(sfd);
}

int SocketDependencies::buffereventSetMaxSingleWrite(struct bufferevent* bev,
                                                     size_t size) {
#if LIBEVENT_VERSION_NUMBER >= 0x02010000
  // In libevent >= 2.1 we can tweak the amount of data libevent sends to
  // the TCP stack at once
  return LD_EV(bufferevent_set_max_single_write)(bev, size);
#else
  // Let older libevent decide for itself.
  return 0;
#endif
}

int SocketDependencies::buffereventSetMaxSingleRead(struct bufferevent* bev,
                                                    size_t size) {
#if LIBEVENT_VERSION_NUMBER >= 0x02010000
  return LD_EV(bufferevent_set_max_single_read)(bev, size);
#else
  return 0;
#endif
}

int SocketDependencies::buffereventEnable(struct bufferevent* bev,
                                          short event) {
  return LD_EV(bufferevent_enable)(bev, event);
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
      Worker::onStartedRunning(run_context);
      Worker::onThisThread()->message_dispatch_->onSent(*msg, st, to, t);
      msg.reset(); // count destructor as part of message's execution time
      Worker::onStoppedRunning(run_context);
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

/**
 * Attempt to set SO_SNDBUF, SO_RCVBUF, TCP_NODELAY, SO_KEEP_ALIVE,
 * TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT, TCP_USER_TIMEOUT options of
 * socket fd to values in getSettings().
 *
 * @param is_tcp If set to false, do not set TCP_NODELAY because this socket is
 * not a tcp socket.
 * @param snd_out  if non-nullptr, report the value of SO_SNDBUF through this.
 *                 Set value to -1 if getsockopt() fails.
 * @param rcv_out  same as snd_out, but for SO_RCVBUF.
 *
 * NOTE that the values reported by getsockopt() for buffer sizes are 2X what
 * is passed in through setsockopt() because that's how Linux does it. See
 * socket(7).
 * NOTE that KEEP_ALIVE options are used only for tcp sockets (when is_tcp is
 * true).
 */
void SocketDependencies::configureSocket(bool is_tcp,
                                         int fd,
                                         int* snd_out,
                                         int* rcv_out,
                                         sa_family_t sa_family,
                                         const uint8_t default_dscp) {
  int sndbuf_size, rcvbuf_size;
  int rv;
  socklen_t optlen;

  sndbuf_size = getSettings().tcp_sendbuf_kb * 1024;
  if (sndbuf_size >= 0) {
    rv = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(int));
    if (rv != 0) {
      ld_error("Failed to set sndbuf size for TCP socket %d to %d: %s",
               fd,
               sndbuf_size,
               strerror(errno));
    }
  }

  if (snd_out) {
    optlen = sizeof(int);
    rv = getsockopt(fd, SOL_SOCKET, SO_SNDBUF, snd_out, &optlen);
    if (rv == 0) {
      *snd_out /= 2; // account for Linux doubling the value
    } else {
      ld_error("Failed to get sndbuf size for TCP socket %d: %s",
               fd,
               strerror(errno));
      *snd_out = -1;
    }
  }

  rcvbuf_size = getSettings().tcp_rcvbuf_kb * 1024;
  if (rcvbuf_size >= 0) {
    rv = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(int));
    if (rv != 0) {
      ld_error("Failed to set rcvbuf size for TCP socket %d to %d: %s",
               fd,
               rcvbuf_size,
               strerror(errno));
    }
  }

  if (rcv_out) {
    optlen = sizeof(int);
    rv = getsockopt(fd, SOL_SOCKET, SO_RCVBUF, rcv_out, &optlen);
    if (rv == 0) {
      *rcv_out /= 2; // account for Linux doubling the value
    } else {
      ld_error("Failed to get rcvbuf size for TCP socket %d: %s",
               fd,
               strerror(errno));
      *rcv_out = -1;
    }
  }

  if (is_tcp) {
    if (!getSettings().nagle) {
      int one = 1;
      rv = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      if (rv != 0) {
        ld_error("Failed to set TCP_NODELAY for TCP socket %d: %s",
                 fd,
                 strerror(errno));
      }
    }
  }

  bool keep_alive = getSettings().use_tcp_keep_alive;
  if (is_tcp && keep_alive) {
    int keep_alive_time = getSettings().tcp_keep_alive_time;
    int keep_alive_intvl = getSettings().tcp_keep_alive_intvl;
    int keep_alive_probes = getSettings().tcp_keep_alive_probes;

    rv = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keep_alive, sizeof(int));
    if (rv != 0) {
      ld_error("Failed to set SO_KEEPIDLE for TCP socket %d: %s",
               fd,
               strerror(errno));
    }

    if (keep_alive_time > 0) {
      rv = setsockopt(fd, SOL_TCP, TCP_KEEPIDLE, &keep_alive_time, sizeof(int));
      if (rv != 0) {
        ld_error("Failed to set TCP_KEEPIDLE for TCP socket %d: %s",
                 fd,
                 strerror(errno));
      }
    }

    if (keep_alive_intvl > 0) {
      rv = setsockopt(
          fd, SOL_TCP, TCP_KEEPINTVL, &keep_alive_intvl, sizeof(int));
      if (rv != 0) {
        ld_error("Failed to set TCP_KEEPINTVL for TCP socket %d: %s",
                 fd,
                 strerror(errno));
      }
    }

    if (keep_alive_probes > 0) {
      rv =
          setsockopt(fd, SOL_TCP, TCP_KEEPCNT, &keep_alive_probes, sizeof(int));
      if (rv != 0) {
        ld_error("Failed to set TCP_KEEPCNT for TCP socket %d: %s",
                 fd,
                 strerror(errno));
      }
    }
  }

#ifdef __linux__
  if (is_tcp) {
    int tcp_user_timeout = getSettings().tcp_user_timeout;

    if (tcp_user_timeout >= 0) {
      rv = setsockopt(
          fd, SOL_TCP, TCP_USER_TIMEOUT, &tcp_user_timeout, sizeof(int));
      if (rv != 0) {
        ld_error("Failed to set TCP_USER_TIMEOUT for TCP socket %d: %s",
                 fd,
                 strerror(errno));
      }
    }
  }
#endif
  rv = setDSCP(fd, sa_family, default_dscp);
  if (rv != 0) {
    ld_error(
        "DSCP(%x) configuration failed: %s", default_dscp, strerror(errno));
  }
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

bool SocketDependencies::authenticationEnabled() {
  if (processor_->security_info_) {
    auto principal_parser = processor_->security_info_->getPrincipalParser();
    return principal_parser != nullptr;
  } else {
    return false;
  }
}

bool SocketDependencies::allowUnauthenticated() {
  return getServerConfig()->allowUnauthenticated();
}

bool SocketDependencies::includeHELLOCredentials() {
  // Only include HELLOCredentials in HELLO_Message when the PrincipalParser
  // will use the data.
  auto principal_parser = processor_->security_info_->getPrincipalParser();
  return principal_parser != nullptr &&
      (principal_parser->getAuthenticationType() ==
       AuthenticationType::SELF_IDENTIFICATION);
}

void SocketDependencies::onStartedRunning(RunContext context) {
  Worker::onStartedRunning(context);
}

void SocketDependencies::onStoppedRunning(RunContext prev_context) {
  Worker::onStoppedRunning(prev_context);
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
  if (client_location_opt.hasValue()) {
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
}} // namespace facebook::logdevice
