/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sender.h"

#include <chrono>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <folly/DynamicConverter.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/json.h>
#include <folly/small_vector.h>

#include "event2/event.h"
#include "logdevice/common/ClientIdxAllocator.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/ShapingContainer.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/CONFIG_ADVISORY_Message.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/protocol/MessageTracer.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

using steady_clock = std::chrono::steady_clock;
using namespace configuration::nodes;

namespace {

// An object of this functor class is registered with every
// client Socket managed by this Sender and is called when the
// Socket closes.
class DisconnectedClientCallback : public SocketCallback {
 public:
  // calls noteDisconnectedClient() of the current thread's Sender
  void operator()(Status st, const Address& name) override;
};

} // namespace

class SenderImpl {
 public:
  explicit SenderImpl(ClientIdxAllocator* client_id_allocator)
      : client_id_allocator_(client_id_allocator) {}

  // a map of all Sockets that have been created on this Worker thread
  // in order to talk to LogDevice servers. Sockets are removed from this map
  // only when the corresponding server is no longer in the config (the
  // generation count of the Server's config record has
  // increased). Sockets are not removed when the connection breaks.
  // When a server goes down, its Socket's state changes to indicate
  // that it is now disconnected (.bev_ is nullptr, .connected_ is
  // false). The Socket object remains in the map. sendMessage() to
  // the node_index_t or NodeID of that Socket will try to reconnect.
  // The rate of reconnection attempts is controlled by a ConnectionThrottle.
  std::unordered_map<node_index_t, std::unique_ptr<Connection>> server_sockets_;

  // a map of all Sockets wrapping connections that were accepted from
  // clients, keyed by 32-bit client ids. This map is empty on clients.
  std::unordered_map<ClientID, std::unique_ptr<Connection>, ClientID::Hash>
      client_sockets_;

  ClientIdxAllocator* client_id_allocator_;

  Timer detect_slow_socket_timer_;
};

bool SenderProxy::canSendToImpl(const Address& addr,
                                TrafficClass tc,
                                BWAvailableCallback& on_bw_avail) {
  auto w = Worker::onThisThread();
  return w->sender().canSendToImpl(addr, tc, on_bw_avail);
}

int SenderProxy::sendMessageImpl(std::unique_ptr<Message>&& msg,
                                 const Address& addr,
                                 BWAvailableCallback* on_bw_avail,
                                 SocketCallback* onclose) {
  auto w = Worker::onThisThread();
  return w->sender().sendMessageImpl(
      std::move(msg), addr, on_bw_avail, onclose);
}

void SenderBase::MessageCompletion::send() {
  auto prev_context = Worker::packRunContext();
  RunContext run_context(msg_->type_);
  Worker::onStartedRunning(run_context);
  Worker::onThisThread()->message_dispatch_->onSent(
      *msg_, status_, destination_, enqueue_time_);
  msg_.reset(); // count destructor as part of message's execution time
  Worker::onStoppedRunning(run_context);
  Worker::unpackRunContext(prev_context);
}

Sender::Sender(std::shared_ptr<const Settings> settings,
               struct event_base* base,
               const configuration::ShapingConfig& tsc,
               ClientIdxAllocator* client_id_allocator,
               bool is_gossip_sender,
               std::shared_ptr<const NodesConfiguration> nodes,
               node_index_t my_index,
               folly::Optional<NodeLocation> my_location,
               StatsHolder* stats)
    : Sender(settings,
             base,
             tsc,
             client_id_allocator,
             is_gossip_sender,
             std::move(nodes),
             my_index,
             my_location,
             std::make_unique<ConnectionFactory>(*settings),
             stats){};

Sender::Sender(std::shared_ptr<const Settings> settings,
               struct event_base* base,
               const configuration::ShapingConfig& tsc,
               ClientIdxAllocator* client_id_allocator,
               bool is_gossip_sender,
               std::shared_ptr<const NodesConfiguration> nodes,
               node_index_t my_index,
               folly::Optional<NodeLocation> my_location,
               std::unique_ptr<IConnectionFactory> connection_factory,
               StatsHolder* stats)
    : settings_(settings),
      connection_factory_(std::move(connection_factory)),
      impl_(new SenderImpl(client_id_allocator)),
      is_gossip_sender_(is_gossip_sender),
      nodes_(std::move(nodes)),
      my_node_index_(my_index),
      my_location_(std::move(my_location)) {
  nw_shaping_container_ = std::make_unique<ShapingContainer>(
      static_cast<size_t>(NodeLocationScope::ROOT) + 1,
      base,
      tsc,
      std::make_shared<NwShapingFlowGroupDeps>(stats));

  auto scope = NodeLocationScope::NODE;
  for (auto& fg : nw_shaping_container_->flow_groups_) {
    fg.setScope(this, scope);
    scope = NodeLocation::nextGreaterScope(scope);
  }
}

Sender::~Sender() {
  deliverCompletedMessages();
}

void Sender::onCompletedMessagesAvailable(void* arg, short) {
  Sender* self = reinterpret_cast<Sender*>(arg);
  self->deliverCompletedMessages();
}

int Sender::addClient(int fd,
                      const Sockaddr& client_addr,
                      ResourceBudget::Token conn_token,
                      SocketType type,
                      ConnectionType conntype) {
  if (shutting_down_) {
    ld_check(false); // listeners are shut down before Senders.
    ld_error("Sender is shut down");
    err = E::SHUTDOWN;
    return -1;
  }

  eraseDisconnectedClients();
  // Activate slow_socket_timer if not active already.
  if (!impl_->detect_slow_socket_timer_.isActive()) {
    impl_->detect_slow_socket_timer_.setCallback(
        [this]() { closeSlowSockets(); });
    impl_->detect_slow_socket_timer_.activate(
        Worker::settings().max_time_to_allow_socket_drain);
  }

  auto w = Worker::onThisThread();
  ClientID client_name(
      impl_->client_id_allocator_->issueClientIdx(w->worker_type_, w->idx_));

  try {
    // Until we have better information (e.g. in a future update to the
    // HELLO message), assume clients are within our region unless they
    // have connected via SSL.
    NodeLocationScope flow_group_scope = conntype == ConnectionType::SSL
        ? NodeLocationScope::ROOT
        : NodeLocationScope::REGION;

    auto& flow_group = nw_shaping_container_->selectFlowGroup(flow_group_scope);

    auto socket = connection_factory_->createConnection(fd,
                                                        client_name,
                                                        client_addr,
                                                        std::move(conn_token),
                                                        type,
                                                        conntype,
                                                        flow_group);

    auto res = impl_->client_sockets_.emplace(client_name, std::move(socket));

    if (!res.second) {
      ld_critical("INTERNAL ERROR: attempt to add client %s (%s) that is "
                  "already in the client map",
                  client_name.toString().c_str(),
                  client_addr.toString().c_str());
      ld_check(0);
      err = E::EXISTS;
      return -1;
    }

    auto* cb = new DisconnectedClientCallback();
    // self-managed, destroyed by own operator()

    int rv = res.first->second->pushOnCloseCallback(*cb);

    if (rv != 0) { // unlikely
      ld_check(false);
      delete cb;
    }
  } catch (const ConstructorFailed&) {
    ld_error("Failed to construct a client socket: error %d (%s)",
             static_cast<int>(err),
             error_description(err));
    return -1;
  }

  return 0;
}

void Sender::noteBytesQueued(size_t nbytes) {
  // nbytes cannot exceed maximum message size
  ld_check(nbytes <= (size_t)Message::MAX_LEN + sizeof(ProtocolHeader));
  bytes_pending_ += nbytes;
  WORKER_STAT_ADD(evbuffer_total_size, nbytes);
  WORKER_STAT_ADD(evbuffer_max_size, nbytes);
}

void Sender::noteBytesDrained(size_t nbytes) {
  ld_check(bytes_pending_ >= nbytes);
  bytes_pending_ -= nbytes;
  WORKER_STAT_SUB(evbuffer_total_size, nbytes);
  WORKER_STAT_SUB(evbuffer_max_size, nbytes);
}

ssize_t Sender::getTcpSendBufSizeForClient(ClientID client_id) const {
  auto it = impl_->client_sockets_.find(client_id);
  if (it == impl_->client_sockets_.end()) {
    ld_error("client socket %s not found", client_id.toString().c_str());
    ld_check(false);
    return -1;
  }

  return it->second->getTcpSendBufSize();
}

void Sender::eraseDisconnectedClients() {
  auto prev = disconnected_clients_.before_begin();
  for (auto it = disconnected_clients_.begin();
       it != disconnected_clients_.end();) {
    const ClientID& cid = *it;
    const auto pos = impl_->client_sockets_.find(cid);
    ld_assert(pos != impl_->client_sockets_.end());
    ld_assert(pos->second->isClosed());
    ++it;
    // Check if there are users that still have reference to the socket. If
    // yes, wait for the clients to drop the socket instead of reclaiming it
    // here.
    if (!pos->second->isZombie()) {
      impl_->client_sockets_.erase(pos);
      impl_->client_id_allocator_->releaseClientIdx(cid);
      disconnected_clients_.erase_after(prev);
    } else {
      ++prev;
    }
  }
}

int Sender::notifyPeerConfigUpdated(Socket& sock) {
  auto server_config = Worker::onThisThread()->getServerConfig();
  config_version_t config_version = server_config->getVersion();
  config_version_t peer_config_version = sock.getPeerConfigVersion();
  if (peer_config_version >= config_version) {
    // The peer config is more recent. nothing to do here.
    return 0;
  }
  // The peer config version on the socket is outdated, so we need to notify
  // the peer and update it.
  std::unique_ptr<Message> msg;
  const Address& addr = sock.peer_name_;
  if (addr.isClientAddress()) {
    // The peer is a client, in the sense that it is the client-side of the
    // connection. It may very well be a node however.

    if (peer_config_version == CONFIG_VERSION_INVALID) {
      // We never received a CONFIG_ADVISORY message on this socket, so we
      // can assume that config synchronization is disabled on this client
      // or it hasn't got a chance to send the message yet. either way,
      // don't do anything yet.
      return 0;
    }

    ld_info("Detected stale peer config (%u < %u). "
            "Sending CONFIG_CHANGED to %s",
            peer_config_version.val(),
            config_version.val(),
            describeConnection(addr).c_str());
    ServerConfig::ConfigMetadata metadata =
        server_config->getMainConfigMetadata();

    // Send a CONFIG_CHANGED message to update the main config on the peer
    CONFIG_CHANGED_Header hdr{
        static_cast<uint64_t>(metadata.modified_time.count()),
        config_version,
        server_config->getServerOrigin(),
        CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
        CONFIG_CHANGED_Header::Action::UPDATE};
    metadata.hash.copy(hdr.hash, sizeof hdr.hash);

    msg = std::make_unique<CONFIG_CHANGED_Message>(
        hdr,
        server_config->toString(
            /* with_logs */ nullptr, /* with_zk */ nullptr, true));
  } else {
    // The peer is a server. Send a CONFIG_ADVISORY to let it know about our
    // config version. Upon receiving this message, if the server config
    // hasn't been updated already, it will either fetch it from us in
    // another round trip, or fetch the newest version directly from the
    // source.
    CONFIG_ADVISORY_Header hdr = {config_version};
    msg = std::make_unique<CONFIG_ADVISORY_Message>(hdr);
  }
  int rv = sendMessageImpl(std::move(msg), sock);
  if (rv != 0) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        10,
        "Failed to send %s with error %s.",
        addr.isClientAddress() ? "CONFIG_CHANGED" : "CONFIG_ADVISORY",
        error_description(err));
    return -1;
  }
  // Message was sent successfully. Peer should now have a version
  // greater than or equal to config_version.
  // Update peer version in socket to avoid sending CONFIG_ADVISORY or
  // CONFIG_CHANGED again on the same connection.
  sock.setPeerConfigVersion(config_version);
  return 0;
}

bool Sender::canSendToImpl(const Address& addr,
                           TrafficClass tc,
                           BWAvailableCallback& on_bw_avail) {
  ld_check(!on_bw_avail.active());

  Socket* sock = findSocket(addr);

  if (!sock) {
    if (addr.isClientAddress()) {
      // With no current client connection, the send is
      // guaranteeed to fail.
      return false;
    } else if (err == E::NOTCONN) {
      // sendMessage() will attempt to connect to a node
      // if no connection to that node currently exists.
      // Since we can't know for sure whether or not a
      // message will be throttled until after we're
      // connected and know the scope for that connection,
      // say that the send is expected to succeed so the
      // caller attempts to send and a connection attempt
      // will be made.
      return true;
    }
    return false;
  }

  Priority p = PriorityMap::fromTrafficClass()[tc];

  auto lock = nw_shaping_container_->lock();
  if (!sock->flow_group_.canDrain(p)) {
    sock->flow_group_.push(on_bw_avail, p);
    err = E::CBREGISTERED;
    FLOW_GROUP_STAT_INCR(Worker::stats(), sock->flow_group_, cbregistered);
    return false;
  }

  return true;
}

int Sender::sendMessageImpl(std::unique_ptr<Message>&& msg,
                            const Address& addr,
                            BWAvailableCallback* on_bw_avail,
                            SocketCallback* onclose) {
  Socket* sock = getSocket(addr, *msg);
  if (!sock) {
    // err set by getSocket()
    return -1;
  }

  // If the message is neither a handshake message nor a config sychronization
  // message, we need to update the client config version on the socket.
  if (!isHandshakeMessage(msg->type_) &&
      !isConfigSynchronizationMessage(msg->type_) &&
      settings_->enable_config_synchronization) {
    int rv = notifyPeerConfigUpdated(*sock);
    if (rv != 0) {
      return -1;
    }
  }

  int rv = sendMessageImpl(std::move(msg), *sock, on_bw_avail, onclose);
  ld_check(rv != 0 ? (bool)msg : !msg); // take ownership on success only
  if (rv != 0) {
    bool no_warning =
        // Some messages are implemented to gracefully handle
        // PROTONOSUPPORT, avoid log spew for them
        err == E::PROTONOSUPPORT && !msg->warnAboutOldProtocol();
    if (!no_warning) {
      RATELIMIT_LEVEL(
          err == E::CBREGISTERED ? dbg::Level::SPEW : dbg::Level::WARNING,
          std::chrono::seconds(10),
          3,
          "Unable to send a message of type %s to %s: error %s",
          messageTypeNames()[msg->type_].c_str(),
          Sender::describeConnection(addr).c_str(),
          error_description(err));
    }
  }
  return rv;
}

int Sender::sendMessageImpl(std::unique_ptr<Message>&& msg,
                            Socket& sock,
                            BWAvailableCallback* on_bw_avail,
                            SocketCallback* onclose) {
  ld_check(!shutting_down_);

  /* verify that we only send allowed messages via gossip socket */
  if (is_gossip_sender_) {
    if (!allowedOnGossipConnection(msg->type_)) {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Unexpected msg type:%u",
                        static_cast<unsigned char>(msg->type_));
      ld_check(false);
      err = E::INTERNAL;
      return -1;
    }
  }

  if (!isHandshakeMessage(msg->type_) && bytesPendingLimitReached()) {
    err = E::NOBUFS;
    return -1;
  }

  auto envelope = sock.registerMessage(std::move(msg));
  if (!envelope) {
    ld_check(err == E::INTERNAL || err == E::NOBUFS || err == E::NOTCONN ||
             err == E::PROTONOSUPPORT || err == E::UNREACHABLE);
    return -1;
  }
  ld_check(!msg);

  if (onclose) {
    sock.pushOnCloseCallback(*onclose);
  }

  auto lock = nw_shaping_container_->lock();
  if (!sock.flow_group_.injectShapingEvent(envelope->priority()) &&
      sock.flow_group_.drain(*envelope)) {
    lock.unlock();
    FLOW_GROUP_STAT_INCR(Worker::stats(), sock.flow_group_, direct_dispatched);
    // Note: Some errors can only be detected during message serialization.
    //       If this occurs just after releaseMessage() and the onSent()
    //       handler for the message responds to the error by queuing
    //       another message, Sender::sendMessage() can be reentered.
    sock.releaseMessage(*envelope);
    return 0;
  }

  // Message has hit a traffic shaping limit.
  if (on_bw_avail == nullptr) {
    // Sender/FlowGroup will release the message once bandwidth is
    // available.
    FLOW_GROUP_STAT_INCR(Worker::stats(), sock.flow_group_, deferred);
    FLOW_GROUP_MSG_STAT_INCR(
        Worker::stats(), sock.flow_group_, &envelope->message(), deferred);
    sock.flow_group_.push(*envelope);
    return 0;
  }

  // Message retransmission is the responsibility of the caller.  Return
  // message ownership to them.
  msg = sock.discardEnvelope(*envelope);

  FLOW_GROUP_STAT_INCR(Worker::stats(), sock.flow_group_, discarded);
  FLOW_GROUP_MSG_STAT_INCR(Worker::stats(), sock.flow_group_, msg, discarded);
  FLOW_GROUP_STAT_INCR(Worker::stats(), sock.flow_group_, cbregistered);
  sock.flow_group_.push(*on_bw_avail, msg->priority());
  sock.pushOnBWAvailableCallback(*on_bw_avail);
  err = E::CBREGISTERED;
  return -1;
}

Socket* Sender::findServerSocket(node_index_t idx) const {
  ld_check(idx >= 0);

  auto it = impl_->server_sockets_.find(idx);
  if (it == impl_->server_sockets_.end()) {
    return nullptr;
  }

  auto s = it->second.get();
  ld_check(s);
  ld_check(!s->peer_name_.isClientAddress());
  ld_check(s->peer_name_.asNodeID().index() == idx);

  return s;
}

folly::Optional<uint16_t>
Sender::getSocketProtocolVersion(node_index_t idx) const {
  auto socket = findServerSocket(idx);
  return socket != nullptr && socket->isHandshaken()
      ? socket->getProto()
      : folly::Optional<uint16_t>();
}

ClientID Sender::getOurNameAtPeer(node_index_t node_index) const {
  Socket* socket = findServerSocket(node_index);
  return socket != nullptr ? socket->getOurNameAtPeer() : ClientID::INVALID;
}

void Sender::deliverCompletedMessages() {
  CompletionQueue moved_queue = std::move(completed_messages_);
  while (!moved_queue.empty()) {
    std::unique_ptr<MessageCompletion> completion(&moved_queue.front());
    moved_queue.pop_front();
    if (!shutting_down_) {
      completion->send();
    }
  }
}

void Sender::resetServerSocketConnectThrottle(NodeID node_id) {
  ld_check(node_id.isNodeID());

  auto socket = findServerSocket(node_id.index());
  if (socket != nullptr) {
    socket->resetConnectThrottle();
  }
}

void Sender::setPeerShuttingDown(NodeID node_id) {
  ld_check(node_id.isNodeID());

  auto socket = findServerSocket(node_id.index());
  if (socket != nullptr) {
    socket->setPeerShuttingDown();
  }
}

int Sender::registerOnSocketClosed(const Address& addr, SocketCallback& cb) {
  Socket* sock;

  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos == impl_->client_sockets_.end()) {
      err = E::NOTFOUND;
      return -1;
    }
    sock = pos->second.get();
    if (sock->isClosed()) {
      err = E::NOTFOUND;
      return -1;
    }
  } else { // addr is a server address
    sock = findServerSocket(addr.asNodeID().index());
    if (!sock) {
      err = E::NOTFOUND;
      return -1;
    }
  }

  return sock->pushOnCloseCallback(cb);
}

void Sender::flushOutputAndClose(Status reason) {
  auto open_socket_count = 0;
  for (const auto& it : impl_->server_sockets_) {
    if (it.second && !it.second->isClosed()) {
      it.second->flushOutputAndClose(reason);
      ++open_socket_count;
    }
  }

  for (auto& it : impl_->client_sockets_) {
    if (it.second && !it.second->isClosed()) {
      if (reason == E::SHUTDOWN) {
        it.second->sendShutdown();
      }
      it.second->flushOutputAndClose(reason);
      ++open_socket_count;
    }
  }

  ld_log(open_socket_count ? dbg::Level::INFO : dbg::Level::SPEW,
         "Number of open sockets : %d",
         open_socket_count);
}
int Sender::closeSocket(Address addr, Status reason) {
  if (addr.isClientAddress()) {
    return closeClientSocket(addr.asClientID(), reason);
  } else {
    return closeServerSocket(addr.asNodeID(), reason);
  }
}

int Sender::closeClientSocket(ClientID cid, Status reason) {
  ld_check(cid.valid());

  auto pos = impl_->client_sockets_.find(cid);
  if (pos == impl_->client_sockets_.end()) {
    err = E::NOTFOUND;
    return -1;
  }

  pos->second->close(reason);
  return 0;
}

int Sender::closeServerSocket(NodeID peer, Status reason) {
  Socket* s = findServerSocket(peer.index());
  if (!s) {
    err = E::NOTFOUND;
    return -1;
  }

  if (!s->isClosed()) {
    s->close(reason);
  }

  return 0;
}

std::pair<uint32_t, uint32_t> Sender::closeAllSockets() {
  std::pair<uint32_t, uint32_t> sockets_closed = {0, 0};

  for (auto& entry : impl_->server_sockets_) {
    if (entry.second && !entry.second->isClosed()) {
      sockets_closed.first++;
      entry.second->close(E::SHUTDOWN);
    }
  }

  for (auto& entry : impl_->client_sockets_) {
    if (!entry.second->isClosed()) {
      sockets_closed.second++;
      entry.second->close(E::SHUTDOWN);
    }
  }

  return sockets_closed;
}

int Sender::closeAllClientSockets(Status reason) {
  int sockets_closed = 0;

  for (auto& it : impl_->client_sockets_) {
    if (!it.second->isClosed()) {
      sockets_closed++;
      it.second->close(reason);
    }
  }

  return sockets_closed;
}

bool Sender::isClosed() const {
  // Go over all sockets at shutdown to find pending work. This could help in
  // figuring which sockets are slow in draining buffers.
  bool go_over_all_sockets = !Worker::onThisThread()->isAcceptingWork();

  int num_open_server_sockets = 0;
  Socket* max_pending_work_server = nullptr;
  size_t server_with_max_pending_bytes = 0;
  for (const auto& it : impl_->server_sockets_) {
    if (it.second && !it.second->isClosed()) {
      if (!go_over_all_sockets) {
        return false;
      }

      ++num_open_server_sockets;
      const auto socket = it.second.get();
      size_t pending_bytes = socket->getBytesPending();
      if (server_with_max_pending_bytes < pending_bytes) {
        max_pending_work_server = socket;
        server_with_max_pending_bytes = pending_bytes;
      }
    }
  }

  int num_open_client_sockets = 0;
  ClientID max_pending_work_clientID;
  Socket* max_pending_work_client = nullptr;
  size_t client_with_max_pending_bytes = 0;
  for (auto& it : impl_->client_sockets_) {
    if (!it.second->isClosed()) {
      if (!go_over_all_sockets) {
        return false;
      }

      ++num_open_client_sockets;
      const auto socket = it.second.get();
      const size_t pending_bytes = socket->getBytesPending();
      if (client_with_max_pending_bytes < pending_bytes) {
        max_pending_work_client = socket;
        max_pending_work_clientID = it.first;
        client_with_max_pending_bytes = pending_bytes;
      }
    }
  }

  // None of the sockets are open return true.
  if (!num_open_client_sockets && !num_open_server_sockets) {
    return true;
  }

  RATELIMIT_INFO(std::chrono::seconds(5),
                 5,
                 "Sockets still open: Server socket count %d (max pending "
                 "bytes: %lu in socket 0x%p), Client socket count %d (max "
                 "pending bytes: %lu for client %s socket 0x%p)",
                 num_open_server_sockets,
                 server_with_max_pending_bytes,
                 (void*)max_pending_work_server,
                 num_open_client_sockets,
                 client_with_max_pending_bytes,
                 max_pending_work_clientID.toString().c_str(),
                 (void*)max_pending_work_client);

  return false;
}

bool Sender::isClosed(const Address& addr) const {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos == impl_->client_sockets_.end() || pos->second->isClosed()) {
      return true;
    }
  } else { // addr is a server address
    Socket* sock = findServerSocket(addr.asNodeID().index());
    if (!sock || sock->isClosed()) {
      return true;
    }
  }
  return false;
}

int Sender::checkConnection(NodeID nid,
                            ClientID* our_name_at_peer,
                            bool allow_unencrypted) {
  if (!nid.isNodeID()) {
    ld_check(false);
    err = E::INVALID_PARAM;
    return -1;
  }

  Socket* s = findServerSocket(nid.index());
  if (!s || !s->peer_name_.asNodeID().equalsRelaxed(nid)) {
    err = E::NOTFOUND;
    return -1;
  }

  if (!s->isSSL() && !allow_unencrypted && useSSLWith(nid)) {
    // We have a plaintext connection, but we need an encrypted one.
    err = E::SSLREQUIRED;
    return -1;
  }

  // check if the socket to destination has reached its buffer limit
  if (s->sizeLimitsExceeded()) {
    err = E::NOBUFS;
    return -1;
  }

  return s->checkConnection(our_name_at_peer);
}

int Sender::connect(NodeID nid, bool allow_unencrypted) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return -1;
  }

  Socket* s = initServerSocket(nid, SocketType::DATA, allow_unencrypted);
  if (!s) {
    return -1;
  }

  return s->connect();
}

bool Sender::useSSLWith(NodeID nid,
                        bool* cross_boundary_out,
                        bool* authentication_out) {
  if (nid.index() == my_node_index_) {
    // Don't use SSL for connections to self
    return false;
  }

  // Determine whether we need to use SSL by comparing our location with the
  // location of the target node.
  bool cross_boundary = false;
  NodeLocationScope diff_level = settings_->ssl_boundary;

  cross_boundary = configuration::nodes::getNodeSSL(
      *nodes_, my_location_, nid.index(), diff_level);

  // Determine whether we need to use an SSL socket for authentication.
  // We will use a SSL socket for authentication when the client or server
  // want to load their certificate.
  bool authentication = false;
  if (settings_->ssl_load_client_cert) {
    authentication = true;
  }

  if (cross_boundary_out) {
    *cross_boundary_out = cross_boundary;
  }
  if (authentication_out) {
    *authentication_out = authentication;
  }

  return cross_boundary || authentication;
}

Socket* Sender::initServerSocket(NodeID nid,
                                 SocketType sock_type,
                                 bool allow_unencrypted) {
  ld_check(!shutting_down_);
  const auto node_cfg = nodes_->getNodeServiceDiscovery(nid.index());

  // Don't try to connect if the node is not in config.
  // If the socket was already connected but the node removed from config,
  // it will be closed once noteConfigurationChanged() executes.
  if (!node_cfg) {
    err = E::NOTINCONFIG;
    return nullptr;
  }

  auto it = impl_->server_sockets_.find(nid.index());
  if (it != impl_->server_sockets_.end()) {
    // for DATA connection:
    //     reconnect if the connection is not SSL but should be.
    // for GOSSIP connection:
    //     reconnect if the connection is not SSL but ssl_on_gossip_port is true
    //     or the connection is SSL but the ssl_on_gossip_port is false.
    const bool should_reconnect =
        (sock_type != SocketType::GOSSIP && !it->second->isSSL() &&
         !allow_unencrypted && useSSLWith(nid)) ||
        (it->second->isSSL() != Worker::settings().ssl_on_gossip_port &&
         sock_type == SocketType::GOSSIP);

    if (should_reconnect) {
      // We have a plaintext connection, but now we need an encrypted one.
      // Scheduling this socket to be closed and moving it out of
      // server_sockets_ to initialize an SSL connection in its place.
      Worker::onThisThread()->add(
          [s = std::move(it->second)] { s->close(E::SSLREQUIRED); });
      impl_->server_sockets_.erase(it);
      it = impl_->server_sockets_.end();
    }
  }

  if (it == impl_->server_sockets_.end()) {
    // Activate slow_socket_timer if not active already.
    if (!impl_->detect_slow_socket_timer_.isActive()) {
      impl_->detect_slow_socket_timer_.setCallback(
          [this]() { closeSlowSockets(); });
      impl_->detect_slow_socket_timer_.activate(
          Worker::settings().max_time_to_allow_socket_drain);
    }
    // Don't try to connect if we expect a generation and it's different than
    // what is in the config.
    if (nid.generation() == 0) {
      nid = nodes_->getNodeID(nid.index());
    } else if (nodes_->getNodeGeneration(nid.index()) != nid.generation()) {
      err = E::NOTINCONFIG;
      return nullptr;
    }

    // Determine whether we should use SSL and what the flow group scope is
    auto flow_group_scope = NodeLocationScope::NODE;
    if (nid.index() != my_node_index_) {
      if (my_location_.hasValue() && node_cfg->location) {
        flow_group_scope =
            my_location_->closestSharedScope(*node_cfg->location);
      } else {
        // Assume within the same region for now, since cross region should
        // use SSL and have a location specified in the config.
        flow_group_scope = NodeLocationScope::REGION;
      }
    }

    bool cross_boundary = false;
    bool ssl_authentication = false;
    bool use_ssl = !allow_unencrypted &&
        useSSLWith(nid, &cross_boundary, &ssl_authentication);

    try {
      auto& flow_group =
          nw_shaping_container_->selectFlowGroup(flow_group_scope);
      if (sock_type == SocketType::GOSSIP) {
        ld_check(is_gossip_sender_);
        if (Worker::settings().send_to_gossip_port) {
          use_ssl = Worker::settings().ssl_on_gossip_port;
        }
      }

      auto sock = std::make_unique<Connection>(
          nid,
          sock_type,
          use_ssl ? ConnectionType::SSL : ConnectionType::PLAIN,
          flow_group);

      auto res = impl_->server_sockets_.emplace(nid.index(), std::move(sock));
      it = res.first;

      if (use_ssl && !cross_boundary) {
        // If the connection does not cross the ssl boundary, limit the ciphers
        // to eNULL ciphers to reduce overhead.
        it->second->limitCiphersToENULL();
      }
    } catch (ConstructorFailed&) {
      if (err == E::NOTINCONFIG || err == E::NOSSLCONFIG) {
        return nullptr;
      }
      ld_check(false);
      err = E::INTERNAL;
      return nullptr;
    }
  }

  ld_check(it->second != nullptr);
  return it->second.get();
}

Sockaddr Sender::getSockaddr(const Address& addr) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);
      return pos->second->peer_sockaddr_;
    }
  } else { // addr is a server address
    auto pos = impl_->server_sockets_.find(addr.asNodeID().index());
    if (pos != impl_->server_sockets_.end() &&
        pos->second->peer_name_.asNodeID().equalsRelaxed(addr.asNodeID())) {
      return pos->second->peer_sockaddr_;
    }
  }

  return Sockaddr::INVALID;
}

ConnectionType Sender::getSockConnType(const Address& addr) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);
      return pos->second->getConnType();
    }
  } else { // addr is a server address
    Socket* s = findServerSocket(addr.asNodeID().index());
    if (s && s->peer_name_.asNodeID().equalsRelaxed(addr.id_.node_)) {
      return s->getConnType();
    }
  }

  return ConnectionType::NONE;
}

Socket* FOLLY_NULLABLE Sender::getSocket(const ClientID& cid) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return nullptr;
  }

  auto pos = impl_->client_sockets_.find(cid);
  if (pos == impl_->client_sockets_.end()) {
    err = E::UNREACHABLE;
    return nullptr;
  }
  return pos->second.get();
}

Socket* FOLLY_NULLABLE Sender::getSocket(const NodeID& nid,
                                         const Message& msg) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return nullptr;
  }

  SocketType sock_type;
  if (is_gossip_sender_) {
    ld_check(allowedOnGossipConnection(msg.type_));
    sock_type = SocketType::GOSSIP;
  } else {
    sock_type = SocketType::DATA;
  }

  Socket* sock = initServerSocket(nid, sock_type, msg.allowUnencrypted());
  if (!sock) {
    // err set by initServerSocket()
    return nullptr;
  }

  int rv = sock->connect();

  if (rv != 0 && err != E::ALREADY && err != E::ISCONN) {
    // err can't be UNREACHABLE because sock must be a server socket
    ld_check(err == E::UNROUTABLE || err == E::DISABLED || err == E::SYSLIMIT ||
             err == E::NOMEM || err == E::INTERNAL);
    return nullptr;
  }

  // sock is now connecting or connected, send msg
  ld_assert(sock->connect() == -1 && (err == E::ALREADY || err == E::ISCONN));
  return sock;
}

Socket* FOLLY_NULLABLE Sender::getSocket(const Address& addr,
                                         const Message& msg) {
  return addr.isClientAddress() ? getSocket(addr.asClientID())
                                : getSocket(addr.asNodeID(), msg);
}

Socket* FOLLY_NULLABLE Sender::findSocket(const Address& addr) {
  if (addr.isClientAddress()) {
    // err, if any, set by getSocket().
    return getSocket(addr.asClientID());
  }

  Socket* sock = nullptr;
  NodeID nid = addr.asNodeID();
  auto it = impl_->server_sockets_.find(nid.index());
  if (it != impl_->server_sockets_.end()) {
    sock = it->second.get();
  }
  if (!sock) {
    err = E::NOTINCONFIG;
  }
  return sock;
}

const PrincipalIdentity* Sender::getPrincipal(const Address& addr) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);
      return pos->second->principal_.get();
    }
  } else { // addr is a server address
    auto pos = impl_->server_sockets_.find(addr.asNodeID().index());
    if (pos != impl_->server_sockets_.end() &&
        pos->second->peer_name_.asNodeID().equalsRelaxed(addr.asNodeID())) {
      // server_sockets_ principals should all be empty, this is because
      // the server_sockets_ will always be on the sender side, as in they
      // send the initial HELLO_Message. This means that they will never have
      // receive a HELLO_Message thus never have their principal set.
      ld_check(pos->second->principal_->type == "");
      return pos->second->principal_.get();
    }
  }

  return nullptr;
}

int Sender::setPrincipal(const Address& addr, PrincipalIdentity principal) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);

      // Whenever a HELLO_Message is sent, a new client socket is
      // created on the server side. Meaning that whenever this function is
      // called, the principal should be empty.
      ld_check(pos->second->principal_->type == "");

      // See if this principal requires specialized traffic tagging.
      Worker* w = Worker::onThisThread();
      auto scfg = w->getServerConfig();
      for (auto identity : principal.identities) {
        auto principal_settings = scfg->getPrincipalByName(&identity.second);
        if (principal_settings != nullptr &&
            principal_settings->egress_dscp != 0) {
          pos->second->setDSCP(principal_settings->egress_dscp);
          break;
        }
      }

      pos->second->principal_ =
          std::make_shared<PrincipalIdentity>(std::move(principal));
      return 0;
    }
  } else { // addr is a server address
    auto pos = impl_->server_sockets_.find(addr.asNodeID().index());
    if (pos != impl_->server_sockets_.end() &&
        pos->second->peer_name_.asNodeID().equalsRelaxed(addr.id_.node_)) {
      // server_sockets_ should never have setPrincipal called as they
      // should always be the calling side, as in they always send the
      // initial HELLO_Message.
      ld_check(false);
      return 0;
    }
  }

  return -1;
}

const std::string* Sender::getCSID(const Address& addr) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);
      return &pos->second->csid_;
    }
  } else { // addr is a server address
    auto pos = impl_->server_sockets_.find(addr.asNodeID().index());
    if (pos != impl_->server_sockets_.end() &&
        pos->second->peer_name_.asNodeID().equalsRelaxed(addr.id_.node_)) {
      // server_sockets_ csid should all be empty, this is because
      // the server_sockets_ will always be on the sender side, as in they
      // send the initial HELLO_Message. This means that they will never
      // have receive a HELLO_Message thus never have their csid set.
      ld_check(pos->second->csid_ == "");
      return &pos->second->csid_;
    }
  }

  return nullptr;
}

int Sender::setCSID(const Address& addr, std::string csid) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);

      // Whenever a HELLO_Message is sent, a new client socket is
      // created on the server side. Meaning that whenever this function is
      // called, the principal should be empty.
      ld_check(pos->second->csid_ == "");
      pos->second->csid_ = std::move(csid);
      return 0;
    }
  } else { // addr is a server address
    auto pos = impl_->server_sockets_.find(addr.asNodeID().index());
    if (pos != impl_->server_sockets_.end() &&
        pos->second->peer_name_.asNodeID().equalsRelaxed(addr.id_.node_)) {
      // server_sockets_ should never have setCSID called as they
      // should always be the calling side, as in they always send the
      // initial HELLO_Message.
      ld_check(false);
      return 0;
    }
  }

  return -1;
}

std::string Sender::getClientLocation(const ClientID& cid) {
  Socket* sock = getSocket(cid);
  if (!sock) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Could not find socket for connection: %s",
                    describeConnection(cid).c_str());
    return "";
  }
  return sock->peer_location_;
}

void Sender::setClientLocation(const ClientID& cid,
                               const std::string& location) {
  Socket* sock = getSocket(cid);
  if (!sock) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Could not find socket for connection: %s",
                    describeConnection(cid).c_str());
    return;
  }
  sock->peer_location_ = location;
}

void Sender::setPeerConfigVersion(const Address& addr,
                                  const Message& msg,
                                  config_version_t version) {
  Socket* sock = getSocket(addr, msg);
  if (!sock) {
    ld_check(err == E::SHUTDOWN);
    ld_info("Shutting down. Cannot set peer config version.");
    return;
  }
  sock->setPeerConfigVersion(version);
}

X509* Sender::getPeerCert(const Address& addr) {
  if (addr.isClientAddress()) {
    auto pos = impl_->client_sockets_.find(addr.id_.client_);
    if (pos != impl_->client_sockets_.end()) {
      ld_check(pos->second->peer_name_ == addr);
      if (pos->second->isSSL()) {
        return pos->second->getPeerCert();
      }
    }
  } else { // addr is a server address
    auto pos = impl_->server_sockets_.find(addr.asNodeID().index());
    if (pos != impl_->server_sockets_.end() &&
        pos->second->peer_name_.asNodeID().equalsRelaxed(addr.id_.node_)) {
      if (pos->second->isSSL()) {
        X509* cert = pos->second->getPeerCert();

        // Logdevice server nodes are required to send their certificate
        // to the client when creating an SSL socket.
        ld_check(cert);
        return cert;
      }
    }
  }

  return nullptr;
}

Sockaddr Sender::thisThreadSockaddr(const Address& addr) {
  Worker* w = Worker::onThisThread();
  return w->sender().getSockaddr(addr);
}

Sockaddr Sender::sockaddrOrInvalid(const Address& addr) {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    return Sockaddr();
  }
  return w->sender().getSockaddr(addr);
}

NodeID Sender::getNodeID(const Address& addr) const {
  if (!addr.isClientAddress()) {
    return addr.id_.node_;
  }

  auto it = impl_->client_sockets_.find(addr.id_.client_);
  return it != impl_->client_sockets_.end() ? it->second->peer_node_id_
                                            : NodeID();
}

void Sender::setPeerNodeID(const Address& addr, NodeID node_id) {
  auto it = impl_->client_sockets_.find(addr.id_.client_);
  if (it != impl_->client_sockets_.end()) {
    NodeID& peer_node = it->second->peer_node_id_;
    peer_node = node_id;
    if (node_id.isNodeID()) {
      it->second->setDSCP(settings_->server_dscp_default);
    }
  }
}

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

int Sender::noteDisconnectedClient(ClientID client_name) {
  ld_check(client_name.valid());
  if (Worker::onThisThread()->shuttingDown()) {
    // This instance might already be (partially) destroyed.
    return 0;
  }

  if (impl_->client_sockets_.count(client_name) == 0) {
    ld_critical("INTERNAL ERROR: the name of a disconnected client socket %s "
                "is not in the client map",
                client_name.toString().c_str());
    ld_check(0);
    err = E::NOTFOUND;
    return -1;
  }

  disconnected_clients_.push_front(client_name);

  return 0;
}

void DisconnectedClientCallback::operator()(Status st, const Address& name) {
  ld_debug("Sender's DisconnectedClientCallback called for socket %s "
           "with status %s",
           Sender::describeConnection(name).c_str(),
           error_name(st));

  ld_check(!active());

  Worker::onThisThread()->sender().noteDisconnectedClient(name.id_.client_);

  delete this;
}

void Sender::noteConfigurationChanged(
    std::shared_ptr<const NodesConfiguration> nodes_configuration) {
  nodes_ = std::move(nodes_configuration);

  auto it = impl_->server_sockets_.begin();
  while (it != impl_->server_sockets_.end()) {
    auto& s = it->second;
    auto i = it->first;
    ld_check(!s->peer_name_.isClientAddress());
    ld_check(s->peer_name_.asNodeID().index() == i);

    const auto node_service_discovery = nodes_->getNodeServiceDiscovery(i);

    if (node_service_discovery != nullptr) {
      node_gen_t generation = nodes_->getNodeGeneration(i);
      const Sockaddr& newaddr = node_service_discovery->getSockaddr(
          s->getSockType(), s->getConnType());
      if (s->peer_name_.asNodeID().generation() == generation &&
          s->peer_sockaddr_ == newaddr) {
        ++it;
        continue;
      } else {
        ld_info("Configuration change detected for node %s. New generation "
                "count is %d. New IP address is %s. Destroying old socket.",
                Sender::describeConnection(Address(s->peer_name_.id_.node_))
                    .c_str(),
                generation,
                newaddr.toString().c_str());
      }

    } else {
      ld_info(
          "Node %s is no longer in cluster configuration. New cluster "
          "size is %zu. Destroying old socket.",
          Sender::describeConnection(Address(s->peer_name_.id_.node_)).c_str(),
          nodes_->clusterSize());
    }

    s->close(E::NOTINCONFIG);
    it = impl_->server_sockets_.erase(it);
  }
}

bool Sender::bytesPendingLimitReached() {
  size_t limit = Worker::settings().outbufs_mb_max_per_thread * 1024 * 1024;
  bool limit_reached = getBytesPending() > limit;
  if (limit_reached) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "ENOBUFS for Sender. Current socket usage: %zu, max: %zu",
                      getBytesPending(),
                      limit);
  }
  return limit_reached;
}

void Sender::queueMessageCompletion(std::unique_ptr<Message> msg,
                                    const Address& to,
                                    Status s,
                                    const SteadyTimestamp t) {
  auto mc = std::make_unique<MessageCompletion>(std::move(msg), to, s, t);
  completed_messages_.push_back(*mc.release());
  if (!delivering_completed_messages_.exchange(true)) {
    Worker::onThisThread()->add([this] {
      delivering_completed_messages_.store(false);
      deliverCompletedMessages();
    });
  }
}

std::string Sender::dumpQueuedMessages(Address addr) const {
  std::map<MessageType, int> counts;
  if (addr.valid()) {
    const Socket* socket = nullptr;
    if (addr.isClientAddress()) {
      const auto it = impl_->client_sockets_.find(addr.id_.client_);
      if (it != impl_->client_sockets_.end()) {
        socket = it->second.get();
      }
    } else {
      const auto idx = addr.asNodeID().index();
      const auto it = impl_->server_sockets_.find(idx);
      if (it != impl_->server_sockets_.end()) {
        socket = it->second.get();
      }
    }
    if (socket == nullptr) {
      // Unexpected but not worth asserting on (crashing the server) since
      // this is debugging code
      return "<socket not found>";
    }
    socket->dumpQueuedMessages(&counts);
  } else {
    for (const auto& entry : impl_->server_sockets_) {
      entry.second->dumpQueuedMessages(&counts);
    }

    for (const auto& entry : impl_->client_sockets_) {
      entry.second->dumpQueuedMessages(&counts);
    }
  }
  std::unordered_map<std::string, int> strmap;
  for (const auto& entry : counts) {
    strmap[messageTypeNames()[entry.first].c_str()] = entry.second;
  }
  return folly::toJson(folly::toDynamic(strmap));
}

void Sender::forEachSocket(std::function<void(const Socket&)> cb) const {
  for (const auto& entry : impl_->server_sockets_) {
    cb(*entry.second);
  }
  for (const auto& entry : impl_->client_sockets_) {
    cb(*entry.second);
  }
}

std::unique_ptr<SocketProxy> Sender::getSocketProxy(const ClientID cid) const {
  ld_check(cid.valid());
  auto pos = impl_->client_sockets_.find(cid);
  if (pos == impl_->client_sockets_.end()) {
    return std::unique_ptr<SocketProxy>();
  }

  return pos->second->getSocketProxy();
}

void Sender::forAllClientSockets(std::function<void(Socket&)> fn) {
  for (auto& it : impl_->client_sockets_) {
    fn(*it.second);
  }
}

void Sender::closeSlowSockets() {
  size_t sockets_closed = 0;
  // Allow this function to run just for 2ms. Stop loop if it goes beyond that
  // time. We will try it again in 10s.
  auto deadline = SteadyTimestamp::now() + std::chrono::milliseconds(2);
  bool end_loop = false;
  auto close_if_slow = [&](Socket& sock) {
    if (sock.slowInDraining()) {
      ++sockets_closed;
      sock.close(E::TIMEDOUT);
      if (SteadyTimestamp::now() > deadline) {
        end_loop = true;
      }
    }
  };

  for (auto& entry : impl_->server_sockets_) {
    if (end_loop) {
      break;
    }
    close_if_slow(*entry.second);
  }
  for (auto& entry : impl_->client_sockets_) {
    if (end_loop) {
      break;
    }
    close_if_slow(*entry.second);
  }
  ld_log(sockets_closed > 0 ? dbg::Level::INFO : dbg::Level::DEBUG,
         "Sender closed %lu slow sockets. ended loop soon %d",
         sockets_closed,
         end_loop);
  impl_->detect_slow_socket_timer_.activate(
      Worker::settings().max_time_to_allow_socket_drain);
}
}} // namespace facebook::logdevice
