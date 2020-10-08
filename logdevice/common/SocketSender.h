/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <forward_list>

#include <folly/ssl/OpenSSLPtrTypes.h>

#include "logdevice/common/ConnectionKind.h"
#include "logdevice/common/Priority.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/configuration/TrafficShapingConfig.h"

struct event;
struct event_base;

namespace facebook { namespace logdevice {

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class BWAvailableCallback;
class ClientIdxAllocator;
class Connection;
class FlowGroup;
class FlowGroupsUpdate;
class IConnectionFactory;
class Processor;
class SenderImpl;
class ShapingContainer;
class Socket;
class SocketProxy;
class StatsHolder;
class Worker;
struct Settings;

/**
 * The implementation of Sender API which based on using raw folly::AsyncSocket
 * for connections. See Connection.h for details.
 */
class SocketSender : public Sender {
 public:
  /**
   * @param node_count   the number of nodes in cluster configuration at the
   *                     time this Sender was created
   */

  SocketSender(
      Worker* worker,
      Processor* processor,
      std::shared_ptr<const Settings> settings,
      struct event_base* base,
      const configuration::ShapingConfig& tsc,
      ClientIdxAllocator* client_id_allocator,
      bool is_gossip_sender,
      std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes,
      node_index_t my_node_index,
      folly::Optional<NodeLocation> my_location,
      std::unique_ptr<IConnectionFactory> connection_factory,
      StatsHolder* stats);

  ~SocketSender() override;

  explicit SocketSender(const Sender&) = delete;
  explicit SocketSender(Sender&&) noexcept = delete;
  SocketSender& operator=(const Sender&) = delete;
  SocketSender& operator=(Sender&&) = delete;

  ///////////  SenderBase API //////////////

  bool canSendToImpl(const Address&,
                     TrafficClass,
                     BWAvailableCallback&) override;

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback* on_bw_avail,
                      SocketCallback* onclose) override;

  ///////////// Sender API ///////////////////

  int connect(node_index_t nid) override;

  int checkServerConnection(node_index_t node) const override;

  int checkClientConnection(ClientID, bool check_peer_is_node) const override;

  int closeConnection(const Address&, Status reason) override;

  bool isClosed(const Address&) const override;

  int registerOnConnectionClosed(const Address&, SocketCallback& cb) override;

  void beginShutdown() override;

  void forceShutdown() override;

  bool isShutdownCompleted() const override;

  void forEachConnection(
      const std::function<void(const ConnectionInfo&)>& cb) const override;

  const ConnectionInfo* FOLLY_NULLABLE
  getConnectionInfo(const Address&) const override;

  bool setConnectionInfo(const Address&, ConnectionInfo&&) override;

  std::pair<ExtractPeerIdentityResult, PrincipalIdentity>
  extractPeerIdentity(const Address& addr) override;

  void fillDebugInfo(InfoSocketsTable& table) const override;

  void setPeerShuttingDown(node_index_t) override;

  void noteConfigurationChanged(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>) override;

  void onSettingsUpdated(std::shared_ptr<const Settings> new_settings) override;

  /////////////// Socket-specific Sender API ///////////////

  /**
   * Dispatch any accumulated message completions. Must be called from
   * a context that can tolerate a completion re-entering Sender.
   */
  void deliverCompletedMessages();

  ShapingContainer* getNwShapingContainer() {
    return nw_shaping_container_.get();
  }

  /**
   * Creates a new client Connection for a newly accepted client connection and
   * inserts it into the client_sockets_ map. Must be called on the thread
   * running this Sender's Worker.
   *
   * @param fd          TCP socket that we got from accept(2)
   * @param client_addr sockaddr we got from accept(2)
   * @param conn_token  an object used for accepted connection accounting
   * @param type        type of socket (DATA/GOSSIP)
   * @param conntype    type of connection (PLAIN/SSL)
   *
   * @return  0 on success, -1 if we failed to create a Connection, sets err to:
   *     EXISTS          a Connection for this ClientID already exists
   *     NOMEM           a libevent function could not allocate memory
   *     NOBUFS          reached internal limit on the number of client
   * Connections (TODO: implement) INTERNAL        failed to set fd non-blocking
   * (unlikely)
   */
  int addClient(int fd,
                const Sockaddr& client_addr,
                ResourceBudget::Token conn_token,
                SocketType type,
                ConnectionType conntype,
                ConnectionKind connection_kind);

  /**
   * Called by a Connection managed by this Sender when bytes are added to one
   * of Connection's queues. These calls should be matched by
   * noteBytesDrained() calls, such that for each message_type (including
   * folly::none) the (queued - drained) value reflects the current in-flight
   * situation.
   * @param peer_type CLIENT or NODE.
   * @param message_type If message bytes are added to a queue, message_type is
   *        the type of that message. If message is passed to a evbuffer,
   *        message_type is folly::none.
   *
   * @param nbytes   how many bytes were appended
   */
  void noteBytesQueued(size_t nbytes,
                       PeerType peer_type,
                       folly::Optional<MessageType> message_type);

  /**
   * Called by a Connection managed by this Sender when some bytes from
   * the Connection output buffer have been drained into the
   * underlying TCP socket.
   * @param peer_type CLIENT or NODE.
   * @param message_type is treated the same way as in
   * noteBytesQueued()
   *
   * @param nbytes   how many bytes were sent
   */
  void noteBytesDrained(size_t nbytes,
                        PeerType peer_type,
                        folly::Optional<MessageType> message_type);

  /**
   * @return   the current total number of bytes in the output evbuffers of
   *           all Connections of all peer types managed by this Sender.
   */
  size_t getBytesPending() const {
    return bytes_pending_;
  }

  /**
   * @return true iff the total number of bytes in the output evbuffers of
   *              all Connections managed by this Sender exceeds the limit set
   *              in this Processor's configuration
   */
  bool bytesPendingLimitReached(PeerType peer_type) const;

  /**
   * Queue a message for a deferred completion. Used from contexts that
   * must be protected from re-entrance into Sender.
   */
  void queueMessageCompletion(std::unique_ptr<Message>,
                              const Address&,
                              Status,
                              SteadyTimestamp t);

  /**
   * Proxy for Connection::getTcpSendBufSize() for a client Connection.  Returns
   * -1 if Connection not found (should never happen).
   */
  ssize_t getTcpSendBufSizeForClient(ClientID client_id) const;

  /**
   * Proxy for Connection::getTcpSendBufOccupancy() for a client Connection.
   * Returns -1 if Connection not found.
   */
  ssize_t getTcpSendBufOccupancyForClient(ClientID client_id) const;

  /**
   * @return if this Sender manages a Connection for the node at configuration
   *         position idx, return that Connection. Otherwise return nullptr.
   *         Deprecated : Do not use this API to get Connection. Use of
   * Connection outside Sender is deprecated.
   */
  Connection* findServerConnection(node_index_t idx) const;

  /**
   * Resets the server Connection's connect throttle. If connection does not
   * exist does nothing.
   */
  void resetServerSocketConnectThrottle(node_index_t node_id);

  /**
   * Add a client id to the list of Connections to be erased from
   * .client_sockets_ next time this object tries to add a new entry to that
   * map.
   *
   * @param client_name   id of client Connection to erase
   *
   * @return  0 on success, -1 if client_name is not in .client_sockets_, err is
   *          set to NOTFOUND.
   */
  int noteDisconnectedClient(ClientID client_name);

  // Dumps a human-readable frequency map of queued messages by type.  If
  // `addr` is valid, only messages queued in that Connection are counted.
  // Otherwise, messages in all Connections are counted.
  std::string dumpQueuedMessages(Address addr) const;

 private:
  // Worker owning this Sender
  Worker* worker_;
  Processor* processor_;
  std::shared_ptr<const Settings> settings_;

  std::unique_ptr<IConnectionFactory> connection_factory_;

  // Network Traffic Shaping
  std::unique_ptr<ShapingContainer> nw_shaping_container_;

  // Pimpl
  friend class SenderImpl;
  std::unique_ptr<SenderImpl> impl_;

  bool is_gossip_sender_;

  std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes_;

  // ids of disconnected Connections to be erased from .client_sockets_
  std::forward_list<ClientID> disconnected_clients_;

  // To avoid re-entering Sender::sendMessage() when low priority messages
  // are trimmed from a FlowGroup's priority queue, trimmed messages are
  // accumulated in a deferred completion queue and then processed from
  // the event loop.
  CompletionQueue completed_messages_;

  std::atomic<bool> delivering_completed_messages_{false};

  // current number of bytes in all output buffers.
  std::atomic<size_t> bytes_pending_{0};
  // current number of bytes in all output buffers of logdevice client
  // connections.
  std::atomic<size_t> bytes_pending_client_{0};
  // current number of bytes in all output buffers of node connections.
  std::atomic<size_t> bytes_pending_node_{0};

  // if true, disallow sending messages and initiating connections
  bool shutting_down_ = false;

  // The id of this node.
  // If running on the client, this will be set to NODE_INDEX_INVALID.
  const node_index_t my_node_index_;

  // The location of this node (or client). Used to determine whether to use
  // SSL when connecting.
  const folly::Optional<NodeLocation> my_location_;

  void disconnectFromNodesThatChangedAddressOrGeneration();

  /**
   * Tells all open Connections to flush output and close, asynchronously.
   * isShutdownCompleted() can be used to find out when this operation
   * completes. Used during shutdown.
   */
  void flushOutputAndClose(Status reason);

  /**
   * Unconditionally close all server and clients Connections.
   */
  void closeAllSockets();

  /**
   * Close the Connection for this server
   *
   * @param peer   Node index for which to close the Connection.
   * @param reason Reason for closing the Connection.
   *
   * @return 0 on success, or -1 if there is no Connection for address `peer`,
   * in which case err is set to E::NOTFOUND.
   */
  int closeServerSocket(node_index_t peer, Status reason);

  /**
   * Close the Connection for a client.
   *
   * @param cid    Client for which to close the Connection.
   * @param reason Reason for closing the Connection.
   *
   * @return 0 on success, or -1 if there is no Connection for client `cid`, in
   *         which case err is set to E::NOTFOUND.
   */
  int closeClientSocket(ClientID cid, Status reason);

  /**
   * A helper method for sending a message to a connected Connection.
   *
   * @param msg   message to send. Ownership is not transferred unless the call
   *              succeeds.
   * @param conn  client server Connection to send the message into
   * @param onclose an optional callback to push onto the list of callbacks
   *                that the Connection through which the message gets sent will
   *                call when that Connection is closed. The callback will NOT
   * be installed if the call fails.
   *
   * @return 0 on success, -1 on failure. err is set to
   *    NOBUFS       Connection send queue limit was reached
   *    TOOBIG       message is too big (exceeds payload size limit)
   *    NOMEM        out of kernel memory for sockets, or malloc() failed
   *    CANCELLED    msg.cancelled() requested message to be cancelled
   *    INTERNAL     bufferevent unexpectedly failed to initiate connection,
   *                 unexpected error from socket(2).
   */
  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      Connection& conn,
                      BWAvailableCallback* on_bw_avail = nullptr,
                      SocketCallback* onclose = nullptr);

  /**
   * Returns a Connection to a given node in the cluster config. If a Connection
   * doesn't exist yet, it will be created.
   *
   * This will rely on ssl_boundary setting and target/own location to determine
   * whether to use SSL.
   *
   * @return a pointer to the valid Connection object; on failure returns
   * nullptr and sets err to NOTINCONFIG  nid is not present in the current
   * cluster config NOSSLCONFIG  Connection to nid must use SSL but SSL is not
   *                      configured for nid
   *         INTERNAL     internal error (debug builds assert)
   */
  Connection* initServerConnection(NodeID nid, SocketType sock_type);

  /**
   * This method gets the Connection associated with a given ClientID. The
   * connection must already exist for this method to succeed.
   *
   * @param cid  peer name of a client Connection expected to be under the
   *             management of this Sender.
   * @return the Connection connected to cid or nullptr if an error occured
   */
  Connection* FOLLY_NULLABLE getConnection(const ClientID& cid);

  /**
   * This method gets the Connection associated with a given NodeID. It will
   * attempt to create the connection if it doesn't already exist.
   *
   * @param nid        peer name of a client Connection expected to be under the
   *                   management of this Sender.
   * @param msg        the Message either being sent or received on the
   *                   Connection.
   * @return the Connection connected to nid or nullptr if an error occured
   */
  Connection* FOLLY_NULLABLE getConnection(const NodeID& nid,
                                           const Message& msg);

  /**
   * This method gets the Connection associated with a given Address. If addr is
   * a NodeID, it will attempt to create the connection if it doesn't already
   * exist.  Otherwise, the connection must already exist.
   *
   * @param addr       peer name of a Connection expected to be under the
   *                   management of this Sender.
   * @param msg        the Message either being sent or received on the
   *                   Connection.
   * @return the Connection connected to nid or nullptr if an error occured
   */
  Connection* FOLLY_NULLABLE getConnection(const Address& addr,
                                           const Message& msg);

  /**
   * This method finds any existing Connection associated with a given Address.
   *
   * @param addr       peer name of a Connection expected to be under the
   *                   management of this Sender.
   * @return the Connection connected to addr or nullptr, with err set, if
   *         the Connection couldn't be found.
   */
  Connection* FOLLY_NULLABLE findConnection(const Address& addr);

  /**
   * @return true iff called on the thread that is running this Sender's
   *         Worker. Used in asserts.
   */
  bool onMyWorker() const;

  /**
   * Remove client Connections on the disconnected client list from the client
   * map. Destroy the Connection objects.
   */
  void eraseDisconnectedClients();

  /**
   * Initializes my_node_id_ and my_location_ from the current config and
   * settings.
   */
  void initMyLocation();

  /**
   * Returns true if SSL should be used with the specified node.
   * If cross_boundary_out or authentication_out are given, outputs in them
   * whether SSL should be used for data encryption or for authentication
   * accordingly.
   */
  bool useSSLWith(node_index_t node_id,
                  bool* cross_boundary = nullptr,
                  bool* authentication = nullptr) const;

  /**
   * Closes all Connections in sockets_to_close_ with the specified error codes
   * and destroys them
   */
  void processSocketsToClose();

  /**
   * Detects and closes connections that are either slow or idle for prolonged
   * period of time.
   */
  void cleanupConnections();

  static void onCompletedMessagesAvailable(void* self, short);

  /**
   * Increment the bytes pending for a given peer type.
   */
  void incrementBytesPending(size_t nbytes, PeerType peer_type) {
    bytes_pending_ += nbytes;
    switch (peer_type) {
      case PeerType::CLIENT:
        bytes_pending_client_ += nbytes;
        return;
      case PeerType::NODE:
        bytes_pending_node_ += nbytes;
        return;
      default:
        ld_check(false);
    }
  }

  /**
   * Decrement the bytes pending for a given peer type.
   */
  void decrementBytesPending(size_t nbytes, PeerType peer_type) {
    ld_check(bytes_pending_ >= nbytes);
    ld_check(getBytesPending(peer_type) >= nbytes);
    bytes_pending_ -= nbytes;
    switch (peer_type) {
      case PeerType::CLIENT:
        bytes_pending_client_ -= nbytes;
        return;
      case PeerType::NODE:
        bytes_pending_node_ -= nbytes;
        return;
      default:
        ld_check(false);
    }
  }

  size_t getBytesPending(PeerType peer_type) const {
    switch (peer_type) {
      case PeerType::CLIENT:
        return bytes_pending_client_;
      case PeerType::NODE:
        return bytes_pending_node_;
      default:
        ld_check(false);
    }
    return 0;
  }

  Connection* FOLLY_NULLABLE findConnection(const Address& addr) const;
  Connection* FOLLY_NULLABLE
  findClientConnection(const ClientID& client_id) const;

  // Iterates over all connections and invokes a callback for each of them.
  void
  forAllConnections(const std::function<void(const Connection&)>& cb) const;
};

}} // namespace facebook::logdevice
