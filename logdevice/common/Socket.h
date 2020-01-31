/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <deque>
#include <memory>

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/futures/Future.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/SSLContext.h>

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event2/bufferevent_ssl.h"
#include "event2/event.h"
#include "event2/event_struct.h"
#include "logdevice/common/Address.h"
#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/ConnectThrottle.h"
#include "logdevice/common/CostQueue.h"
#include "logdevice/common/Envelope.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/PriorityQueue.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/RunContext.h"
#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/libevent/LibEventCompatibility.h"
#include "logdevice/common/network/SocketWriteCallback.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

class BWAvailableCallback;
class FlowGroup;
class ProtocolHandler;
class ResourceBudget;
class SocketAdapter;
class SocketCallback;
class SocketImpl;
class SocketProxy;
class StatsHolder;
struct Settings;

/**
 * @file  a Socket is an endpoint of a connection that can send and
 *        receive messages.  Internally a Socket encapsulates a bufferevent
 *        running in a libevent 2.x event loop on a Worker thread. Sockets are
 *        not shared between Worker threads. All operations on a Socket,
 *        including its creation, must be performed on the same thread.
 */

// A note on memory management for Sockets.
//
// Every Socket object is emplaced on a map with the address of the
// peer end as key. The map is local to a Worker object on which
// the Socket runs. Upper layers of LD code, such as Request objects,
// are not expected to send messages directly into Sockets. Instead
// they send them through Sender objects (one per Worker) to a
// destination Address, which can be a client or a server address. The
// Sender looks up a Socket by address and sends the Message into that
// Socket. If no Socket is found for Address, an error is reported. When
// the socket is no longer needed it is erased from its map. Pointers to
// Sockets are not stored anywhere other than in the address map and in
// local variables. Since Sockets are not shared among threads, this
// guarantees that when a Socket is destroyed, no dangling pointers are
// left around.
//
// This is an alternative to passing shared_ptr<Socket>s around. I find it
// simpler and more transparent than using shared pointers. The disadvantage
// is that this will work only if Sockets cannot be shared among threads, but
// that is currently not needed.

// Defined later in this file.
class SocketDependencies;
/**
 * adding multiple classes here which will get renamed/refactored throguh a
 * a stack of diffs to separate different logical component which are now alloc
 * inside socket
 */
/**
 * Will be used as a stable interface of supported stuff by basic socket
 * our current socket knows too much about traffic shaping. SocketBase will note
 * SocketBase will be renamed to Socket once current socket gets cleand up
 */
class SocketBase {};
/*
 * temporary container for all traffic shapping related stuff from socket
 */
class TrafficShappingSocket : public SocketBase {
 public:
  using PendingQueue = PriorityQueue<Envelope, &Envelope::links_>;
  using EnvelopeQueue = CostQueue<Envelope, &Envelope::links_>;
};
/**
 * this gets an open socket and negotiates connection to decouple code and
 * remove special handling of messages before/after protocol negotiation
 */
class ConnectionNegotiator {};

class Socket_DEPRECATED : public TrafficShappingSocket {
 public:
  /**
   * Constructs a new Socket, to be connected to a LogDevice
   * server. The calling thread must be a Worker thread.
   *
   * @param server_name     id of server to connect to
   * @param socketType      type of socket
   * @param connectionType  type of connection
   * @param peerType        type of peer
   * @param flow_group      traffic shaping state shared between sockets
   *                        with the same bandwidth constraints.
   *
   * @return  on success, a new fully constructed Socket is returned. It is
   *          expected that the Socket will be registered with the Worker's
   *          Sender under server_name. On failure throws ConstructorFailed and
   *          sets err to:
   *
   *     INVALID_THREAD  current thread is not running a Worker (debug build
   *                     asserts)
   *     NOTINCONFIG     server_name does not appear in cluster config
   *     INTERNAL        failed to initialize a libevent timer (unlikely)
   */
  Socket_DEPRECATED(NodeID server_name,
                    SocketType socketType,
                    ConnectionType connectionType,
                    PeerType peerType,
                    FlowGroup& flow_group,
                    std::unique_ptr<SocketDependencies> deps);
  /**
   * Constructs a new Socket from a TCP socket fd that was returned by accept().
   * The thread must run a Worker. On success the socket is emplaced on this
   * Sender's .client_sockets_ map with client_name as key.
   *
   * @param fd        fd of the accepted socket. The caller passes
   *                  responsibility for closing fd to the constructor.
   * @param client_name local identifier assigned to this passively accepted
   *                    connection (aka "client address")
   * @param client_addr sockaddr we got from accept() for this client connection
   * @param conn_token  used to keep track of all accepted connections
   * @param type        type of socket
   * @param flow_group  traffic shaping state shared between sockets
   *                    with the same bandwidth constraints.
   *
   * @return  on success, a new fully constructed Socket is returned. On
   *          failure throws ConstructorFailed and sets err to:
   *
   *     INVALID_THREAD  current thread is not running a Worker (debug build
   *                     asserts)
   *     NOMEM           a libevent function could not allocate memory
   *     INTERNAL        failed to set fd non-blocking (unlikely) or failed to
   *                     initialize a libevent timer (unlikely).
   */
  Socket_DEPRECATED(int fd,
                    ClientID client_name,
                    const Sockaddr& client_addr,
                    ResourceBudget::Token conn_token,
                    SocketType type,
                    ConnectionType conntype,
                    FlowGroup& flow_group,
                    std::unique_ptr<SocketDependencies> deps);

  // These two contructors are final two constructors that will be in use once
  // we deprecate this class completely.
  Socket_DEPRECATED(NodeID server_name,
                    SocketType type,
                    ConnectionType conntype,
                    PeerType peerType,
                    FlowGroup& flow_group,
                    std::unique_ptr<SocketDependencies> deps,
                    std::unique_ptr<SocketAdapter> sock_adapter);

  Socket_DEPRECATED(int fd,
                    ClientID client_name,
                    const Sockaddr& client_addr,
                    ResourceBudget::Token conn_token,
                    SocketType type,
                    ConnectionType conntype,
                    FlowGroup& flow_group,
                    std::unique_ptr<SocketDependencies> deps,
                    std::unique_ptr<SocketAdapter> sock_adapter);
  /**
   * Disconnects, deletes the underlying bufferevent, and closes the TCP socket.
   */
  virtual ~Socket_DEPRECATED();

  Socket_DEPRECATED(const Socket_DEPRECATED&) = delete;
  Socket_DEPRECATED(Socket_DEPRECATED&&) = delete;
  Socket_DEPRECATED& operator=(const Socket_DEPRECATED&) = delete;
  Socket_DEPRECATED& operator=(Socket_DEPRECATED&&) = delete;

  Sockaddr peerSockaddr() const {
    return peer_sockaddr_;
  }

  /**
   * For Testing only!
   */
  void enableChecksumTampering(bool enable) {
    tamper_ = enable;
  }

  void setPeerNodeId(const NodeID node_id) {
    peer_node_id_ = node_id;
    if (peer_name_.isClientAddress() && !peer_node_id_.isNodeID()) {
      peer_type_ = PeerType::CLIENT;
    } else {
      peer_type_ = PeerType::NODE;
    }
  }

  // LogDevice-level address of peer end-point at the other end of the
  // connection
  const Address peer_name_;

  // struct sockaddr of peer end point
  const Sockaddr peer_sockaddr_;

  // A numan-readable string like
  // "C22566784 ([abcd:1234:5678:90ef:1111:2222:3333:4444]:41406)"
  std::string conn_description_;

  // Node location of peer end point.
  // In format "{region}.{datacenter}.{cluster}.{row}.{rack}"
  // Currently only used for passing client location from client to server
  // for local SCD reading.
  std::string peer_location_;

  // Used to identify the client for permission checks. Set after successfull
  // authentication
  std::shared_ptr<PrincipalIdentity> principal_ =
      std::make_shared<PrincipalIdentity>();

  // CSID, Client Session ID
  // Used to uniquely identify client sessions. Supplied by client
  std::string csid_;

  // NodeID of the peer if this is a client (incoming) connection with another
  // node from the cluster on the other end.
  NodeID peer_node_id_;

  // Type of the peer this socket is connecte to (CLIENT or NODE)
  PeerType peer_type_{PeerType::NODE};

  // Traffic shaping state shared between Sockets with the same bandwidth
  // constraints.
  FlowGroup& flow_group_;

  // indicates purpose of this socket
  const SocketType type_;

  /**
   * Initiate an asynchronous connect and handshake on the socket. The socket's
   * .peer_name_ must resolve to an ip:port to which we can connect. Currently
   * this means that .peer_name_ must be a server address. The function MUST
   * be called on the Worker thread that runs this Socket.
   *
   * @return  0 if connection was successfully initiated. -1 on failure, err
   *          is set to:
   *
   *    ALREADY         the socket is already in CONNECTING or HANDSHAKE
   *    ISCONN          the socket is CONNECTED
   *    UNREACHABLE     attempt to connect to a client. Reported for
   *                    disconnected client sockets.
   *    UNROUTABLE      the peer endpoint of a server socket has an IP address
   *                    to which there is no route. This may happen if a network
   *                    interface has been taken down, e.g., during system
   *                    shutdown.
   *    DISABLED        connection was not initiated because the server
   *                    is temporarily marked down (disabled) after a series
   *                    of unsuccessful connection attempts
   *    SYSLIMIT        out of file descriptors or ephemeral ports
   *    NOMEM           out of kernel memory for sockets, or malloc() failed
   *    INTERNAL        bufferevent unexpectedly failed to initiate connection,
   *                    unexpected error from socket(2).
   */
  virtual int connect();

  /**
   * Register a message with this socket and return the enclosing message
   * Envelope. Connectivity, outstanding byte limits, and Message validation
   * checks are performed at this time and will cause this function to fail.
   * However, certain classes of errors can occur or be detected between
   * registerMessage() and releaseMessage(). These errors are reported via
   * Message::OnSent().
   *
   * @param msg  Message to register. It is passed by rvalue reference to a
   *             unique_ptr so that the function can pass ownership of msg to
   *             Envelope conditionally. If the call succeeds msg is reset
   *             and the returned Envelope is owned by this Socket. This Socket
   *             is responsible for destroying the Envelope after it is sent
   *             (post releaseMessage()) or discarded.

   * @return a pointer to a newly created Envelope if the message was
   *         successfully registered. Otherwise a nullptr with err
   *         set to:
   *
   *    NOTCONN        Socket is not connected.
   *    UNREACHABLE    attempt to send a message other than ACK to a client
   *                   before handshake is completed
   *    NOBUFS         send queue size limit is reached
   *    INTERNAL       a message serialization function failed
   *    PROTONOSUPPORT the handshaken protocol is not compatible with this
   *                   message.
   */
  Envelope* registerMessage(std::unique_ptr<Message>&&);

  /**
   * Release an envelope for delivery (i.e. there is sufficient bandwidth
   * credit for transmission to proceed).
   */
  void releaseMessage(Envelope&);

  /**
   * Unregister an envelope. The envelope is destroyed by this
   * action. The message will be too if not "received" by the
   * caller.
   */
  std::unique_ptr<Message> discardEnvelope(Envelope&);

  /**
   * Closes our end of the connection, deletes the bufferevent, and
   * disposes of all pending messages calling their onSent() functions
   *
   * @param reason reason for closing our end. This is currently one of the
   *               following: ACCESS, CONNFAILED, BADMSG, SHUTDOWN, TIMEDOUT
   *                          NOTINCONFIG, INTERNAL, PROTO, PROTONOSUPPORT,
   *                          DESTINATION_MISMATCH, PEER_CLOSED,
   *                          PEER_UNAVAILABLE, INVALID_CLUSTER.
   */
  virtual void close(Status reason);

  /**
   * Make sure any enqueued message is sent and close the connection.
   *
   * @param reason for closing. See close().
   */
  void flushOutputAndClose(Status reason);

  /**
   * Set the RFC 2474 "Differentiated Services Field Code Point" value
   * to be used on all packets sent from this socket.
   */
  void setDSCP(uint8_t dscp);

  /**
   * Set the mark for each packet sent through this socket. Setting this option
   * requires the CAP_NET_ADMIN capability.
   */
  void setSoMark(uint32_t so_mark);

  /**
   * @return true iff close() has been called on the socket, or if it is
   *         a server socket that has never been connected
   */
  bool isClosed() const;

  /**
   * @return true iff socket is in a good state to be used for reading and
   * writing.
   */
  bool good() const;
  /**
   * @return true iff close() has been called on the socket and all clients have
   * dropped references.
   */
  bool isZombie() const {
    ld_check(isClosed());
    if (conn_closed_.use_count() > 1) {
      return true;
    }

    return false;
  }

  /**
   * State machines can use ClientID to send replies to client when made a
   * request. But ClientId space is 32bit which means long running state machine
   * can acccidentally send a message to incorrect client if the id's wrapped
   * around. Such long running state machines can get socket token to make sure
   * that the socket exists before trying to use the clientId to send message.
   * The check should be performed on the same thread on which the socket was
   * created to avoid socket getting closed between check and actually sending
   * the message.
   * @return conn_closed_ which gets the socket closed status for clients to
   * cache.
   */
  std::shared_ptr<const std::atomic<bool>> getSocketToken() {
    if (isClosed()) {
      return nullptr;
    }

    return conn_closed_;
  }

  /**
   * Push a callback onto the end of the list of callbacks that will be
   * called when this Socket closes.
   *
   * @param cb callback to push. Must not be on any callback lists.
   *
   * @return 0 on success, -1 with INVALID_PARAM if cb is already on some
   *           Socket callback list (debug mode asserts).
   */
  int pushOnCloseCallback(SocketCallback& cb);

  int pushOnBWAvailableCallback(BWAvailableCallback& cb);

  /**
   * Changes the soft limit on the number of bytes that can be pending
   * in this Sockets' output evbuffer.
   */
  void setOverflowLevel(size_t nbytes) {
    outbuf_overflow_ = nbytes;
  }

  /**
   * Exposes the tcp sendbuf size that the socket was configured with (or the
   * OS-provided default if the setting wasn't specified).
   */
  size_t getTcpSendBufSize() const;

  /**
   * Exposes the tcp recvbuf size that the socket was configured with (or the
   * OS-provided default if the setting wasn't specified).
   */
  virtual size_t getTcpRecvBufSize() const;

  /**
   * Returns the tcp recvbuf occupancy or -1 in case of error.
   */
  virtual ssize_t getTcpRecvBufOccupancy() const;

  /**
   * Returns total bytes received by socket since it was created.
   */
  virtual uint64_t getNumBytesReceived() const;

  /**
   * Checks if this is a server Socket that is currently connected to its
   * destination and that has received an ACK message.
   *
   * If a working connection exists, this method returns 0 and stores the
   * ClientID that the destination has assigned to its end of the connection
   * in our_name_at_peer. Otherwise, -1 is returned with err set to DISABLED,
   * INVALID_PARAM, ALREADY or NOTCONN (see Sender::checkConnection for the
   * description of these error codes).
   */
  int checkConnection(ClientID* our_name_at_peer);

  /**
   * Check if we reached any of the buffer size limits.
   *
   * We maintain 2 buffer size limits:
   * (1) a per-socket limit on bytes pending on that Socket's output evbuffer
   *     and the size of messages queued to this socket in the priority queue.
   * (2) a limit on the total number of bytes pending in all Sockets on
   *     this Worker thread.
   *
   * @return true iff neither of the limits have been exceeded.
   */
  bool sizeLimitsExceeded() const;

  /**
   * Prevents a subsequent connection attempt from being throttled. Useful when
   * there was an external notification about the availability of the node
   * (e.g. we just received a message from that node on another socket) and we
   * want to retry connecting to it immediately.
   */
  void resetConnectThrottle() {
    connect_throttle_->connectSucceeded();
  }

  void setConnectThrottle(ConnectThrottle* throttle) {
    connect_throttle_ = throttle;
  }

  void dumpQueuedMessages(std::map<MessageType, int>* out) const;

  /**
   * Add a raw to `table` with information about this socket.
   * @see logdevice/common/AdminCommandTable.h
   */
  void getDebugInfo(InfoSocketsTable& table) const;

  void setPeerConfigVersion(config_version_t version) {
    peer_config_version_ = version;
  }

  /**
   * @return the last known config version that the peer advertised
   */
  config_version_t getPeerConfigVersion() const {
    return peer_config_version_;
  }

  PeerType getPeerType() const {
    return peer_type_;
  }

  /**
   * @return True if the peer is a LogDevice client.
   */
  bool peerIsClient() const;

  /**
   * @return whether the socket is an SSL socket.
   */
  bool isSSL() const {
    return conntype_ == ConnectionType::SSL;
  }

  bool isHandshaken() const {
    return handshaken_;
  }

  /**
   * @return protocol version used by this Socket. Returns
   * Settings::max_protocol if !isHandshaken().
   */
  uint16_t getProto() const {
    return proto_;
  }

  /**
   * @return Get the ClientID that the other end assigned to our connection and
   * reported in the ACK.  Only for Sockets that initiated an outgoing
   * connection to a server.
   */
  ClientID getOurNameAtPeer() const {
    return our_name_at_peer_;
  }

  /**
   * @return should only be called if the socket is SSL enabled. Returns
   *         the peers certificate if one was provided and nullptr otherwise.
   */
  folly::ssl::X509UniquePtr getPeerCert() const;

  void setPeerShuttingDown() {
    peer_shuttingdown_ = true;
  }

  /**
   * Sends a SHUTDOWN message. Called by the sender when it goes over all
   * existing connection to flush and close the socket.
   */
  void sendShutdown();

  SocketType getSockType() const {
    return type_;
  }

  ConnectionType getConnType() const {
    return conntype_;
  }

  const Settings& getSettings();

  /**
   * Minimum guaranteed outbuf budget limit reached. This will ensure minimal
   * traffic flow on sockets that are not using sender's outbuf.
   */
  bool minOutBufLimitReached() const {
    return (getBytesPending() > outbufs_min_budget_);
  }

  /**
   * The amount of bytes waiting to be sent on this socket.
   */
  size_t getBytesPending() const;

  /**
   * The amount of bytes buffered in the socket layer underneath this
   * connection. For example bytes buffered in asyncsocket or evbuffer.
   */
  size_t getBufferedBytesSize() const;

  /**
   * Run checks to make sure if the socket performing as expected.
   */

  SocketDrainStatusType checkSocketHealth();

  /**
   * Get socket throughput calculated for last socket_health_check_period.
   */
  double getSocketThroughput() const {
    return cached_socket_throughput_;
  }

 protected:
  /**
   * Called by bev_ when the underlying connection is established
   */
  void onConnected();
  void transitionToConnected();

  virtual int dispatchMessageBody(ProtocolHeader header,
                                  std::unique_ptr<folly::IOBuf> msg_buffer);

  /**
   * Called when connection timeout occurs. Either we could not establish the
   * TCP connection after multiple retries or the LD handshake did not complete
   * in time.
   */
  void onConnectTimeout();

  /**
   * Called when LD handshake doesn't complete in the allottted time.
   */
  void onHandshakeTimeout();

  /**
   * Called when the TCP connection could not be established in time.
   * If n_retries_left_ is positive, will try to connect again.
   */
  void onConnectAttemptTimeout();

  enum class SendStatus : uint8_t {
    SCHEDULED, // Buffer is scheduled to be written into the socket.
    SENT,  // Buffer was written into the socket. Does not guarantee that bytes
           // were actually received by the endpoint.
    ERROR, // Hit errors when writing the bytes.
  };
  /**
   * Writes a serialized buffer into the socket.
   * @returns SendStatus based on the status of the write.
   */
  virtual SendStatus sendBuffer(std::unique_ptr<folly::IOBuf>&& buffer_chain);
  void onSent(std::unique_ptr<Envelope>,
              Status,
              Message::CompletionMethod = Message::CompletionMethod::IMMEDIATE);

  /**
   * Called by bev_ when connection closes because of an error. This
   * generally causes the Socket to enter DISCONNECTED state.
   *
   * @param direction  BEV_EVENT_READING or BEV_EVENT_WRITING to indicate
   *                   if the error occurred on a read or a write
   */
  void onError(short direction, int socket_errno);

  /**
   * Called by bev_ when the other end closes connection.
   */
  void onPeerClosed();

  /**
   * Called by the underlying layer implementation to indicate that the bytes
   * have been admitted to be sent to the remote endpoint. If the underlying
   * layer is evbuffer based, this is invoked once we have written into the tcp
   * socket. In case of AsyncSocket based implementation, this is invoked as
   * soon as bytes are added to the AsyncSocket. For the messages corresponding
   * to the admitted bytes we invoke onSent at this point.
   *
   * @param nbytes  number of bytes transferred from buffer to
   *                        the underlying TCP connection
   */
  void onBytesAdmittedToSend(size_t nbytes);

  /**
   * Update sender level stats once bytes are drained into the socket.
   */
  virtual void onBytesPassedToTCP(size_t nbytes);

  SocketDependencies* getDeps() const {
    return deps_.get();
  }

  /**
   * This is strictly a delegating constructor. It sets all members
   * other than peer_name_, peer_sockaddr_ and conntype_ to defaults.
   *
   * @param deps          @see SocketDependencies.
   * @param peer_name     LD-level 4-byte id of the other endpoint
   * @param peer_sockaddr sockaddr of the other endpoint
   * @param type          type of socket
   */
  explicit Socket_DEPRECATED(std::unique_ptr<SocketDependencies>& deps,
                             Address peer_name,
                             const Sockaddr& peer_sockaddr,
                             SocketType type,
                             ConnectionType conntype,
                             FlowGroup& flow_group);

  /**
   * Perform Message and connection validation that is possible from
   * both registerMessage() and send() context.
   */
  int preSendCheck(const Message& msg);

  /**
   * Send a message without blocking through this socket. If the socket is
   * connected, serialize the message in the output buffer and move Envelope
   * to the sendq_. If the socket is not connected, enqueue the message in
   * serializeq_ for later serialization.
   *
   * All errors are reported via Message::OnSent() since actual sending
   * can be deferred due to traffic shaping.
   */
  void send(std::unique_ptr<Envelope> envelope);

  bool isChecksummingEnabled(MessageType msgtype);

  /**
   * Serialize a message and write it to the output buffer.
   * Create an envelope in sendq_ to track its delivery.
   * @param msg    Message to be serialized.
   * @param msglen Size of the message (including ProtocolHeader).
   * @return 0 if the message was successfully queued up for delivery. -1 on
   *         failure and err is set to E::INTERNAL.
   */
  int serializeMessage(std::unique_ptr<Envelope>&& msg);

  /**
   * Helper functions to split serializeMessage() functionality.
   * This will be used when:
   * - SSL is enabled
   * - checksumming is disabled
   * - Message Type is ACK/HELLO
   *
   * @return serialized buffer if no errors, returns a nullptr otherwise. err
   *         contains the actual reason.
   */
  std::unique_ptr<folly::IOBuf> serializeMessage(const Message& msg);

  /**
   * Allow the async message error simulator to optionally take ownership of
   * this message just before it is sent.
   *
   * @return  True if the simulator has taken ownership of and handled the
   *          envelope.
   */
  bool injectAsyncMessageError(std::unique_ptr<Envelope>&& msg);

  /**
   * Invoked by connect() to initiate the connection to peer.
   * Returns Future that is fulfilled once the connection completes.
   */
  folly::Future<Status> asyncConnect();

  /**
   * Called by connect().
   *
   * Used to run basic checks before attempting a new connection.
   */
  int preConnectAttempt();

  /**
   * Called by connect() and by onConnectAttemptTimeout().
   *
   * Initiate an asynchronous connect and handshake on the socket. The socket's
   * .peer_name_ must resolve to an ip:port to which we can connect. Currently
   * this means that .peer_name_ must be a server address.
   *
   * This expects the socket to be not connected.
   *
   * @return  0 if connection was successfully initiated. -1 on failure, err
   *          is set to:
   *
   *    UNROUTABLE      the peer endpoint of a server socket has an IP address
   *                    to which there is no route, see connect() above
   *    SYSLIMIT        out of file descriptors or ephemeral ports
   *    NOMEM           out of kernel memory for sockets, or malloc() failed
   *    INTERNAL        bufferevent unexpectedly failed to initiate connection,
   *                    unexpected error from socket(2).
   */
  int doConnectAttempt();

  /**
   * Called by onBytesAvailable() to process either a protocol header
   * or the rest of a message.
   *
   * @return 0 on success, -1 if an error occurred and the socket must
   *         be closed. err is set to the value to pass to Socket::close():
   *   INTERNAL, BADMSG, TOOBIG, PROTO, PROTONOSUPPORT, ACCESS
   */

  int readMessageHeader(struct evbuffer* inbuf);

  /**
   * Called by dataReadCallback() or readMoreCallback() to read one or
   * more messages in bev_.input evbuffer.
   *
   * @param fresh   if true, the call is made by dataReadCallback() as
   *                fresh data is added to the input evbuffer. If false,
   *                the function is called by readMoreCallback() to
   *                continue reading the input evbuffer contents.
   *
   * NOTE: this function may close the Socket
   */
  void onBytesAvailable(bool fresh);

  /**
   * Write the next message in serializeq_ to the output buffer.
   * Called by flushSerializeQueue() and onConnected().
   */
  void flushNextInSerializeQueue();

  /**
   * Flush serializeq_ by writing all the messages it contains to the output
   * buffer.
   * Called by receiveMessage() once we are handshaken.
   */
  void flushSerializeQueue();

  /**
   * Queues up a HELLO message for delivery. The Socket must not be connected.
   * The message will be sent as soon as the connection is established.
   */
  void sendHello();

  /**
   * Verifies checksum by matching checksum received in the header with checksum
   * computed on the received message body. Returns true if checksum matches or
   * checksum verification is disabled. Returns false if the message did not
   * match.
   */
  bool verifyChecksum(ProtocolHeader ph, ProtocolReader& reader);
  /**
   * Kitchen sink for running basic checks on the received message after
   * checksum verification but before dispatching it to the state machines.
   */
  bool validateReceivedMessage(const Message* msg) const;

  /**
   * In case of handshake message, some fields of the Socket object are
   * initialized after processing the message. This methods does the leftover
   * initialization of the Socket.
   */
  bool processHandshakeMessage(const Message* msg);

  /**
   * @return the number of bytes that receiveMessage() expects to find in
   *         the input evbuffer of bev_ next time it is caleld.
   */
  size_t bytesExpected();

  /**
   * A helper method for creating bufferevents. Mostly registers callbacks
   * and converts errnos to E:: codes. May set output_buffer_ and ssl_context_.
   *
   * @param   sfd   if non-negative, the fd of TCP socket to use. If -1,
   *                the function will call socket(2)
   * @param   sa_family  address family for socket (AF_INET or AF_INET6)
   *
   * @param   rcvbuf_size_out   if non-NULL and TCP receive buffer size for
   *                            sfd was successfully obtained, the size is
   *                            returned through this parameter
   * @param   ssl_state BUFFEREVENT_SSL_ACCEPTING or BUFFEREVENT_SSL_CONNECTING
   *                    Used only if (conntype_ == SSL)
   * @return  a new bufferevent on success, nullptr on failure. err is set to
   *             SYSLIMIT        out of file descriptors
   *             NOMEM           out of kernel memory for sockets,
   *                             or malloc() failed
   *             INTERNAL        unexpected error from socket(2) or fcntl(2).
   *
   */
  struct bufferevent* newBufferevent(int sfd,
                                     sa_family_t sa_family,
                                     size_t* sndbuf_size_out,
                                     size_t* rcvbuf_size_out,
                                     bufferevent_ssl_state ssl_state,
                                     const uint8_t default_dcsp);

  /**
   * Set bev_ read watermarks so that a read callback is triggered as
   * soon as we have enough bytes for a protocol header. Clears
   * recv_message_ph_.len, keeping recv_message_ph_.type unchanged.
   */
  void expectProtocolHeader();

  /**
   * @return true if we are expecting to read a protocol header, false
   *         if we are expecting a message body
   */
  bool expectingProtocolHeader() const {
    return expecting_header_;
  }

  /**
   * Set bev_ read watermarks so that a read callback is triggered only
   * after we got recv_message_ph_.len bytes constituting the body of a
   * message.
   */
  void expectMessageBody();

  /**
   * A helper method for setting up a timer event used to detect handshake
   * timeouts (@see handshake_timeout_event_).
   */
  void addHandshakeTimeoutEvent();

  /**
   * A helper method for setting up a timer event used to detect TCP connection
   * timeouts. (@see connect_timeout_event_).
   */
  void addConnectAttemptTimeoutEvent();

  /**
   * A helper function to determine the reason for lower socket throughput.
   * Returns a decision for the socket slow if it can determine it otherwise
   * returns NONE. Also retuns, what percent of time was socket limited by
   * network, or limited by receiver or limited by unavailability of sendbufs.
   */
  SocketDrainStatusType getSlowSocketReason(unsigned* network_limited,
                                            unsigned* rwnd_limited,
                                            unsigned* sndbuf_limited);

  // Reference holder that holds socket pointer and is distributed to
  // whoever wants cache the socket. It is encapsulated in SocketProxy. The
  // socket instance itself is not reclaimed till all the references on the
  // ref_holder go away.  This way we guarantee that the ClientID if valid
  // does not get reclaimed.
  std::shared_ptr<Socket_DEPRECATED> socket_ref_holder_;

  friend class SocketImpl;
  std::unique_ptr<SocketImpl> impl_;

  std::unique_ptr<SocketDependencies> deps_;

  // Envelopes that have been created via registerMessage, but have yet
  // to be released on this Socket.
  PendingQueue pendingq_;

  // A queue of Envelopes released from pendingq_ but waiting to be
  // serialized to the output buffer.  Messages sent to this socket before
  // it finished handshake are enqueued here. This queue is drained once
  // the socket finishes handshake and is never used again.
  EnvelopeQueue serializeq_;

  // A queue of Envelopes whose messages have been copied into bev_ but
  // bev_ have not yet fully written their contents into the underlying TCP
  // socket.
  EnvelopeQueue sendq_;

  // next Envelope to be written into bev_ will get its Envelope::pos_ set
  // to this value. See Envelope.h.
  message_pos_t next_pos_;

  // next byte to be transferred from bev_'s output buffer into the underlying
  // TCP socket will have this offset in the logical stream of bytes sent into
  // this Socket. See Envelope::pos_ in Envelope.h.
  message_pos_t drain_pos_;

  // libevent 2.x bufferevent underlying this Socket. This is nullptr if we
  // are not either connecting or connected.
  struct bufferevent* bev_;

  // set to true if this Socket has an established TCP connection to
  // its peer.  Otherwise false. If this is false and bev_ is set,
  // then a connection is in progress.
  bool connected_;

  // set to true iff an LD-level handshake has been performed for this Socket
  // For Sockets connected to clients this means that a HELLO message has been
  // received. For Sockets connected to servers this means that a positive
  // ACK has been received.
  bool handshaken_;

  // set to true if we are currently inside a Socket::close(). Used to prevent
  // recursively calling other closes.
  bool closing_{false};

  // set to true if the peer is a server and sent us a SHUTDOWN message.
  // Used to distinguish graceful server shutdown from
  // ungraceful server shutdown
  bool peer_shuttingdown_{false};

  // Protocol version negotiated following handshake.
  // Only set when the socket is handshaken.
  // This is passed to serialization and deserialization handlers.
  // The only messages that can be serialized/deserialized before we actually
  // set this value are ACK and HELLO messages. However, the default value
  // of Settings::max_protocol (set in constructor) will do as
  // getMinProtocolVersion() should return
  // Compatibility::MIN_PROTOCOL_SUPPORTED for them.
  uint16_t proto_;

  // The highest config version known to the peer of the connection.
  // The peer_config_version_ is commmunicated to the server using a
  // CONFIG_ADVISORY message. Servers can update the peer_config_version_
  // using a CONFIG_CHANGED message, which updates the client config.
  config_version_t peer_config_version_{0};

  // For Sockets that initiated an outgoing connection to a server and received
  // a positive ACK, this is the ClientID that the other end assigned to our
  // connection and reported in the ACK. For all other Sockets, or if an
  // LD-level handshake has not yet completed this is an invalid ClientID.
  ClientID our_name_at_peer_;

  // Body length and type from the last received header.
  ProtocolHeader recv_message_ph_;

  // true if we are waiting to read a full protocol header.
  // false if we have just read the header and are now reading the message body.
  bool expecting_header_{true};

  // if the peer is a server, this object throttles the rate of connection
  // attempts
  ConnectThrottle* connect_throttle_{nullptr};

  // If the successful association of a message via registerMessage()
  // causes getBytesPending() to exceed outbuf_overflow_, future calls to
  // registerMessage() will fail with NOBUFS, until message processing
  // through to TCP drops getBytesPending() below this limit.
  size_t outbuf_overflow_;

  // If the sender's outbufs limit is reached , allow a minimum budget of
  //  outbufs_min_budget_
  size_t outbufs_min_budget_;

  // This is a timer event with timeout of 0 that we use to regain
  // control after we processed incoming_messages_max_per_socket
  // messages from bev_ and yielded.
  EvTimer read_more_;

  // The message payload is copied into this from evbuffer. If the system
  // cannot accept anymore data from socket, this helps in avoiding copying the
  // message payload from the evbuffer again hence saving cpu incase of large
  // payload and too many sockets per worker.
  std::unique_ptr<folly::IOBuf> msg_pending_processing_;

  // Timer event used to give up the TCP connection if it is not established
  // within some reasonable period of time.
  EvTimer connect_timeout_event_;

  // Number of times we have tried connecting so far. This value is incremented
  // each time the connection times out until it reaches the maximum specified
  // in settings.
  size_t retries_so_far_;

  // Timer event used to close the connection if the LD protocol handshake
  // (HELLO/ACK message received) is not fully established within some
  // reasonable period of time.
  EvTimer handshake_timeout_event_;

  // Indicates that we haven't had a fully established connection (including
  // the handshake) to the peer yet. Used by checkConnection() to differentiate
  // between initial connection attempt and reconnects.
  bool first_attempt_;

  // cache ttl is set to 1 sec
  // size : Size of the send buffer of the underlying TCP socket as reported by
  // getsockopt(SO_SNDBUF). If getsockopt() fails, a default value set in the
  // constructor is used. This is NOT supposed to be accessed directly but
  // through getTcpSendBufSize()
  // update_time : The time when tcp_sndbuf_size_cache_ was last updated
  mutable struct {
    size_t size;
    std::chrono::steady_clock::time_point update_time;
  } tcp_sndbuf_cache_;

  // Size of the receive buffer of the underlying TCP socket as reported by
  // getsockopt(SO_RCVBUF). If getsockopt() fails, a default value set in the
  // constructor is used.
  size_t tcp_rcvbuf_size_;

  // flushOutputAndClose uses this to keep track of the error to be used when
  // the write callback is called and we close the connection.
  Status close_reason_;

  // Total number of messages received since this socket was created.
  // Used for debugging.
  size_t num_messages_sent_;

  // Total number of messages sent since this socket was created.
  // Used for debugging.
  size_t num_messages_received_;

  // Total number of bytes received since this socket was created.
  size_t num_bytes_received_;

  // Set of stats that are are used to detect low socket performance.
  struct HealthStats {
    void clear() {
      active_start_time_ = SteadyTimestamp::min();
      active_time_ = std::chrono::milliseconds(0);
      num_bytes_sent_ = 0;
      busy_time_ = std::chrono::milliseconds(0);
      rwnd_limited_time_ = std::chrono::milliseconds(0);
      sndbuf_limited_time_ = std::chrono::milliseconds(0);
    }

    // Timestamp when socket switched from idle to active. This is used to
    // calculate total amount of time when bytes enqueued in the socket was
    // above idle-threshold.
    SteadyTimestamp active_start_time_{SteadyTimestamp::min()};

    // Total time in last health-check-period when bytes enqueued in the socket
    // were above idle-threshold. Socket throughput is calculated over this
    // time. This is updated on active to idle transition or during socket
    // health check.
    std::chrono::milliseconds active_time_{0};

    // This is sum of bytes written to the socket in the last health check
    // period.
    size_t num_bytes_sent_{0};

    // Amount of time when socket had bytes available to send since it was
    // created.
    std::chrono::milliseconds busy_time_{0};

    // Portion of busy time since the socket was created, when receiver was not
    // able to expand its window to accept all sender's pending bytes.
    std::chrono::milliseconds rwnd_limited_time_{0};

    // Portion of busy time since the socket was created, when sender was not
    // able to enqueue more because of insufficient sendbuf.
    std::chrono::milliseconds sndbuf_limited_time_{0};
  };

  HealthStats health_stats_;

  // Calculated socket throughput in last socket-health-check-period in
  // KBps. This value caches the socket throughput for InfoSocket command and
  // the value here is valid only if socket-health-check-period is non-zero.
  double cached_socket_throughput_{0};

  // Indicates whether this is an SSL socket
  ConnectionType conntype_{ConnectionType::PLAIN};

  // The SSL context. We have to hold it alive as long as the SSL* object which
  // we submit to bufferevent_openssl_new() is in use
  std::shared_ptr<folly::SSLContext> ssl_context_;

  // true if we accepted a TCP connection, but the SSL handshake didn't
  // complete yet
  bool expecting_ssl_handshake_ = false;

  // true if the message error injection code has decided to rewind
  // a message stream. All traffic for this socket will be diverted until
  // the end of the event loop, at which time the messages will be delivered
  // with the error code specified by Sender::getMessageErrorInjectionErrorCode
  // via the onSent() callback.
  bool message_error_injection_rewinding_stream_ = false;

  // event for signalling there are pending callbacks for various deferred
  // events in the queue for this socket. See Socket::eventCallback()
  EvTimer deferred_event_queue_event_;

  // event signalling the need to terminate the current simulated stream
  // rewind event as soon as control is returned to the event loop.
  EvTimer end_stream_rewind_event_;

  // The number of messages that were asynchronously failed with NOBUFS
  // duing the current simulated stream rewind event.
  uint64_t message_error_injection_rewound_count_ = 0;

  // The number of messages that have been processed normally since the last
  // simulated stream rewind event.
  uint64_t message_error_injection_pass_count_ = 0;

  struct SocketEvent {
    short what;
    int socket_errno;
  };

  // callback queue for deferred events. See Socket::eventCallback()
  std::deque<SocketEvent> deferred_event_queue_;

  // These two members are used to correctly maintain the number of available
  // fds for all accepted and client-only connections.
  ResourceBudget::Token conn_incoming_token_;
  ResourceBudget::Token conn_external_token_;

  // Output buffer in front of bev's output buffer. Used mostly for SSL to
  // batch up small writes.
  struct evbuffer* buffered_output_{nullptr};

  // The zero-timeout timer used to flush the output buffer
  EvTimer buffered_output_flush_event_;

  // called by bev_ when all bytes we have been waiting for arrive
  static void dataReadCallback(struct bufferevent*, void*, short);

  static void onOutputEmpty(struct bufferevent* bev, void* arg, short);

  // called for items in the deferred_event_queue_ whenever the event
  // is handled
  static void deferredEventQueueEventCallback(void* instance, short);

  // called when the end_stream_rewind_event_ is signed.
  static void endStreamRewindCallback(void* instance, short);

  // called by deferredEventQueueEventCallback to actually process the deferred
  // event queue
  void processDeferredEventQueue();

  // called by endStreamRewindCallback to terminate the rewind on a parituclar
  // socket instance.
  void endStreamRewind();

  /**
   * Called by bev_ when an important event occurs
   *
   * @param what  a conjunction of flags: BEV_EVENT_READING or
   *              BEV_EVENT_WRITING to indicate if the error was encountered
   *              on the read or write path, and one of the following flags:
   *              BEV_EVENT_EOF, BEV_EVENT_ERROR, BEV_EVENT_TIMEOUT,
   *              BEV_EVENT_CONNECTED
   */
  static void eventCallback(struct bufferevent*, void*, short what);

  /**
   * Implementation of eventCallback()
   */
  void eventCallbackImpl(SocketEvent e);

  /**
   * Called by libevent when read_more_ timer event fires.
   */
  static void readMoreCallback(void* arg, short what);

  /**
   * Called by the output evbuffer of bev_ after some bytes have been
   * drained from the buffer into the underlying TCP socket.
   */
  static void bytesSentCallback(struct evbuffer*,
                                const struct evbuffer_cb_info*,
                                void*);

  static void handshakeTimeoutCallback(void*, short);

  static void connectAttemptTimeoutCallback(void*, short);

  void enqueueDeferredEvent(SocketEvent e);

  /**
   * A callback on write to buffered_output_
   */
  static void onBufferedOutputWrite(struct evbuffer* buffer,
                                    const struct evbuffer_cb_info* info,
                                    void* arg);
  /**
   * Flushes the local output buffer to bev's output buffer
   */
  void flushBufferedOutput();

  /**
   * A callback for buffered_output_flush_event_
   */
  static void onBufferedOutputTimerEvent(void* instance, short);

  /**
   * Gets the sum of sizes of output buffers
   */
  size_t getTotalOutbufLength();

  // The file descriptor of the underlying OS socket. Set to -1 in situations
  // where the file descriptor is not known (e.g., before connecting).
  int fd_;

  // Avoid invoking Socket::close on socket that is already closed.
  std::shared_ptr<std::atomic<bool>> conn_closed_;

  // Set to true for socket based on libevent otherwise this is false.
  const bool legacy_connection_;

  // Protocol Handler layer to which owns the AsyncSocket and is responsible for
  // sending data over the socket.
  std::shared_ptr<ProtocolHandler> proto_handler_;

  // Read callback installed in AsyncSocket to read data and pass it to higher
  // layers.
  std::unique_ptr<folly::AsyncSocket::ReadCallback> read_cb_;

  // If receive of a message hit ENOBUFS then we will retry the same message
  // again till it succeeds. This will all stop reading more messages from the
  // socket.
  EvTimer retry_receipt_of_message_;

  SocketWriteCallback sock_write_cb_;

  // This IOBuf chain buffers writes till we can add them to asyncSocket at next
  // eventloop iteration. This helps in getting better socket performance in
  // case very high number of small writes. Note: sendChain can grow large do
  // not invoke computeChainDataLength on it frequently.
  std::unique_ptr<folly::IOBuf> sendChain_;

  // Timer used to schedule event as soon as data is added to sendChain_.The
  // callback of this timer add data into the asyncsocket.
  EvTimer sched_write_chain_;

  // Used to note down delays in writing into the asyncsocket.
  SteadyTimestamp sched_start_time_;

  /**
   * For Testing only!
   */
  bool shouldTamperChecksum() {
    return tamper_;
  }

  bool tamper_{false};

  friend class SocketTest;
  friend class ClientSocketTest;
  friend class ServerSocketTest;
  friend class ClientConnectionTest;
  friend class Connection;
};

}} // namespace facebook::logdevice
