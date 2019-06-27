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
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

class BWAvailableCallback;
class FlowGroup;
class ResourceBudget;
class SocketCallback;
class SocketImpl;
class SocketProxy;
class StatsHolder;
class Worker;
class Sender;
class Processor;

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

class Socket : public TrafficShappingSocket {
 public:
  /**
   * Constructs a new Socket, to be connected to a LogDevice
   * server. The calling thread must be a Worker thread.
   *
   * @param server_name     id of server to connect to
   * @param type            type of socket
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
  explicit Socket(NodeID server_name,
                  SocketType type,
                  ConnectionType conntype,
                  FlowGroup& flow_group);

  /**
   * Used for tests.
   */
  Socket(NodeID server_name,
         SocketType type,
         ConnectionType conntype,
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
  Socket(int fd,
         ClientID client_name,
         const Sockaddr& client_addr,
         ResourceBudget::Token conn_token,
         SocketType type,
         ConnectionType conntype,
         FlowGroup& flow_group);

  /**
   * Used for tests.
   */
  Socket(int fd,
         ClientID client_name,
         const Sockaddr& client_addr,
         ResourceBudget::Token conn_token,
         SocketType type,
         ConnectionType conntype,
         FlowGroup& flow_group,
         std::unique_ptr<SocketDependencies> deps);

  /**
   * Disconnects, deletes the underlying bufferevent, and closes the TCP socket.
   */
  virtual ~Socket();

  Socket(const Socket&) = delete;
  Socket(Socket&&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket& operator=(Socket&&) = delete;

  Sockaddr peerSockaddr() const {
    return peer_sockaddr_;
  }

  /**
   * For Testing only!
   */
  void enableChecksumTampering(bool enable) {
    tamper_ = enable;
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
  int connect();

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
   * @return true iff close() has been called on the socket, or if it is
   *         a server socket that has never been connected
   */
  bool isClosed() const;

  /**
   * @return true iff close() has been called on the socket and all clients have
   * dropped references.
   */
  bool isZombie() const {
    ld_check(isClosed());
    if (socket_ref_holder_.use_count() > 1) {
      return true;
    }

    return false;
  }

  /**
   * @return SocketProxy, this guarantees the encapsulated socket stays around
   * till returned instance does not go out of scope. One use case of this is to
   * stop ClientID from getting reused.
   */
  std::unique_ptr<SocketProxy> getSocketProxy() {
    if (isClosed()) {
      return std::unique_ptr<SocketProxy>();
    }

    return std::make_unique<SocketProxy>(socket_ref_holder_);
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
    connect_throttle_.connectSucceeded();
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
  X509* getPeerCert() const;

  /**
   * Sets the cipher the the socket will use. This should only be called
   * if the socket is SSL enabled.
   */
  void limitCiphersToENULL();

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
   * The amount of bytes waiting to be sent on this socket.
   */
  size_t getBytesPending() const;

  /**
   * Check if the socket cannot drain a message in
   * max_time_to_allow_socket_drain. Higher layer can take action whether to
   * close the Socket and keep it as is.
   */
  bool slowInDraining();

 protected:
  /**
   * Called by bev_ when the underlying connection is established
   */
  virtual void onConnected();

  virtual int onReceived(ProtocolHeader ph, struct evbuffer* inbuf);

  /**
   * Called when connection timeout occurs. Either we could not establish the
   * TCP connection after multiple retries or the LD handshake did not complete
   * in time.
   */
  virtual void onConnectTimeout();

  /**
   * Called when LD handshake doesn't complete in the allottted time.
   */
  virtual void onHandshakeTimeout();

  /**
   * Called when the TCP connection could not be established in time.
   * If n_retries_left_ is positive, will try to connect again.
   */
  virtual void onConnectAttemptTimeout();
  virtual void
      onSent(std::unique_ptr<Envelope>,
             Status,
             Message::CompletionMethod = Message::CompletionMethod::IMMEDIATE);

  /**
   * Called by bev_ when connection closes because of an error. This
   * generally causes the Socket to enter DISCONNECTED state.
   *
   * @param direction  BEV_EVENT_READING or BEV_EVENT_WRITING to indicate
   *                   if the error occurred on a read or a write
   */
  virtual void onError(short direction, int socket_errno);

  /**
   * Called by bev_ when the other end closes connection.
   */
  virtual void onPeerClosed();

  /**
   * Called by the output evbuffer when some bytes have been transferred from
   * it into the underlying TCP socket (or placed in the outgoing SSL buffer).
   *
   * @param nbytes_drained  number of bytes transferred from buffer to
   *                        the underlying TCP connection
   */
  virtual void onBytesPassedToTCP(size_t nbytes_drained);

 private:
  /**
   * This is strictly a delegating constructor. It sets all members
   * other than peer_name_, peer_sockaddr_ and conntype_ to defaults.
   *
   * @param deps          @see SocketDependencies.
   * @param peer_name     LD-level 4-byte id of the other endpoint
   * @param peer_sockaddr sockaddr of the other endpoint
   * @param type          type of socket
   */
  explicit Socket(std::unique_ptr<SocketDependencies>& deps,
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
  int serializeMessage(std::unique_ptr<Envelope>&& msg, size_t msglen);

  /**
   * Helper functions to split serializeMessage() functionality.
   * This will be used when:
   * - SSL is enabled
   * - checksumming is disabled
   * - Message Type is ACK/HELLO
   * @return 0 for success, -1 for failure
   */
  int serializeMessageWithoutChecksum(const Message& msg,
                                      size_t msglen,
                                      struct evbuffer* outbuf);
  /**
   * @return 0 for success, -1 for failure
   */
  int serializeMessageWithChecksum(const Message& msg,
                                   size_t msglen,
                                   struct evbuffer* outbuf);

  /**
   * Allow the async message error simulator to optionally take ownership of
   * this message just before it is sent.
   *
   * @return  True if the simulator has taken ownership of and handled the
   *          envelope.
   */
  bool injectAsyncMessageError(std::unique_ptr<Envelope>&& msg);

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
  int receiveMessage();

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
   * Flush serializeq_ by writting all the messages it contains to the output
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

  // Reference holder that holds socket pointer and is distributed to whoever
  // wants cache the socket. It is encapsulated in SocketProxy. The socket
  // instance itself is not reclaimed till all the references on the ref_holder
  // go away.  This way we guarantee that the ClientID if valid does not get
  // reclaimed.
  std::shared_ptr<Socket> socket_ref_holder_;

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
  ConnectThrottle connect_throttle_;

  // If the successful association of a message via registerMessage()
  // causes getBytesPending() to exceed outbuf_overflow_, future calls to
  // registerMessage() will fail with NOBUFS, until message processing
  // through to TCP drops getBytesPending() below this limit.
  size_t outbuf_overflow_;

  // This is a timer event with timeout of 0 that we use to regain
  // control after we processed incoming_messages_max_per_socket
  // messages from bev_ and yielded.
  struct event read_more_;

  // Timer event used to give up the TCP connection if it is not established
  // within some reasonable period of time.
  struct event connect_timeout_event_;

  // Number of times we have tried connecting so far. This value is incremented
  // each time the connection times out until it reaches the maximum specified
  // in settings.
  size_t retries_so_far_;

  // Timer event used to close the connection if the LD protocol handshake
  // (HELLO/ACK message received) is not fully established within some
  // reasonable period of time.
  struct event handshake_timeout_event_;

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
  // Used for debugging.
  size_t num_bytes_received_;

  // Indicates whether this is an SSL socket
  ConnectionType conntype_{ConnectionType::PLAIN};

  // Defines if the socket will be encrypted. Only used if conntype_ is SSL
  bool null_ciphers_only_ = false;

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
  struct event deferred_event_queue_event_;

  // event signalling the need to terminate the current simulated stream
  // rewind event as soon as control is returned to the event loop.
  struct event end_stream_rewind_event_;

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
  struct event buffered_output_flush_event_;

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

  /**
   * The file descriptor of the underlying OS socket. Set to -1 in situations
   * where the file descriptor is not known (e.g., before connecting).
   */
  int fd_;

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
  friend class Connection;
};

/**
 * Interface to access dependencies of Socket.
 * Unit tests implement a derived class, @see common/test/SocketTest.cpp.
 */
class SocketDependencies {
 public:
  SocketDependencies(Processor* processor,
                     Sender* sender,
                     const Address& peer_name);

  virtual const Settings& getSettings() const;
  virtual StatsHolder* getStats() const;
  virtual std::shared_ptr<Configuration> getConfig() const;
  virtual std::shared_ptr<ServerConfig> getServerConfig() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;
  virtual void noteBytesQueued(size_t nbytes);
  virtual void noteBytesDrained(size_t nbytes);
  virtual size_t getBytesPending() const;

  virtual std::shared_ptr<folly::SSLContext>
  getSSLContext(bufferevent_ssl_state, bool) const;
  virtual bool shuttingDown() const;
  virtual std::string dumpQueuedMessages(Address addr) const;
  virtual const Sockaddr& getNodeSockaddr(NodeID nid,
                                          SocketType type,
                                          ConnectionType conntype);
  virtual int eventAssign(struct event* ev,
                          void (*cb)(evutil_socket_t, short what, void* arg),
                          void* arg);
  virtual void eventActive(struct event* ev, int what, short ncalls);
  virtual void eventDel(struct event* ev);
  virtual int eventPrioritySet(struct event* ev, int priority);
  virtual int evtimerAssign(struct event* ev,
                            void (*cb)(evutil_socket_t, short what, void* arg),
                            void* arg);
  virtual void evtimerDel(struct event* ev);
  virtual int evtimerPending(struct event* ev, struct timeval* tv = nullptr);
  virtual const struct timeval* getCommonTimeout(std::chrono::milliseconds t);
  virtual const timeval*
  getTimevalFromMilliseconds(std::chrono::milliseconds t);
  virtual const struct timeval* getZeroTimeout();
  virtual int evtimerAdd(struct event* ev, const struct timeval* timeout);
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
  virtual ResourceBudget& getConnBudgetExternal();
  virtual std::string getClusterName();
  virtual ServerInstanceId getServerInstanceId();
  virtual const std::string& getHELLOCredentials();
  virtual const std::string& getCSID();
  virtual std::string getClientBuildInfo();
  virtual bool authenticationEnabled();
  virtual bool allowUnauthenticated();
  virtual bool includeHELLOCredentials();
  virtual void onStartedRunning(RunContext context);
  virtual void onStoppedRunning(RunContext prev_context);
  virtual ResourceBudget::Token getResourceToken(size_t payload_size);

  virtual ~SocketDependencies() {}

 private:
  Processor* const processor_;
  Sender* sender_;
};

/**
 * SocketProxy is a simple reference to the socket that makes sure that the
 * socket instance stays around till there is some one in the system is still
 * holding this object. It also lets the holders of proxy know that the socket
 * was closed. It also captures logic to safely decrement the ref count on the
 * socket once the user goes out of scope. This class is non-copyable or
 * assignable so that users explicitly have to get an instance if they want
 * the socket to stay around.  Once the users holding socket ref come to know
 * that socket is closed it is expected that they drop the corresponding ref
 * as soon as possible, so that the zombie socket is reclaimed.
 *
 */
class SocketProxy {
 public:
  explicit SocketProxy(std::shared_ptr<Socket> socket_ref_holder)
      : socket_ref_holder_(std::move(socket_ref_holder)) {}

  SocketProxy(const SocketProxy&) = delete;
  SocketProxy& operator=(const SocketProxy&) = delete;

  bool isClosed() {
    auto socket = socket_ref_holder_.get();
    if (!socket || socket->isClosed()) {
      return true;
    }

    return false;
  }

  const Socket* get() const {
    return socket_ref_holder_.get();
  }
  // TODO: Add proxy methods here that allow messages to be enqueued to
  // network threadpool which will eventually be written to network socket.

 private:
  std::shared_ptr<Socket> socket_ref_holder_;
};

}} // namespace facebook::logdevice
