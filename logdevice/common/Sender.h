/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/CppAttributes.h>
#include <folly/IntrusiveList.h>
#include <folly/Optional.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/ConnectionInfo.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/Err.h"

// Think twice before adding new includes here!  This file is included in many
// translation units and increasing its transitive dependency footprint will
// slow down the build.  We use forward declaration and the pimpl idiom to
// offload most header inclusion to the .cpp file; scroll down for details.

namespace facebook { namespace logdevice {

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class BWAvailableCallback;
struct Settings;

/**
 * @file a Sender sends Messages to Addresses by mapping an Address to a
 *       Connection then passing the Message to that Connection for sending. A
 *       Sender object is local to a Worker and only operates on Connections
 *       running on that Worker's event loop.
 *
 *       The interface for Sender is encapsulated in an abstract class to
 *       simplify the task of modifying sender behavior or intercepting
 *       outbound messages in tests.
 */

/**
 * Interface for sending messages to either a client or node.
 */
class SenderBase {
 public:
  class MessageCompletion {
   public:
    MessageCompletion(std::unique_ptr<Message> msg,
                      const Address& dest,
                      Status s,
                      SteadyTimestamp time)
        : msg_(std::move(msg)),
          enqueue_time_(time),
          destination_(dest),
          status_(s) {}

    void send();

    folly::IntrusiveListHook links;

   private:
    std::unique_ptr<Message> msg_;
    const SteadyTimestamp enqueue_time_;
    const Address destination_;
    const Status status_;
  };
  using CompletionQueue =
      folly::IntrusiveList<MessageCompletion, &MessageCompletion::links>;
  virtual ~SenderBase() = default;

  /**
   * Attempts to send a message to a specified Address, which can identify
   * either a client or a server connection. The method asserts that it
   * is called on this Sender's Worker thread.
   *
   * @param msg  message to send. The caller loses ownership of msg if the
   *             call succeeds, retains ownership on failure.
   *
   * @param addr address to send the message to. If this is a client address,
   *             the call succeeds only if a Connection wrapping a TCP
   *             Connection accepted from that client is already running on this
   *             Worker. If addr is a server address for which there is no
   *             Connection on this Worker, the function attempts to create one.
   *             If it's a server address, and generation()
   *             == 0, takes the generation from the current config.
   *
   * @param onclose an optional callback functor to push onto the list of
   *                callbacks that the Connection through which the message gets
   *                sent will call on this Worker thread when that Connection is
   *                closed. The callback MUST NOT yet be on any callback list.
   *                The callback will NOT be installed if the call fails,
   *                except CBREGISTERED. Note that if you provide both
   *                `on_bw_avail` and `onclose`, and the Connection closes while
   *                waiting for bandwidth, then both callbacks will be notified:
   *                first `on_bw_avail->cancelled()`, then `(*onclose)()`.
   *
   *                NOTE a: the Sender does NOT take ownership of the callback.
   *                NOTE b: it is safe for caller to destroy the callback while
   *                        it is installed. Callback's destructor will remove
   *                        it from list.
   *                NOTE c: Connection::close() removes callbacks from list
   *                before calling them (one at a time)
   *
   * @return 0 if the message was successfully queued up on a Connection send
   *         queue. Note that this does not guarantee that the Message will be
   *         written into a TCP connection, much less delivered. The final
   *         disposition of this attempt to send will be communicated through
   *         msg.onSent(). On failure -1 is returned. err is set to
   *
   *            CBREGISTERED   traffic throttling is in effect. The provided
   *                           callback will be invoked once bandwidth is
   *                           available.
   *            UNREACHABLE    addr is a client address for which no connected
   *                           and handshaken Connection exists on this Worker.
   *            NOTINCONFIG    if addr is a NodeID that is not present
   *                           in the current cluster config of Processor to
   *                           which this Sender belongs, or if it is a
   *                           node_index_t that is greater than the largest
   *                           node index in config.
   *            NOSSLCONFIG    Connection to the recipient must use SSL but the
   *                           recipient is not configured for SSL connections.
   *            UNROUTABLE     if addr is a NodeID and the corresponding IP
   *                           address in the cluster config has no route.
   *                           This may happen if a network interface has been
   *                           taken down, e.g., during system shutdown.
   *            NOBUFS         Connection send queue limit was reached
   *            DISABLED       destination is temporarily marked down
   *            TOOBIG         message is too big (exceeds payload size limit)
   *            SYSLIMIT       out of file descriptors or ephemeral ports
   *            NOMEM          out of kernel memory for Connections, or malloc()
   *                           failed
   *            CANCELLED      msg.cancelled() requested message to be cancelled
   *            SHUTDOWN       Sender has been shut down
   *            INTERNAL       bufferevent unexpectedly failed to initiate
   *                           connection, unexpected error from socket(2).
   *            PROTONOSUPPORT the handshaken protocol is not compatible with
   *                           this message.
   *            NOTCONN        Underlying socket is disconnected.
   */
  template <typename MessageT>
  int sendMessage(typename std::unique_ptr<MessageT>&& msg,
                  const Address& addr,
                  BWAvailableCallback* on_bw_avail = nullptr,
                  SocketCallback* onclose = nullptr) {
    // Ensure the temporary necessary to convert from
    // unique_ptr<DerivedMessage> to unique_ptr<Message>
    // is explicitly controlled by SenderBase, and the
    // result of a failed sendMessage (no move should
    // occur) is properly reflected to the caller.
    std::unique_ptr<Message> base = std::move(msg);
    int rv = sendMessageImpl(std::move(base), addr, on_bw_avail, onclose);
    msg.reset(static_cast<MessageT*>(base.release()));
    return rv;
  }

  template <typename MessageT>
  int sendMessage(typename std::unique_ptr<MessageT>&& msg,
                  ClientID cid,
                  BWAvailableCallback* on_bw_avail = nullptr,
                  SocketCallback* onclose = nullptr) {
    return sendMessage(std::move(msg), Address(cid), on_bw_avail, onclose);
  }

  template <typename MessageT>
  int sendMessage(typename std::unique_ptr<MessageT>&& msg,
                  NodeID nid,
                  BWAvailableCallback* on_bw_avail = nullptr,
                  SocketCallback* onclose = nullptr) {
    return sendMessage(std::move(msg), Address(nid), on_bw_avail, onclose);
  }

  template <typename MessageT>
  int sendMessage(typename std::unique_ptr<MessageT>&& msg,
                  ClientID cid,
                  SocketCallback* onclose) {
    return sendMessage(std::move(msg), Address(cid), nullptr, onclose);
  }

  template <typename MessageT>
  int sendMessage(typename std::unique_ptr<MessageT>&& msg,
                  NodeID nid,
                  SocketCallback* onclose) {
    return sendMessage(std::move(msg), Address(nid), nullptr, onclose);
  }

  /**
   * Verify that transmission to the given address is expected to be successful.
   *
   * When testing a client address:
   *   - The client is still connected.
   *   - Traffic shapping will allow at least one message of the specified
   *     traffic class to be transmitted.
   *
   * When testing a server node address:
   *   - The node is a member of the current config.
   *   - Either a connection doesn't yet exist for the node or
   *     Traffic shapping will allow at least one message of the specified
   *     traffic class to be transmitted.
   *
   * The method asserts that it is called on this Sender's Worker thread.
   *
   * @param addr address for sending a future message.
   *
   * @param tc   The traffic class that will be used to construct the message
   *             that will be sent.
   *
   * @return true if the future message transmission is expected to be
   *         successfull. Otherwise false with one of the following error
   *         codes set:
   *
   *         CBREGISTERED traffic throttling is in effect. The provided
   *                      callback will be invoked once bandwidth is
   *                      available.
   *
   *         NOTINCONFIG  if addr is a NodeID that is not present
   *                      in the current cluster config of Processor to which
   *                      this Sender belongs, or if it is a node_index_t
   *                      that is greater than the largest node index in
   *                      config.
   *
   *         NOTCONN      if addr is a ClientID and there is no current
   *                      connection to that client.
   */
  bool canSendTo(NodeID nid,
                 TrafficClass tc,
                 BWAvailableCallback& on_bw_avail) {
    return canSendToImpl(Address(nid), tc, on_bw_avail);
  }

  bool canSendTo(ClientID cid,
                 TrafficClass tc,
                 BWAvailableCallback& on_bw_avail) {
    return canSendToImpl(Address(cid), tc, on_bw_avail);
  }

  bool canSendTo(const Address& address,
                 TrafficClass tc,
                 BWAvailableCallback& on_bw_avail) {
    return canSendToImpl(address, tc, on_bw_avail);
  }

 protected:
  // These two methods need to be implemented in concrete implementations
  // of Sender behavior.
  virtual bool canSendToImpl(const Address&,
                             TrafficClass,
                             BWAvailableCallback&) = 0;

  virtual int sendMessageImpl(std::unique_ptr<Message>&& msg,
                              const Address& addr,
                              BWAvailableCallback* on_bw_avail,
                              SocketCallback* onclose) = 0;
};

/**
 * Implements the standard practice of routing all concrete
 * Sender APIs through the Sender associated with the current
 * thread's worker.
 *
 * For classes that can only be tested by modifying Sender
 * behavior, a SenderProxy is instantiated and used for all
 * Sender API calls. During normal operation, the SenderProxy
 * will forward to the standard Sender owned by the current
 * thread's worker. When the class is under test, the SenderProxy
 * is replaced by a SenderTestProxy so the test can intercept
 * all API calls.
 */
class SenderProxy : public SenderBase {
 protected:
  bool canSendToImpl(const Address&,
                     TrafficClass,
                     BWAvailableCallback&) override;

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback* on_bw_avail,
                      SocketCallback* onclose) override;
};

/**
 * Extension of SenderBase interface, exposing additonal methods related to
 * connection management, e.g. it allows to check for connection state, initiate
 * a new connection or suscribe for connection close event.
 * It also provide some essential information about connected peers in a form of
 * ConnectionInfo. This information available as a whole (getConnectionInfo) and
 * in peice (e.g. getNodeID/getCSID/etc). The latter methods are merely
 * shortcuts returning some sensible defaults in case connection does not exist
 * or requested piece of information is not known.
 */
class Sender : public SenderBase {
 public:
  /////////////////// Connection management ////////////////////
  /**
   * Initiate an asynchronous attempt to connect to a given node unless a
   * working connection already exists or is in progress.
   *
   * @return 0 on sucesss, -1 otherwise with err set to
   *    SHUTDOWN        Sender has been shut down
   *    NOTINCONFIG     if nid is not present in the current cluster config
   *    NOSSLCONFIG     Connection to nid must use SSL but SSL is not
   *                    configured for nid
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
  virtual int connect(node_index_t) = 0;

  /**
   * Check if a working connection to a given node exists.
   *
   * If connection to node is currently in progress, this method will report
   * either ALREADY or NOTCONN, depending on whether this is the first attempt
   * to connect to the given node or not.
   *
   * @return  0 if a connection to nid is already established and handshake
   *          completed, -1 otherwise with err set to
   *          NOTFOUND   nid does not identify a server Connection managed
   *                     by this Sender
   *          ALREADY    Initial connection attempt to nid is in progress
   *          NOTCONN    No active connection to nid is available
   *          NOBUFS     Connection to destination is valid but reaches its
   *                     buffer limit
   *          DISABLED   Connection is currently marked down after an
   *                     unsuccessful connection attempt INVALID_PARAM  nid is
   *                     invalid (debug build asserts)
   */
  virtual int checkServerConnection(node_index_t) const = 0;

  /**
   * Check if a working connection to a give client exists. If peer_is_client is
   * set extra checks is made to make sure peer is logdevice client.
   *
   * @return  0 if a connection to nid is already established and handshake
   *          completed, -1 otherwise with err set to
   *          NOTFOUND     cid does not exist the socket must be closed.
   *          NOTCONN      The socket is not connected or not hanshaken.
   *          NOTANODE     If check_peer_is_node is set, peer is not a
   *                       logdevice node.
   */
  virtual int checkClientConnection(ClientID,
                                    bool check_peer_is_node) const = 0;

  /**
   * Close the connection to this address if it exists.
   *
   * @param peer   Address for which to close the Connection.
   * @param reason Reason for closing the Connection.
   *
   * @return 0 on success, or -1 if there is no connection for given address,
   *         in which case err is set to E::NOTFOUND.
   */
  virtual int closeConnection(const Address&, Status reason) = 0;

  /**
   * Checks whether connection with given addresss is closed.
   */
  virtual bool isClosed(const Address&) const = 0;

  /**
   * If addr identifies a connection managed by this Sender, registers callback
   * to be called when the connection is closed. See sendMessage() docblock
   * above for important notes and restrictions.
   *
   * @return 0 on success, -1 if callback could not be installed. Sets err to
   *         NOTFOUND       if addr does not identify a active connection
   *                        managed by this Sender
   *         INVALID_PARAM  if cb is already on some callback list
   */
  virtual int registerOnConnectionClosed(const Address& addr,
                                         SocketCallback& cb) = 0;

  /////////////////// Shutdown ////////////////////

  /**
   * Flushes buffered data to network and disallows initiating new connections
   * or sending messages.
   *
   * isShutdownCompleted() can be used to find out when this operation
   * completes.
   */
  virtual void beginShutdown() = 0;

  /**
   * Esnures all connections are closed. Must be be called as last step of
   * Sender shutdown. Normally you want to call beginShutdown() first to flush
   * buffers to network and shutdown gracefully but you do not have to, for
   * example when shutting down Sender on emergency path or in tests.
   */
  virtual void forceShutdown() = 0;

  /**
   * @return true iff all owned Connections are closed.
   */
  virtual bool isShutdownCompleted() const = 0;

  /////////////////// Connection attributes ////////////////////

  /**
   * Invokes a callback for each connection with its info structure.
   */
  virtual void forEachConnection(
      const std::function<void(const ConnectionInfo&)>& cb) const = 0;

  /**
   * Returns connection info for the connection with given address. Retuned
   * pointer should not outlive the connection itself so if caller needs to
   * store information somwhere for the future usage it must be copied.
   *
   * @return Pointer to info struct if connection exists or nullptr
   *         otherwise.
   */
  virtual const ConnectionInfo* FOLLY_NULLABLE
  getConnectionInfo(const Address&) const = 0;

  /**
   * Updates connection info by replacing with given version.
   *
   * @return whether connection info was successfully updated. This operation
   *         may fail only if connection with given address is not found.
   */
  virtual bool setConnectionInfo(const Address&, const ConnectionInfo&) = 0;

  /**
   * @return a peer Sockaddr for the connection, or an invalid Sockaddr if
   *         no connections known to this Sender match addr
   */
  virtual Sockaddr getSockaddr(const Address&) const;

  /**
   * Get client socket token for the socket associated with client-id 'cid'.
   */
  virtual std::shared_ptr<const std::atomic<bool>>
      getConnectionToken(ClientID) const;

  /**
   * @return a pointer to the identity associated with connection to given
   *         address or nullptr of no corresponing connection found
   */
  virtual const PrincipalIdentity* getPrincipal(const Address&) const;

  // An enum representing the result of the peer identity extractions.
  enum class ExtractPeerIdentityResult {
    // Indicates that the connection wasn't found, and translates to
    // PrincipalIdentity::UNAUTHENTICATED.
    NOT_FOUND,
    // Indicates that the connection wasnt SSL, and translates to
    // PrincipalIdentity::UNAUTHENTICATED.
    NOT_SSL,
    // Indicates that the parsing the certificate fails and translates to
    // PrincipalIdentity::INVALID.
    PARSING_FAILED,
    // Indicates that the parsing was successfull and the parsed identity is
    // returned.
    SUCCESS,
  };

  /**
   * Extracts the underlying connection certificate and parses the principal
   * identity out of it.
   * NOTES:
   *  - The caller must guarantee that this function is only called on SSL
   *    conections.
   *
   * @returns  An enum representing the result of the parsing and the
   *           corresponding identity. Check documentation
   *           ExtractPeerIdentityResult for what each result translates to.
   */
  virtual std::pair<ExtractPeerIdentityResult, PrincipalIdentity>
  extractPeerIdentity(const Address&) = 0;

  /**
   * Returns the node index of the peer with the given address.
   *
   * @return if addr is a server address or corresponds to a client connection
   *         from another node, that id will be returned; otherwise, this method
   *         returns folly::none
   */
  virtual folly::Optional<node_index_t> getNodeIdx(const Address&) const;

  /**
   * Returns protocol version of the Connection or folly::none if connection
   * either not known or has not been handshaken yet.
   */
  virtual folly::Optional<uint16_t>
      getSocketProtocolVersion(node_index_t) const;

  /**
   * Returns ID assigned by the peer to outgoing connection to this address if
   * known or ClientID::INVALID.
   */
  virtual ClientID getOurNameAtPeer(node_index_t) const;

  /**
   * Returns the CSID held in the Connection matching the address or
   * folly::none if no Connection is known to this Sender match address
   * or CSID for connection is not known.
   */
  virtual folly::Optional<std::string> getCSID(const Address&) const;

  /**
   * Returns client location if known or empty string.
   */
  virtual std::string getClientLocation(const ClientID&) const;

  /**
   * Fills give table with debug information about all connections managed by
   * this sender.
   */
  virtual void fillDebugInfo(InfoSocketsTable& table) const = 0;

  /**
   * @return getSockaddr()  for this thread's worker (if exists), otherwise it
   * returns an INVALID Sockaddr instance. Useful for trace loggers.
   */
  static Sockaddr sockaddrOrInvalid(const Address&);

  /////////////////// Connection decription ////////////////////

  /**
   * Returns a string of the form "W<idx>:<socket-id> (<ip>)" where
   *    <idx> is the index of Worker in its Processor
   *    <socket-id> is address.briefString()
   *    <ip> is the IP of peer to which the connection is connected to, or
   *         UNKNOWN if there is no such connection on this Worker.
   * If this method is called on a thread other than a Worker thread, it returns
   * just address.briefString().
   *
   */
  static std::string describeConnection(const Address&);
  static std::string describeConnection(const ClientID& client) {
    return describeConnection(Address(client));
  }
  static std::string describeConnection(const NodeID& node) {
    return describeConnection(Address(node));
  }
  static std::string describeConnection(node_index_t node) {
    return describeConnection(Address(node));
  }

  /////////////////// Notification callbacks ////////////////////

  /**
   * Notifies Sender that the node with given id is going down soon so the
   * connection (if exists) can gracefully closed.
   */
  virtual void setPeerShuttingDown(node_index_t) = 0;

  /**
   * Called when configuration has changed. Sender closes any open
   * connections to nodes that are no longer in the config.
   */
  void virtual noteConfigurationChanged(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>) = 0;

  virtual void onSettingsUpdated(std::shared_ptr<const Settings>) = 0;
};

}} // namespace facebook::logdevice
