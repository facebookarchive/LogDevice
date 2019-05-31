/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <limits>

#include <folly/Executor.h>
#include <folly/dynamic.h>

#include "logdevice/common/PriorityMap.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/ProtocolHeader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class ProtocolReader;
class ProtocolWriter;
struct Address;
struct MessageReadResult;

/**
 * @file  an object of class Message represents a message that was received
 *        from a Socket, was composed locally, or was copy-constructed from
 *        another Message. A Message can be sent into a socket by placing
 *        it in an Envelope. A Message can be placed in exactly one Envelope,
 *        at which point the Envelope takes ownership of the Message and is
 *        responsible for destroying it once it is sent.
 *
 *        If a caller wants to broadcast a message to multiple recipients, it
 *        must copy-construct a separate instance of Message for each
 *        destination.
 */

struct Message {
  enum class CompletionMethod { IMMEDIATE, DEFERRED };

  explicit Message(MessageType type, TrafficClass tc) : type_(type), tc_(tc) {}

  /**
   * Every message knows how to serialize itself into a stream of bytes that
   * deserialize() will read.
   */
  virtual void serialize(ProtocolWriter&) const = 0;

  /**
   * The type of a static factory that constructs a Message from a
   * ProtocolReader (which wraps a read evbuffer of a bufferevent managed by a
   * Socket). Every subclass of Message representing a particular message type
   * must define a deserialize() method with this signature.
   */
  using deserializer_t = MessageReadResult(ProtocolReader&);

  Priority priority() const {
    return PriorityMap::fromTrafficClass()[tc_];
  }

  virtual int8_t getExecutorPriority() const {
    return folly::Executor::MID_PRI;
  }

  /**
   * If this method returns true, this message can be sent in plain text, even
   * if global settings say SSL should be used for communication between the
   * sender and the recipient.
   */
  virtual bool allowUnencrypted() const {
    return false;
  }

  /**
   * Calculates how much space the message will take when transmitted,
   * including the protocol header.
   */
  size_t size(uint16_t proto = Compatibility::MAX_PROTOCOL_SUPPORTED) const;

  /**
   * @return true if the message should be cancelled.
   *
   * cancelled() is useful for messages, like STORED, that are expensive
   * to send and are sent to more nodes than necessary to complete an
   * operation in order to reduce latency. Once enough responses are
   * received, the extra messages can be cancelled, saving network
   * bandwidth.
   */
  virtual bool cancelled() const {
    return false;
  }

  /**
   * This enum lists actions that a Socket may take after calling
   * Message::onReceived() on a newly received message.
   */
  enum class Disposition {
    // Delete the message. This is the success case for most messages.
    NORMAL,

    // Do not delete the message. The message must have inserted
    // itself into some data structure that now owns the message and
    // will destroy it upon some future event. This value is currently
    // used by APPEND and STORE messages only. APPEND will no longer need it
    // after #2925007. STORE may still need it for zero-copy forwarding
    // of  messages along a chain.
    KEEP,

    // The message is invalid or unexpected. The sender has violated the
    // LogDevice protocol. Delete the message and close the Socket from
    // which it was read.
    // When returning this status, assign to `err` one of: E::ACCESS,
    // E::PROTONOSUPPORT, E::PROTO, E::BADMSG, E::DESTINATION_MISMATCH,
    // E::INVALID_CLUSTER, E::INTERNAL.
    ERROR
  };

  /**
   * A Socket calls this function on a new Message after a deserialize()
   * function of a Message subclass constructed the Message from an
   * evbuffer that reads from a TCP connection managed by the Socket.
   *
   * At the time of this call the message is not owned by any other
   * object. The method is responsible for destroying the message or storing
   * it somewhere it can be found later.
   *
   * NOTE: We also support decentralised message event handlers to allow the
   * handling code to live outside common/.  Some messages do not implement
   * this method but have their handlers dispatched through
   * ServerMessageDispatch or ClientMessageDispatch.
   *
   * @param from   address this message was read from. This is the peer address
   *               of the socket that read the message.
   *
   * @return one of Disposition values defined above. 0 on success. If
   *         Disposition::ERROR is returned, the caller will pass err to
   *         Socket::close(). See enum Disposition for allowed values of err.
   */
  virtual Disposition onReceived(const Address& from) = 0;

  /**
   * A Socket calls this virtual function when it removes this message's
   * Envelope from the send queue because either the message has been sent, or
   * the Socket has determined that the message cannot be sent at all (e.g.,
   * because the other end closed connection).
   *
   * NOTE: We also support decentralised message event handlers to allow the
   * handling code to live outside common/.  Some messages do not implement
   * this method but have their handlers dispatched through
   * ServerMessageDispatch or ClientMessageDispatch.
   *
   * NOTE: during Worker destruction Sockets residing in that Worker's Sender
   *       do NOT call onSent() for messages that are still pending on their
   *       send queues. This is done to avoid accidentally referencing members
   *       of Worker that may be already destroyed. Because of this Messages
   *       should not rely on onSent() to release any resources they may own.
   *
   * @param st   E::OK if the message has been sent.
   *
   *             All other status codes indicate that the message has not
   *             been sent:
   *
   *             E::CONNFAILED one of the following occurred before the message
   *                           was fully sent into a TCP socket:
   *                            * connection attempt was refused by the other
   *                              end
   *                            * libevent signalled a connection error
   *             E::PEER_CLOSED connection was closed by the other end before
   *                            this message was fully sent
   *             E::BADMSG   connection was closed before this message was
   *                         fully sent because the other end sent an invalid
   *                         message
   *             E::PROTO    if the Socket on which this message was pending
   *                         to be sent was closed because the other side
   *                         sent a valid but unexpected message
   *             E::ACCESS   if the other side denied access during
   *                         handshake (e.g., invalid credentials)
   *             E::INVALID_CLUSTER  if the other side denied access during
   *                                 handshake due to mismatching cluster names
   *             E::NOTINCONFIG  destination to which the message was pending
   *                             is no longer in cluster config
   *             E::NOSSLCONFIG  Connection to the recipient must use SSL but
   *                             the recipient is not configured for SSL
   *                             connections.
   *             E::PROTONOSUPPORT   if this message could not be sent because
   *                                 the other end does not understand the
   *                                 version of the protocol this message is
   *                                 for. The Socket was either closed if we
   *                                 could not negociate a protocol version with
   *                                 the other end, or it is connected using a
   *                                 lower protocol version.
   *             E::DESTINATION_MISMATCH  the message destination node id does
   *                                      not match the node id of the node
   *                                      receiving the message
   *             E::CANCELLED cancelled() method of the message returned true
   *             E::INTERNAL an internal error occurred
   *             E::TIMEDOUT Timed out connecting to the socket.
   *             E::TOOBIG message is too big (exceeds payload size limit)
   *             E::SHUTDOWN The connection is closed because other end did
   *                         a graceful shutdown.
   *
   * @param to            peer address of Socket this message was pending on
   * @param enqueue_time  When the message was submitted for transmission.
   */
  virtual void onSent(Status st,
                      const Address& to,
                      const SteadyTimestamp enqueue_time) const;

  // For message handlers that don't care about equeue_time, the default
  // onSent(st, to, enqueue_time) implementation will call this legacy
  // interface.
  virtual void onSent(Status /*st*/, const Address& /*to*/) const {};

  /**
   * Get the minimum protocol version that is compatible with this message.
   * Socket will verify this against the protocol negotiated with the other end.
   * If the protocol version negotiated is lower than the value returned by this
   * function, onSent(E::PROTONOSUPPORT) will be called on that Message.
   *
   * Subclasses of Message are expected to override this method if the Message
   * is not compatible with all protocol versions that can be negotiated.
   *
   * @return Minimum version of the protocol that this message is compatible
   *         with.
   */
  virtual uint16_t getMinProtocolVersion() const {
    return Compatibility::MIN_PROTOCOL_SUPPORTED;
  }

  /**
   * By default, when a message is sent over a socket running a protocol older
   * than getMinProtocolVersion(), the messaging layer emits warnings into the
   * error log.  However, if a new message type is carefully implemented to
   * handle this situation gracefully, it can override this method to suppress
   * the warnings.
   */
  virtual bool warnAboutOldProtocol() const {
    return true;
  }

  /**
   * Returns debug information specific to a particular message type as a set of
   * key-value pairs. All keys should be unique.
   */
  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const {
    return {};
  }

  Message& operator=(const Message&) = delete;
  virtual ~Message() {}

  const MessageType type_; // type of this message
  const TrafficClass tc_;  // traffic class for this message.

  // maximum size of serialized representation of a Message in bytes is
  // max payload size + MAX_HDR_LEN. The size of ProtocolHeader is not
  // included in this limit.
  static const message_len_t MAX_HDR_LEN = 16 * 1024;
  static const message_len_t MAX_LEN = MAX_PAYLOAD_SIZE_INTERNAL + MAX_HDR_LEN;

  static_assert(MAX_PAYLOAD_SIZE_INTERNAL <
                    (size_t)std::numeric_limits<message_len_t>::max() -
                        MAX_HDR_LEN,
                "Message::MAX_LEN overflows message_len_t");
};

}} // namespace facebook::logdevice
