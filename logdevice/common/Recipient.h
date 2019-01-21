/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <limits>
#include <memory>
#include <utility>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class Appender;

/**
 * @file a Recipient represents a node to which an Appender sent a copy
 *       of a particular log record as a STORE message.
 */

class Recipient {
 public:
  class SocketClosedCallback : public SocketCallback {
   public:
    explicit SocketClosedCallback(Appender& appender, ShardID shard)
        : appender_(&appender), shard_(shard) {}

    Appender* appender() const {
      return appender_;
    }

    // called when a connection to a storage node in the most recent wave
    // of STORE messages gets closed
    void operator()(Status st, const Address& name) override;

    // Register ourselves to the Appender.
    void activate();

    void swap(SocketClosedCallback& other) noexcept {
      SocketCallback::swap(other);
      std::swap(shard_, other.shard_);
      ld_check(appender_ == other.appender_);
    }

   private:
    Appender* appender_; // back pointer to the Appender that has this
                         // Recipient object in its RecipientSet
    ShardID shard_;
  };

  class ResendStoreCallback : public BWAvailableCallback {
   public:
    ResendStoreCallback(Appender& a, Recipient& r)
        : appender_(&a),
          shard_(r.getShardID()),
          set_message_ts_(std::chrono::milliseconds(0)) {}

    Appender* appender() const {
      return appender_;
    }

    void operator()(FlowGroup&, std::mutex&) override;

    void cancelled(Status) override;

    void swap(ResendStoreCallback& other) noexcept {
      BWAvailableCallback::swap(other);
      std::swap(shard_, other.shard_);
      msg_.swap(other.msg_);
      std::swap(set_message_ts_, other.set_message_ts_);
      ld_check(appender_ == other.appender_);
    }

    void setMessage(std::unique_ptr<STORE_Message> msg) {
      msg_ = std::move(msg);
      set_message_ts_ = std::chrono::steady_clock::now();
    }

   private:
    Appender* appender_;
    ShardID shard_;
    std::unique_ptr<STORE_Message> msg_;
    std::chrono::steady_clock::time_point set_message_ts_;
  };

  enum class State : uint8_t {
    // We still have not sent STORE to that recipient.
    REQUEST_PENDING = 0,

    // We are waiting for the recipient to reply with a STORED message.
    OUTSTANDING = 1,

    // We successfully stored a copy on that recipient.
    STORED = 2,

    // storage node is out of disk space
    NO_SPC = 3,
    // failed to pass to TCP for delivery
    SEND_FAILED = 4,
    // socket closed by peer before reply
    SOCK_CLOSE = 5,
    // storage node is shutting down
    SHUTDOWN = 6,
    // storage node failed the STORE request
    STORE_FAILED = 7,
    // storage node's local log store is in persistent error state or rebuilding
    STORE_DISABLED = 8,
    // the request was dropped
    DROPPED = 9,
    // the appender was preempted only by soft seals during draining
    SOFT_PREEMPTED = 10,
    // STORE rejected because some node in copyset is in rebuilding set;
    // it may be this node or some other node
    SOMEONE_IS_REBUILDING = 11,
    // the appender was preempted
    PREEMPTED = 12,
    // STORE rejected because checksum verification failed in storage node
    CHECKSUM_MISMATCH = 13,

    Count
  };

  static const char* reasonString(State reason);

  Recipient(ShardID shard, Appender* appender)
      : shard_(shard),
        on_socket_closed_(*appender, shard),
        on_bw_avail_(*appender, *this) {}

  Recipient(const Recipient& other)
      : shard_(other.shard_),
        on_socket_closed_(*other.on_socket_closed_.appender(), other.shard_),
        on_bw_avail_(*other.on_bw_avail_.appender(), *this) {}

  /**
   * Change the state of this recipient.
   * @param New state for this recipient. Should not be State::REQUEST_PENDING.
   */
  void setState(State state);

  /**
   * @return if we still haven't got confirmation that STORE was sent to that
   * node.
   */
  bool requestPending() const;

  /**
   * @return True if we know we failed to store a copy in this recipient.
   */
  bool failed() const;

  /**
   * @return True if we successfully stored a copy on this recipient.
   */
  bool stored() const;

  /**
   * @return True if we know if we were able to store on that recipient.
   */
  bool outcomeKnown() const {
    return failed() || stored();
  }

  ShardID getShardID() const {
    return shard_;
  }

  void swap(Recipient& other) {
    if (this == &other) {
      return;
    }

    // unfortunately we can't just std::swap() the two Recipients because
    // folly::IntrusiveListHook (boost::intrusive_list_hook) doesn't seem to
    // support copy- or move-assignment. --march

    std::swap(this->shard_, other.shard_);
    this->on_socket_closed_.swap(other.on_socket_closed_);
    this->on_bw_avail_.swap(other.on_bw_avail_);
    std::swap(this->state_, other.state_);

    // the two Recipient::SocketCloseCallback objects have the same
    // .appender_ pointers. No need to swap those.
    ld_check(this->on_socket_closed_.appender() ==
             other.on_socket_closed_.appender());
  }

  /**
   * @return true iff cb is the socket callback object of this Recipient
   */
  bool hasCallback(Recipient::SocketClosedCallback* cb) {
    return &on_socket_closed_ == cb;
  }

  ResendStoreCallback& bwAvailCB() {
    return on_bw_avail_;
  }

 private:
  // node to which a STORE message with a copy of the record was sent
  ShardID shard_;

  // one-byte field indicating the state of the recipient.
  State state_ = State::REQUEST_PENDING;

  // this functor is called when a Socket through which we sent a
  // STORE messages to this recipient closes before a reply is
  // received. The effect is to decrement Appender's count of copies
  // outstanding, and if that falls below R, send another wave of
  // STORE messages in the next iteration of libevent loop.
  SocketClosedCallback on_socket_closed_;

  ResendStoreCallback on_bw_avail_;

  friend class RecipientSet;
};

}} // namespace facebook::logdevice
