/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>

#include <boost/noncopyable.hpp>
#include <folly/IntrusiveList.h>
#include <folly/Optional.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file Implementation detail of ClientReadStream.  Maintains state for one
 *       sender.
 */

class ClientReadStream;
class ClientReadStreamDependencies;

class ClientReadStreamSenderState : boost::noncopyable {
 public:
  /**
   * State of the storage node regarding whether it can deliver a record
   * of next_lsn_to_deliver_. Used for gap detection.
   */
  enum class GapState {

    // The storage node may deliver us a record with next_lsn_to_deliver_.
    // This is the initial value of GapState for storage nodes, meaning the
    // ClientReadStream does not have the information to conclude the storage
    // node won't send a record with next_lsn_to_deliver_.
    NONE = 0,

    // The storage node cannot send us a record with next_lsn_to_deliver_
    // because we already received a record or a gap (with its right end >=
    // next_lsn_to_deliver_) from it. Since storage nodes deliver records/gaps
    // in the order of increasing LSNs, we know that the smallest record this
    // storage node can deliver is larger than next_lsn_to_deliver_.
    GAP = 1,

    // Similar to GAP, but the storage node has indicated that it is
    // possibly missing records by setting the UNDER_REPLICATED_REGION flag
    // in a record, or issuing an UNDER_REPLICATED Gap. The node's response
    // cannot count toward an f-majority for dataloss gap detection, but
    // has confirmed it does not have a record at this LSN.
    UNDER_REPLICATED = 2
  };

  /**
   * State of the storage node regarding whether it is being blacklisted by the
   * client for this read stream. In SINGLE_COPY_DELIVERY mode, clients may
   * blacklist shards if they are down or if they are very slow. If blacklisted,
   * other storage shards will send records that this sender was supposed send.
   */
  enum class BlacklistState {
    // The Shard is not filtered out. This is the default state and must also
    // be used in ALL_SEND_ALL mode.
    NONE = 0,
    // The Shard is filtered out because the client thinks it is down.
    DOWN = 1,
    // The Shard is filtered out because the client thinks it is slow.
    SLOW = 2
  };

  /**
   * @param client_read_stream pointer to owner ClientReadStream instance
   * @param deps               pointer to ClientReadStreamDependencies instance
   * @param shard_id           ID of the shard to read from
   * @param node_d             ID of the node this shard belongs to.
   * @param reconnect_timer    timer to use when reconnecting
   * @param started_timer      timer to use when waiting for STARTED
   * @param retry_window_timer timer to use when resending WINDOW messages
   */
  ClientReadStreamSenderState(
      ClientReadStream* client_read_stream,
      ShardID shard_id,
      std::unique_ptr<BackoffTimer>&& reconnect_timer,
      std::unique_ptr<BackoffTimer>&& started_timer,
      std::unique_ptr<BackoffTimer>&& retry_window_timer);

  /**
   * The largest LSN such that this node sent a data record that is >= next_lsn_
   * with this LSN.
   * Reset each time we rewind the stream.
   * This is used to detect when a storage node that is in the known down list
   * started sending records that fall in the current window again.
   */
  lsn_t max_data_record_lsn;

  /**
   * The number of times the node had failed to send a record when the
   * grace_period_ period timer expired. Any node where
   * next_lsn_ <= grace_round_next_lsn_ is expected to send a record when in
   * all send all mode.
   */
  int grace_counter;

  // This is used to prevent a race condition where we decide to failover to all
  // send all mode, but then receive records that were already enqueued by the
  // storage nodes. We don't want to process these records because we are in all
  // send all mode and might be tricked into thinking that there is a gap.
  // So we only accept records that are sent after we receive a STARTED message
  // with this filter_version in the header so that we are sure the RECORD
  // messages that follow were properly filtered.
  filter_version_t filter_version;

  // The last valid filter version seen in a STARTED message. A filter
  // version is considered valid if it greater than or equal to the
  // last filter version received in a STARTED message. This field is
  // used to detect and provide timeout extensions for nodes that are
  // behind the current filter version, but are catching up.
  filter_version_t last_received_filter_version;

  // BlacklistState of the sender shard, initialized to NONE. Must remain
  // NONE if single-copy-delivery is not enabled.
  BlacklistState blacklist_state = BlacklistState::NONE;

  /**
   * True if the sender reported a potentially underreplicated region
   * (GapState::UNDER_REPLICATED), and we believe that it may actually be mising
   * records. We'll add such senders to known down list on next rewind.
   * Set to true when a gap is detected, and this sender is in
   * GapState::UNDER_REPLICATED. Set to false when gap state changes to GAP,
   * i.e. the sender leaves underreplicated region.
   *
   * Blacklisting of underreplicated senders is deferred like this because
   * storage nodes' underreplication reports are often greatly overestimated.
   * Most of the time the sender is not actually missing any relevant records,
   * and we'll be able to go through the unrerreplicated region without any
   * gaps and rewinds. Delaying the rewind also reduces number of rewinds if
   * multiple senders report underreplication at about the same time.
   * On the other hand, delaying the rewind may increase the amount of wasted
   * work on the servers when the rewind does happen. So it's a tradeoff.
   */
  bool should_blacklist_as_under_replicated = false;

  /**
   * This sender shipped UNDER_REPLICATED gaps up to, and excluding, this LSN.
   * Cleared on rewind.
   */
  lsn_t under_replicated_until = LSN_INVALID;

  // Pointer to owner
  ClientReadStream* client_read_stream_;

  // @see ClientReadStream::RecordState
  folly::IntrusiveListHook list_hook_;

  // Sets a new upper end for the window.
  void setWindowHigh(lsn_t new_window_high);

  // Gets window upper end.
  lsn_t getWindowHigh() const {
    return window_high_;
  }

  // Sets a new next_lsn.
  void setNextLsn(lsn_t new_next_lsn);

  lsn_t getNextLsn() const {
    return next_lsn_;
  }

  enum class ConnectionState {
    // Initial state, should not stay here
    INITIAL = 0,

    // We asked for a START message to be sent out and are waiting for the
    // messaging layer to tell us if it was sent out successfully via
    // onStartSent().
    CONNECTING = 1,

    // START message was sent out over the network.  Waiting for the server to
    // respond with a STARTED message.  started_timer_ is on; if the server
    // does not respond in time we will try again.
    START_SENT = 2,

    // STARTED message was received with status OK or REBUILDING.  We leave
    // this state when the server disconnects (see onNodeDisconnected()).
    READING = 3,

    // Socket was closed or the server reported a transient error.  The
    // reconnect timer is active.
    RECONNECT_PENDING = 4,

    // The server reported a persistent error.  We should not retry until the
    // socket is closed, suggesting the server restarted and the persistent
    // error may be gone.
    PERSISTENT_ERROR = 5,

    // Transient state for when the cluster shrinks
    DYING = 6,
  };

  void setConnectionState(ConnectionState connection);

  void onConnectionAttemptFailed();

  ConnectionState getConnectionState() const {
    return connection_state_;
  }

  const char* getConnectionStateName() const;

  void setGapState(GapState gap_state);

  GapState getGapState() const {
    return gap_state_;
  }

  bool isHealthy() const {
    return connection_state_ == ConnectionState::READING;
  }

  AuthoritativeStatus getAuthoritativeStatus() const {
    return authoritative_status_;
  }

  bool isFullyAuthoritative() const {
    return authoritative_status_ == AuthoritativeStatus::FULLY_AUTHORITATIVE;
  }

  void setAuthoritativeStatus(AuthoritativeStatus st) {
    authoritative_status_ = st;
  }

  /**
   * Activate the timer to reconnect after some amount of time.
   */
  void activateReconnectTimer();

  void cancelReconnectTimer() {
    reconnect_timer_->cancel();
  }

  void resetReconnectTimer() {
    reconnect_timer_->reset();
  }

  bool reconnectTimerIsActive() const {
    return reconnect_timer_->isActive();
  }

  void activateStartedTimer();

  /**
   * If the provided filter version closes the gap between
   * the last filter version the node has provided via STARTED
   * and the current filter version, grant a new started timer
   * quantum.
   */
  void extendStartedTimer(filter_version_t);

  void resetStartedTimer() {
    started_timer_->reset();
  }

  void cancelStartedTimer() {
    started_timer_->cancel();
  }

  void activateRetryWindowTimer() {
    retry_window_timer_->activate();
  }

  void resetRetryWindowTimer() {
    retry_window_timer_->reset();
  }

  ShardID getShardID() const {
    return shard_id_;
  }

  filter_version_t getFilterVersion() const {
    return filter_version;
  }

  /**
   * Exposes the SocketCallback object that should be notified if the
   * connection to this server gets closed.
   */
  SocketCallback* getSocketClosedCallback() {
    return &on_socket_close_;
  }

  // Called when reconnect_timer_ goes off.
  void reconnectTimerCallback();

  // Called when started_timer_ goes off.
  void startedTimerCallback();

 private:
  /**
   * Upper end of the window that this storage node knows about.  We keep
   * track of this to avoid sending duplicate WINDOW messages.
   *
   * Updated when we send out START and WINDOW, checked before we send out
   * WINDOW.
   */
  lsn_t window_high_;

  /**
   * The largest LSN such that:
   *   a) the storage node sent a record with LSN next_lsn-1, or
   *   b) a gap message was sent with (inclusive) end LSN next_lsn-1
   * The next record or gap this node sends us is expected to start at this LSN.
   * We're only interested in this value if it's > `next_lsn_to_deliver_' in
   * ClientReadStream. Otherwise we don't bother updating it and it might be
   * underestimated.
   */
  lsn_t next_lsn_;

  // Id of the shard we are reading from.
  ShardID shard_id_;

  // NodeID of the node this shard belongs to.
  // Note: this is necessary because the Sender API requires a NodeID instead of
  // just a node_index_t which we can retrieve with `shard_id_.node()`. We
  // should remove this when we remove node generation and NodeID from the code
  // base.
  NodeID node_id_;

  // Connection state
  ConnectionState connection_state_ = ConnectionState::INITIAL;

  // GapState of the sender node, initialized to NONE
  GapState gap_state_ = GapState::NONE;

  // Authoritative status of this node. @see AuthoritativeStatus.h
  AuthoritativeStatus authoritative_status_ =
      AuthoritativeStatus::FULLY_AUTHORITATIVE;

  class SocketClosedCallback : public SocketCallback {
   public:
    explicit SocketClosedCallback(ClientReadStreamSenderState* state)
        : state_(state) {}
    void operator()(Status st, const Address& name) override;

   private:
    ClientReadStreamSenderState* state_;
  };

  SocketClosedCallback on_socket_close_;

  std::unique_ptr<BackoffTimer> reconnect_timer_;
  std::unique_ptr<BackoffTimer> started_timer_;
  std::unique_ptr<BackoffTimer> retry_window_timer_;
  friend class ClientReadStreamTest;
  friend class MockClientReadStreamDependencies;
};
}} // namespace facebook::logdevice
