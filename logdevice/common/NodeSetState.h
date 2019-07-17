/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <limits>
#include <unordered_map>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/*
 * @file NodeSetState keeps tracks of the temporarily unavailable states
 *       for nodes in a node set. The state is specified by a NotAvailableReason
 *       flag as well as a time_point of notAvailableUntil.
 */

// the type representing number of unavailable nodes
// we use a signed type here because the value could be -1 in some
// situations (see comments below for availability_counters_).
typedef node_index_t nodeset_ssize_t;

static_assert(std::numeric_limits<nodeset_ssize_t>::is_signed,
              "nodeset_ssize_t must be a signed type");

class NodeSetState : public std::enable_shared_from_this<NodeSetState> {
 public:
  enum class NotAvailableReason : uint8_t {
    // node is considered available
    NONE = 0,
    // node's low watermark for space has been crossed
    // node is still considered available
    LOW_WATERMARK_NOSPC = 1,

    // node is overloaded
    OVERLOADED = 2,
    // node is out of free disk space
    NO_SPC = 3,
    // node is unroutable
    UNROUTABLE = 4,
    // node's local log store is persisitenly not accepting writes:
    // persistent error state or rebuilding
    STORE_DISABLED = 5,
    // If a storage node couldn't process a STORE within configured timeout,
    // move it to a temporary unavailable state. If many nodes enter this state
    // thereby affecting picking of copysets from remaining pool of nodes,
    // just remove all nodes that are in this state.
    SLOW = 6,
    // node's health is currently being probed by
    // CheckNodeHealthRequest
    PROBING = 7,

    // number of reasons
    Count
  };

  /*
   * If the health check is ENABLED for the nodeset, when the node is
   * unavailable for a certain reason and the deadline expires, the node's
   * NodeSetState status will be updated to PROBING and a CHECK_NODE_HEALTH
   * message will be sent. If health check is DISABLED, on expiration of the
   * deadline, node is made available by clearing the NotAvailableReason.
   *
   */

  enum class HealthCheck { ENABLED, DISABLED };

  enum class GrayListResetReason : uint8_t {
    CANT_PICK_COPYSET = 0,
    THRESHOLD_REACHED = 1,
    DISABLED_IN_SETTINGS = 2,
    // number of reasons
    Count
  };

  explicit NodeSetState(const StorageSet& nodes,
                        logid_t log_id,
                        HealthCheck healthCheck);

  virtual ~NodeSetState();

  /**
   * @return a human-readable string with the name of reason.
   */
  static const char* reasonString(NotAvailableReason reason);

  /**
   * Get the not_available_until timestamp indicating the time point until which
   * shard [shard] is temporarily disabled because it was reported either
   * running out of free disk space or overloaded.
   *
   * @param  shard   Shard that must belong to the storage set.
   * @return         std::chrono::steady_clock::time_point::min() if there is no
   *                 recent report of unavailable state for the shard
   */
  std::chrono::steady_clock::time_point
  getNotAvailableUntil(ShardID shard) const;

  /**
   * Get the NodeAvailableReason for a shard in the storage set.
   *
   * @param  shard   Shard that must belong to the storage set.
   */
  virtual NotAvailableReason getNotAvailableReason(ShardID shard) const;

  /**
   * Get the deadline (time till which node is considered unavailable for given
   * reason) for a node in the nodeset based on retry interval mentioned in
   * settings for the given reason
   *
   * @param reason Reason why a not is not available
   */
  virtual std::chrono::steady_clock::time_point
  getDeadline(NotAvailableReason reason) const;

  /**
   * Clear the not_available_until timestamp unconditionally.
   * this indicates that the node is currently not known to be either out of
   * space or overloaded
   *
   * @param  shard   Shard that must belong to the storage set.
   */
  void clearNotAvailableUntil(ShardID shard);

  /**
   * given the current time as a timepoint, atomically check if the
   * notAvailableUntil timestamp is expired, and
   *  if health check is enabled,
   *    set the state to PROBING and send a probe
   *  otherwise clear the timestamp and set NotAvailableReason to NONE
   *
   * @param  shard   Shard that must belong to the storage set.
   * @param  now     current time_point
   *
   * @return  the current not_available_until timestamp, or
   * std::chrono::steady_clock::time_point::min() if the shard is available
   */
  std::chrono::steady_clock::time_point
  checkNotAvailableUntil(ShardID shard,
                         std::chrono::steady_clock::time_point now);

  StatsHolder* getStats() const {
    return Worker::stats();
  }

  /**
   * Clear the gray list (nodes in SLOW state). This should be called if:
   * a) too many nodes are in SLOW state OR
   * b) picking a copyset couldn't be accomplished
   * c) graylisting was disabled in settings
   */
  virtual void resetGrayList(GrayListResetReason);

  virtual const Settings* getSettings() const;

  virtual const std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const;

  /**
   * Update the not_available_until timestamp.
   * Note that we do not update the timestamp if the shard specified has already
   * been marked as not available, even if the until_time provided is newer.
   * Also, when probes are enabled, the only valid state transition are:
   * Available   -> Unavailable
   * Unavailable -> Available
   * PROBING -> Unavailable
   * PROBING -> Available
   *
   * @param  shard      The shard for which to set the timestamp. The function
   *                    does nothing.
   * @param  until_time time until which the shard is considered as unavailable
   * @param  reason     Reason why shard is not available
   *
   * @return true if the reason and until_time were successfully updated.
   */
  bool setNotAvailableUntil(
      ShardID shard,
      std::chrono::steady_clock::time_point until_time,
      NotAvailableReason reason,
      folly::Optional<NotAvailableReason> allow_transition_from = folly::none);

  /**
   * Update the not_available_until timestamp as above except find the
   * until_time based on the retry interval specified in setting for
   * the given reason
   */
  bool setNotAvailableUntil(
      ShardID shard,
      NotAvailableReason reason,
      folly::Optional<NotAvailableReason> allow_transition_from = folly::none);

  /**
   * Return the number of nodes that are not available for a specified reason.
   */
  virtual nodeset_ssize_t
  numNotAvailableShards(NotAvailableReason reason) const;

  size_t numShards() const {
    return all_shards_cnt_;
  }

  bool containsShard(ShardID shard) const {
    return shard_states_.count(shard);
  }

  std::shared_ptr<NodeSetState> getSharedPtr() {
    return shared_from_this();
  }

  const nodeset_state_id_t id_;

  // refresh NodeSetState by checking ShardState for each node against
  // the current time. This function is rate-limited by
  // Worker::settings().nodeset_state_refresh_interval.
  void refreshStates();

  bool isHealthCheckEnabled() const {
    return healthCheck_ == HealthCheck::ENABLED;
  }

  bool consideredAvailable(NotAvailableReason reason) const {
    return reason == NotAvailableReason::NONE ||
        reason == NotAvailableReason::LOW_WATERMARK_NOSPC;
  }

  bool grayListingEnabledInSettings() const;

  /**
   * Should we clear the nodes in SLOW state.
   */
  bool shouldClearGrayList() const;

 protected:
  virtual void postHealthCheckRequest(ShardID shard,
                                      bool perform_space_based_retention);
  virtual void checkAndSendSpaceBasedTrims();

 private:
  // ShardState for a node is used to keep track of whether a node is
  // temporarily marked as not available in the nodeset, until what time it
  // is still being considered as not available, and the reason why it is
  // considered as not available.
  // It is implemented as a 64-bit unsigned integer in which the highest
  // * bits store a enum class NotAvailableReason, and the lower 56 bits
  // store the number of ticks (in milliseconds) of std::chrono::steady_clock
  // representing the duration since the clock epoch.
  class ShardState {
   public:
    explicit ShardState() noexcept {}

    static const size_t DURATION_SHIFT = (64 - 8);
    static const uint64_t DURATION_MASK = ~((uint64_t)0xff << DURATION_SHIFT);

    ShardState(NotAvailableReason reason,
               std::chrono::steady_clock::duration duration)
        : val_((static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        duration)
                        .count()) &
                DURATION_MASK) |
               (static_cast<uint64_t>(reason) << DURATION_SHIFT)) {
      ld_check(reason < NotAvailableReason::Count);
    }

    std::chrono::steady_clock::duration getDuration() const {
      return std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(
              val_ & DURATION_MASK)));
    }

    std::chrono::steady_clock::time_point getTimePoint() const {
      return std::chrono::steady_clock::time_point(getDuration());
    }

    NotAvailableReason getReason() const {
      return static_cast<NotAvailableReason>(val_ >> DURATION_SHIFT);
    }

   private:
    uint64_t val_{0};
  } __attribute__((__aligned__(8), packed));

  /**
   * Get the setting space_based_retention_node_threshold. It is the fraction
   * of NOSPC nodes in the nodeset which the sequencer will try to keep below,
   * by initiating space-based retention when it is reached or exceeded.
   */
  virtual double getSpaceBasedRetentionThreshold() const;

  /**
   * When a node is in probing state, this function will indicate whether or
   * not to tell nodes to perform space-based retention, if enabled.
   * @param old_reason    NotAvailableReason prior to entering PROBING state
   */
  virtual bool shouldPerformSpaceBasedRetention() const;

  /**
   * Get the setting gray_list_nodes_threshold. It is the max percentage
   * of nodeset nodes that can be in graylist at any given time.
   */
  virtual double getGrayListThreshold() const;

  using shard_state_atomic_t = std::atomic<ShardState>;

  // total number of nodes in cluster configuration that belong to this
  // NodeSet, excluding those with zero weight
  size_t all_shards_cnt_;

  // When enabled, we will send a probe to determine the current status
  // of the node and update node's state to PROBING
  NodeSetState::HealthCheck healthCheck_;

  // An auxiliary map of node index to shard_state_atomic_t
  // Keys of the map are fixed after construction and the map is
  // considered as thread-safe.
  std::unordered_map<ShardID, shard_state_atomic_t, ShardID::Hash>
      shard_states_;

  // No. of different types of nodes that are available/not-available.
  // This is an array of NotAvailableReason::Count elments of type
  // std::atomic<nodeset_ssize_t>
  // Due to a race condition happened after the CAS loop that sets
  // the ShardState and before increasing/decreasing the availability_counters_
  // counter, this value may occasionally be out of bounds, which could be
  // either -num_threads or all_shards_cnt_ + num_threads. This is still
  // considered OK, as it is guranteed that the value will EVENTUALLY be
  // correct.
  std::atomic<nodeset_ssize_t> availability_counters_[static_cast<uint8_t>(
      NotAvailableReason::Count)] = {};

  // controls when NodeSetState can be refreshed next
  std::atomic<std::chrono::steady_clock::duration> no_refresh_until_{
      std::chrono::steady_clock::duration::min()};

  // id of the log to which this nodeset state and in turn the node set belongs
  const logid_t log_id_;

  // prevents attempts to check space based trimming conditions for a
  // nodeset until this time
  std::atomic<std::chrono::steady_clock::duration> next_trimming_check_{
      std::chrono::steady_clock::duration::min()};

  // UID for every instance of NodeSet. Used to dedupe health check requests
  static std::atomic<nodeset_state_id_t::raw_type> next_id;

  // Saving the enabled/disabled state of graylisting, so that
  // when settings change, we don't need to update each and every NodeSetState
  // object.
  std::atomic<bool> graylisting_enabled_;
};

}} // namespace facebook::logdevice
