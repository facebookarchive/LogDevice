/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <random>
#include <unordered_set>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/CopySetSelectorFactory.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

/**
 * @file  StorageSetAccessor is a utility class that handles the common pattern
 *        of requesting access (usually sending messages) to nodes in a nodeset
 *        of, a log and waiting for a subset of nodes that meet a specific
 *        property to reply before concluding the operation. There are two
 *        supported, failure-domain-aware, properties: F-MAJORITY and
 *        REPLICATION.
 *
 *        StorageSetAccessor accesses nodes in waves, for each wave, it attempts
 *        to access a subset of nodes. For F-MAJORITY property, it tries to
 *        access all nodes that are not yet reported success or permanent
 *        failure in each wave. For REPLICATION property, it attempts to send to
 *        a subset of nodes that may form a valid copyset together w/ the
 *        existing nodes that reported successful, plus a pre-defined amount of
 *        extra nodes.  StorageSetAccessor sends waves with a back-off retry
 *        timeout, and it keeps sending waves until receives enough replies from
 *        nodes to make a decision or the entire operation timed out.
 */

namespace facebook { namespace logdevice {

class Timer;

class StorageSetAccessor {
 public:
  enum class Result {
    // operation to the node is successful
    SUCCESS,
    // operation to the node encounters a transient error that may be able to
    // recover later, node may still be selected in the following waves
    TRANSIENT_ERROR,
    // operation to the node encounters a permanent failure, and the node will
    // be blacklisted and never be picked in the following waves
    PERMANENT_ERROR,
    // operation to the node resulted in an error such that the current
    // accessor should abort immediately
    ABORT
  };

  struct ResultStatus {
    Result result;
    Status status;
  };

  // type of result of sending the access message to a node in the node set
  using SendResult = ResultStatus;
  // type of result of the reply from a node in the node set
  using AccessResult = ResultStatus;

  // information about the how a node is accessed with other nodes in a wave
  struct WaveInfo {
    // wave number
    uint32_t wave;
    // a list of shards in the wave
    StorageSet wave_shards;
    // offset of the node to be accessed in the wave nodes
    size_t offset;
  };

  // invalid wave number as wave number must be positive
  static constexpr uint32_t WAVE_INVALID = 0;

  // type of the desired property that determine the outcome of node set
  // access based on replies received. Can be:
  //  ANY:          at least one node replies with successful access result
  //
  //  F-MAJORITY:   the nodes that replies successful access result can form an
  //                f-majority subset.
  //
  //  REPLICATION:  the nodes that replies successful access result can form a
  //                legit copyset regarding the replication property
  enum class Property { ANY, FMAJORITY, REPLICATION };

  using ShardAccessFunc =
      std::function<SendResult(ShardID, const WaveInfo& wave_info)>;
  using CompletionFunc = std::function<void(Status)>;
  using CompletionCondition = std::function<bool()>;

  /**
   * Construct a StorageSetAccessor object
   *
   * @param property      the requirement for the set of nodes to access.
   *                      For Property::REPLICATION please use
   *                      the other constructor whenever possible because
   *                      this one won't propagate weights from EpochMetaData
   *                      to CopySetSelector.
   * @param nodeset, replication   Together specify the replication property
   *                               of the log. To operate on multiple epochs,
   *                               use union of their nodesets and the
   *                               "narrowest" of their replication properties.
   *
   * @param node_access   function that sends access message to nodes
   *                      in the nodeset
   * @param completion    callback function called when the operation completes
   *                      Possible statuses:
   *                       * OK
   *                       * TIMEDOUT - `timeout` expired,
   *                       * ABORTED - AccessResult::ABORT was returned from
   *                                   `node_access`,
   *                       * FAILED - too many shards have error.
   *
   * @param timeout       time out period for the whole operation, by default
   *                      it is zero (no timeout)
   */
  StorageSetAccessor(
      logid_t log_id,
      StorageSet nodeset,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      ReplicationProperty replication,
      ShardAccessFunc node_access,
      CompletionFunc completion,
      Property property,
      std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

  /**
   * With Property::REPLICATION this constructor is strongly preferred over the
   * other one because this constructor propagates weights from EpochMetaData
   * to CopySetSelector. For Property::FMAJORITY both constructors are good.
   *
   * If you need to operate on multiple epochs (using union of nodesets),
   * use the other constructor; supposedly this is never needed for
   * Property::REPLICATION because replicating to a union nodeset makes no,
   * sense, and intersection nodeset is often empty.
   */
  StorageSetAccessor(
      logid_t log_id,
      EpochMetaData epoch_metadata,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      ShardAccessFunc node_access,
      CompletionFunc completion,
      Property property,
      std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

  /**
   * Set the number of extra nodes to access on each wave. Larger number
   * will make the procedure more likely to finish sooner.
   */
  void setExtras(copyset_size_t extras) {
    extras_ = extras;
  }

  /**
   * If set, StorageSetAccessor will complete with E::OK status if all nodes
   * in the nodeset are successfully accessed, regardless of the failure domain
   * requirements.
   *
   * Must be called before start() is called.
   */
  void successIfAllShardsAccessed();

  /**
   * If set, StorageSetAccessor will only complete with E::OK status iff all
   * nodes which are successfully accessed and satisfy the required property are
   * from the _same_ wave. In other words, once a new wave is started, the
   * success state of nodes from the previous wave will be disregarded.
   *
   * Must be called before start() is called.
   */
  void requireStrictWaves();

  /**
   * If set, StorageSetAccessor will not abort even f-majority can't be
   * achieved.  This is for cases when we could provide useful partial response,
   * i.e. FindKeyRequest
   *
   * Must be called before start() is called.
   */
  void noEarlyAbort();

  /**
   * Provide a list of nodes that must be successfully accessed in order for
   * StorageSetAccessor to successfully finish in addition to the failure domain
   * property. These required nodes will be attempted for each wave until
   * successfully accessed. Note: only consider nodes in @param required_shards
   * that are also in the nodeset used to create the StorageSetAccessor.
   *
   * Must be called before start() is called.
   */
  void setRequiredShards(const StorageSet& required_shards);

  /**
   * If called, provide a callback function to be called at the beginning of
   * each wave, right before picking nodes for the wave.
   */
  void setWavePreflightFunc(std::function<void(void)> wave_preflight) {
    wave_preflight_ = std::move(wave_preflight);
  }

  /**
   * Set the interval of min and max time out used in exponential backoff timer
   * for sending access messages to nodes. Effective only if called before
   * start() is called.
   */
  void
  setWaveTimeout(chrono_interval_t<std::chrono::milliseconds> wave_timeout) {
    wave_timeout_min_ = wave_timeout.lo;
    wave_timeout_max_ = std::max(wave_timeout.lo, wave_timeout.hi);
  }

  /**
   * Start the StorageSetAccessor object.
   */
  void start();

  /**
   * Called by the user of the class, this is to notify StorageSetAccessor the
   * result (@param result) of accessing a node (indexed by @param node) in the
   * NodeSet. Note that StorageSetAccessor may conclude on getting such result.
   *
   * If requireStrictWaves() was called before start(), @param wave is checked
   * against the current wave and stale reply will be ignored. Note that if the
   * default value (WAVE_INVALID) is provided, the result will be considered
   * from the latest wave thus _not_ considered stale.
   */
  void onShardAccessed(ShardID node,
                       AccessResult result,
                       uint32_t wave = WAVE_INVALID);

  /**
   * Set the authoritative status of a node, by default all nodes are
   * FULLY_AUTHORITATIVE. Note that for certain properties, changing
   * authoritative status can change whether or not the property is satisfied,
   * so this function has the potential to conclude StorageSetAccessor and call
   * the complete function.
   */
  void setShardAuthoritativeStatus(ShardID node, AuthoritativeStatus st);

  /**
   * Accessor to the list of nodes involved in that nodeset
   */
  const StorageSet& getShards() const {
    return epoch_metadata_.shards;
  }

  /**
   * @return  if the @param node is managed by the StorageSetAccessor
   */
  bool containsShard(ShardID shard) const {
    return failure_domain_.containsShard(shard);
  }

  /**
   * Human-readable string with information about current wave and current
   * states of shards.
   * If @param all_shards is true, all shards of the nodeset are included.
   * Otherwise, the "uninteresting" ones are omitted: if Property is FMAJORITY,
   * successfully accessed shards are omitted; if Property is ANY or
   * REPLICATION, and copyset selection was successful, unaccessed shards are
   * omitted.
   */
  std::string describeState(bool all_shards = false) const;

  /**
   * Returns human-readable concise log of events that happened, such as
   * starting waves, access attempts, access results, errors.
   * enableDebugTrace() needs to be called first.
   * Semicolon-separated sequence of events, each event one of:
   *  - required:{<shards>} - setRequiredShards(<shards>)
   *  - A:<shard>:<status> - authoritative status of <shard> set to <status>
   *  - F - failed to select copyset (but proceeding with the wave anyway)
   *  - W<n>[<shards>] - starting wave number <n>
   *  - X:<shard>:<result>:<status> - shard access began
   *  - Y<wave>:<shard>:<result>:<status> - shard accessed
   *  - done:<reason>:<status> - completed
   * Note that the representation of potentially frequent events is kept short,
   * while one-time events can be longer.
   */
  const std::string& getDebugTrace() const {
    return trace_;
  }

  void enableDebugTrace(size_t size_limit = 1000) {
    ld_check(!started_);
    trace_size_limit_ = size_limit;
  }

  virtual ~StorageSetAccessor() {}

  /**
   * Handles the situation where a shard's authoritative status changed.
   * If the request is not yet done: applies the change, and checks if this
   * brings the request to completion.
   * @return true if the request has completed (either it was already done, or
   *              it completed as a result of this status change).
   */
  bool onShardStatusChanged();

  /**
   * When the property is FMAJORITY, this function changes the mode of
   * operation as described below. Must be called before start.
   *
   * When this mode has been enabled, we may delay termination although an
   * f-majority of nodes responded. The grace period starts when we get a
   * response that brings us to an f-majority of responses. That and any
   * following responses will cause us to check completion_cond. Execution will
   * continue until:
   * 1) completion_cond returns true, or
   * 2) we hit the job timeout, or
   * 3) the grace period runs out.
   *
   * Note that the situation above will only happen when we would normally have
   * terminated with status E::OK. As such, any of the above three cases will
   * terminate with that same status, since this part is merely best-effort.
   */
  virtual void setGracePeriod(std::chrono::milliseconds grace_period,
                              CompletionCondition completion_cond);

  FailedShardsMap
  getFailedShards(std::function<bool(Status)> failure_status_predicate) const;

  const std::unordered_map<ShardID, AuthoritativeStatus, ShardID::Hash>
  getFailureDomainShardAuthoritativeStatusMap() {
    return failure_domain_.getShardAuthoritativeStatusMap();
  }

 protected:
  virtual std::unique_ptr<Timer> createJobTimer(std::function<void()> callback);
  virtual std::unique_ptr<Timer>
  createGracePeriodTimer(std::function<void()> callback);
  virtual void cancelJobTimer();
  virtual void activateJobTimer();
  virtual void onGracePeriodTimedout();
  virtual void activateGracePeriodTimer();
  virtual void cancelGracePeriodTimer();
  virtual std::unique_ptr<BackoffTimer>
  createWaveTimer(std::function<void()> callback);
  virtual std::unique_ptr<CopySetSelector> createCopySetSelector(
      logid_t log_id,
      const EpochMetaData& epoch_metadata,
      std::shared_ptr<NodeSetState> nodeset_state,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration);

  virtual ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap();

 protected:
  bool setShardAuthoritativeStatusImpl(ShardID node, AuthoritativeStatus st);
  // different stages where a node can be in
  enum class ShardState {
    NOT_SENT = 0,
    INPROGRESS = 1,
    SUCCESS = 2,
    TRANSIENT_ERROR = 3,
    PERMANENT_ERROR = 4,

    Count
  };

  static std::string getShardState(ShardState s) {
    switch (s) {
      case ShardState::NOT_SENT:
        return "U";
        break;
      case ShardState::INPROGRESS:
        return "I";
        break;
      case ShardState::SUCCESS:
        return "S";
        break;
      case ShardState::TRANSIENT_ERROR:
        return "T";
        break;
      case ShardState::PERMANENT_ERROR:
        return "P";
        break;
      default:
        static_assert((int)ShardState::Count == 5, "");
        ld_check(false);
        return "invalid";
    }
  }

  struct ShardStatus {
    ShardState state;
    // is it imperative that we successfully access this node
    bool required;
    // detail status, can be used for debugging and logging
    Status status;

    struct Hash {
      size_t operator()(const ShardStatus& status) const {
        return ((size_t)status.required << 32) | (size_t)status.state |
            (size_t)status.status;
      }
    };
    bool operator==(const ShardStatus& rhs) const {
      return state == rhs.state && required == rhs.required &&
          status == rhs.status;
    }
  };

  const logid_t log_id_;
  const Property property_;
  // timeout for the nodeset access job
  const std::chrono::milliseconds timeout_;
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
      nodes_configuration_;

  // Given responses from an f-majority of the nodes, if completion_cond is not
  // satisfied we wait for more responses for up to at most this grace_period.
  folly::Optional<std::chrono::milliseconds> grace_period_;

  // information regarding the replication property
  EpochMetaData epoch_metadata_;

  std::shared_ptr<NodeSetState> nodeset_state_;

  std::unique_ptr<CopySetSelector> copyset_selector_;

  ShardAccessFunc shard_func_;
  CompletionFunc completion_func_;
  folly::Optional<CompletionCondition> completion_cond_ = folly::none;

  using FailureDomain = FailureDomainNodeSet<ShardStatus, ShardStatus::Hash>;
  // for deciding the current set of the nodes meet the completion requirement
  // or not
  FailureDomain failure_domain_;

  // nodes that are required to successfully accessed
  std::unordered_set<ShardID, ShardID::Hash> required_shards_;

  bool allow_success_if_all_accessed_{false};
  bool require_strict_waves_{false};
  bool no_early_abort_{false};
  bool started_{false};
  bool finished_{false};
  uint32_t wave_{0};
  std::unique_ptr<BackoffTimer> wave_timer_;
  std::unique_ptr<Timer> grace_period_timer_;

  // These are used by describeState().
  StorageSet wave_shards_;
  SteadyTimestamp wave_start_time_;
  // true if copyset selector returned an error on current wave.
  bool copyset_selection_failed_ = false;

  // min and max timeout for sending waves in a backoff manner
  std::chrono::milliseconds wave_timeout_min_{500};
  std::chrono::milliseconds wave_timeout_max_{10000};

  std::function<void(void)> wave_preflight_{nullptr};

  // timer for controlling the timeout the entire operation
  std::unique_ptr<Timer> job_timer_;

  // extra nodes to send to for each wave, by default send one extras each wave
  copyset_size_t extras_{1};

  // Random number generator to use when selecting copysets.
  // Can be overridden in tests to make it deterministic.
  RNG* rng_ = &DefaultRNG::get();

  // Concise debug information about the sequence of events so far.
  std::string trace_;
  // Limit on the length of trace_.
  size_t trace_size_limit_ = 0;

  // helper function that gets an array of nodes in certain ShardStatus
  StorageSet getShardsInStatus(std::function<bool(ShardStatus)>) const;

  // helper function that gets an array of nodes in certain ShardState
  StorageSet getShardsInState(ShardState) const;

  void complete(Status status, const char* trace);
  void onJobTimedout();

  // pick a wave of nodes to send using copyset selector, the method tries to
  // pick a (partial) copyset to complement already successful nodes to meet
  // the replication requirements.
  StorageSet pickWaveFromCopySet();
  void sendWave();

  // apply the current shard authoritative status to storage nodes
  void applyShardStatus();

  // Check if the operation can be completed, if so, conclude NodeSetAccess
  // by calling complete() with E::OK or E::FAILED.
  //
  // @return true if the StorageSetAccessor is concluded, false otherwise
  bool checkIfDone();
  AccessResult accessShard(ShardID shard, const WaveInfo& wave_info);

  // disable a node in NodeSetState so that it will never be picked again in
  // copyset selection in sendWave()
  void disableShard(ShardID shard);

  // if the node is disabled in the nodeset, re-enable it
  void enableShard(ShardID shard);

  bool isRequired(ShardID shard) const;

  // utility function for changing the state of a node
  void setShardState(ShardID shard,
                     ShardState state,
                     Status detail_status = Status::UNKNOWN);

  static const char* resultString(Result result);
  static const char* resultShortString(Result result);
  static const char* stateString(ShardState state);

  friend class NodeSetAccessorTest;
  friend class GetEpochRecoveryMetadataRequestTest;
};

}} // namespace facebook::logdevice
