/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/DistributedRequest.h"
#include "logdevice/common/NodeSetAccessor.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_Message.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_REPLY_Message.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Request that runs in the client library in order to satisfy an
 * application's call to the isLogEmpty() API. The goal is to find out
 * whether a particular log is empty. Empty is defined here as, there is no
 * record past the trim point on any storage node of the cluster for the
 * given log.
 *
 * Because one storage node contains a subset of the log's records, we need to
 * talk to more than one node to find the correct answer. This class
 * broadcasts an IS_LOG_EMPTY_Message to all storage nodes. Each storage node
 * replies with either true, false or an error status. The state of the log
 * falls into one of the following situations:
 *   - At least a copyset of nodes reply false: this means that there may be
 *     records sufficiently replicated and not (partially) trimmed such that we
 *     should consider the log non empty. In this case, this class terminates
 *     the processing early.
 *   - At least an f-majority of nodes reply true or are authoritative empty:
 *     this means that there is no record in that log, on at least f nodes. the
 *     log can be considered empty. In other words, if the log was not empty,
 *     at least one of these nodes would return false, therefore the log must
 *     be empty.
 *   - At least an f-majority of nodes responded, but none of the above were
 *     fullfilled. We started a grace period and waited for more nodes to chime
 *     in, but this remained the case until either the client timeout or grace
 *     period ran out, or only nodes that are being rebuilt or in some other
 *     transient or permanent error state remain. The log will be considered
 *     non-empty in this case, and we'll finish with status PARTIAL to indicate
 *     that the result isn't completely certain.
 *   - We did not yet receive an f-majority of responses, but all remaining
 *     nodes are either in rebuilding, or in some transient or permanent error
 *     state. Finish early and consider the log non-empty: if some nodes failed
 *     to respond because they were in some permanent error state, consider
 *     this request FAILED. Otherwise, meaning all nodes that didn't respond
 *     had some transient error or was being rebuilt, consider this a PARTIAL
 *     result.
 *   - Some nodes reply with error or didn't reply at all until the client
 *     timeout expired, such that we do not have an f-majority of nodes that
 *     chimed in on the state of the log: this means that there may be some
 *     network issues or the cluster is in an error state (possibly transient).
 *     In that case the class terminates the processing with either FAILED or
 *     PARTIAL status depending on wether it received at least one successful
 *     response.
 *
 * On early termination, any further responses from other nodes can be safely
 * ignored.
 *
 * Upon receiving an AGAIN error status from a storage node, this class attempts
 * to retry sending the request after some delay.
 *
 * This class is a shameless copypasta from FindKeyRequest.
 */

class IsLogEmptyRequest;

/**
 * Additionally adds access to the request object.
 * See is_empty_callback_t for more details
 */
typedef std::function<void(const IsLogEmptyRequest&, Status status, bool empty)>
    is_empty_callback_ex_t;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct IsLogEmptyRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<IsLogEmptyRequest>,
                     request_id_t::Hash>
      map;
};

class IsLogEmptyRequest : public DistributedRequest,
                          public ShardAuthoritativeStatusSubscriber {
 public:
  // NodeSetAccessor takes a range of allowed wave timeouts, which is the
  // starting point and maximum of exponential backoff. Choose the lower bound
  // depending on the client timeout and clamp to the below range. This gives a
  // little bit of flexibility for low-latency clusters to retry faster, while
  // more throughput-oriented users will frequently spare storage servers from
  // answering requests twice in case of a wave timeout, at the cost of less
  // frequent hits to latency.
  static const long WAVE_TIMEOUT_LOWER_BOUND_MIN = 500;
  static const long WAVE_TIMEOUT_LOWER_BOUND_MAX = 1500;

  IsLogEmptyRequest(logid_t log_id,
                    std::chrono::milliseconds client_timeout,
                    is_empty_callback_ex_t callback,
                    std::chrono::milliseconds grace_period =
                        std::chrono::milliseconds::zero())
      : DistributedRequest(RequestType::IS_LOG_EMPTY),
        log_id_(log_id),
        client_timeout_(client_timeout),
        callback_(callback),
        grace_period_(grace_period) {}

  IsLogEmptyRequest(logid_t log_id,
                    std::chrono::milliseconds client_timeout,
                    is_empty_callback_t callback,
                    std::chrono::milliseconds grace_period =
                        std::chrono::milliseconds::zero())
      : IsLogEmptyRequest(
            log_id,
            client_timeout,
            [callback](const IsLogEmptyRequest&, Status status, bool empty) {
              callback(status, empty);
            },
            grace_period) {}

  void setWorkerThread(worker_id_t worker) {
    worker_ = worker;
  }
  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  Execution execute() override;

  /**
   * Called by the messaging layer after it successfully sends out our
   * IS_LOG_EMPTY message, or fails to do so.
   */
  void onMessageSent(ShardID to, Status status);

  /**
   * Called when we receive a IS_LOG_EMPTY_REPLY message from a storage node.
   */
  void onReply(ShardID from, Status status, bool empty);

  /**
   * Initializes state and broadcasts initial messages to all servers.
   */
  void start(Status status);

  void onShardStatusChanged() override {
    onShardStatusChanged(false);
  }
  // If 'initialize' is set, don't expect to find existing state for shards,
  // and don't update NodeSetAccessor, since it updates itself when started.
  void onShardStatusChanged(bool initialize);

  logid_t getLogID() const {
    return log_id_;
  }

  ~IsLogEmptyRequest() override;

 protected:
  using shard_status_t = uint8_t;
  static const shard_status_t SHARD_HAS_RESULT = 1u << 0;
  static const shard_status_t SHARD_IS_EMPTY = 1u << 1;
  static const shard_status_t SHARD_HAS_ERROR = 1u << 2;
  // A shard may be considered to be in rebuilding if its authoritative status
  // is either UNDERREPLICATION or UNAVAILABLE.
  static const shard_status_t SHARD_IS_REBUILDING = 1u << 3;
  using FailureDomain = FailureDomainNodeSet<shard_status_t>;
  using AuthoritativeStatusMap = FailureDomain::AuthoritativeStatusMap;

  std::unique_ptr<FailureDomain> failure_domain_;
  bool completion_cond_called_ = false;

  // Below are used to determine if we are ready to terminate with a result.
  bool haveNonEmptyCopyset() const;
  bool haveEmptyFMajority() const;
  // Checks if there are enough outstanding non-underreplicated nodes to reach
  // any of the above two.
  bool haveDeadEnd() const;
  // Checks if we've got an f-majority of unambiguous responses
  bool haveFmajorityOfResponses() const;
  // Checks whether some nodes weren't able to give us an answer for other
  // reasons than being rebuilt
  bool haveOnlyRebuildingFailures() const;

  // ------------------  overridden in unit tests  ------------------
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;
  virtual void deleteThis();
  virtual std::unique_ptr<NodeSetFinder> makeNodeSetFinder();
  void initNodeSetFinder();
  virtual std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion);
  /**
   * Construct an IS_LOG_EMPTY_Message and send it to given shard.
   * can be used as node_access callback in StorageSetAccessor
   * @returns SUCCESS if operation succeeded.
   *          PERMANENT_ERROR if unrecoverable error occurred.
   *          TRANSIENT_ERROR if an error occurred but the operation can be
   *            retried.
   */
  virtual StorageSetAccessor::SendResult sendTo(ShardID shard);
  virtual ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap();

  static chrono_interval_t<std::chrono::milliseconds>
  getWaveTimeoutInterval(std::chrono::milliseconds timeout);

  // Print any differences between shard statuses of failure_domain_, and the
  // corresponding status according to nodeset_accessor_. Return true if there
  // is a difference.
  bool haveShardAuthoritativeStatusDifferences(
      const AuthoritativeStatusMap* fd_before = nullptr,
      const AuthoritativeStatusMap* na_before = nullptr) const;

  // Create a human-readable string with details on all shards that aren't
  // considered fully authoritative. Returned string might be an error message.
  std::string getHumanReadableShardStatuses() const;

  std::string getNonEmptyShardsList() const;

  void applyShardStatus(bool initialize_unknown);

 private:
  const logid_t log_id_;
  const std::chrono::milliseconds client_timeout_;
  const is_empty_callback_ex_t callback_;
  const std::chrono::milliseconds grace_period_;

  worker_id_t worker_ = worker_id_t(-1);
  std::unique_ptr<NodeSetFinder> nodeset_finder_{nullptr};

  static bool node_empty_filter(shard_status_t val) {
    return (val & SHARD_HAS_RESULT) && (val & SHARD_IS_EMPTY);
  }

  static bool node_non_empty_filter(shard_status_t val) {
    return (val & SHARD_HAS_RESULT) && (~val & SHARD_IS_EMPTY);
  }

  bool completion_cond_allowed_termination_ = false;

  // Make sure to call the client callback exactly once
  bool callback_called_{false};

  // Only to be used for debugging purposes
  bool started_{false};

  // save results
  Status status_{E::UNKNOWN};

  StorageSet shards_;

  void initStorageSetAccessor();

  // check if request is done and call finalize
  void finalizeIfDone();

  // Invokes the application-supplied callback and destroys the Request
  void finalize(Status status, bool empty, bool delete_this = true);

  void setShardAuthoritativeStatus(ShardID shard,
                                   AuthoritativeStatus st,
                                   bool initialize_unknown);

  std::unique_ptr<FailureDomain> makeFailureDomain(
      StorageSet shards,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration,
      ReplicationProperty replication) {
    return std::make_unique<FailureDomain>(
        shards, *nodes_configuration, replication);
  }

  void completion(Status st);

  // Human-readable representation of shard_status_t
  static std::string getShardState(shard_status_t s);
};

}} // namespace facebook::logdevice
