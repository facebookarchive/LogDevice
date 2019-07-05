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
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * Request that runs in the client library in order to satisfy an
 * application's call to the dataSize() API. The goal is to get the approximate
 * size of the data in a particular log for a particular time range.
 *
 * Given a log ID and a time range, this request attempts to give an estimate
 * of the size of the data for the log in that time range. To do so, we need to
 * talk to all potentially relevant storage nodes and ask them roughly how much
 * data they have fitting these parameters. For details about how storage nodes
 * calculate this estimate, check out PartitionedRocksDBStore::dataSize.
 * This request will terminate when we find ourselves in one of these states:
 *  - At least an f-majority of the nodes successfully gave us an estimate.
 *    We'll fill in the remaining nodes with the current average answer, to
 *    get a more accurate answer. The accuracy of this assumption depends on
 *    how large the time range we're looking at is, in relation to how
 *    consistently the log is being written to. Finish with status OK.
 *  - All nodes that do not have permanent errors are in some transient error
 *    state, such as being rebuilt. In this case, we choose not to wait for
 *    the rebuilding to finish and other transient error states to go away, but
 *    give our best estimate in the same way as we did above: assume that all
 *    nodes that didn't respond due to some transient error have the average
 *    amount of data on them that match this request's parameters. Finish with
 *    status PARTIAL.
 *  - Before we got to any of the above cases, the client timeout ran out.
 *    Finish with status TIMEDOUT and size 0.
 *  - We didn't get to any of the above, but too many nodes responded with
 *    some permanent error state: finish with FAILED and size 0.
 *
 * On early termination, any further responses from other nodes can be safely
 * ignored.
 *
 * Upon receiving an AGAIN error status from a storage node, this class attempts
 * to retry sending the request after some delay.
 */

class DataSizeRequest;

/**
 * Additionally adds access to the request object.
 * See data_size_callback_t for more details
 */
typedef std::function<void(const DataSizeRequest&, Status status, size_t size)>
    data_size_callback_ex_t;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct DataSizeRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<DataSizeRequest>,
                     request_id_t::Hash>
      map;
};

static_assert(static_cast<int>(DataSizeAccuracy::COUNT) == 1,
              "Some values of DataSizeAccuracy not supported!");

class DataSizeRequest : public DistributedRequest,
                        public ShardAuthoritativeStatusSubscriber {
 public:
  DataSizeRequest(logid_t log_id,
                  std::chrono::milliseconds start,
                  std::chrono::milliseconds end,
                  DataSizeAccuracy /*accuracy*/,
                  data_size_callback_ex_t callback,
                  std::chrono::milliseconds client_timeout)
      : DistributedRequest(RequestType::DATA_SIZE),
        log_id_(log_id),
        start_(start),
        end_(end),
        client_timeout_(client_timeout),
        callback_(callback) {}

  DataSizeRequest(logid_t log_id,
                  std::chrono::milliseconds start,
                  std::chrono::milliseconds end,
                  DataSizeAccuracy accuracy,
                  data_size_callback_t callback,
                  std::chrono::milliseconds client_timeout)
      : DataSizeRequest(
            log_id,
            start,
            end,
            accuracy,
            [callback](const DataSizeRequest&, Status st, size_t size) {
              callback(st, size);
            },
            client_timeout) {}

  void setWorkerThread(worker_id_t worker) {
    worker_ = worker;
  }
  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  Execution execute() override;

  /**
   * Called by the messaging layer after it successfully sends out our
   * DATA_SIZE message, or fails to do so.
   */
  void onMessageSent(ShardID to, Status status);

  /**
   * Called when we receive a DATA_SIZE_REPLY message from a storage node.
   */
  void onReply(ShardID from, Status status, size_t size);

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

  ~DataSizeRequest() override;

 protected: // overridden in tests
  using shard_status_t = uint8_t;
  // A shard is considered to have a result when it either responded to the
  // request, or the shard is AUTHORITATIVE_EMPTY, in which case it has no data.
  static const shard_status_t SHARD_HAS_RESULT = 1u << 0;
  static const shard_status_t SHARD_HAS_ERROR = 1u << 1;
  // A shard may be considered to be in rebuilding if its authoritative status
  // is either UNDERREPLICATION or UNAVAILABLE.
  static const shard_status_t SHARD_IS_REBUILDING = 1u << 2;
  using FailureDomain = FailureDomainNodeSet<shard_status_t>;

  std::unique_ptr<FailureDomain> failure_domain_;
  bool completion_cond_called_ = false;

  static constexpr bool node_has_result_filter(shard_status_t val) {
    return val & SHARD_HAS_RESULT;
  }
  static constexpr bool potential_response_filter(shard_status_t val) {
    return ~val & SHARD_HAS_RESULT && ~val & SHARD_IS_REBUILDING &&
        ~val & SHARD_HAS_ERROR;
  }

  // Checks if there are enough outstanding non-underreplicated nodes to reach
  // any of the above two.
  bool haveDeadEnd();

  // ------------------  overridden in unit tests  ------------------

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual void deleteThis();
  virtual std::unique_ptr<NodeSetFinder> makeNodeSetFinder();
  void initNodeSetFinder();
  virtual std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion);
  /**
   * Construct a DATA_SIZE_Message and send it to given shard.
   * can be used as node_access callback in StorageSetAccessor
   * @returns SUCCESS if operation succeeded.
   *          PERMANENT_ERROR if unrecoverable error occurred.
   *          TRANSIENT_ERROR if an error occurred but the operation can be
   *            retried.
   */
  virtual StorageSetAccessor::SendResult sendTo(ShardID shard);
  virtual ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap();

 private:
  const logid_t log_id_;
  const std::chrono::milliseconds start_;
  const std::chrono::milliseconds end_;
  const std::chrono::milliseconds client_timeout_;
  const data_size_callback_ex_t callback_;
  size_t data_size_ = 0;

  int replication_factor_ = 0;
  worker_id_t worker_ = worker_id_t(-1);
  std::unique_ptr<NodeSetFinder> nodeset_finder_{nullptr};

  // Make sure to call the client callback exactly once
  bool callback_called_{false};

  // save results
  Status status_{E::UNKNOWN};

  void initStorageSetAccessor();

  // Invokes the application-supplied callback and destroys the Request
  void finalize(Status status, bool delete_this = true);

  void applyShardStatus(bool initialize_unknown);
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
};

}} // namespace facebook::logdevice
