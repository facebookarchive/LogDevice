/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "logdevice/common/DistributedRequest.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/FINDKEY_Message.h"
#include "logdevice/common/protocol/FINDKEY_REPLY_Message.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * == General concept ==
 *
 * Request that runs in the client library in order to satisfy an
 * application's call to the findTime() or findKey() API.  As described in the
 * APIs, the goal is to find a record at or after the key (in the case of
 * findTime(), the timestamp is the key) passed to the API.
 * See logdevice/include.Client.h.
 *
 * Because one storage node contains a subset of the log's records, we need to
 * talk to more than one node to find the correct answer.  This class
 * broadcasts a FINDKEY_Message to all storage nodes.  Each storage node
 * replies with a range (lo, hi] for which it can conclude that the overall
 * answer must be in, based on the contents of its local log store.  Edge
 * cases aside, `lo` will be the LSN of the latest record strictly before the
 * target key, while `hi` will be the LSN of the earliest record at or
 * after the target key.  By combining result ranges from all storage
 * nodes, taking max(lo) and min(hi), this class calculates the actual answer
 * as max(lo) + 1.
 *
 * If not all storage nodes send back a successful reply, we still may be able
 * to find the exact answer.  For example, if, after combining the ranges we
 * did get the result range is (8, 9], then we know that 9 is the answer.
 * If, however, the range is (8, 11], then any of 9, 10 or 11 may be the
 * correct answer.  The storage nodes containing information to resolve this
 * ambiguity failed to reply.  In this case, we return 9 to the application
 * (along with a status indicating partial success) so that, if the
 * application starts reading at that LSN, it will still read all records
 * since the desired timestamp, although possibly some earlier ones as well.
 *
 * For the findTime() API, there are two special cases (that conveniently don't
 * need special handling in the client).  If a timestamp is requested that is
 * earlier than all records in the log, all storage nodes return (0, x] with
 * different x, so the API returns LSN 1.  If a timestamp is requested that is
 * later than all records, all storage nodes return (last_record, LSN_MAX] with
 * different last_record, and the API returns max(last_record) + 1, which should
 * be the LSN that will be issued to the next written record.
 *
 * In order to satisfy our request, a storage node needs to know the last
 * released LSN for a log.  Normally this is kept in memory but if there has
 * not been any activity with the log since startup, the storage node needs
 * some time to find the last released LSN (in its local log store or by
 * asking the sequencer).  In that case it quickly responds with E::AGAIN and
 * the client retries after a short delay.  This is a design choice to make
 * implementation easier; the storage node could also block until the last
 * released LSN is found and only then respond to our message.
 *
 * == approximate result ==
 * If accuracy option is FindKeyAccuracy::APPROXIMATE, findTime() will
 * only perform binary search on the partition directory in order to find the
 * newest partition whose timestamp in the directory is <= given timestamp.
 * Then it will return first lsn of given log_id in this partition.
 * The result lsn can be several minutes earlier than biggest lsn which
 * timestamp is <= given timestamp. But execution of findtime() will be much
 * faster since instead of binary search over partitions in memory + binary
 * search inside partition on disk we only do first search.
 * NOTE: Result lsn may not exists in current local storage. Most likely record
 * with this lsn it will exists somewhere in cluster. But it can be
 * last released + 1, gap or bridge record.
 */

class FindKeyRequest;
struct Settings;

/**
 * Additionally adds access to the request object.
 * See find_time_callback_t for more details
 */
typedef std::function<void(const FindKeyRequest& req, Status, lsn_t result)>
    find_time_callback_ex_t;

/**
 * Additionally adds access to the request object.
 * See find_key_callback_t for more details
 */
typedef std::function<void(const FindKeyRequest& req, FindKeyResult)>
    find_key_callback_ex_t;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct FindKeyRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<FindKeyRequest>,
                     request_id_t::Hash>
      map;
};

class FindKeyRequest : public DistributedRequest,
                       public ShardAuthoritativeStatusSubscriber {
 public:
  FindKeyRequest(logid_t log_id,
                 std::chrono::milliseconds timestamp,
                 folly::Optional<std::string> key,
                 std::chrono::milliseconds client_timeout,
                 find_time_callback_ex_t callback,
                 find_key_callback_ex_t callback_key,
                 FindKeyAccuracy accuracy)
      : DistributedRequest(RequestType::FIND_KEY),
        log_id_(log_id),
        timestamp_(timestamp),
        key_(std::move(key)),
        client_timeout_(client_timeout),
        callback_(callback),
        callback_key_(callback_key),
        accuracy_(accuracy),
        running_timer_timeout_(client_timeout) {}

  FindKeyRequest(logid_t log_id,
                 std::chrono::milliseconds timestamp,
                 folly::Optional<std::string> key,
                 std::chrono::milliseconds client_timeout,
                 find_time_callback_t callback,
                 find_key_callback_t callback_key,
                 FindKeyAccuracy accuracy)
      : FindKeyRequest(
            log_id,
            timestamp,
            key,
            client_timeout,
            [callback](const FindKeyRequest&, Status st, lsn_t result) {
              callback(st, result);
            },
            [callback_key](const FindKeyRequest&, FindKeyResult res) {
              callback_key(res);
            },
            accuracy) {}

  Execution execute() override;

  void onShardStatusChanged() override;

  logid_t getLogID() const {
    return log_id_;
  }

  /**
   * Called by the messaging layer after it successfully sends out our
   * FINDKEY message, or fails to do so.
   */
  void onMessageSent(ShardID to, Status);

  /**
   * Called when we receive a FINDKEY_REPLY message from a storage node.
   */
  void onReply(ShardID from, Status, lsn_t lo, lsn_t hi);

  /**
   * Called when client_timeout_timer_ fires.
   */
  virtual void onClientTimeout();

  // Initializes NodeSetFinder.  Public for tests.
  void initNodeSetFinder();

  ~FindKeyRequest() override;

 protected: // tests can override
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual std::unique_ptr<NodeSetFinder>
  makeNodeSetFinder(logid_t log_id,
                    std::chrono::milliseconds timeout,
                    std::function<void(Status status)> cb) const;
  virtual void deleteThis();

  virtual int sendOneMessage(const FINDKEY_Header& header, ShardID to);

  /**
   * Construct a FINDKEY_Message and send it to the node at index
   * idx. Can be used as node_access callback in StorageSetAccessor
   * @returns SUCCESS if operation succeeded.
   *          PERMANENT_ERROR if unrecoverable error occurred.
   *          TRANSIENT_ERROR if an error occurred but the operation can be
   *            retried.
   */
  virtual StorageSetAccessor::SendResult sendTo(ShardID shard);

  virtual std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc shard_access,
      StorageSetAccessor::CompletionFunc completion);

  void initStorageSetAccessor();

 private:
  const logid_t log_id_;
  const std::chrono::milliseconds timestamp_;
  const folly::Optional<std::string> key_;
  const std::chrono::milliseconds client_timeout_;
  const find_time_callback_ex_t callback_;
  const find_key_callback_ex_t callback_key_;
  const FindKeyAccuracy accuracy_;

  std::chrono::milliseconds running_timer_timeout_;
  std::unique_ptr<Timer> client_timeout_timer_;
  std::chrono::time_point<std::chrono::steady_clock> client_timer_expiry_;

  // Result range.  Initially (0, LSN_MAX].  As storage nodes reply to our
  // FINDKEY messages, this range gets narrower.
  lsn_t result_lo_ = 0;
  lsn_t result_hi_ = LSN_MAX;

  // Make sure to call the client callback exactly once
  bool callback_called_ = false;

  // Invokes the application-supplied callback and (in most cases) destroys
  // the Request
  void finalize(Status, bool delete_this = true);

  // Called when we were able to determine the union of all nodesets for the
  // log.
  void start(Status status);

  // Received some valid responses from nodes. Could callback with E:PARTIAL in
  // worst case
  bool hasResponse() {
    return result_lo_ != 0 || result_hi_ != LSN_MAX;
  }

 protected:
  std::unique_ptr<NodeSetFinder> nodeset_finder_;
};

}} // namespace facebook::logdevice
