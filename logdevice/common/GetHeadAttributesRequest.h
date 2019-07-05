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
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_Message.h"
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_REPLY_Message.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Request that runs in the client library in order to satisfy an
 * application's call to the getHeadAttributes() API. The goal is to find out
 * log's head attributes such as trim point and trim point timestamp.
 *
 * Sends broadcast request to all nodes in union of all historical nodesets to
 * get local data about trim point and its approximate timestamp.
 * Then use results of f-majority replies and sets trim point as maximum of
 * received trim points and trim point timestamp as minimum of received trim
 * points timestamp.
 * The main feature of this request - being efficient when underlying log
 * storage is partitioned rocksdb. In this case we can set approximate
 * timestamp of trim point as min timestamp of oldest partition which have
 * records bigger than trim point. This means that the order of error is limited
 * by partition duration (see RocksDBSettings::partition_duration_) and log
 * append rate (how much time pass between two records being appended).
 *
 * This class is a shameless copypasta from FindKeyRequest.
 */

class GetHeadAttributesRequest;

/**
 * Additionally adds access to the request object.
 * See get_head_attributes_callback_t for more details
 */
typedef std::function<void(const GetHeadAttributesRequest&,
                           Status status,
                           std::unique_ptr<LogHeadAttributes>)>
    get_head_attributes_callback_ex_t;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct GetHeadAttributesRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<GetHeadAttributesRequest>,
                     request_id_t::Hash>
      map;
};

class GetHeadAttributesRequest : public DistributedRequest,
                                 public ShardAuthoritativeStatusSubscriber {
 public:
  GetHeadAttributesRequest(logid_t log_id,
                           std::chrono::milliseconds client_timeout,
                           get_head_attributes_callback_ex_t callback)
      : DistributedRequest(RequestType::GET_HEAD_ATTRIBUTES),
        log_id_(log_id),
        client_timeout_(client_timeout),
        callback_(callback) {}

  GetHeadAttributesRequest(logid_t log_id,
                           std::chrono::milliseconds client_timeout,
                           get_head_attributes_callback_t callback)
      : GetHeadAttributesRequest(
            log_id,
            client_timeout,
            [callback](const GetHeadAttributesRequest&,
                       Status status,
                       std::unique_ptr<LogHeadAttributes> attrs) {
              callback(status, std::move(attrs));
            }) {}

  Execution execute() override;

  void onShardStatusChanged() override;

  /**
   * Called by the messaging layer after it successfully sends out our
   * GET_HEAD_ATTRIBUTES message, or fails to do so.
   */
  void onMessageSent(ShardID to, Status status);

  /**
   * Called when we receive a GET_HEAD_ATTRIBUTES_REPLY message from a storage
   * node.
   */
  void onReply(ShardID from, Status status, LogHeadAttributes attributes);

  /**
   * Called when client_timeout_timer_ fires.
   */
  void onClientTimeout();

  /**
   * Initializes state and broadcasts initial messages to all servers.
   */
  void start(Status status);

  ~GetHeadAttributesRequest() override;

  logid_t getLogID() const {
    return log_id_;
  }

 protected:
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual void deleteThis();
  virtual std::unique_ptr<NodeSetFinder> makeNodeSetFinder();
  void initNodeSetFinder();
  virtual std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration,
      StorageSet nodes,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion);
  void initStorageSetAccessor();

  /**
   * Construct a GET_HEAD_ATTRIBUTES_Message and send it to the node at index
   * idx. Can be used as node_access callback in StorageSetAccessor
   * @returns SUCCESS if operation succeeded.
   *          PERMANENT_ERROR if unrecoverable error occurred.
   *          TRANSIENT_ERROR if an error occurred but the operation can be
   *            retried.
   */
  virtual StorageSetAccessor::SendResult sendTo(ShardID shard);

 private:
  const logid_t log_id_;
  const std::chrono::milliseconds client_timeout_;
  const get_head_attributes_callback_ex_t callback_;

  std::unique_ptr<Timer> client_timeout_timer_{nullptr};
  std::unique_ptr<NodeSetFinder> nodeset_finder_{nullptr};

 protected: // overridden in unit tests
  // Make sure to call the client callback exactly once
  bool callback_called_{false};

  // save results
  lsn_t trim_point_ = LSN_INVALID;
  std::chrono::milliseconds trim_point_timestamp_ =
      std::chrono::milliseconds::max();

  // Invokes the application-supplied callback and destroys the Request
  void finalize(Status status, bool delete_this = true);
};

}} // namespace facebook::logdevice
