/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/NodeSetAccessor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file GetEpochRecoveryMetadataRequest is the state machine responsible for
 * getting EpochRecoveryMetadata(ERM) for a given range of epochs.
 */

class GetEpochRecoveryMetadataRequest
    : public ShardAuthoritativeStatusSubscriber,
      public Request {
 public:
  // The callback that gets called when this request completes.
  // Status can either be E:OK or E:ABORTED.
  // If the request find a server running an older version of
  // protocol, E:ABORTED is returned. Otherwise, request
  // runs to completion and returns E:OK
  // EpochRecoveryStateMap is constructed based on the responses
  // from servers for the requested epochs. Possible values for
  // status and corresponding EpochRecoveryMetadata are
  // Status     EpochRecoveryMetadata
  // E::OK      VALID
  // E::EMPTY   INVALID
  // E::UNKNOWN INVALID
  // See GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h for this is
  // constructed for individual epochs in the requested range
  using CompletionCallback =
      std::function<void(Status status,
                         std::unique_ptr<EpochRecoveryStateMap> map)>;

  GetEpochRecoveryMetadataRequest(worker_id_t worker_id,
                                  logid_t log_id,
                                  shard_index_t shard,
                                  epoch_t start,
                                  epoch_t end,
                                  epoch_t purge_to,
                                  std::shared_ptr<EpochMetaData> metadata,
                                  CompletionCallback cb);

  Execution execute() override;

  int getThreadAffinity(int nthreads) override;

  void onSent(ShardID to, Status st);

  void onReply(ShardID from,
               Status status,
               std::unique_ptr<EpochRecoveryStateMap> epochRecoveryStateMap);

  void onShardStatusChanged() override;

  /**
   * Static handler for GET_EPOCH_RECOVERY_METADATA message sent notifications
   * from the messaging layer.  Looks up the GetEpochRecoveryStateRequest state
   * machine and calls the instance method below.
   */
  static void onSent(const GET_EPOCH_RECOVERY_METADATA_Message& msg,
                     Status,
                     const Address& to);

  // This is required becasue the multi index container in worker,
  // which is indexed by request id cannot take a const-member as key for
  // index. To overcome this, we use const_mem_fun key extractor which allows
  // value returned by constant member function to be the key of the index
  request_id_t getRequestId() const {
    return id_;
  }
  logid_t getLogId() const {
    return log_id_;
  }
  epoch_t getStartEpoch() const {
    return start_;
  }
  epoch_t getPurgeTo() const {
    return purge_to_;
  }
  shard_index_t getShard() const {
    return shard_;
  }

  ~GetEpochRecoveryMetadataRequest() override {}

 protected:
  virtual void registerWithWorker();

  // Removes itself from the worker's map
  virtual void deleteThis();

  virtual std::unique_ptr<StorageSetAccessor>
  makeStorageSetAccessor(StorageSetAccessor::ShardAccessFunc node_access,
                         StorageSetAccessor::CompletionFunc completion);

  virtual std::unique_ptr<BackoffTimer> createRetryTimer();

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual const Settings& getSettings() const;

  virtual const ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap();

  virtual std::unique_ptr<Timer>
  createDeferredCompleteTimer(std::function<void()> callback);

  virtual void activateDeferredCompleteTimer();

  virtual void deferredComplete();

  // for sending messages
  std::unique_ptr<SenderBase> sender_;
  const std::shared_ptr<EpochMetaData> epoch_metadata_;

  // A zero delay timer that on firing, calls the provided
  // callback and deletes the request from worker's map
  std::unique_ptr<Timer> deferredCompleteTimer_;

 private:
  // We'll keep sending GET_EPOCH_RECOVERY_METADATA messages to storage nodes
  // until one of:
  //  (1) we've got a nonempty reply,
  //  (2) we've got empty replies from a copyset of nodes,
  //  (3) we've got any replies from all fully authoritative nodes.
  // storage_set_accessor_ is used for choosing which nodes to send to and for
  // detecting condition (2). nodes_responded_ is used for checking
  // condition (3).

  std::unique_ptr<StorageSetAccessor> storage_set_accessor_;
  using ResponseNodeSet = FailureDomainNodeSet<bool>;
  std::unique_ptr<ResponseNodeSet> nodes_responded_;

  std::unique_ptr<BackoffTimer> retry_timer_;

  void start();

  void createStorageSetAccessorAndStart();

  StorageSetAccessor::SendResult
  sendGetEpochRecoveryMetadataRequest(ShardID shard);

  void onStorageSetAccessorComplete(Status status);

  StorageSetAccessor::AccessResult
  processResponse(ShardID from,
                  Status status,
                  std::unique_ptr<EpochRecoveryStateMap> epochRecoveryStateMap);

  // Check if all fully authoritative nodes in the nodeset have sent us a
  // response, complete purging for the epoch regardless of the response.
  // If at least one node responds with E::EMPTY, consider the epoch empty,
  // otherwise, skip purging for the epoch and increase local LCE.
  //
  // Note that this is currently a work around to prevent purging from being
  // stuck after unlikely cases in non-authoritative recovery. Data in the epoch
  // might become inconsistent.
  void checkAllFullyAuthoritativeNodesResponded();

  // Check if we have a valid metadata for all the epochs in the
  // range [start_, end_].
  // This is useful if we have received partial response from
  // many nodes however the union of responses has a valid
  // epoch recovery metadata for all epochs we care about
  bool checkIfDone();

  // Builds the EpochRecoveryStateMap containing
  // the status and EpochRecoveryMetadata (if available) for all the epochs
  // in range [start_, end_]
  void buildEpochRecoveryStateMap();

  void complete(Status status);

  // calls the provided callback and deletes
  // the request from worker's map
  void done();

  // Used to identify if the request is for
  // fetching EpochRecoveryMetadata for a range
  // of epochs.
  bool isRangeRequest() const {
    return start_ != end_ && end_ != EPOCH_INVALID;
  }

  virtual NodeID getMyNodeID();

  // retry delay used when StorageSetAccessor fails its execution
  static const std::chrono::milliseconds INITIAL_RETRY_DELAY;
  static const std::chrono::milliseconds MAX_RETRY_DELAY;

  const worker_id_t worker_id_;
  const logid_t log_id_;
  const shard_index_t shard_;
  const epoch_t start_;
  const epoch_t end_;
  const epoch_t purge_to_;
  CompletionCallback cb_;

  using EpochRecoveryMetadataResponses =
      std::unordered_map<epoch_t::raw_type,
                         std::pair<EpochRecoveryMetadata,
                                   /*a set of shards that returned E:EMPTY*/
                                   std::set<ShardID>>>;

  EpochRecoveryMetadataResponses epochRecoveryMetadataResponses_;
  std::unique_ptr<EpochRecoveryStateMap> epochRecoveryStateMap_;
  // Status to track the completion of the request. Request is
  // considered complete when result is a valid status other than E::UNKNOWN
  // A valid status on completetion is E:OK or E:ABORTED
  Status result_{E::UNKNOWN};
  friend class GetEpochRecoveryMetadataRequestTest;
};

// It is a multi index map only to maintain backward compatibility
// with servers running an older verion of the protocol.
// TODO:T23693338 remove once all servers are running new protocol
struct GetEpochRecoveryMetadataRequestMap {
  struct RequestIndex {};
  struct PurgeIndex {};

  using request_id_member = boost::multi_index::const_mem_fun<
      GetEpochRecoveryMetadataRequest,
      request_id_t,
      &GetEpochRecoveryMetadataRequest::getRequestId>;

  using log_id_member = boost::multi_index::const_mem_fun<
      GetEpochRecoveryMetadataRequest,
      logid_t,
      &GetEpochRecoveryMetadataRequest::getLogId>;

  using purge_to_member = boost::multi_index::const_mem_fun<
      GetEpochRecoveryMetadataRequest,
      epoch_t,
      &GetEpochRecoveryMetadataRequest::getPurgeTo>;

  using shard_member = boost::multi_index::const_mem_fun<
      GetEpochRecoveryMetadataRequest,
      shard_index_t,
      &GetEpochRecoveryMetadataRequest::getShard>;

  using epoch_member = boost::multi_index::const_mem_fun<
      GetEpochRecoveryMetadataRequest,
      epoch_t,
      &GetEpochRecoveryMetadataRequest::getStartEpoch>;

  boost::multi_index::multi_index_container<
      std::unique_ptr<GetEpochRecoveryMetadataRequest>,
      boost::multi_index::indexed_by<
          // Index by request_id
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<RequestIndex>,
              request_id_member,
              request_id_t::Hash>,
          // Index by (log_id, purge_to, shard, epoch)
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<PurgeIndex>,
              boost::multi_index::composite_key<GetEpochRecoveryMetadataRequest,
                                                log_id_member,
                                                purge_to_member,
                                                shard_member,
                                                epoch_member>,
              boost::multi_index::composite_key_hash<logid_t::Hash,
                                                     epoch_t::Hash,
                                                     std::hash<shard_index_t>,
                                                     epoch_t::Hash>>>>
      requests;
};

}} // namespace facebook::logdevice
