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

#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_Message.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_REPLY_Message.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

struct CheckNodeHealthRequestSet;

/**
 * @file This request is a node health check state machine.
 * Currently, its created by NodeSetState.
 */
class CheckNodeHealthRequest : public Request {
 public:
  CheckNodeHealthRequest(ShardID shard,
                         std::shared_ptr<NodeSetState> nodeset_state,
                         logid_t log_id,
                         nodeset_state_id_t nodeset_state_id,
                         std::chrono::seconds request_timeout,
                         bool enable_space_based_trimming);

  ShardID shard_;
  logid_t log_id_;
  nodeset_state_id_t nodeset_state_id_;
  std::chrono::seconds request_timeout_;

  Request::Execution execute() override;

  int getThreadAffinity(int nthreads) override;

  // This is required becasue the multi index container in worker,
  // which is indexed by request id cannot take a const-member as key for
  // index. To overcome this, we use const_mem_fun key extractor which allows
  // value returned by constant member function to be the key of the index
  request_id_t getRequestId() const {
    return id_;
  }

  void onReply(Status status);
  void noReply();

  ~CheckNodeHealthRequest() override;

 private:
  // callback used to detect when connection to `shard_` is closed
  // and set the NodeSetState appropriately
  class SocketClosedCallback : public SocketCallback {
   public:
    explicit SocketClosedCallback(CheckNodeHealthRequest* request)
        : request_(request) {}
    void operator()(Status st, const Address& /*unused*/) override {
      ld_debug("CheckNodeHealthRequest::SocketClosedCallback called with"
               "status %s ",
               error_name(st));
      request_->noReply();
    }

   private:
    CheckNodeHealthRequest* request_;
  };

  // Creates a CHECK_NODE_HEALTH message and updates map on worker
  // if send was successful.
  int createAndSendCheckNodeHealthMessage();

  // Remove this object from Worker's pending_health_check, causing it to be
  // deleted
  void deleteThis();

  std::weak_ptr<NodeSetState> nodeset_state_;
  std::chrono::steady_clock::time_point request_sent_time_;
  std::unique_ptr<Timer> request_timeout_timer_;
  const bool enable_space_based_trimming_;
  SocketClosedCallback on_socket_close_;

 protected:
  std::unique_ptr<SenderBase> sender_;
  virtual CheckNodeHealthRequestSet& pendingHealthChecks();
  virtual const Settings& getSettings();
  virtual void activateRequestTimeoutTimer();
};

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct CheckNodeHealthRequestSet {
  // A multi index container to keep track of the outstanding health check
  // requests. Index by request id helps in routing the response to appropriate
  // CheckNodeHealthRequest state machine and index by NodeSetStateId,
  // NodeId helps in deduping requests for the same node per nodeset.
  struct RequestIndex {};
  struct NodeSetStateIdShardIDIndex {};

  using request_id_member =
      boost::multi_index::const_mem_fun<CheckNodeHealthRequest,
                                        request_id_t,
                                        &CheckNodeHealthRequest::getRequestId>;

  using nodesetstate_id_member =
      boost::multi_index::member<CheckNodeHealthRequest,
                                 nodeset_state_id_t,
                                 &CheckNodeHealthRequest::nodeset_state_id_>;

  using shard_member = boost::multi_index::
      member<CheckNodeHealthRequest, ShardID, &CheckNodeHealthRequest::shard_>;

  boost::multi_index::multi_index_container<
      std::unique_ptr<CheckNodeHealthRequest>,
      boost::multi_index::indexed_by<
          // index by request_id
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<RequestIndex>,
              request_id_member,
              request_id_t::Hash>,
          // index by (nodeset_state_id, shard)
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<NodeSetStateIdShardIDIndex>,
              boost::multi_index::composite_key<CheckNodeHealthRequest,
                                                nodesetstate_id_member,
                                                shard_member>,
              boost::multi_index::composite_key_hash<nodeset_state_id_t::Hash,
                                                     ShardID::Hash>>>>
      set;
};

}} // namespace facebook::logdevice
