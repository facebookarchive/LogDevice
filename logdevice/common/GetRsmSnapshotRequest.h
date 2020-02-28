/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <set>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_Message.h"
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_REPLY_Message.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class GetRsmSnapshotRequest;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct GetRsmSnapshotRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<GetRsmSnapshotRequest>,
                     request_id_t::Hash>
      map;
};

class GetRsmSnapshotRequest : public Request {
 public:
  explicit GetRsmSnapshotRequest(int thread_affinity,
                                 WorkerType worker_type,
                                 std::string key,
                                 lsn_t min_ver,
                                 RSMSnapshotStore::snapshot_cb_t cb,
                                 GET_RSM_SNAPSHOT_flags_t flags = 0)
      : Request(RequestType::GET_RSM_SNAPSHOT),
        sender_(std::make_unique<SenderProxy>()),
        worker_type_(worker_type),
        thread_affinity_(thread_affinity),
        key_(key),
        min_ver_(min_ver),
        cb_(cb),
        flags_(flags) {}

  Execution execute() override;

  /**
   * Called when we receive a GET_RSM_SNAPSHOT_REPLY message from a server node
   */
  void onReply(const Address& from, const GET_RSM_SNAPSHOT_REPLY_Message& msg);

  /**
   * Called when request timer fires
   */
  virtual void onRequestTimeout();
  virtual void onWaveTimeout();
  virtual void onError(Status status, NodeID dest);

  ~GetRsmSnapshotRequest() override {}

  int getThreadAffinity(int /* unused */) override {
    return thread_affinity_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 protected:
  /**
   * Construct a GET_RSM_SNAPSHOT_Message and send it to given node.
   */
  void sendTo(NodeID to, bool force = false);

  virtual bool init();
  virtual void initTimers();

  virtual void cancelRequestTimer();
  virtual void cancelWaveTimer();
  virtual void activateWaveTimer();
  /**
   * start request timer
   */
  virtual void start();
  bool populateCandidates();
  bool retryLimitReached() {
    return retry_cnt_ >= retry_limit_;
  }

  virtual const Settings& getSettings() const;

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  void finalize(Status st, std::string snapshot_blob, lsn_t snapshot_ver);
  void finalize(const GET_RSM_SNAPSHOT_REPLY_Message& msg);
  virtual ClusterState* getClusterState() const;
  virtual void registerWithWorker();
  virtual void destroyRequest();
  std::unique_ptr<SenderBase> sender_;
  // Holds the candidate server nodes to which this request can be sent
  std::unordered_set<node_index_t> candidates_;

 private:
  WorkerType worker_type_;
  int thread_affinity_;
  std::string key_;
  lsn_t min_ver_;
  RSMSnapshotStore::snapshot_cb_t cb_;
  GET_RSM_SNAPSHOT_flags_t flags_;
  NodeID last_dest_;
  std::unique_ptr<Timer> request_timer_;
  std::unique_ptr<BackoffTimer> wave_timer_;
  // If we don't put a limit to reusing cluster nodes, it can cause
  // stack overflow in certain scenarios e.g. if all cluster nodes
  // are unreachable.
  int retry_cnt_{0};
  int retry_limit_{3};
  friend class MockGetRsmSnapshotRequest;
};

}} // namespace facebook::logdevice
