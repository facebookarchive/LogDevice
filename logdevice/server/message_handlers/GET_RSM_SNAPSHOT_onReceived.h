/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/Address.h"
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_Message.h"
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_REPLY_Message.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

struct Address;

// This Request gets executed on the RSM worker, which can be different
// from the worker which received the GET_RSM_SNAPSHOT_Message.
// Once done, ReturnRsmSnapshotFromMemoryRequest is used to send the
// reply back to the original worker, which then sends the reply back to client.
class GetRsmSnapshotFromMemoryRequest : public Request {
 public:
  explicit GetRsmSnapshotFromMemoryRequest(WorkerType worker_type,
                                           int target_thread,
                                           WorkerType src_worker_type,
                                           int src_thread,
                                           logid_t rsm_type,
                                           lsn_t min_ver,
                                           Address from,
                                           request_id_t rqid)
      : Request(RequestType::GET_RSM_SNAPSHOT),
        worker_type_(worker_type),
        target_thread_(target_thread),
        src_worker_type_(src_worker_type),
        src_thread_(src_thread),
        rsm_type_(rsm_type),
        min_ver_(min_ver),
        from_(from),
        rqid_(rqid) {}

  ~GetRsmSnapshotFromMemoryRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /* unused */) override {
    return target_thread_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  WorkerType worker_type_;
  int target_thread_;
  WorkerType src_worker_type_;
  int src_thread_;
  logid_t rsm_type_;
  lsn_t min_ver_;
  Address from_;
  request_id_t rqid_;
};

// This class returns the snapshot blob to the client from the worker
// that accepted the GET_RSM_SNAPSHOT_Message
class ReturnRsmSnapshotFromMemoryRequest : public Request {
 public:
  explicit ReturnRsmSnapshotFromMemoryRequest(
      WorkerType worker_type,
      int target_thread,
      Address to,
      GET_RSM_SNAPSHOT_REPLY_Header reply_hdr,
      std::string snapshot_blob)
      : Request(RequestType::GET_RSM_SNAPSHOT),
        worker_type_(worker_type),
        target_thread_(target_thread),
        to_(to),
        reply_hdr_(reply_hdr),
        snapshot_blob_(std::move(snapshot_blob)) {}

  ~ReturnRsmSnapshotFromMemoryRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /* unused */) override {
    return target_thread_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  WorkerType worker_type_;
  int target_thread_;
  Address to_;
  GET_RSM_SNAPSHOT_REPLY_Header reply_hdr_;
  std::string snapshot_blob_;
};

Message::Disposition GET_RSM_SNAPSHOT_onReceived(GET_RSM_SNAPSHOT_Message* msg,
                                                 const Address& from);

}} // namespace facebook::logdevice
