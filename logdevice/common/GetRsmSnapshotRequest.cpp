/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/GetRsmSnapshotRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_Message.h"

namespace facebook { namespace logdevice {

void GetRsmSnapshotRequest::registerWithWorker() {
  auto& map = Worker::onThisThread()->runningGetRsmSnapshotRequests().map;
  map.insert(std::make_pair(id_, std::unique_ptr<GetRsmSnapshotRequest>(this)));
}

Request::Execution GetRsmSnapshotRequest::execute() {
  ld_debug(
      "id_:%lu, key:%s, min_ver:%s, worker:%s",
      id_.val_,
      key_.c_str(),
      lsn_to_string(min_ver_).c_str(),
      Worker::getName(worker_type_, worker_id_t(thread_affinity_)).c_str());
  registerWithWorker();
  if (!init()) {
    RSMSnapshotStore::SnapshotAttributes snapshot_attrs(
        LSN_INVALID, std::chrono::milliseconds(0));
    cb_(E::FAILED, "", snapshot_attrs);
    return Execution::COMPLETE;
  }
  start();
  return Execution::CONTINUE;
}

void GetRsmSnapshotRequest::initTimers() {
  request_timer_ = std::make_unique<Timer>([this] { onRequestTimeout(); });
  request_timer_->activate(getSettings().rsm_snapshot_request_timeout);

  // wave timer will be activated before sending out the request
  wave_timer_ = std::make_unique<ExponentialBackoffTimer>(
      [this]() { onWaveTimeout(); },
      Worker::settings().rsm_snapshot_request_wave_timeout);
}

bool GetRsmSnapshotRequest::populateCandidates() {
  ld_check(!candidates_.size());
  const auto& nodes_cfg = getNodesConfiguration();
  auto& service_discovery = nodes_cfg->getServiceDiscovery();
  ClusterState* cs = getClusterState();
  ld_check(cs);

  for (const auto& kv : *service_discovery) {
    if (cs->isNodeAlive(kv.first)) {
      candidates_.insert(kv.first);
    }
  }

  if (!candidates_.size()) {
    ld_error("No cluster nodes found to send the request(id:%lu).", id_.val_);
    return false;
  }
  return true;
}

bool GetRsmSnapshotRequest::init() {
  if (!populateCandidates()) {
    return false;
  }
  initTimers();
  return true;
}

ClusterState* GetRsmSnapshotRequest::getClusterState() const {
  return Worker::getClusterState();
}

void GetRsmSnapshotRequest::start() {
  if (!candidates_.size()) {
    auto limit_reached = retryLimitReached();
    if (limit_reached) {
      ld_info("Exhausted all possible nodes, id_:%lu", id_.val_);
    }
    if (limit_reached || !populateCandidates()) {
      finalize(E::FAILED, "", LSN_INVALID);
      return;
    }
    retry_cnt_++;
  }

  const auto& nodes_cfg = getNodesConfiguration();
  auto random_it = std::next(
      candidates_.begin(),
      folly::Random::rand32(static_cast<uint32_t>(candidates_.size())));
  NodeID dest = nodes_cfg->getNodeID(*random_it);
  if (nodes_cfg->isNodeInServiceDiscoveryConfig(dest.index())) {
    sendTo(dest);
  } else {
    ld_debug("Removing N%hu as it's no longer in config", dest.index());
    candidates_.erase(*random_it);
    start();
  }
}

const Settings& GetRsmSnapshotRequest::getSettings() const {
  return Worker::settings();
}

void GetRsmSnapshotRequest::onError(Status st, NodeID dest) {
  ld_debug("st:%s, removing N%hu, and calling start(), id_:%lu, key_:%s",
           error_name(st),
           dest.index(),
           id_.val_,
           key_.c_str());
  cancelWaveTimer();
  candidates_.erase(dest.index());
  start();
}

void GetRsmSnapshotRequest::onWaveTimeout() {
  onError(E::TIMEDOUT, last_dest_);
}

void GetRsmSnapshotRequest::sendTo(NodeID to, bool force) {
  ld_debug("Sending GET_RSM_SNAPSHOT_Message to Node %s%s, id_:%lu, key:%s",
           Sender::describeConnection(to).c_str(),
           force ? " forcefully" : "",
           id_.val_,
           key_.c_str());
  auto flags = flags_;
  flags |= force ? GET_RSM_SNAPSHOT_Message::FORCE : 0;
  GET_RSM_SNAPSHOT_Header header = {min_ver_, id_, flags};
  auto msg = std::make_unique<GET_RSM_SNAPSHOT_Message>(header, key_);
  activateWaveTimer();
  last_dest_ = to;
  int rv = sender_->sendMessage(std::move(msg), to);
  if (rv != 0) {
    ld_error("Failed to send a GET_RSM_SNAPSHOT_Message to %s, st:%s, rqid:%lu",
             Sender::describeConnection(to).c_str(),
             error_description(err),
             id_.val_);
    onError(err, to);
  }
}

void GetRsmSnapshotRequest::onReply(const Address& from,
                                    const GET_RSM_SNAPSHOT_REPLY_Message& msg) {
  auto& msg_hdr = msg.getHeader();
  auto& msg_snapshot_blob = msg.getSnapshotBlob();
  RATELIMIT_INFO(
      std::chrono::seconds(1),
      2,
      "from:%s, st:%s, delta_log_id:%lu, rqid:%lu, redirect_node:%hd, "
      "snapshot_ver:%s, blob:[%s], snapshot blob size:%zu",
      Sender::describeConnection(from).c_str(),
      error_name(msg_hdr.st),
      msg_hdr.delta_log_id.val_,
      msg_hdr.rqid.val_,
      msg_hdr.redirect_node,
      lsn_to_string(msg_hdr.snapshot_ver).c_str(),
      hexdump_buf(
          msg_snapshot_blob.data(), std::min(30ul, msg_snapshot_blob.size()))
          .c_str(),
      msg_snapshot_blob.size());

  switch (msg_hdr.st) {
    case E::OK:
    case E::NOTSUPPORTED:
    case E::TOOBIG:
    case E::STALE:
      finalize(msg);
      break;
    case E::FAILED:
    case E::EMPTY:
    case E::SHUTDOWN:
    case E::NOTFOUND:
      onError(msg_hdr.st, from.asNodeID());
      break;
    case E::REDIRECTED: {
      cancelWaveTimer();
      const auto& nodes_cfg = getNodesConfiguration();
      auto dest = nodes_cfg->getNodeID(msg_hdr.redirect_node);
      ld_debug("Following REDIRECT from %s to %s",
               Sender::describeConnection(from).c_str(),
               Sender::describeConnection(dest).c_str());
      sendTo(dest, true /*force*/);
    } break;
    default:
      ld_error("Received invalid status:%s from %s",
               error_name(msg_hdr.st),
               Sender::describeConnection(from).c_str());
  };
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GetRsmSnapshotRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

void GetRsmSnapshotRequest::destroyRequest() {
  Worker* worker = Worker::onThisThread();
  auto& map = worker->runningGetRsmSnapshotRequests().map;
  auto it = map.find(id_);
  ld_check(it != map.end());
  map.erase(it); // destroys unique_ptr which owns this
}

void GetRsmSnapshotRequest::cancelRequestTimer() {
  if (request_timer_) {
    request_timer_->cancel();
  }
}

void GetRsmSnapshotRequest::cancelWaveTimer() {
  if (wave_timer_) {
    wave_timer_->cancel();
  }
}

void GetRsmSnapshotRequest::activateWaveTimer() {
  if (wave_timer_) {
    wave_timer_->activate();
  }
}

void GetRsmSnapshotRequest::onRequestTimeout() {
  RATELIMIT_INFO(std::chrono::seconds(1),
                 2,
                 "Request Timedout, id_:%lu, key_:%s",
                 id_.val_,
                 key_.c_str());
  finalize(E::TIMEDOUT, "", LSN_INVALID);
}

void GetRsmSnapshotRequest::finalize(Status st,
                                     std::string snapshot_blob,
                                     lsn_t snapshot_ver) {
  cancelRequestTimer();
  cancelWaveTimer();
  RSMSnapshotStore::SnapshotAttributes snapshot_attrs(
      snapshot_ver, std::chrono::milliseconds(0));
  cb_(st, std::move(snapshot_blob), snapshot_attrs);
  destroyRequest();
}

void GetRsmSnapshotRequest::finalize(
    const GET_RSM_SNAPSHOT_REPLY_Message& msg) {
  auto& msg_hdr = msg.getHeader();
  auto& msg_snapshot_blob = msg.getSnapshotBlob();
  ld_log(msg_hdr.st == E::OK ? dbg::Level::DEBUG : dbg::Level::INFO,
         "Finalized request(id_:%lu, key_:%s), st:%s, snapshot(ver:%s, "
         "blob:[%s], size:%lu",
         id_.val_,
         key_.c_str(),
         error_name(msg_hdr.st),
         lsn_to_string(msg_hdr.snapshot_ver).c_str(),
         hexdump_buf(
             msg_snapshot_blob.data(), std::min(30ul, msg_snapshot_blob.size()))
             .c_str(),
         msg_snapshot_blob.size());

  finalize(msg_hdr.st, msg_snapshot_blob, msg_hdr.snapshot_ver);
}

}} // namespace facebook::logdevice
