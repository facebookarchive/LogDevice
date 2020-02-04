/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/GET_RSM_SNAPSHOT_onReceived.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/protocol/GET_RSM_SNAPSHOT_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

static void send_reply(const Address& to,
                       const GET_RSM_SNAPSHOT_REPLY_Header& hdr,
                       std::string snapshot_blob) {
  RATELIMIT_DEBUG(
      std::chrono::seconds(1),
      5,
      "Sending GET_RSM_SNAPSHOT_REPLY_Message: hdr(%s, delta_log_id:%lu, "
      "rqid:%lu, snapshot_ver:%s, snapshot(blob:[%s], size:%lu) to %s",
      error_name(hdr.st),
      hdr.delta_log_id.val_,
      hdr.rqid.val_,
      lsn_to_string(hdr.snapshot_ver).c_str(),
      hexdump_buf(snapshot_blob.data(), std::min(30ul, snapshot_blob.size()))
          .c_str(),
      snapshot_blob.size(),
      to.toString().c_str());
  auto msg = std::make_unique<GET_RSM_SNAPSHOT_REPLY_Message>(
      hdr, std::move(snapshot_blob));
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

Request::Execution ReturnRsmSnapshotFromMemoryRequest::execute() {
  send_reply(to_, reply_hdr_, std::move(snapshot_blob_));
  return Execution::COMPLETE;
}

Request::Execution GetRsmSnapshotFromMemoryRequest::execute() {
  GET_RSM_SNAPSHOT_REPLY_Header reply_hdr;
  reply_hdr.delta_log_id = rsm_type_;
  reply_hdr.rqid = rqid_;
  lsn_t snapshot_ver_out{LSN_INVALID};
  std::string snapshot_blob = "";
  ServerWorker* w = ServerWorker::onThisThread();

  switch (rsm_type_.val_) {
    case configuration::InternalLogs::EVENT_LOG_DELTAS.val_: {
      auto rsm = w->getEventLogStateMachine();
      if (!rsm) {
        ld_error("Event Log State Machine not found");
        reply_hdr.st = E::NOTFOUND;
        break;
      }
      reply_hdr.st =
          rsm->getSnapshotFromMemory(min_ver_, snapshot_ver_out, snapshot_blob);
      reply_hdr.snapshot_ver = snapshot_ver_out;
    } break;

    case configuration::InternalLogs::CONFIG_LOG_DELTAS.val_: {
      auto& lcm = w->logsconfig_manager_;
      LogsConfigStateMachine* rsm{nullptr};
      if (lcm) {
        rsm = lcm->getStateMachine();
      } else {
        ld_error("LCM not present");
      }
      if (!lcm || !rsm) {
        ld_error("LogsConfig RSM not found");
        reply_hdr.st = E::NOTFOUND;
        break;
      }
      reply_hdr.st =
          rsm->getSnapshotFromMemory(min_ver_, snapshot_ver_out, snapshot_blob);
      reply_hdr.snapshot_ver = snapshot_ver_out;
    } break;

    default:
      reply_hdr.st = E::INVALID_PARAM;
  }

  if (snapshot_blob.size() > Message::MAX_LEN) {
    ld_warning("Snapshot blob size(%lu) for rsm:%lu is greater than max "
               "message length supported in LD protocol at the moment.",
               snapshot_blob.size(),
               rsm_type_.val_);
    reply_hdr.st = E::TOOBIG;
    snapshot_blob = "";
  }

  ld_log(reply_hdr.st == E::OK ? dbg::Level::DEBUG : dbg::Level::INFO,
         "State machine for RSM type:%lu, st:%s, min_ver_:%s, "
         "snapshot(ver:%s, blob:[%s], size:%lu)",
         rsm_type_.val_,
         error_name(reply_hdr.st),
         lsn_to_string(min_ver_).c_str(),
         lsn_to_string(reply_hdr.snapshot_ver).c_str(),
         hexdump_buf(snapshot_blob.data(), std::min(30ul, snapshot_blob.size()))
             .c_str(),
         snapshot_blob.size());

  std::unique_ptr<Request> req =
      std::make_unique<ReturnRsmSnapshotFromMemoryRequest>(
          src_worker_type_,
          src_thread_,
          from_,
          reply_hdr,
          std::move(snapshot_blob));
  ServerProcessor* processor = w->processor_;
  processor->postRequest(req);
  return Execution::COMPLETE;
}

static void thisNodeServesTheRequest(GET_RSM_SNAPSHOT_Message* msg,
                                     const Address& from,
                                     logid_t rsm_type,
                                     lsn_t min_ver,
                                     GET_RSM_SNAPSHOT_REPLY_Header& reply_hdr) {
  ServerWorker* w = ServerWorker::onThisThread();
  ServerProcessor* processor = w->processor_;
  WorkerType dest_worker_type;
  int dest_worker_id;
  reply_hdr.delta_log_id = rsm_type;
  reply_hdr.rqid = msg->header_.rqid;

  switch (rsm_type.val_) {
    case configuration::InternalLogs::EVENT_LOG_DELTAS.val_:
      dest_worker_type = EventLogStateMachine::workerType(processor);
      dest_worker_id = EventLogStateMachine::getWorkerIdx(
          processor->getWorkerCount(dest_worker_type));
      break;
    case configuration::InternalLogs::CONFIG_LOG_DELTAS.val_: {
      dest_worker_type = LogsConfigManager::workerType(processor);
      dest_worker_id = LogsConfigManager::getLogsConfigManagerWorkerIdx(
          processor->getWorkerCount(dest_worker_type));
    } break;
    case configuration::InternalLogs::MAINTENANCE_LOG_DELTAS.val_:
      reply_hdr.st = E::NOTSUPPORTED;
      send_reply(from, reply_hdr, "");
      return;
      break;
    default:
      ld_error("RSM type:%lu is invalid", rsm_type.val_);
      reply_hdr.st = E::INVALID_PARAM;
      send_reply(from, reply_hdr, "");
      return;
  }

  std::unique_ptr<Request> req =
      std::make_unique<GetRsmSnapshotFromMemoryRequest>(
          dest_worker_type,
          dest_worker_id,
          w->worker_type_, // src worker type
          w->idx_.val(),   // src thread
          rsm_type,
          min_ver,
          from,
          msg->header_.rqid);
  processor->postRequest(req);
}

static Status
pickRandomNode(std::multimap<lsn_t, node_index_t, std::greater<lsn_t>>&
                   versions_in_cluster,
               node_index_t& node_out,
               lsn_t& max_rsm_ver) {
  ld_check(!versions_in_cluster.empty());
  auto begin_it = versions_in_cluster.begin();
  max_rsm_ver = begin_it->first;

  std::vector<node_index_t> candidates;
  for (; (begin_it != versions_in_cluster.end()) &&
       (begin_it->first == max_rsm_ver);
       ++begin_it) {
    candidates.push_back(begin_it->second);
  }
  auto rand_idx = folly::Random::rand32((uint32_t)candidates.size());
  node_out = candidates[rand_idx];
  ld_debug("Picked N%hu(rsm ver:%s), candidates.size():%zu, map.size():%zu",
           node_out,
           lsn_to_string(max_rsm_ver).c_str(),
           candidates.size(),
           versions_in_cluster.size());
  return E::OK;
}

Message::Disposition GET_RSM_SNAPSHOT_onReceived(GET_RSM_SNAPSHOT_Message* msg,
                                                 const Address& from) {
  // This method decides which cluster node is best suited to provide
  // RSM snapshot blob to the client.
  // If the node is up to date, or forced or doesn't have cluster information,
  // it will serve the request.
  // Otherwise, it will pick a random node from cluster nodes(with max version),
  // and send a REDIRECT
  RATELIMIT_INFO(std::chrono::seconds(1),
                 2,
                 "Received GET_RSM_SNAPSHOT_Message from %s, key:%s, "
                 "hdr(min_ver:%s, rqid:%lu, flags:%hhu)",
                 Sender::describeConnection(from).c_str(),
                 msg->key_.c_str(),
                 lsn_to_string(msg->header_.min_ver).c_str(),
                 msg->header_.rqid.val_,
                 msg->header_.flags);
  auto delta_log_id = logid_t(folly::to<logid_t::raw_type>(msg->key_));
  GET_RSM_SNAPSHOT_REPLY_Header reply_hdr;
  reply_hdr.delta_log_id = delta_log_id;
  reply_hdr.rqid = msg->header_.rqid;
  auto flags = msg->header_.flags;

  ServerWorker* worker = ServerWorker::onThisThread();
  ServerProcessor* processor = worker->processor_;
  if (!worker->isAcceptingWork()) {
    reply_hdr.st = E::SHUTDOWN;
    send_reply(from, reply_hdr, "");
    return Message::Disposition::NORMAL;
  }

  auto fd = processor->failure_detector_.get();
  if (!fd || flags & GET_RSM_SNAPSHOT_Message::FORCE) {
    thisNodeServesTheRequest(
        msg, from, delta_log_id, msg->header_.min_ver, reply_hdr);
    return Message::Disposition::NORMAL;
  }

  // 1. Fetch in memory versions present in the cluster for the requested RSM
  std::multimap<lsn_t, node_index_t, std::greater<lsn_t>> versions_in_cluster;
  auto st = fd->getAllRSMVersionsInCluster(delta_log_id, versions_in_cluster);
  auto begin_it = versions_in_cluster.begin();
  reply_hdr.st = st;
  if (st == E::OK) {
    if (begin_it == versions_in_cluster.end()) {
      // Return E::EMPTY if we don't have version information for this RSM
      reply_hdr.st = E::EMPTY;
    } else if (begin_it->first < msg->header_.min_ver) {
      ld_debug("Returning E::STALE since the cluster has a lower version(%s) "
               "than requested(%s) for log:%lu",
               lsn_to_string(begin_it->first).c_str(),
               lsn_to_string(msg->header_.min_ver).c_str(),
               delta_log_id.val_);
      reply_hdr.st = E::STALE;
      reply_hdr.snapshot_ver = begin_it->first;
    }
  }

  if (reply_hdr.st != E::OK) {
    ld_info("RSM version information unavailable, delta_log:%lu, reason:%s",
            delta_log_id.val_,
            error_name(reply_hdr.st));
    send_reply(from, reply_hdr, "");
    return Message::Disposition::NORMAL;
  }

  // 2. If this node has best version in the cluster, it should serve the
  // request
  node_index_t my_node_id = processor->getMyNodeID().index();
  lsn_t rsm_ver_on_this_node{LSN_INVALID};
  if (E::OK ==
      fd->getRSMVersion(my_node_id, delta_log_id, rsm_ver_on_this_node)) {
    if (rsm_ver_on_this_node == begin_it->first) {
      thisNodeServesTheRequest(
          msg, from, delta_log_id, msg->header_.min_ver, reply_hdr);
      return Message::Disposition::NORMAL;
    }
  }

  // 3. Otherwise, pick any node from the cluster that has the max version
  //    and redirect the client there.
  node_index_t random_redirect_node;
  lsn_t max_rsm_ver;
  auto rand_st =
      pickRandomNode(versions_in_cluster, random_redirect_node, max_rsm_ver);
  ld_check(rand_st == E::OK);
  reply_hdr.st = E::REDIRECTED;
  reply_hdr.redirect_node = random_redirect_node;
  reply_hdr.snapshot_ver = max_rsm_ver;
  send_reply(from, reply_hdr, "");
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
