/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CheckNodeHealthRequest.h"

#include <folly/hash/Hash.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_Message.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using NotAvailableReason = NodeSetState::NotAvailableReason;

int CheckNodeHealthRequest::getThreadAffinity(int nthreads) {
  return folly::hash::twang_mix64(log_id_.val_) % nthreads;
}

CheckNodeHealthRequest::CheckNodeHealthRequest(
    ShardID shard,
    std::shared_ptr<NodeSetState> nodeset_state,
    logid_t log_id,
    nodeset_state_id_t nodeset_state_id,
    std::chrono::seconds request_timeout,
    bool enable_space_based_trimming)
    : Request(RequestType::CHECK_NODE_HEALTH),
      shard_(shard),
      log_id_(log_id),
      nodeset_state_id_(nodeset_state_id),
      request_timeout_(request_timeout),
      nodeset_state_(nodeset_state),
      enable_space_based_trimming_(enable_space_based_trimming),
      on_socket_close_(this),
      sender_(std::make_unique<SenderProxy>()) {}

Request::Execution CheckNodeHealthRequest::execute() {
  ld_spew("Executing request with id %lu", id_.val_);

  auto nodeset_state = nodeset_state_.lock();
  if (nodeset_state == nullptr) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "weak ptr to NodeSetState "
                   "expired. Nothing to do. Completing execution "
                   "of %" PRIu64 "NodeSetStateHealthCheckRequest",
                   uint64_t(id_));
    return Execution::COMPLETE;
  }

  NotAvailableReason reason = nodeset_state->getNotAvailableReason(shard_);
  // Probe can also be sent from Low watermark and NO_SPC states
  if (reason != NodeSetState::NotAvailableReason::PROBING &&
      reason != NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC &&
      reason != NodeSetState::NotAvailableReason::NO_SPC) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "%s is no longer in %s state. Completing execution of "
                   "request with rid: %" PRIu64,
                   shard_.toString().c_str(),
                   NodeSetState::reasonString(reason),
                   uint64_t(id_));
    return Execution::COMPLETE;
  }

  auto& pending_health_check = pendingHealthChecks().set;

  // 1. Is there a request in flight for this node?
  auto key = boost::make_tuple(nodeset_state_id_, shard_);
  auto& index =
      pending_health_check
          .get<CheckNodeHealthRequestSet::NodeSetStateIdShardIDIndex>();
  auto it = index.find(key);
  if (it != index.end()) {
    // we have a request in flight. Check when it was sent
    auto next_retry_time = (*it)->request_sent_time_.time_since_epoch() +
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                               getSettings().node_health_check_retry_interval);

    if (next_retry_time < std::chrono::steady_clock::now().time_since_epoch()) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Found an inflight CheckNodeHealthRequest(%lu) towards %s"
                     ", that has expired. Cancelling it.",
                     (*it)->id_.val_,
                     shard_.toString().c_str());
    } else {
      // We have a request in flight and it has not yet expired.
      // We are sending another request even before the previous one has expired
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Found an inflight "
                      "CheckNodeHealthRequest(%lu) towards %s that has NOT "
                      "expired. We are sending more requests than expected."
                      "Cancelling previous request",
                      (*it)->id_.val_,
                      shard_.toString().c_str());
    }
    // cancel the previous request.
    index.erase(it);
  }

  // 2. We do not have any request inflight or we found it had expired.
  int rv = createAndSendCheckNodeHealthMessage();
  if (rv != 0) {
    // Its ok if the send failed. a new request will be sent once
    // the deadline for this node expires.
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "Failed to send a CHECK_NODE_HEALTH to %s: %s",
                      shard_.toString().c_str(),
                      error_description(err));
    if (err == E::UNROUTABLE) {
      nodeset_state->setNotAvailableUntil(
          shard_, NodeSetState::NotAvailableReason::UNROUTABLE);
    } else if (err == E::PROTONOSUPPORT) {
      // The other end of this connection does not know about probes.
      nodeset_state->setNotAvailableUntil(
          shard_, NodeSetState::NotAvailableReason::NONE);
    } else {
      // There was some other failure - it's likely the node is not available.
      // We should change the state to UNROUTABLE, but only if it's still
      // PROBING
      nodeset_state->setNotAvailableUntil(
          shard_,
          NodeSetState::NotAvailableReason::UNROUTABLE,
          NodeSetState::NotAvailableReason::PROBING);
    }
    return Execution::COMPLETE;
  }

  // successfully sent a CHECK_NODE_HEALTH message
  request_sent_time_ = std::chrono::steady_clock::now();
  std::unique_ptr<CheckNodeHealthRequest> request(this);

  auto insert_it = pendingHealthChecks().set.insert(std::move(request));
  ld_check(insert_it.second);
  activateRequestTimeoutTimer();
  return Execution::CONTINUE;
}

int CheckNodeHealthRequest::createAndSendCheckNodeHealthMessage() {
  ld_spew("Sending CHECK_NODE_HEALTH message to %s", shard_.toString().c_str());

  // Set header flags
  CHECK_NODE_HEALTH_flags_t flags = 0;
  if (enable_space_based_trimming_) {
    ld_debug("Sending CHECK_NODE_HEALTH message with space based trimming"
             " directive to %s for log:%lu",
             shard_.toString().c_str(),
             log_id_.val());

    flags |= CHECK_NODE_HEALTH_Header::SPACE_BASED_RETENTION;
    WORKER_STAT_INCR(sbt_num_seq_initiated_trims);
  }

  CHECK_NODE_HEALTH_Header header{id_, shard_.shard(), flags};
  auto msg = std::make_unique<CHECK_NODE_HEALTH_Message>(header);
  int rv = sender_->sendMessage(
      std::move(msg), NodeID(shard_.node()), &on_socket_close_);
  return rv;
}

void CheckNodeHealthRequest::onReply(Status status) {
  ld_debug("Received reply with status:%s for request:%lu",
           error_name(status),
           id_.val_);

  auto nodeset_state = nodeset_state_.lock();
  if (nodeset_state == nullptr) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "weak ptr to NodeSet expired. Removing %" PRIu64
                   "NodeSetStateHealthCheckRequest from map",
                   uint64_t(id_));
    deleteThis();
    return;
  }

  // Try to update the status of node to new reason and corresponding
  // deadline. If the transition is not valid, the update will be ignored.
  // Check setNotAvailableUntil for details.
  switch (status) {
    case Status::NOSPC:
      nodeset_state->setNotAvailableUntil(
          shard_, NodeSetState::NotAvailableReason::NO_SPC);
      break;

    case Status::LOW_ON_SPC:
      /*
       check-node health reply sends LOW_ON_SPC only if the storage
       node doesn't have any other unavailability reasons.
       It always comes back on the same connection, so we can
       assume it's the latest state to apply.
      */
      nodeset_state->setNotAvailableUntil(
          shard_,
          std::chrono::steady_clock::time_point::min(),
          NodeSetState::NotAvailableReason::NONE);
      nodeset_state->setNotAvailableUntil(
          shard_,
          std::chrono::steady_clock::time_point::min(),
          NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC);
      break;

    case Status::DISABLED:
    case Status::NOTSTORAGE:
      nodeset_state->setNotAvailableUntil(
          shard_, NodeSetState::NotAvailableReason::STORE_DISABLED);
      break;

    case Status::OVERLOADED:
      nodeset_state->setNotAvailableUntil(
          shard_, NodeSetState::NotAvailableReason::OVERLOADED);
      break;

    case Status::UNROUTABLE:
      nodeset_state->setNotAvailableUntil(
          shard_, NodeSetState::NotAvailableReason::UNROUTABLE);
      break;

    case Status::OK:
      nodeset_state->setNotAvailableUntil(
          shard_,
          std::chrono::steady_clock::time_point::min(),
          NodeSetState::NotAvailableReason::NONE);
      break;

    default:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Received status %s that does not affect "
                        "NodeSetState for %s",
                        error_name(status),
                        shard_.toString().c_str());
  }

  // Remove from map
  deleteThis();
}

void CheckNodeHealthRequest::deleteThis() {
  auto& pending_health_check = pendingHealthChecks().set;
  auto& index =
      pending_health_check.get<CheckNodeHealthRequestSet::RequestIndex>();
  auto it = index.find(id_);
  ld_check(it != index.end());
  index.erase(it);
}

void CheckNodeHealthRequest::noReply() {
  ld_debug(
      "CheckNodeHealthRequest with request id %" PRIu64 "to node %s "
      "received no reply. Deleting this request from pending health check map",
      uint64_t(id_),
      shard_.toString().c_str());
  // We didn't get a response to our message, so setting this node to
  // UNROUTABLE, unless it has transitioned away from the PROBING state already
  auto nodeset_state = nodeset_state_.lock();
  if (nodeset_state) {
    nodeset_state->setNotAvailableUntil(
        shard_,
        NodeSetState::NotAvailableReason::UNROUTABLE,
        NodeSetState::NotAvailableReason::PROBING);
  }
  deleteThis();
}

CheckNodeHealthRequestSet& CheckNodeHealthRequest::pendingHealthChecks() {
  return Worker::onThisThread()->pendingHealthChecks();
}

const Settings& CheckNodeHealthRequest::getSettings() {
  return Worker::settings();
}

void CheckNodeHealthRequest::activateRequestTimeoutTimer() {
  request_timeout_timer_ = std::make_unique<Timer>([this] { this->noReply(); });
  request_timeout_timer_->activate(request_timeout_);
}

CheckNodeHealthRequest::~CheckNodeHealthRequest() {
  ld_spew("Destroying CheckNodeHealthRequest with id:%lu", id_.val_);
}

}} // namespace facebook::logdevice
