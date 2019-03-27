/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SequencerRouter.h"

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

const std::string SequencerRouter::Handler::UNINITIALIZED_REQ_TYPE_STRING =
    "UNINITIALIZED_REQ_TYPE";
const std::string SequencerRouter::Handler::GET_SEQ_STATE_REQ_TYPE_STRING =
    "GET_SEQ_STATE_REQ_TYPE";
const std::string SequencerRouter::Handler::APPEND_REQ_TYPE_STRING =
    "APPEND_REQ_TYPE";

void SequencerRouter::start() {
  const auto force_sequencer_choice = getSettings().force_sequencer_choice;
  if (force_sequencer_choice.isNodeID()) {
    ld_debug("Sending to %s because of force-sequencer-choice setting",
             force_sequencer_choice.toString().c_str());
    sendTo(force_sequencer_choice, REDIRECT_CYCLE);
    return;
  }
  // If this SequencerRouter object gets destroyed before the callback is
  // called, trying to access its variables will cause a crash. Using
  // WeakRefHolder to prevent that.
  auto ref = holder_.ref();
  auto sequencer_located = [ref](Status status, logid_t logid, NodeID node_id) {
    auto router = const_cast<SequencerRouter*>(ref.get());
    if (!router) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "SequencerRouter doesn't exist anymore. log-id: %lu",
                        logid.val_);
      return;
    }
    if (status != E::OK) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Error during sequencer lookup for log %lu (%s), "
                      "reporting NOSEQUENCER.",
                      router->log_id_.val_,
                      error_name(status));
      router->onFailure(E::NOSEQUENCER);
      return;
    }

    if (router->last_unavailable_.first == node_id) {
      // SequencerLocator returned the same node we were unable to reach
      // previously: fail with the existing status code
      router->onFailure(router->last_unavailable_.second);
      return;
    }

    router->sendTo(node_id, flags_t(0));
  };

  int rv = getSequencerLocator().locateSequencer(
      // Metadata log is handled by the same sequencer that runs its
      // corresponding data log, here we mask the MetaDataLog::ID_SHIFT
      // bit so that it can find the correct sequencer for both kinds of logs
      MetaDataLog::dataLogID(log_id_),
      sequencer_located,
      sequencers_.hasValue() ? &sequencers_.value() : nullptr);
  if (rv != 0) {
    ld_check(err != E::OK);
    sequencer_located(err, log_id_, NodeID());
  }
}

const std::string& SequencerRouter::Handler::getRequestTypeName() {
  switch (this->rqtype_) {
    case Handler::SRRequestType::GET_SEQ_STATE_REQ_TYPE:
      return GET_SEQ_STATE_REQ_TYPE_STRING;
    case Handler::SRRequestType::APPEND_REQ_TYPE:
      return APPEND_REQ_TYPE_STRING;
    case Handler::SRRequestType::UNINITIALIZED_REQ_TYPE:
      break;
  }

  return UNINITIALIZED_REQ_TYPE_STRING;
}

void SequencerRouter::onFailure(Status status) {
  RATELIMIT_ERROR(std::chrono::seconds(1),
                  1,
                  "Error during sequencer routing for log %lu (%s), "
                  "request[type:%s, handler:%p].",
                  log_id_.val_,
                  error_name(status),
                  handler_->getRequestTypeName().c_str(),
                  handler_);
  if (status == E::NOSEQUENCER && !getSettings().server) {
    auto cs = getClusterState();
    if (cs) {
      cs->refreshClusterStateAsync();
    }
  }

  handler_->onSequencerRoutingFailure(status);
}

void SequencerRouter::onTimeout() {
  RATELIMIT_INFO(std::chrono::seconds(1),
                 10,
                 "Request: log:%lu, %lums internal timer expired",
                 log_id_.val_,
                 getSettings().sequencer_router_internal_timeout.count());
  auto cs = getClusterState();
  if (cs) {
    cs->refreshClusterStateAsync();
  }
  handler_->onClusterStateRefreshTimedOut();
}

void SequencerRouter::onShouldRetry(NodeID node, Status status) {
  ld_check(status == E::SSLREQUIRED);
  sendTo(node, flags_t(0));
}

void SequencerRouter::onRedirected(NodeID from, NodeID to, Status status) {
  ld_check(status == E::REDIRECTED || status == E::PREEMPTED);
  ld_check(handler_);

  if (facebook::logdevice::dbg::currentLevel <
      facebook::logdevice::dbg::Level::DEBUG) {
    RATELIMIT_INFO(std::chrono::seconds(5),
                   5,
                   "Got redirected from %s to %s for log:%lu, status:%s"
                   ", request[type:%s, handler:%p]",
                   from.toString().c_str(),
                   to.toString().c_str(),
                   log_id_.val_,
                   error_name(status),
                   handler_->getRequestTypeName().c_str(),
                   handler_);
  } else {
    // NOTE: This log statement is duplicated at debug level to:
    //       a) avoid spew
    //       b) track a particular log-id if needed during debugging
    ld_debug("Got redirected from %s to %s for log:%lu, status:%s"
             ", request[type:%s, handler:%p]",
             from.toString().c_str(),
             to.toString().c_str(),
             log_id_.val_,
             error_name(status),
             handler_->getRequestTypeName().c_str(),
             handler_);
  }

  if (!last_reply_.node.isNodeID()) {
    // Add the first visited node to the `redirected_' set. This reduces the
    // number of messages sent in case of a redirect cycle.
    redirected_[from] = attempts_;
  }

  last_reply_.node = from;
  last_reply_.status = status;
  last_reply_.flags = flags_;
  ++attempts_;

  if (status == E::PREEMPTED) {
    preempted_nodes_.insert(std::make_pair(from, false));

    if (preempted_nodes_.size() > MAX_PREEMPTIONS) {
      // We were preempted, but already followed preemption redirects too many
      // times. Log and fail the request.
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "number of preemption redirects for log %lu exceeded "
                        "the limit (%d)",
                        log_id_.val_,
                        MAX_PREEMPTIONS);
      onFailure(E::NOSEQUENCER);
      return;
    }
  }

  const int limit = std::max<size_t>(
      10, 3 * getNodesConfiguration()->getSequencersConfig().nodes.size());

  if (attempts_ > limit) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Number of attempts for log %lu exceeded the limit (%d), "
                      "failing the request with E::NOSEQUENCER.",
                      log_id_.val_,
                      limit);
    onFailure(E::NOSEQUENCER);
    return;
  }

  if (!to.isNodeID()) {
    flags_t flags = last_reply_.flags;
    if (flags & FORCE_REACTIVATION) {
      // We got redirected again, even though sendTo() was already called with
      // either REDIRECT_CYCLE or PREEMPTED set. Fail the request.
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Redirected for log:%lu to an unavailable "
                        " node from %s (flags used: %u)",
                        log_id_.val_,
                        from.toString().c_str(),
                        flags);
      onFailure(E::NOSEQUENCER);
      return;
    }

    // we got redirected to a dead node and it was stripped out by the handler
    // before calling onRedirected. In that case, we resend the request with
    // force flags, to ensure the sequencer reactivates.
    sendTo(from, FORCE_REACTIVATION);
    return;
  }

  if (status == E::REDIRECTED) {
    std::string nodes_in_cycle;
    // Check if we were redirected to the same node before (i.e. there's a
    // redirect cycle). When this happens, we'll pick the node with the highest
    // id (if multiple clients get stuck in the same cycle, we'd want them all
    // to eventually append to the same node in order to prevent multiple
    // reactivations) in the cycle and call sendTo() with REDIRECT_CYCLE flag
    // set.
    auto it = redirected_.find(to);
    if (it != redirected_.end()) {
      // Collect all nodes in redirected_ with the discovery time greater or
      // equal than that of `to' (this eliminates the tail of the cycle).
      NodeID target;
      int d = it->second;
      for (auto& e : redirected_) {
        if (e.second >= d) {
          nodes_in_cycle += e.first.toString() + ",";
          if (!target.isNodeID() || e.first.index() > target.index()) {
            target = e.first;
          }
        }
      }
      ld_check(target.isNodeID());

      flags_t flags = REDIRECT_CYCLE;
      if (preempted_nodes_.find(target) != preempted_nodes_.end()) {
        RATELIMIT_INFO(
            std::chrono::seconds(1),
            100,
            "Setting PREEMPTED flag for log:%lu, request[type:%s, handler:%p]",
            log_id_.val_,
            handler_->getRequestTypeName().c_str(),
            handler_);
        flags |= PREEMPTED;
      }

      RATELIMIT_INFO(
          std::chrono::seconds(1),
          100,
          "Following redirect for log:%lu, target node is %s, "
          "request[type:%s, handler:%p]. Nodes in Redirect cycle:[%s]",
          log_id_.val_,
          target.toString().c_str(),
          handler_->getRequestTypeName().c_str(),
          handler_,
          nodes_in_cycle.c_str());
      sendTo(target, flags);
      return;
    }
    redirected_[to] = attempts_;
  }

  if (!getSettings().server) {
    // This block only applies to clients - on server-side, the Failure
    // detector is solely responsible for updating the cluster state.

    auto cs = getClusterState();
    if (cs) {
      if (status == E::REDIRECTED) {
        // in case of redirect (not preempted), we know the server has already
        // checked that the node is in one of the alive states, so we
        // can mark it as such, locally according to whether logid belongs to
        // an internal log.
        if (configuration::InternalLogs::isInternal(log_id_) &&
            cs->getNodeState(to.index()) !=
                ClusterState::NodeState::FULLY_STARTED) {
          cs->setNodeState(to.index(), ClusterState::NodeState::STARTING);
        } else {
          cs->setNodeState(to.index(), ClusterState::NodeState::FULLY_STARTED);
        }
      }

      // we received a redirect or prempted from this sequencer, this may mean
      // that our view of the cluster is wrong and we should have contacted
      // another node first. so we trigger a cluster state refresh to fetch the
      // latest state from a node of the cluster.
      cs->refreshClusterStateAsync();
    }
  }

  sendTo(to, flags_t(0));
}

void SequencerRouter::onDeadNode(NodeID node, Status status) {
  RATELIMIT_INFO(std::chrono::seconds(1),
                 10,
                 "Node %s unavailable for log:%lu, status=%s. "
                 "request[type:%s, handler_:%p]",
                 node.toString().c_str(),
                 log_id_.val_,
                 error_name(status),
                 handler_->getRequestTypeName().c_str(),
                 handler_);

  auto node_state = ClusterState::NodeState::FAILING_OVER;
  switch (status) {
    case E::DISABLED:
      node_state = ClusterState::NodeState::DEAD;
    case E::SHUTDOWN:
      if (!getSettings().server) {
        ld_info("Node %s unavailable for log:%lu, status=%s. "
                "Marking node as DEAD in cluster state",
                node.toString().c_str(),
                log_id_.val_,
                error_name(status));
        auto cs = getClusterState();
        if (cs) {
          cs->setNodeState(node.index(), node_state);
        }
      }
    case E::TIMEDOUT:
    case E::CONNFAILED:
    case E::PEER_CLOSED:
      if (!getSettings().server) {
        // this block only applies to client.
        //
        // these error codes indicate that the node might be dead.
        // schedule a cluster state refresh
        auto cs = getClusterState();
        if (cs) {
          cs->refreshClusterStateAsync();
        }
      }
    case E::NOTREADY:
    case E::REBUILDING:
    case E::CANCELLED:
    case E::PEER_UNAVAILABLE:
      blacklist(node);
      break;
    default:
      break;
  }
}

void SequencerRouter::onNodeUnavailable(NodeID node, Status status) {
  NodeID last_node = last_reply_.node;

  onDeadNode(node, status);

  if (!last_node.isNodeID()) { // not set
    // If we haven't reached any sequencer node yet (this is the first attempt),
    // mark `node' as unavailable and try to pick another one.
    if (status != E::NOTINCONFIG) {
      last_unavailable_ = std::make_pair(node, status);
    } else if (attempts_ == 0) {
      // cluster configuration may've changed in the meantime; let's try once
      // again
      ++attempts_;
      ld_spew("Attempt #%d for log:%lu", attempts_, log_id_.val_);
    } else {
      onFailure(E::NOSEQUENCER);
      return;
    }

    ld_spew("Unavailable node for log:%lu was %s, "
            "attempting another sequencer search.",
            log_id_.val_,
            node.toString().c_str());
    start();

    return;
  }

  // Figure out whether it makes sense to send another request (with appropriate
  // flags set) to `last_node'. Fail the request otherwise.
  if (node == last_node) {
    ld_spew("Unavailable node %s for log:%lu is same as last node, "
            "returning with E::NOSEQUENCER",
            node.toString().c_str(),
            log_id_.val_);
    onFailure(E::NOSEQUENCER);
    return;
  }

  Status reason = last_reply_.status;
  if (reason != E::REDIRECTED && reason != E::PREEMPTED) {
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       5,
                       "Unexpected status: %s from %s for log:%lu",
                       error_name(reason),
                       node.toString().c_str(),
                       log_id_.val_);
    ld_check(false);
  }

  if (reason == E::PREEMPTED) {
    // Since sequencer nodes usually don't detect they've been preempted until
    // actually trying to process an append, it's possible that we've received
    // a preemption redirect from a node which should bring up the sequencer.
    // Node we got redirected to (`node') normally sends a redirect back to this
    // preempted node (`last_node', which then reactivates a sequencer), but
    // here we were not able to talk to `node'. Let's send another append to
    // `last_node' just in case. The bool in preempted_nodes_ is used to prevent
    // doing this more than once.

    auto it = preempted_nodes_.find(last_node);
    if (it == preempted_nodes_.end()) {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1),
          5,
          "Did not find %s in map of preempted nodes for log:%lu",
          last_node.toString().c_str(),
          log_id_.val_);
      ld_check(false);
    }

    if (!it->second) {
      ld_debug("Redirecting back to preempted node %s since %s is unavailable,"
               " status=%s",
               last_node.toString().c_str(),
               node.toString().c_str(),
               error_name(status));

      it->second = true;
      sendTo(last_node, flags_t(0));
      return;
    }
  }

  if (status == E::NOTINCONFIG || status == E::NOTREADY) {
    // This client got redirected to a node it can't talk to. If this happened
    // because it doesn't have the new `node' in the config yet, or because
    // `node' doesn't accept appends yet, we'll try to persuade the old node
    // to process the message.

    flags_t flags = last_reply_.flags;
    if (flags & FORCE_REACTIVATION) {
      // We got redirected again, even though sendTo() was already called with
      // either REDIRECT_CYCLE or PREEMPTED set. Fail the request.
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        10,
                        "Redirected for log:%lu to an unavailable (%s)"
                        " node %s from %s (flags used: %u)",
                        log_id_.val_,
                        error_name(status),
                        node.toString().c_str(),
                        last_node.toString().c_str(),
                        flags);
      onFailure(E::NOSEQUENCER);
      return;
    }

    // force reactivation of the sequencer even if it is in preempted state
    flags = FORCE_REACTIVATION;
    RATELIMIT_INFO(std::chrono::seconds(1),
                   5,
                   "Redirected (%s) for log %lu to an unavailable node %s, "
                   "sending back to previous node %s (flags = %u)",
                   error_name(reason),
                   log_id_.val_,
                   node.toString().c_str(),
                   last_node.toString().c_str(),
                   flags);
    sendTo(last_node, flags);
    return;
  }

  ld_debug("Failing the request for log:%lu, unavailable node:%s, status:%s",
           log_id_.val_,
           node.toString().c_str(),
           error_name(status));
  onFailure(status);
}

void SequencerRouter::sendTo(NodeID dest, flags_t flags) {
  ld_debug("Sending to %s for log:%lu, request[type:%s, handler:%p]",
           dest.toString().c_str(),
           log_id_.val_,
           handler_->getRequestTypeName().c_str(),
           handler_);
  if (!getSettings().server) {
    startClusterStateRefreshTimer();
  }
  flags_ = flags;
  handler_->onSequencerKnown(dest, flags);
}

bool SequencerRouter::blacklist(NodeID node) {
  if (!sequencers_.hasValue()) {
    // make a copy of the sequencer list from the cluster config
    sequencers_ = getNodesConfiguration()->getSequencersConfig();
  }

  ServerConfig::SequencersConfig& cfg = sequencers_.value();
  ld_check(cfg.weights.size() == cfg.nodes.size());

  auto it = std::find(cfg.nodes.begin(), cfg.nodes.end(), node);
  if (it != cfg.nodes.end()) {
    auto& w = cfg.weights[std::distance(cfg.nodes.begin(), it)];
    if (!w) {
      ld_debug("%s already blacklisted for log:%lu",
               node.toString().c_str(),
               log_id_.val_);
      return false; // already blacklisted
    }
    w = 0.0;
    ld_debug(
        "Blacklisted %s for log:%lu", node.toString().c_str(), log_id_.val_);
  } else {
    // New config which no longer includes this node. Unlikely.
    ld_debug("Node %s to blacklist not in the sequencers_ list",
             node.toString().c_str());
  }

  return true;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
SequencerRouter::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

const Settings& SequencerRouter::getSettings() const {
  return Worker::onThisThread()->settings();
}

SequencerLocator& SequencerRouter::getSequencerLocator() const {
  return *Worker::onThisThread()->processor_->sequencer_locator_;
}

ClusterState* SequencerRouter::getClusterState() const {
  return Worker::getClusterState();
}

void SequencerRouter::startClusterStateRefreshTimer() {
  if (getSettings().sequencer_router_internal_timeout <
          std::chrono::milliseconds::max() &&
      !getSettings().server) {
    auto cs = getClusterState();
    if (cs && !cluster_state_refresh_timer_.isAssigned()) {
      cluster_state_refresh_timer_.assign([this] { onTimeout(); });
      cluster_state_refresh_timer_.activate(
          getSettings().sequencer_router_internal_timeout);
    }
  }
}

}} // namespace facebook::logdevice
