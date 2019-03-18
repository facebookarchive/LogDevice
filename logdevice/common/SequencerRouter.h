/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file  SequencerRouter is a utility class used to route a message to the
 *        sequencer for a log.  Since some nodes in the cluster may not be
 *        available, finding a sequencer is more involved than just hashing
 *        the log id; this is especially true for appends, which may in some
 *        cases even need to trigger sequencer reactivations.
 *
 *        SequencerRouter follows redirects and rehashes when a node is
 *        unavailable. Classes that use SequencerRouter implement its interface
 *        (SequencerRouter::Handler) and use start() to kick off the state
 *        machine, as well as onRedirect() and onNodeUnavailable() to influence
 *        the decision on which node to pick next.
 *
 *        Each SequencerRouter object should only be used from a single Worker
 *        thread.
 */

class SequencerLocator;
struct Settings;

class SequencerRouter {
 public:
  using flags_t = uint8_t;

  // Interface implemented by users.  All calls happen on the same worker
  // thread.  It's safe to destroy the parent SequencerRouter object from any of
  // these methods.
  struct Handler {
    virtual ~Handler() {}

    // Called when routing is complete.  Flags provide more context about the
    // node; see below for a list of supported flags.
    //
    // The handler usually composes a message and sends it to the destination
    // node.
    virtual void onSequencerKnown(NodeID dest, SequencerRouter::flags_t) = 0;

    // Called when SequencerRouter was unable to find the correct destination
    // for the log. Status can be one of the following:
    //   CONNFAILED    unable to reach the sequencer node
    //   NOSEQUENCER   failed to determine which node runs the sequencer
    virtual void onSequencerRoutingFailure(Status status) = 0;

    enum class SRRequestType {
      UNINITIALIZED_REQ_TYPE,
      GET_SEQ_STATE_REQ_TYPE,
      APPEND_REQ_TYPE
    };

    SRRequestType rqtype_{SRRequestType::UNINITIALIZED_REQ_TYPE};

    void setRequestType(SRRequestType type) {
      rqtype_ = type;
    }

    // Called when a cluster state refresh timeout expires.
    //
    // onClusterStateRefreshTimeout is called when Request seems to be
    // taking a long time. the request will not be terminated just yet, but a
    // cluster state refresh will be scheduled, to check in the meantime whether
    // there are dead nodes in the cluster.
    virtual void onClusterStateRefreshTimedOut() {}

    const std::string& getRequestTypeName();
    static const std::string UNINITIALIZED_REQ_TYPE_STRING;
    static const std::string GET_SEQ_STATE_REQ_TYPE_STRING;
    static const std::string APPEND_REQ_TYPE_STRING;
  };

  // Flag that's present when a redirect cycle is detected.
  static const flags_t REDIRECT_CYCLE = 1 << 0;
  // Sequencer for `log_id_' on the destination node is known to be preempted.
  static const flags_t PREEMPTED = 1 << 1;
  // Force re-activation (combination of both flags)
  static const flags_t FORCE_REACTIVATION = REDIRECT_CYCLE | PREEMPTED;

  SequencerRouter(logid_t log_id, Handler* handler)
      : log_id_(log_id), handler_(handler), holder_(this) {
    ld_check(log_id_ != LOGID_INVALID);
    ld_check(handler_ != nullptr);
  }

  virtual ~SequencerRouter() {}

  // Start the state machine: locate a sequencer and send a message to it.
  void start();

  // Called when a redirect reply is received from a node.
  void onRedirected(NodeID from, NodeID to, Status status);

  // Called when we're not able to communicate to the given node (e.g. failed
  // to connect, node is not in the config, etc.)
  void onNodeUnavailable(NodeID node, Status status);

  // Called when we should retry sending to the same node. Currently the only
  // case when it is sensible to do so is when status == E::SSLREQUIRED
  void onShouldRetry(NodeID node, Status status);

  // Checks the error status and blacklists the node in this SequencerRouter.
  // Also schedules a ClusterState update if needed.
  // Does not call back into the handler.
  void onDeadNode(NodeID node, Status status);

 protected: // tests can override
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual const Settings& getSettings() const;

  // Returns a reference to the SequencerLocator object used to map logs to
  // sequencer nodes.
  virtual SequencerLocator& getSequencerLocator() const;

  // Returns a pointer to the ClusterState object to check cluster/nodes health
  virtual ClusterState* getClusterState() const;

  // Called when cluster_state_refresh_timer_ expires, and initiates an
  // asynchronous cluster state refresh
  virtual void onTimeout();

  // Initializes and activates cluster_state_refresh_timer_
  virtual void startClusterStateRefreshTimer();

  // Called when the SequencerRouter cannot complete the request.
  // Calls the handler onSequencerRoutingFailure method.
  virtual void onFailure(Status status);

 private:
  logid_t log_id_;
  Handler* handler_;
  WeakRefHolder<SequencerRouter> holder_;

  // limit on the number of E::PREEMPTED redirects we'll follow before giving up
  static constexpr int MAX_PREEMPTIONS = 3;

  // which attempt at sending this is
  int attempts_{0};

  // Keeps track of the most recent node that replied, as well as the status and
  // flags that were used to send a message to it.
  struct {
    NodeID node{};
    Status status{Status::UNKNOWN};
    flags_t flags{0};
  } last_reply_;

  // Sequencer nodes in the cluster and their weights. Some of them may have
  // weights explicitly set to zero in order to exclude them from the search:
  // these are normally nodes that we weren't able to connect to.
  folly::Optional<ServerConfig::SequencersConfig> sequencers_;

  // Set of sequencer nodes that are known to be preempted for the log. A flag,
  // indicating whether a message was resent to the preempted node (see comment
  // in onNodeUnavailable() for more details), is kept for each node in the set.
  std::unordered_map<NodeID, bool, NodeID::Hash> preempted_nodes_;

  // Nodes that we received replies from already. Used to break redirect cycles.
  // `Discovery time' (i.e. when the first request was sent) is associated with
  // each node in the map.
  std::unordered_map<NodeID, int, NodeID::Hash> redirected_;

  // Flags used in the most recent call to sendTo().
  flags_t flags_{0};

  // If we haven't received a reply from any of the nodes yet (i.e. we're still
  // trying to find the sequencer node), this will record which node was
  // attempted to be reached last. The goal is to prevent sending to the same
  // node indefinitely if SequencerLocator::locateSequencer() keeps returning
  // it, but we already failed to send to it.
  std::pair<NodeID, Status> last_unavailable_{NodeID(), E::UNKNOWN};

  // Calls handler_->sendTo() and updates flags_.
  void sendTo(NodeID dest, flags_t flags);

  // Mark node as dead to prevent it from being selected by the
  // SequencerLocator. Returns true on success, false if the node was already
  // blacklisted.
  bool blacklist(NodeID node);

  // Timer for cluster state refresh; It starts before onSequencerKnown() is
  // called. When this timer expires, it calls callback to refresh cluster
  // state. Here we also set a timeout for cluster_state_refresh_timer_.
  Timer cluster_state_refresh_timer_;
};

}} // namespace facebook::logdevice
