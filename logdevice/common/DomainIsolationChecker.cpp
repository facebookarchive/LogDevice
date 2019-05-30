/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DomainIsolationChecker.h"

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/DomainIsolationUpdatedRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

void DomainIsolationChecker::init() {
  ld_check(inited_on_ == worker_id_t(-1));
  inited_on_ = getThreadID();
  rebuildState();
}

void DomainIsolationChecker::rebuildState() {
  ld_check(inited_on_ == getThreadID());

  auto nodes_configuration = getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);
  const auto& sd_config = nodes_configuration->getServiceDiscovery();

  const NodeID this_node = getMyNodeID();
  ld_check(sd_config->hasNode(this_node.index()));

  // disable detection when changing the internal state, all domains
  // are considered not isolated
  enabled_.store(false);
  // reset all scopes to be not isolated
  scope_info_.fill(DomainInfo());
  nodes_.clear();

  for (const auto& kv : *sd_config) {
    nodes_.insert(kv.first);
  }

  for (auto& e : isolated_scopes_) {
    e.store(false);
  }

  const auto& my_location_optional =
      sd_config->nodeAttributesAt(this_node.index()).location;
  if (!my_location_optional.hasValue()) {
    ld_warning("This node (%s) does not have location information in config "
               "unable to detect domain isolation!",
               this_node.toString().c_str());
    // detection will be disabled until next configuration changes
    return;
  }

  const NodeLocation& this_location = my_location_optional.value();
  for (const auto& kv : *sd_config) {
    const node_index_t index = kv.first;
    const configuration::nodes::NodeServiceDiscovery& sd = kv.second;
    // we use a conservative approach so that nodes which do NOT have
    // location info are treated as nodes _outside_ of the domain
    if (!sd.location.hasValue()) {
      ld_warning("Node %hd does not have location information in config. "
                 "Domain isolation detection may not be accurate!",
                 index);
      continue;
    }
    const NodeLocation& location = sd.location.value();
    for (NodeLocationScope scope = NodeLocationScope::NODE;
         scope < NodeLocationScope::ROOT;
         scope = NodeLocation::nextGreaterScope(scope)) {
      if (this_node.index() == index ||
          this_location.sharesScopeWith(location, scope)) {
        // the node belongs to the same domain as this node in scope
        scope_info_[static_cast<size_t>(scope)].domain_nodes.insert(index);
      }
    }
  }

  ld_check(scope_info_[static_cast<size_t>(NodeLocationScope::NODE)]
               .domain_nodes.size() == 1);
  ld_check(scope_info_[static_cast<size_t>(NodeLocationScope::NODE)]
               .domain_nodes.count(this_node.index()) == 1);

  enabled_.store(true);

  // rebuild the internal state from the current state of nodes in the
  // cluster by setting all current dead nodes
  for (const auto nid : nodes_) {
    if (!isNodeAlive(nid)) {
      setNodeState(nid, /*alive=*/false);
    }
  }
}

bool DomainIsolationChecker::isMyDomainIsolated(NodeLocationScope scope) const {
  if (!enabled_.load() || scope == NodeLocationScope::ROOT) {
    // ROOT scope is the entire cluster, and we consider it always not isolated
    return false;
  }

  ld_check(scope >= NodeLocationScope::NODE && scope < NodeLocationScope::ROOT);
  return isolated_scopes_[static_cast<size_t>(scope)].load();
}

void DomainIsolationChecker::onNodeStateChanged(node_index_t index,
                                                bool alive) {
  ld_check(inited_on_ == getThreadID());

  ld_check(index >= 0);
  // in case isolation detection is not enabled or the node is not in the
  // current config, nothing to do.
  if (!enabled_.load() || !nodes_.count(index)) {
    return;
  }

  if (isNodeAlive(index) == alive) {
    ld_critical("INTERNAL ERROR: Marking Node %hd as %s while it is already "
                "in that state!",
                index,
                alive ? "alive" : "dead");
    ld_check(false);
    return;
  }

  setNodeState(index, alive);
}

void DomainIsolationChecker::setNodeState(node_index_t index, bool alive) {
  ld_check(nodes_.count(index));
  for (NodeLocationScope scope = NodeLocationScope::NODE;
       scope < NodeLocationScope::ROOT;
       scope = NodeLocation::nextGreaterScope(scope)) {
    auto& info = scope_info_[static_cast<size_t>(scope)];
    if (info.domain_nodes.count(index) == 0) {
      alive ? --info.dead_nodes_outside : ++info.dead_nodes_outside;
      ld_check(info.dead_nodes_outside >= 0);
      ld_check(info.dead_nodes_outside < nodes_.size());
      ld_check(info.dead_nodes_outside + info.domain_nodes.size() <=
               nodes_.size());

      if (static_cast<size_t>(info.dead_nodes_outside) +
              info.domain_nodes.size() ==
          nodes_.size()) {
        // all nodes outside of the domain in the scope is dead
        ld_check(info.dead_nodes_outside > 0);
        isolated_scopes_[static_cast<size_t>(scope)].store(true);
      } else {
        isolated_scopes_[static_cast<size_t>(scope)].store(false);
      }
    }
  }

  // check the invariant that if the local domain of a smaller scope is
  // isolated, local domains of all higher scope must be isolated as well
  bool must_be_isolated = false;
  for (NodeLocationScope scope = NodeLocationScope::NODE;
       scope < NodeLocationScope::ROOT;
       scope = NodeLocation::nextGreaterScope(scope)) {
    if (isolated_scopes_[static_cast<size_t>(scope)].load()) {
      must_be_isolated = true;
    } else {
      // once must_be_isolated is set, the domain must either be isolated,
      // or it has ALL cluster nodes (and considered not isolated)
      ld_check(!must_be_isolated || localDomainContainsWholeCluster(scope));
    }
  }
}

void DomainIsolationChecker::processIsolationUpdates() {
  // omit isolation updates if the checker is not enabled
  if (!enabled_.load()) {
    return;
  }

  auto worker = Worker::onThisThread(false);

  for (NodeLocationScope scope = NodeLocationScope::NODE;
       scope < NodeLocationScope::ROOT;
       scope = NodeLocation::nextGreaterScope(scope)) {
    auto& info = scope_info_[static_cast<size_t>(scope)];
    bool isolated = isolated_scopes_[static_cast<size_t>(scope)].load();
    if (std::chrono::steady_clock::now() > info.next_notification_ &&
        info.last_isolated_ != isolated) {
      if (worker != nullptr) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       5,
                       "Notifying all workers that isolation state changed "
                       "for scope %s, new state: %s",
                       NodeLocation::scopeNames()[scope].c_str(),
                       isolated ? "ISOLATED" : "NOT ISOLATED");

        // notify all workers
        worker->processor_->applyToWorkerPool(
            [&](Worker& w) {
              std::unique_ptr<Request> req =
                  std::make_unique<DomainIsolationUpdatedRequest>(
                      w.idx_, scope, isolated);
              if (w.processor_->postRequest(req) != 0) {
                ld_error("error processing domain isolation update on "
                         "worker #%d: postRequest() failed with status %s",
                         w.idx_.val(),
                         error_description(err));
              }
            },
            Processor::Order::FORWARD,
            // Appenders only work on the GENERAL pool.
            WorkerType::GENERAL);
      }

      // update info
      info.next_notification_ =
          std::chrono::steady_clock::now() + min_notification_interval_;
      info.last_isolated_ = isolated;
    }
  }
}

bool DomainIsolationChecker::localDomainContainsWholeCluster(
    NodeLocationScope scope) const {
  return (scope_info_[static_cast<size_t>(scope)].domain_nodes.size() ==
          nodes_.size());
}

void DomainIsolationChecker::onNodeDead(node_index_t index) {
  onNodeStateChanged(index, false);
}

void DomainIsolationChecker::onNodeAlive(node_index_t index) {
  onNodeStateChanged(index, true);
}

void DomainIsolationChecker::noteConfigurationChanged() {
  ld_check(inited_on_ == getThreadID());
  rebuildState();
}

/// functions may be overridden in tests
worker_id_t DomainIsolationChecker::getThreadID() const {
  return Worker::onThisThread()->idx_;
}

bool DomainIsolationChecker::isNodeAlive(node_index_t index) const {
  auto cs = Worker::getClusterState();
  if (!cs) {
    // if failure detector is not running, always consider node as alive
    return true;
  }

  ld_check(inited_on_ == getThreadID());

  return cs->isNodeAlive(index);
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
DomainIsolationChecker::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

NodeID DomainIsolationChecker::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

std::string DomainIsolationChecker::getDebugInfo() const {
  std::string out;
  const bool enabled = enabled_.load();
  out += "DOMAIN_ISOLATION enabled ";
  out += enabled ? "true" : "false";
  out += "\r\n";

  if (enabled) {
    for (NodeLocationScope scope = NodeLocationScope::NODE;
         scope < NodeLocationScope::ROOT;
         scope = NodeLocation::nextGreaterScope(scope)) {
      out += "DOMAIN_ISOLATION ";
      out += NodeLocation::scopeNames()[scope];
      out += isMyDomainIsolated(scope) ? " ISOLATED" : " NOT_ISOLATED";
      out += "\r\n";
    }
  }

  return out;
}
}} // namespace facebook::logdevice
