/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/HashBasedSequencerLocator.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/hash.h"

namespace facebook { namespace logdevice {

int HashBasedSequencerLocator::locateSequencer(
    logid_t log_id,
    Completion cf,
    const configuration::SequencersConfig* sequencers) {
  if (!getSettings().use_sequencer_affinity) {
    locateContinuation(log_id, cf, sequencers, nullptr);
    return 0;
  }
  auto config = getConfig();
  WeakRef<HashBasedSequencerLocator> ref = holder_.ref();
  config->getLogGroupByIDAsync(
      log_id,
      [ref, log_id, cf, sequencers](
          const std::shared_ptr<const LogsConfig::LogGroupNode> log_group) {
        // If this SequencerLocator object gets destroyed before the callback is
        // called, trying to call locateContinuation will cause a crash. Using
        // WeakRefHolder to prevent that.
        if (ref) {
          const_cast<HashBasedSequencerLocator*>(ref.get())->locateContinuation(
              log_id,
              cf,
              sequencers,
              log_group ? &log_group->attrs() : nullptr);
        } else {
          RATELIMIT_WARNING(
              std::chrono::seconds(10),
              10,
              "SequencerLocator for log %lu doesn't exist anymore.",
              log_id.val_);
        }
      });
  return 0;
}

void HashBasedSequencerLocator::locateContinuation(
    logid_t log_id,
    Completion cf,
    const configuration::SequencersConfig* sequencers,
    const logsconfig::LogAttributes* log_attrs) {
  auto nodes_configuration = getNodesConfiguration();
  auto cs = getClusterState();
  ld_check(cs);
  NodeID res;
  auto rv = locateSequencer(
      log_id, nodes_configuration.get(), log_attrs, cs, &res, sequencers);
  if (rv == 0) {
    cf(E::OK, log_id, res);
  } else {
    cf(err, log_id, NodeID());
  }
}

node_index_t HashBasedSequencerLocator::getPrimarySequencerNode(
    logid_t log_id,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const logsconfig::LogAttributes* log_attrs) {
  NodeID res;
  auto rv = locateSequencer(log_id,
                            &nodes_configuration,
                            log_attrs,
                            /* cs */ nullptr,
                            &res);
  if (rv == 0) {
    return res.index();
  } else {
    ld_check(err == E::NOTFOUND);
    return nodes_configuration.getSequencersConfig().nodes[0].index();
  }
}

int HashBasedSequencerLocator::locateSequencer(
    logid_t log_id,
    const configuration::nodes::NodesConfiguration* nodes_configuration,
    const logsconfig::LogAttributes* log_attrs,
    ClusterState* cs,
    NodeID* out_sequencer,
    const configuration::SequencersConfig* sequencers) {
  ld_check(nodes_configuration != nullptr);
  ld_check(out_sequencer != nullptr);

  if (sequencers == nullptr) {
    sequencers = &nodes_configuration->getSequencersConfig();
  }

  ld_check(sequencers != nullptr);
  ld_check(sequencers->weights.size() == sequencers->nodes.size());

  std::shared_ptr<NodeLocation> sequencerAffinity;
  if (log_attrs) {
    const auto& aff = log_attrs->sequencerAffinity();
    if (aff.hasValue() && aff.value().hasValue()) {
      // populating sequencerAffinity
      sequencerAffinity = std::make_shared<NodeLocation>();
      if (sequencerAffinity->fromDomainString(aff.value().value()) != 0) {
        // failed
        sequencerAffinity = nullptr;
      }
    }
  }

  // Use weighted consistent hashing to pick a sequencer node for log_id.
  // Weights are also used here as a blacklisting mechanism: upper layers may
  // pass in a SequencersConfig with weights of nodes that should be excluded
  // from the search (e.g. those that were determined to be unavailable) set to
  // zero.

  auto can_we_route_to = [&sequencers, cs, log_id](uint64_t node, double w) {
    if (w <= 0.0) {
      return false;
    } else if (!cs) {
      return true;
    } else {
      bool is_internal_log = configuration::InternalLogs::isInternal(log_id);
      bool is_node_ready =
          cs->isNodeFullyStarted(sequencers->nodes[node].index()) ||
          // Sequencers on nodes that are starting can operate on internal logs
          // without logsconfig being fully loaded (logsconfig itself is an
          // internal log).
          (is_internal_log &&
           cs->isNodeStarting(sequencers->nodes[node].index()));
      return is_node_ready && !cs->isNodeBoycotted(node);
    }
  };

  bool found_in_location = false;
  int64_t idx;
  if (sequencerAffinity && !sequencerAffinity->isEmpty()) {
    // trying to find sequencer according to location affinity
    auto weight_fn_with_loc = [&](uint64_t node) {
      double w = sequencers->weights[node];
      if (can_we_route_to(node, w)) {
        const auto* serv_disc =
            nodes_configuration->getNodeServiceDiscovery(node);
        if (serv_disc != nullptr) {
          const auto location = serv_disc->location;
          if (location.hasValue()) {
            if (sequencerAffinity->sharesScopeWith(
                    location.value(), sequencerAffinity->lastScope())) {
              return w;
            }
          } else {
            RATELIMIT_WARNING(
                std::chrono::seconds(10), 10, "Node %lu has no location", node);
          }
        }
      }
      return 0.0;
    };

    idx = hashing::weighted_ch(
        log_id.val_, sequencers->nodes.size(), weight_fn_with_loc);
    ld_check(idx < static_cast<int64_t>(sequencers->nodes.size()));
    if (idx != -1) {
      found_in_location = true;
    }
  }
  if (!found_in_location) {
    // trying to find sequencer regardless of location
    auto weight_fn = [&](uint64_t node) {
      double w = sequencers->weights[node];
      if (can_we_route_to(node, w)) {
        return w;
      }
      return 0.0;
    };

    idx =
        hashing::weighted_ch(log_id.val_, sequencers->nodes.size(), weight_fn);
    ld_check(idx < static_cast<int64_t>(sequencers->nodes.size()));

    if (idx == -1) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "No available sequencer node for log %lu. "
                      "All sequencer nodes are unavailable.",
                      log_id.val_);
      err = E::NOTFOUND;
      return -1;
    }

    ld_assert_gt(sequencers->weights.at(idx), 0);
  }
  ld_check(sequencers->nodes[idx].isNodeID());
  ld_spew("Mapping log %lu to node %s",
          log_id.val_,
          sequencers->nodes[idx].toString().c_str());

  *out_sequencer = sequencers->nodes[idx];
  return 0;
}

bool HashBasedSequencerLocator::isAllowedToCache() const {
  // Sequencer nodes reply with a redirects if we send messages to wrong ones
  // (e.g. our cache is stale), so caching is safe and expected to improve
  // performance.
  return true;
}

ClusterState* HashBasedSequencerLocator::getClusterState() const {
  return Worker::getClusterState();
}

std::shared_ptr<const Configuration>
HashBasedSequencerLocator::getConfig() const {
  return Worker::getConfig();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
HashBasedSequencerLocator::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

const Settings& HashBasedSequencerLocator::getSettings() const {
  return Worker::settings();
}

}} // namespace facebook::logdevice
