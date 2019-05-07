/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/json.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoGossip : public AdminCommand {
 private:
  node_index_t node_idx_{-1};
  bool json_{false};

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "node-idx", boost::program_options::value<node_index_t>(&node_idx_));
    opts.add_options()("json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("node-idx", 1);
  }

  std::string getUsage() override {
    return "info gossip [--json] [node idx]";
  }

  void run() override {
    if (json_) {
      printJson();
    } else {
      printPretty();
    }
  }

  void printJson() {
    auto serialized = folly::json::serialize(
        composeJson(), folly::json::serialization_opts());
    out_.write(serialized);
    out_.write("\r\n");
  }

  folly::dynamic composeJson() {
    folly::dynamic obj = folly::dynamic::object;
    auto detector = server_->getServerProcessor()->failure_detector_.get();

    if (detector == nullptr) {
      obj["error"] = "Failure detector not used.";
      return obj;
    }

    const auto& nodes_configuration =
        server_->getProcessor()->getNodesConfiguration();

    node_index_t lo = 0;
    node_index_t hi = nodes_configuration->getMaxNodeIndex();

    if (node_idx_ != node_index_t(-1)) {
      if (node_idx_ > hi) {
        obj["error"] = folly::sformat(
            "Node index expected to be in the [0, %u] range", hi);
        return obj;
      }
      lo = hi = node_idx_;
    }

    auto cs = server_->getProcessor()->cluster_state_.get();
    auto& states = obj["states"] = folly::dynamic::array;
    for (node_index_t idx = lo; idx <= hi; ++idx) {
      if (!nodes_configuration->isNodeInServiceDiscoveryConfig(idx)) {
        continue;
      }
      folly::dynamic row = folly::dynamic::object;
      row["node_id"] = folly::sformat("N{}", idx);
      row["status"] = cs->isNodeAlive(idx) ? "ALIVE" : "DEAD";
      row["detector"] = detector->getStateJson(idx);
      row["boycott_status"] = cs->isNodeBoycotted(idx) ? "BOYCOTTED" : "-";
      states.push_back(row);
    }

    if (node_idx_ == node_index_t(-1)) {
      // print domain isolation status in case "info gossip" is issued
      // without index
      obj["domain_isolation"] = detector->getDomainIsolationString().c_str();
      // print current ISOLATION value
      obj["isolated"] = detector->isIsolated() ? "true" : "false";
    }
    return obj;
  }

  void printPretty() {
    auto detector = server_->getServerProcessor()->failure_detector_.get();

    if (detector == nullptr) {
      out_.printf("Failure detector not used.\r\n");
      return;
    }

    const auto& nodes_configuration =
        server_->getProcessor()->getNodesConfiguration();

    node_index_t lo = 0;
    node_index_t hi = nodes_configuration->getMaxNodeIndex();

    if (node_idx_ != node_index_t(-1)) {
      if (node_idx_ > hi) {
        out_.printf("Node index expected to be in the [0, %u] range\r\n", hi);
        return;
      }
      lo = hi = node_idx_;
    }

    auto cs = server_->getProcessor()->cluster_state_.get();
    for (node_index_t idx = lo; idx <= hi; ++idx) {
      if (!nodes_configuration->isNodeInServiceDiscoveryConfig(idx)) {
        continue;
      }
      out_.printf("GOSSIP N%u %s %s %s\r\n",
                  idx,
                  cs->isNodeAlive(idx) ? "ALIVE" : "DEAD",
                  detector->getStateString(idx).c_str(),
                  cs->isNodeBoycotted(idx) ? "BOYCOTTED" : "-");
    }

    if (node_idx_ == node_index_t(-1)) {
      // print domain isolation status in case "info gossip" is issued
      // without index
      out_.printf("%s", detector->getDomainIsolationString().c_str());
      // print current ISOLATION value
      out_.printf("ISOLATED %s\r\n", detector->isIsolated() ? "true" : "false");
    }
  }
};

}}} // namespace facebook::logdevice::commands
