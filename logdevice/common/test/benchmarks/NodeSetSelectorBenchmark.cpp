/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Benchmark.h>
#include <folly/Singleton.h>

#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"

namespace facebook { namespace logdevice {

// Run with --bm_min_usec=1000000

static void do_benchmark(NodeSetSelectorType type, unsigned iterations) {
  std::shared_ptr<Configuration> config;
  BENCHMARK_SUSPEND {
    configuration::Nodes nodes;
    const int nodes_per_rack = 20;
    for (int rack = 0; rack < 10; ++rack) {
      for (int node = 0; node < nodes_per_rack; ++node) {
        configuration::Node n;
        node_index_t id = rack * nodes_per_rack + node;
        n.address = Sockaddr("::1", std::to_string(id));
        NodeLocation loc;
        int rv = loc.fromDomainString("region.dc.cluster.row.rack" +
                                      std::to_string(rack));
        ld_check(rv == 0);
        n.location = std::move(loc);
        n.addStorageRole(1);

        nodes[id] = std::move(n);
      }
    }

    Configuration::NodesConfig nodes_config(std::move(nodes));

    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicateAcross(
        {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, 2),
        "NodeSetSelectorBenchmark",
        log_attrs);

    config = std::make_shared<Configuration>(
        ServerConfig::fromDataTest(
            "NodeSetSelectorBenchmark", std::move(nodes_config)),
        std::move(logs_config));
  }

  for (unsigned i = 0; i < iterations; ++i) {
    auto selector = NodeSetSelectorFactory::create(type);
    auto res = selector->getStorageSet(
        logid_t(1),
        config.get(),
        *config->getNodesConfigurationFromServerConfigSource(),
        20,
        0,
        nullptr,
        nullptr);
    ld_check(res.decision == NodeSetSelector::Decision::NEEDS_CHANGE);
  }
}

BENCHMARK(WeightAwareNodeSetSelectorBenchmark, n) {
  do_benchmark(NodeSetSelectorType::WEIGHT_AWARE, n);
}

BENCHMARK(ConsistentHashingNodeSetSelectorBenchmark, n) {
  do_benchmark(NodeSetSelectorType::CONSISTENT_HASHING, n);
}

}} // namespace facebook::logdevice

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
#endif
