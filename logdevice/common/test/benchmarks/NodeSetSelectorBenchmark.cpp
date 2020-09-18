/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Benchmark.h>
#include <folly/Singleton.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/nodeset_selection/NodeSetSelectorFactory.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

namespace facebook { namespace logdevice {

// Run with --bm_min_usec=1000000

static void do_benchmark(NodeSetSelectorType type, unsigned iterations) {
  std::shared_ptr<Configuration> config;
  BENCHMARK_SUSPEND {
    configuration::Nodes nodes;
    const int nodes_per_rack = 20;
    for (int rack = 0; rack < 10; ++rack) {
      for (int node = 0; node < nodes_per_rack; ++node) {
        nodes[rack * nodes_per_rack + node] =
            configuration::Node::withTestDefaults(rack * nodes_per_rack + node)
                .setIsMetadataNode(true)
                .setLocation("region.dc.cluster.row.rack" +
                             std::to_string(rack))
                .addStorageRole(1);
      }
    }

    auto nodes_config =
        NodesConfigurationTestUtil::provisionNodes(std::move(nodes));

    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    auto log_attrs = logsconfig::LogAttributes().with_replicateAcross(
        {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, 2),
        "NodeSetSelectorBenchmark",
        log_attrs);

    config = std::make_shared<Configuration>(
        ServerConfig::fromDataTest("NodeSetSelectorBenchmark"),
        std::move(logs_config),
        std::move(nodes_config));
  }

  for (unsigned i = 0; i < iterations; ++i) {
    auto selector = NodeSetSelectorFactory::create(type);
    auto res = selector->getStorageSet(logid_t(1),
                                       config.get(),
                                       *config->getNodesConfiguration(),
                                       20,
                                       0,
                                       nullptr,
                                       NodeSetSelector::Options{});
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
