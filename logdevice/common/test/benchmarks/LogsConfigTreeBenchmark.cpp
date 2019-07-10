/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/ScopeGuard.h>
#include <folly/Singleton.h>

#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/include/LogAttributes.h"

DEFINE_int32(num_directories,
             250000,
             "Number of directories in the logs config tree");

DEFINE_int32(num_log_groups,
             500000,
             "Number of log groups in the logs config tree");

using namespace facebook::logdevice;
using namespace facebook::logdevice::logsconfig;

std::unique_ptr<LogsConfigTree> createTestTree() {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  auto defaults = DefaultLogAttributes().with_replicationFactor(3);
  for (int i = 1; i <= FLAGS_num_directories; i++) {
    auto dir =
        tree->addDirectory(tree->root(), "dir" + std::to_string(i), defaults);
    // We want to distribute the log-groups as evenly as possible over the
    // directories.
    for (int j = 1; j <= (FLAGS_num_log_groups / FLAGS_num_directories); j++) {
      tree->addLogGroup(dir,
                        "log-" + std::to_string(j),
                        logid_range_t{logid_t(j), logid_t(j)},
                        LogAttributes(),
                        false);
    }
  }
  return tree;
}

// Clone a tree N times. The tree is defined by num_log_groups and
// num_directories.
BENCHMARK(LogsConfigTreeCloning, n) {
  folly::BenchmarkSuspender benchmark_suspender;
  std::unique_ptr<LogsConfigTree> tree = createTestTree();
  benchmark_suspender.dismiss();
  // The actual benchmark is testing the copy performance
  for (int i = 1; i <= n; i++) {
    tree->copy();
  }
}

BENCHMARK(LogsConfigTreeAdd, n) {
  folly::BenchmarkSuspender benchmark_suspender;
  auto tree = LogsConfigTree::create();
  benchmark_suspender.dismiss();
  for (int i = 1; i <= n; i++) {
    tree->addLogGroup("/log-" + std::to_string(i),
                      logid_range_t{logid_t(i), logid_t(i)},
                      LogAttributes().with_replicationFactor(3),
                      false);
  }
}

BENCHMARK(LogsConfigTreeFindDir, n) {
  folly::BenchmarkSuspender benchmark_suspender;
  std::unique_ptr<LogsConfigTree> tree = createTestTree();
  std::string dirname1 = folly::sformat("/dir{}", FLAGS_num_directories / 2);
  benchmark_suspender.dismiss();

  for (int i = 1; i <= n; i++) {
    tree->findDirectory(dirname1);
  }
}

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::CRITICAL;
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
#endif
