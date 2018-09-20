/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <deque>
#include <random>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/include/types.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

using namespace facebook::logdevice;

const size_t N_LOGS = 200000; // logid starts from 1

const shard_index_t SHARD_IDX = 0;

/**
 * @file: a benchmark for testing time spent on accessing LogStorageStateMap
 *        populated with different logids. The performance is directly related
 *        to the internal AtomicHashMap, and specifically, its hash function.
 */

// range 1..100000
static inline void populateLogs(LogStorageStateMap* map, bool metadata_log) {
  ld_check(map);
  for (size_t i = 0; i < N_LOGS / 2; ++i) {
    const logid_t logid(1 + i);
    map->insertOrGet(logid, SHARD_IDX);
    if (metadata_log) {
      map->insertOrGet(MetaDataLog::metaDataLogID(logid), SHARD_IDX);
    } else {
      map->insertOrGet(logid_t(N_LOGS / 2 + i + 1), SHARD_IDX);
    }
  }
}

static inline void accessMap(LogStorageStateMap* map,
                             int n_threads,
                             size_t n_iters) {
  ld_check(map);
  static std::mt19937_64 rnd{std::random_device()()};
  std::uniform_int_distribution<logid_t::raw_type> dis(1, N_LOGS / 2);

  std::vector<std::thread> threads;
  for (int i = 0; i < n_threads; ++i) {
    threads.emplace_back([map, &dis, n_iters]() {
      for (size_t j = 0; j < n_iters; ++j) {
        folly::doNotOptimizeAway(
            map->insertOrGet(logid_t(dis(rnd)), SHARD_IDX));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

DEFINE_int32(num_threads, 32, "Number of threads for benchmarks.");

BENCHMARK(LogStorageStateMapWithDataLogs, iters) {
  std::unique_ptr<LogStorageStateMap> map = nullptr;

  BENCHMARK_SUSPEND {
    map.reset(new LogStorageStateMap(1));
    populateLogs(map.get(), false);
  }

  accessMap(map.get(), FLAGS_num_threads, iters / FLAGS_num_threads);
}

BENCHMARK(LogStorageStateMapWithMetaDataLogs, iters) {
  std::unique_ptr<LogStorageStateMap> map = nullptr;

  BENCHMARK_SUSPEND {
    map.reset(new LogStorageStateMap(1));
    populateLogs(map.get(), true);
  }

  accessMap(map.get(), FLAGS_num_threads, iters / FLAGS_num_threads);
}

BENCHMARK_DRAW_LINE();

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  gflags::SetCommandLineOptionWithMode(
      "bm_min_iters", "100000000", gflags::SET_FLAG_IF_DEFAULT);
  folly::runBenchmarks();
  return 0;
}
