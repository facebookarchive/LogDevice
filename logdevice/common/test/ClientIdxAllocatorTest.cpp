/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientIdxAllocator.h"

#include <iterator>
#include <set>
#include <unordered_set>
#include <vector>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"

using namespace facebook::logdevice;

TEST(ClientIdxAllocator, Reuse) {
  WorkerType type = WorkerType::GENERAL;
  worker_id_t worker_id_0(0);
  worker_id_t worker_id_1(0);
  worker_id_t worker_id_120(120);

  ClientIdxAllocator alloc;
  alloc.setMaxClientIdx(4);
  ASSERT_EQ(ClientID(1), alloc.issueClientIdx(type, worker_id_0));
  ASSERT_EQ(ClientID(2), alloc.issueClientIdx(type, worker_id_0));
  alloc.releaseClientIdx(ClientID(1));
  ASSERT_EQ(ClientID(3), alloc.issueClientIdx(type, worker_id_1));
  ASSERT_EQ(ClientID(4), alloc.issueClientIdx(type, worker_id_1));
  ASSERT_EQ(ClientID(1), alloc.issueClientIdx(type, worker_id_1));
  alloc.releaseClientIdx(ClientID(3));
  ASSERT_EQ(ClientID(3), alloc.issueClientIdx(type, worker_id_120))
      << "should not reuse 2 because it is still in use";
  alloc.releaseClientIdx(ClientID(1));
  ASSERT_EQ(ClientID(1), alloc.issueClientIdx(type, worker_id_120));
}

// Stress-test in a scenario with multiple workers.  Repeatedly acquire and
// release client indexes, ensuring invariants are preserved (unique client
// indices across all workers).
TEST(ClientIdxAllocatorTest, MultiWorkerStress) {
  const int num_workers = 4;
  const int max_idx = 400;
  const int ops = 100000;

  ClientIdxAllocator alloc;
  alloc.setMaxClientIdx(max_idx);

  struct PerWorkerState {
    std::unordered_set<int> in_use;
    // Candidates for releasing (we'll hold onto some indexes for long to
    // check that they are skipped on wraparound)
    std::vector<int> release_candidates;
  };
  std::vector<PerWorkerState> states(num_workers);

  std::unordered_set<int> all_in_use;
  std::mt19937_64 rng(0xbabadeda);
  int exhaust_count = 0;
  for (int i = 0; i < ops; ++i) {
    uint32_t worker_idx = folly::Random::rand32(num_workers, rng);
    PerWorkerState& state = states[worker_idx];
    if (state.in_use.size() == max_idx / num_workers) {
      // Exhausted the space for this worker, release all
      for (auto client_idx : state.in_use) {
        alloc.releaseClientIdx(ClientID(client_idx));
        all_in_use.erase(client_idx);
      }
      state.in_use.clear();
      state.release_candidates.clear();
      ++exhaust_count;
    }

    if (!state.release_candidates.empty() && folly::Random::oneIn(2)) {
      // Release a client index
      int j = folly::Random::rand32(state.release_candidates.size());
      int client_idx = state.release_candidates[j];
      std::swap(state.release_candidates[j], state.release_candidates.back());
      alloc.releaseClientIdx(ClientID(client_idx));
      state.in_use.erase(client_idx);
      state.release_candidates.pop_back();
      all_in_use.erase(client_idx);
    } else {
      int client_idx =
          alloc.issueClientIdx(WorkerType::GENERAL, (worker_id_t)worker_idx)
              .getIdx();
      ASSERT_TRUE(state.in_use.insert(client_idx).second);
      // Most of the time, make the client index available for releasing.
      // Hold on to some indexes for longer to check they are skipped on
      // wraparound.
      if (folly::Random::randDouble01(rng) < 0.95) {
        state.release_candidates.push_back(client_idx);
      }
      // Make sure another worker doesn't have the same index
      ASSERT_TRUE(all_in_use.insert(client_idx).second);
    }
  }

  ld_info("exhaust_count = %d", exhaust_count);
  EXPECT_GT(exhaust_count, 0);
}
