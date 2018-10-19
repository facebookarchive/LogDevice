/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/LogStorageStateMap.h"

#include <deque>
#include <thread>
#include <vector>

#include <boost/noncopyable.hpp>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

const int STRESS_TEST_THREADS = 32;
const int STRESS_TEST_LOGS = 8;
const int STRESS_TEST_LSNS = 10000;

const shard_index_t THIS_SHARD = 0;

struct StressTestLSNUpdatingThread : boost::noncopyable {
  explicit StressTestLSNUpdatingThread(LogStorageStateMap* map) : map_(map) {}

  void operator()() {
    for (int log_id = 1; log_id <= STRESS_TEST_LOGS; ++log_id) {
      LogStorageState& state = map_->get(logid_t(log_id), THIS_SHARD);
      for (int lsn = 1; lsn <= STRESS_TEST_LSNS; ++lsn) {
        int rv = state.updateLastReleasedLSN(
            lsn, LogStorageState::LastReleasedSource::RELEASE);
        if (rv == 0) {
          won_.emplace_back(logid_t(log_id), lsn);
        } else {
          ld_check(err == E::UPTODATE);
        }
      }
    }
  }

  LogStorageStateMap* const map_;
  std::vector<std::pair<logid_t, lsn_t>> won_;
};

/**
 * Fires up a bunch of threads that try to release all LSNs in sequence for a
 * few logs.  For every LSN, LogStorageStateMap::updateLastReleasedLSN should
 * return 0 exactly once despite many threads trying.
 *
 * Note that this is not representative of a production workload.  In
 * production, almost every LSN will be put into the map by only one thread.
 * However, it is a useful stress test for the map's implementation.
 */
TEST(LogStorageStateMapTest, StressTestLSNUpdating) {
  LogStorageStateMap map(1);

  // Initialize LogStorageState instances for all logs
  for (int log_id = 1; log_id <= STRESS_TEST_LOGS; ++log_id) {
    LogStorageState* log_state = map.insertOrGet(logid_t(log_id), THIS_SHARD);
    ASSERT_NE(log_state, nullptr);
    auto lsn_state = log_state->getLastReleasedLSN();
    ASSERT_FALSE(lsn_state.hasValue());
  }

  std::deque<StressTestLSNUpdatingThread> instances;
  std::vector<std::thread> threads;
  for (int i = 0; i < STRESS_TEST_THREADS; ++i) {
    instances.emplace_back(&map);
    threads.emplace_back(std::ref(instances[i]));
  }

  for (std::thread& thread : threads) {
    thread.join();
  }

  std::vector<std::pair<logid_t, lsn_t>> expected, observed;
  // We expect to see every logid/lsn pair exactly once
  for (int log_id = 1; log_id <= STRESS_TEST_LOGS; ++log_id) {
    for (int lsn = 1; lsn <= STRESS_TEST_LSNS; ++lsn) {
      expected.emplace_back(logid_t(log_id), lsn);
    }
  }

  for (int i = 0; i < STRESS_TEST_THREADS; ++i) {
    const auto& thread_won = instances[i].won_;
    ld_spew("thread %d won the update %zu times", i, thread_won.size());
    observed.insert(observed.end(), thread_won.begin(), thread_won.end());
  }
  std::sort(observed.begin(), observed.end());

  ASSERT_EQ(expected, observed);

  for (int log_id = 1; log_id <= STRESS_TEST_LOGS; ++log_id) {
    auto state = map.get(logid_t(log_id), THIS_SHARD).getLastReleasedLSN();
    ASSERT_TRUE(state.hasValue());
    EXPECT_EQ(lsn_t(STRESS_TEST_LSNS), state.value());
  }
}

/**
 * Basic test for worker subscriptions.
 */
TEST(LogStorageStateMapTest, WorkerSubscriptions) {
  LogStorageStateMap map(1);
  LogStorageState log_state(logid_t(22), THIS_SHARD, &map);
  const int nworkers = 8;

  for (int i = 0; i < nworkers; ++i) {
    ASSERT_FALSE(log_state.isWorkerSubscribed(worker_id_t(i)));
  }

  const worker_id_t sub(4);
  log_state.subscribeWorker(sub);
  for (int i = 0; i < nworkers; ++i) {
    worker_id_t id(i);
    if (id == sub) {
      ASSERT_TRUE(log_state.isWorkerSubscribed(id));
    } else {
      ASSERT_FALSE(log_state.isWorkerSubscribed(id));
    }
  }

  log_state.unsubscribeWorker(sub);
  ASSERT_FALSE(log_state.isWorkerSubscribed(sub));
}

TEST(LogStorageStateMapTest, LastReleasedLSNSource) {
  LogStorageStateMap map(1);
  LogStorageState log_state(logid_t(42), THIS_SHARD, &map);

  int rv = log_state.updateLastReleasedLSN(
      1, LogStorageState::LastReleasedSource::LOCAL_LOG_STORE);
  ASSERT_EQ(0, rv);

  rv = log_state.updateLastReleasedLSN(
      2, LogStorageState::LastReleasedSource::RELEASE);
  ASSERT_EQ(0, rv);

  auto released_state = log_state.getLastReleasedLSN();
  ASSERT_TRUE(released_state.hasValue());
  EXPECT_EQ(2, released_state.value());
  EXPECT_EQ(
      LogStorageState::LastReleasedSource::RELEASE, released_state.source());
}
