/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/stats/Stats.h"

#include <array>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/stats/BucketedTimeSeries.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/stats/ClientHistograms.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

// This tests basic aggregation functionality of the Stats class.
TEST(StatsTest, StatsAggregateTest) {
  FastUpdateableSharedPtr<StatsParams> params(std::make_shared<StatsParams>());
  params.get()->setIsServer(false); // client stats
  Stats s1(&params), s2(&params), total(&params);

  ++s1.records_delivered;
  ++s2.records_delivered;
  ++s2.records_delivered;

  total.aggregate(s1, StatsAgg::SUM);
  total.aggregate(s2);

  EXPECT_EQ(3, total.records_delivered);

  total.aggregate(s1, StatsAgg::SUBTRACT);

  EXPECT_EQ(2, total.records_delivered);

  s1.records_delivered = 42;
  total.aggregate(s1, StatsAgg::ASSIGN);

  EXPECT_EQ(42, total.records_delivered);

  total = s2;
  EXPECT_EQ(2, total.records_delivered);
}

// This test creates N threads, each of which increments num_connections and
// store_synced stats. Before threads exit, test asserts that both
// aggregated counters are N. After threads exit, test that
// asserts num_connections 0 and store_synced is still N.
TEST(StatsTest, MultipleThreadsTest) {
  StatsHolder holder(StatsParams().setIsServer(true));
  constexpr int nthreads = 8;

  std::vector<std::thread> threads;

  std::mutex m1, m2;
  std::condition_variable cv1, cv2;
  bool done = false;
  int n = 0;

  for (int i = 0; i < nthreads; ++i) {
    threads.emplace_back([&]() {
      ++holder.get().num_connections;
      ++holder.get().store_synced;
      {
        std::lock_guard<std::mutex> l(m1);
        ++n;
      }
      cv1.notify_one();

      // wait before exiting
      {
        std::unique_lock<std::mutex> l(m2);
        cv2.wait(l, [&] { return done; });
      }
    });
  }

  // wait for all threads to start
  {
    std::unique_lock<std::mutex> l(m1);
    cv1.wait(l, [&] { return n >= nthreads; });
  }

  Stats total = holder.aggregate();
  EXPECT_EQ(nthreads, total.num_connections);
  EXPECT_EQ(nthreads, total.store_synced);

  // terminate all threads
  {
    std::lock_guard<std::mutex> l(m2);
    done = true;
    cv2.notify_all();
  }

  for (int i = 0; i < nthreads; ++i) {
    threads[i].join();
  }

  total = holder.aggregate();
  EXPECT_EQ(0, total.num_connections);
  EXPECT_EQ(nthreads, total.store_synced);
}

TEST(StatsTest, LatencyPercentileTest) {
  FastUpdateableSharedPtr<StatsParams> params(std::make_shared<StatsParams>());
  Stats s(&params);
  int64_t p;

  auto& h = s.client.histograms->append_latency;

  p = h.estimatePercentile(.5);
  EXPECT_EQ(0, p);

  h.add(85);
  p = h.estimatePercentile(.5);
  EXPECT_GT(p, 10);
  EXPECT_LT(p, 100);

  // 5000s
  h.add((int64_t)5e9);
  p = h.estimatePercentile(.99);
  EXPECT_GT(p, (int64_t)1e9);
  EXPECT_LT(p, (int64_t)1e10);

  // ~5 years
  h.add((int64_t)16e13);
  p = h.estimatePercentile(.99);
  EXPECT_GT(p, (int64_t)1e14);
  EXPECT_LT(p, (int64_t)1e15);

  // min and max percentile
  p = h.estimatePercentile(0.);
  EXPECT_EQ(p, 80);
  p = h.estimatePercentile(1.);
  EXPECT_EQ(p, (int64_t)2e14);

  // estimate percentiles in batch
  const double pcts[] = {0., .01, .1, .25, .5, .75, .9, .99, 1.};
  constexpr size_t npcts = sizeof(pcts) / sizeof(pcts[0]);
  int64_t samples[npcts];
  h.estimatePercentiles(pcts, npcts, samples);
  EXPECT_EQ(samples[0], 80);
  EXPECT_EQ(samples[1], 80);
  EXPECT_GT(samples[2], 80);
  EXPECT_LT(samples[2], 90);
  EXPECT_GT(samples[3], 80);
  EXPECT_LT(samples[3], 90);
  EXPECT_GT(samples[4], (int64_t)1e9);
  EXPECT_LT(samples[4], (int64_t)1e10);
  EXPECT_GT(samples[5], (int64_t)1e14);
  EXPECT_LT(samples[5], (int64_t)2e14);
  EXPECT_GT(samples[6], (int64_t)1e14);
  EXPECT_LT(samples[6], (int64_t)2e14);
  EXPECT_GT(samples[7], (int64_t)1e14);
  EXPECT_LT(samples[7], (int64_t)2e14);
  EXPECT_EQ(samples[8], (int64_t)2e14);
}

TEST(StatsTest, HistogramConcurrencyTest) {
  static constexpr size_t NUM_THREADS = 16;
  static constexpr size_t ADDS_PER_THREAD = 10'000'000;

  // Per-thread state. Each thread has a histogram and an output sum.
  std::array<LatencyHistogram, NUM_THREADS> histograms;
  std::array<std::atomic<uint64_t>, NUM_THREADS> sums;
  for (size_t i = 0; i < NUM_THREADS; ++i) {
    std::atomic_init(&sums[i], uint64_t(0));
  }

  // Worker thread main function.
  std::atomic<bool> abort(false);
  std::atomic<size_t> threads_done(0);
  auto thread_main = [&abort, &threads_done](
                         LatencyHistogram* h, std::atomic<uint64_t>* sum) {
    folly::ThreadLocalPRNG prng;
    uint64_t s = 0;
    for (size_t i = 0; !abort && i < ADDS_PER_THREAD; ++i) {
      uint64_t value = prng();
      h->add(value);
      s += value;
    }
    *sum = s;
    ++threads_done;
  };

  // Start worker threads.
  std::array<std::unique_ptr<std::thread>, NUM_THREADS> threads;
  for (size_t i = 0; i < NUM_THREADS; ++i) {
    threads[i] =
        std::make_unique<std::thread>(thread_main, &histograms[i], &sums[i]);
  }

  // Join worker thread on exit.
  SCOPE_EXIT {
    abort = true;
    for (size_t i = 0; i < NUM_THREADS; ++i) {
      threads[i]->join();
    }
  };

  // Test thread-safe copy and make sure getCountAndSum() does not regress.
  std::array<std::pair<uint64_t, int64_t>, NUM_THREADS> counts_and_sums;
  for (size_t i = 0; i < NUM_THREADS; ++i) {
    counts_and_sums[i].first = 0;
    counts_and_sums[i].second = 0;
  }
  while (threads_done < NUM_THREADS) {
    LatencyHistogram histogram_copy;
    for (size_t i = 0; i < NUM_THREADS; ++i) {
      std::pair<uint64_t, int64_t> cs;
      if (i % 2) {
        // Test thread-safe copy assignment.
        histogram_copy = histograms[i];
        cs = histogram_copy.getCountAndSum();
      } else {
        // Call countAndSum() directly.
        cs = histograms[i].getCountAndSum();
      }
      ASSERT_GE(cs.first, counts_and_sums[i].first);
      ASSERT_GE(cs.second, counts_and_sums[i].second);
      counts_and_sums[i] = cs;
    }
  };

  // Compare final counts and sums to histograms. Any discrepancy would
  // indicate that add()/mergeStagedValues() lost or corrupted staged values.
  for (size_t i = 0; i < NUM_THREADS; ++i) {
    auto cs = histograms[i].getCountAndSum();
    ASSERT_EQ(cs.first, ADDS_PER_THREAD);
    ASSERT_EQ(cs.second, sums[i].load());
  }
}

TEST(StatsTest, PerNodeTimeSeriesSingleThread) {
  StatsHolder holder(
      StatsParams().setIsServer(false).setNodeStatsRetentionTimeOnClients(
          DEFAULT_TEST_TIMEOUT));
  NodeID id1{1}, id2{2};

  StatsHolder* holder_ptr = &holder;

  PER_NODE_STAT_ADD(holder_ptr, id1, AppendSuccess);
  PER_NODE_STAT_ADD(holder_ptr, id1, AppendSuccess);
  PER_NODE_STAT_ADD(holder_ptr, id1, AppendFail);

  PER_NODE_STAT_ADD(holder_ptr, id2, AppendFail);

  std::unordered_map<NodeID, size_t, NodeID::Hash> append_success;
  std::unordered_map<NodeID, size_t, NodeID::Hash> append_fail;

  const auto now = std::chrono::steady_clock::now();
  // the only thread is this one though, so it'll only loop once
  holder.runForEach([&](Stats& stats) {
    for (auto& thread_node_stats :
         stats.synchronizedCopy(&Stats::per_node_stats)) {
      thread_node_stats.second->updateCurrentTime(now);

      append_success[thread_node_stats.first] +=
          thread_node_stats.second->sumAppendSuccess(
              now - DEFAULT_TEST_TIMEOUT, now);

      append_fail[thread_node_stats.first] +=
          thread_node_stats.second->sumAppendFail(
              now - DEFAULT_TEST_TIMEOUT, now);
    }
  });

  EXPECT_EQ(2, append_success[id1]);
  EXPECT_EQ(1, append_fail[id1]);

  EXPECT_EQ(0, append_success[id2]);
  EXPECT_EQ(1, append_fail[id2]);
}

TEST(StatsTest, PerNodeTimeSeriesMultiThread) {
  StatsHolder holder(
      StatsParams().setIsServer(false).setNodeStatsRetentionTimeOnClients(
          DEFAULT_TEST_TIMEOUT));
  NodeID id1{1}, id2{2};
  constexpr int nthreads = 8;
  constexpr int per_thread_count = 100000;

  std::vector<std::thread> threads;
  Semaphore sem1, sem2;

  StatsHolder* holder_ptr = &holder;

  for (int i = 0; i < nthreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < per_thread_count; ++j) {
        PER_NODE_STAT_ADD(holder_ptr, id1, AppendSuccess);
        PER_NODE_STAT_ADD(holder_ptr, id1, AppendSuccess);
        PER_NODE_STAT_ADD(holder_ptr, id1, AppendFail);

        PER_NODE_STAT_ADD(holder_ptr, id2, AppendSuccess);
        PER_NODE_STAT_ADD(holder_ptr, id2, AppendFail);
        PER_NODE_STAT_ADD(holder_ptr, id2, AppendFail);
      }

      sem1.post();

      // have to loop over all threads before exiting thread
      sem2.wait();
    });
  }

  for (int i = 0; i < nthreads; ++i) {
    sem1.wait();
  }

  const auto now = std::chrono::steady_clock::now();
  std::unordered_map<NodeID, size_t, NodeID::Hash> append_success;
  std::unordered_map<NodeID, size_t, NodeID::Hash> append_fail;

  // collect stats
  holder.runForEach([&](Stats& stats) {
    for (auto& thread_node_stats :
         stats.synchronizedCopy(&Stats::per_node_stats)) {
      thread_node_stats.second->updateCurrentTime(now);

      append_success[thread_node_stats.first] +=
          thread_node_stats.second->sumAppendSuccess(
              now - DEFAULT_TEST_TIMEOUT, now);

      append_fail[thread_node_stats.first] +=
          thread_node_stats.second->sumAppendFail(
              now - DEFAULT_TEST_TIMEOUT, now);
    }
  });

  // id1 increments success twice, and fail once per thread and per_thread_count
  EXPECT_EQ(nthreads * per_thread_count * 2, append_success[id1]);
  EXPECT_EQ(nthreads * per_thread_count, append_fail[id1]);

  // id2 increments success once, and fail twice per thread and per_thread_count
  EXPECT_EQ(nthreads * per_thread_count, append_success[id2]);
  EXPECT_EQ(nthreads * per_thread_count * 2, append_fail[id2]);

  // release all threads
  for (int i = 0; i < nthreads; ++i) {
    sem2.post();
  }
  // make sure that all threads are done before quitting
  for (int i = 0; i < nthreads; ++i) {
    threads[i].join();
  }
}

TEST(StatsTest, PerNodeTimeSeriesUpdateAggregationTime) {
  auto initial_retention_time = std::chrono::seconds(10);
  PerNodeTimeSeriesStats per_node_stats(initial_retention_time);

  EXPECT_EQ(initial_retention_time, per_node_stats.retentionTime());
  EXPECT_EQ(initial_retention_time,
            per_node_stats.getAppendSuccess()->rlock()->duration());
  EXPECT_EQ(initial_retention_time,
            per_node_stats.getAppendFail()->rlock()->duration());

  auto updated_retention_time = std::chrono::seconds(50);
  per_node_stats.updateRetentionTime(updated_retention_time);

  EXPECT_EQ(updated_retention_time, per_node_stats.retentionTime());
  EXPECT_EQ(updated_retention_time,
            per_node_stats.getAppendSuccess()->rlock()->duration());
  EXPECT_EQ(updated_retention_time,
            per_node_stats.getAppendFail()->rlock()->duration());
}

TEST(StatsTest, PerNodeTimeSeriesUpdateAggregationTimeDecreaseDuration) {
  const auto initial_retention_time = std::chrono::seconds{50};
  PerNodeTimeSeriesStats per_node_stats(initial_retention_time);

  const auto now = std::chrono::steady_clock::now();

  per_node_stats.addAppendSuccess(now - std::chrono::seconds{20});
  per_node_stats.addAppendFail(now - std::chrono::seconds{20});

  per_node_stats.addAppendSuccess(now);
  per_node_stats.addAppendFail(now);

  // less than latest_time - earliest_time
  const auto updated_retention_time = std::chrono::seconds{10};
  per_node_stats.updateRetentionTime(updated_retention_time);

  EXPECT_EQ(updated_retention_time, per_node_stats.retentionTime());
  EXPECT_EQ(updated_retention_time,
            per_node_stats.getAppendSuccess()->rlock()->duration());
  EXPECT_EQ(updated_retention_time,
            per_node_stats.getAppendFail()->rlock()->duration());
}

// make sure nothing crazy happens when summing over old values
// added because of the comment in StatsThroughputAppends.cpp:77-79 noting that
// things might start barfing if asking for old data
TEST(StatsTest, PerNodeTimeSeriesSumOld) {
  std::chrono::seconds retention_time{5};
  const auto now = std::chrono::steady_clock::now();
  PerNodeTimeSeriesStats stats(retention_time);

  stats.addAppendSuccess(now);
  EXPECT_EQ(1, stats.sumAppendSuccess(now - retention_time, now));

  stats.updateCurrentTime(now + retention_time * 2);
  EXPECT_EQ(
      0,
      stats.sumAppendSuccess(now - retention_time, now + retention_time * 2));
}

TEST(StatsTest, StatsParamsBuilder) {
  auto is_server = true;
  auto node_stats_retention_time_on_clients = std::chrono::seconds(42);
  auto params =
      StatsParams().setIsServer(is_server).setNodeStatsRetentionTimeOnClients(
          node_stats_retention_time_on_clients);

  EXPECT_EQ(is_server, params.is_server);
  EXPECT_EQ(node_stats_retention_time_on_clients,
            params.node_stats_retention_time_on_clients);
}

TEST(StatsTest, StatsParamsUpdateAcrossThread) {
  Semaphore sem1, sem2;

  StatsHolder holder(StatsParams().setIsServer(true));
  std::thread thread([&]() {
    auto& thread_stats = holder.get();
    EXPECT_TRUE(thread_stats.params->get()->is_server);
    sem1.post();
    sem2.wait();
    EXPECT_FALSE(thread_stats.params->get()->is_server);
  });

  sem1.wait();

  holder.params_.update(
      std::make_shared<StatsParams>(StatsParams().setIsServer(false)));

  sem2.post();

  thread.join();
}

// Test that looks for race conditions in how StatsHolder handles thread
// destruction (dead_stats_).
// Create and destroy lots of threads that increment a stat. In another thread
// keep polling the stat and check that it doesn't decrease. Do random sleeps
// everywhere to try to hit different interleavings of threads executions.
TEST(StatsTest, ThreadDestructionStressTest) {
  StatsHolder holder(StatsParams().setIsServer(false));

  // Body of a short-living thread that increments the stat.
  auto stat_bumping_thread_func = [&holder] {
    if (folly::Random::rand32() % 2) {
      /* sleep override */
      std::this_thread::sleep_for(
          std::chrono::microseconds(folly::Random::rand32() % 2000));
    }
    STAT_INCR(&holder, post_request_total);
    if (folly::Random::rand32() % 2) {
      /* sleep override */
      std::this_thread::sleep_for(
          std::chrono::microseconds(folly::Random::rand32() % 2000));
    }
  };

  std::atomic<bool> shutdown{false};
  std::atomic<uint64_t> threads_spawned{0};

  // Thread that keeps polling the stat and checks that it doesn't decrease.
  std::thread monitoring_thread([&] {
    int64_t prev = 0;
    while (!shutdown.load()) {
      if (folly::Random::rand32() % 2) {
        /* sleep override */
        std::this_thread::sleep_for(
            std::chrono::microseconds(folly::Random::rand32() % 2000));
      }
      int64_t cur = holder.aggregate().post_request_total;
      EXPECT_GE(cur, prev);
      EXPECT_LE(cur, threads_spawned.load()); // load after reading the stat

      prev = cur;
    }
  });

  std::thread thread_spawning_thread([&] {
    std::vector<std::thread> ts(100);
    while (!shutdown.load()) {
      if (folly::Random::rand32() % 2) {
        /* sleep override */
        std::this_thread::sleep_for(
            std::chrono::microseconds(folly::Random::rand32() % 2000));
      }

      threads_spawned.fetch_add(1); // increment before starting the thread
      size_t i = folly::Random::rand32() % ts.size();
      std::thread& t = ts[i];
      if (t.joinable()) {
        t.join();
      }
      t = std::thread(stat_bumping_thread_func);
    }

    for (std::thread& t : ts) {
      if (t.joinable()) {
        t.join();
      }
    }
  });

  // Let it run for 5 seconds or 2000 threads spawned, whichever happens later.
  // The sleeps are such that we spawn 2000 threads per second on average.

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(5));

  while (threads_spawned.load() < 2000) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  shutdown.store(true);
  thread_spawning_thread.join();
  EXPECT_EQ(threads_spawned.load(), holder.aggregate().post_request_total);
  monitoring_thread.join();

  ld_info("Spawned %lu threads", threads_spawned.load());
}
