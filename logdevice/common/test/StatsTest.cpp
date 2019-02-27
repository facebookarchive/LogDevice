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

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/ThreadCachedInt.h>
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

  ++s1.client.records_delivered;
  ++s2.client.records_delivered;
  ++s2.client.records_delivered;

  total.aggregate(s1, StatsAgg::SUM);
  total.aggregate(s2);

  EXPECT_EQ(3, total.client.records_delivered);

  total.aggregate(s1, StatsAgg::SUBTRACT);

  EXPECT_EQ(2, total.client.records_delivered);

  s1.client.records_delivered = 42;
  total.aggregate(s1, StatsAgg::ASSIGN);

  EXPECT_EQ(42, total.client.records_delivered);

  total = s2;
  EXPECT_EQ(2, total.client.records_delivered);
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

/**
 * Creates two maps where the key is a NodeID and the value is the sum of the
 * appends on that node. The maps are called append_success and append_fail.
 * It loops over all threads, clients, and nodes, and sums the stats together
 * into the above mentioned maps
 */
#define PER_CLIENT_NODE_STAT_COLLECT()                               \
  std::unordered_map<NodeID, uint32_t, NodeID::Hash> append_success; \
  std::unordered_map<NodeID, uint32_t, NodeID::Hash> append_fail;    \
                                                                     \
  const auto now = std::chrono::steady_clock::now();                 \
  holder.runForEach([&](Stats& stats) {                              \
    stats.per_client_node_stats.wlock()->updateCurrentTime(now);     \
    auto total = stats.per_client_node_stats.rlock()->sum();         \
    for (auto& bucket : total) {                                     \
      append_success[bucket.node_id] += bucket.value.successes;      \
      append_fail[bucket.node_id] += bucket.value.failures;          \
    }                                                                \
  });

TEST(StatsTest, PerClientNodeTimeSeriesStats_SingleClientSimple) {
  StatsHolder holder(
      StatsParams().setNodeStatsRetentionTimeOnNodes(DEFAULT_TEST_TIMEOUT));

  ClientID client_id{1};
  NodeID node_id{1};

  auto holder_ptr = &holder;

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id, node_id, 10, 5);

  PER_CLIENT_NODE_STAT_COLLECT();

  EXPECT_EQ(10, append_success[node_id]);
  EXPECT_EQ(5, append_fail[node_id]);
}

// check that values are summed together
TEST(StatsTest, PerClientNodeTimeSeriesStats_SingleClientAppendMany) {
  StatsHolder holder(
      StatsParams().setNodeStatsRetentionTimeOnNodes(DEFAULT_TEST_TIMEOUT));

  ClientID client_id{1};
  NodeID node_id{1};

  auto holder_ptr = &holder;

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id, node_id, 10, 0);
  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id, node_id, 50, 0);
  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id, node_id, 2000, 0);

  PER_CLIENT_NODE_STAT_COLLECT();

  EXPECT_EQ(2060, append_success[node_id]);
}

TEST(StatsTest, PerClientNodeTimeSeriesStats_ManyNodes) {
  StatsHolder holder(
      StatsParams().setNodeStatsRetentionTimeOnNodes(DEFAULT_TEST_TIMEOUT));

  ClientID client_id{1};
  NodeID node_id_1{1}, node_id_2{2};

  auto holder_ptr = &holder;

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id, node_id_1, 10, 100);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id, node_id_2, 20, 200);

  PER_CLIENT_NODE_STAT_COLLECT();

  EXPECT_EQ(10, append_success[node_id_1]);
  EXPECT_EQ(100, append_fail[node_id_1]);

  EXPECT_EQ(20, append_success[node_id_2]);
  EXPECT_EQ(200, append_fail[node_id_2]);
}

TEST(StatsTest, PerClientNodeTimeSeriesStats_ManyClients) {
  StatsHolder holder(
      StatsParams().setNodeStatsRetentionTimeOnNodes(DEFAULT_TEST_TIMEOUT));

  ClientID client_id_1{1}, client_id_2{2};
  NodeID node_id{1};

  auto holder_ptr = &holder;

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id_1, node_id, 10, 100);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id_2, node_id, 20, 200);

  PER_CLIENT_NODE_STAT_COLLECT();

  EXPECT_EQ(30, append_success[node_id]);
  EXPECT_EQ(300, append_fail[node_id]);
}

/**
 * each thread will execute 100k times, adding 10 to success and 5 to fail,
 * for both clients and nodes
 */
TEST(StatsTest, PerClientNodeTimeSeriesStats_ManyThreads) {
  StatsHolder holder(
      StatsParams().setNodeStatsRetentionTimeOnNodes(DEFAULT_TEST_TIMEOUT));

  ClientID client_id_1{1}, client_id_2{2};
  NodeID node_id_1{1}, node_id_2{2};

  constexpr int nthreads = 8;
  constexpr int per_thread_count = 100000;

  constexpr auto success_expected = per_thread_count * nthreads * 10 * 2;
  constexpr auto fail_expected = per_thread_count * nthreads * 5 * 2;

  std::vector<std::thread> threads;
  Semaphore sem1, sem2;

  StatsHolder* holder_ptr = &holder;

  for (int i = 0; i < nthreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < per_thread_count; ++j) {
        // client 1, node 1
        PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id_1, node_id_1, 10, 5);

        // client 1, node 2
        PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id_1, node_id_2, 10, 5);

        // client 2, node 1
        PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id_2, node_id_1, 10, 5);

        // client 2, node 2
        PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_id_2, node_id_2, 10, 5);
      }

      sem1.post();

      // have to loop over all threads before exiting thread
      sem2.wait();
    });
  }

  for (int i = 0; i < nthreads; ++i) {
    sem1.wait();
  }

  PER_CLIENT_NODE_STAT_COLLECT();

  EXPECT_EQ(success_expected, append_success[node_id_1]);
  EXPECT_EQ(fail_expected, append_fail[node_id_1]);

  EXPECT_EQ(success_expected, append_success[node_id_2]);
  EXPECT_EQ(fail_expected, append_fail[node_id_2]);

  // release all threads
  for (int i = 0; i < nthreads; ++i) {
    sem2.post();
  }
  // make sure that all threads are done before quitting
  for (int i = 0; i < nthreads; ++i) {
    threads[i].join();
  }
}

TEST(StatsTest, PerClientNodeTimeSeries_UpdateAggregationTime) {
  std::chrono::seconds initial_retention_time{10};
  ClientID client_id{1};
  NodeID node_id{1};
  PerClientNodeTimeSeriesStats per_client_stats(initial_retention_time);

  // add values to create an time series for that node, to be able to check that
  // the retention time gets updated
  per_client_stats.append(client_id, node_id, 1, 1);

  // check initial retention time
  EXPECT_EQ(initial_retention_time, per_client_stats.retentionTime());
  EXPECT_EQ(initial_retention_time, per_client_stats.timeseries()->duration());

  std::chrono::seconds updated_retention_time{50};
  per_client_stats.updateRetentionTime(updated_retention_time);

  // check update retention time
  EXPECT_EQ(updated_retention_time, per_client_stats.retentionTime());
  EXPECT_EQ(updated_retention_time, per_client_stats.timeseries()->duration());
}

// if latest_timepoint - earliest_timepoint > retention_time
// caused a crash earlier, make sure to catch this case
TEST(StatsTest, PerClientNodeTimeSeries_UpdateAggregationTimeDecreaseDuration) {
  const auto initial_retention_time = std::chrono::seconds{50};
  ClientID client_id{1};
  NodeID node_id{1};
  PerClientNodeTimeSeriesStats per_client_stats(initial_retention_time);

  const auto now = std::chrono::steady_clock::now();

  per_client_stats.append(
      client_id, node_id, 1, 1, now - std::chrono::seconds{25});
  per_client_stats.append(client_id, node_id, 1, 1, now);

  // less than latest_timepoint - earliest_timepoint
  const auto updated_retention_time = std::chrono::seconds{10};
  per_client_stats.updateRetentionTime(updated_retention_time);

  // check update retention time
  EXPECT_EQ(updated_retention_time, per_client_stats.retentionTime());
  EXPECT_EQ(updated_retention_time, per_client_stats.timeseries()->duration());
}

// benchmarks for a few different stats implementations (only multithreaded
// increments are used)

DEFINE_int32(num_threads, 8, "Number of threads for benchmarks.");

static inline void stats_benchmark(int nthreads,
                                   int niters,
                                   std::function<void()> f) {
  std::vector<std::thread> threads;
  for (int i = 0; i < nthreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < niters; ++j) {
        f();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

BENCHMARK(BM_stats_thread_local, iters) {
  const int pt = iters / FLAGS_num_threads;
  StatsHolder stats(StatsParams().setIsServer(true));

  stats_benchmark(
      FLAGS_num_threads, pt, [&stats]() { ++stats.get().num_connections; });
}

BENCHMARK_RELATIVE(BM_stats_thread_cached_int, iters) {
  const int pt = iters / FLAGS_num_threads;
  folly::ThreadCachedInt<int64_t> counter;

  stats_benchmark(FLAGS_num_threads, pt, [&counter]() { ++counter; });
}

BENCHMARK_RELATIVE(BM_stats_atomic_increment, iters) {
  const int pt = iters / FLAGS_num_threads;
  std::atomic<int64_t> counter{};

  stats_benchmark(FLAGS_num_threads, pt, [&counter]() { ++counter; });
}

BENCHMARK_DRAW_LINE();

#if 0
#include <folly/experimental/symbolizer/SignalHandler.h>

int main(int argc, char *argv[]) {
  folly::symbolizer::installFatalSignalHandler();

  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  gflags::SetCommandLineOptionWithMode(
    "bm_min_iters", "100000000", gflags::SET_FLAG_IF_DEFAULT
  );
  if (FLAGS_benchmark) {
    folly::runBenchmarks();
  }
  return RUN_ALL_TESTS();
}
#endif
