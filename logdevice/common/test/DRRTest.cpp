/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/DRRScheduler.h"
#include "logdevice/common/types_internal.h"

using namespace facebook::logdevice;

class DRRIntegrationTest : public ::testing::Test {};

// Globals
const uint64_t KB = 1024;
const uint64_t MB = 1024 * 1024;
std::atomic_int taskId{0};
std::atomic_bool stopTest{false};

class TestTask {
 public:
  explicit TestTask(uint64_t size) {
    reqSize(size);
  }

  uint64_t reqSize() {
    return size_;
  }

  void reqSize(uint64_t size) {
    size_ = size;
  }

  ~TestTask() {}

  int principal_;
  uint64_t size_;
  uint64_t id_;
  folly::IntrusiveListHook schedulerQHook_;
};

class TestParams {
 public:
  // Default test settings
  TestParams()
      : numThreads(4),
        numIOPrincipals(10),
        maxOutstanding(1000),
        maxEnqueues(1000000),
        statDuration(2),
        sleepMax(0),
        sleepMin(4),
        quanta(0),
        isFixedReqSize(true),
        maxReqSize(0) {}

  int numThreads;
  int numIOPrincipals = 10; // E.g., rebuild, user-reads, etc.
  int maxOutstanding;
  int maxEnqueues;
  int statDuration; // print stats every x secs
  int sleepMax;
  int sleepMin;
  int quanta;
  bool isFixedReqSize;
  int maxReqSize;
  DRRScheduler<TestTask, &TestTask::schedulerQHook_>* q;
};

void printStatsThread(TestParams params) {
  DRRStatsSnapshot stats;
  int numBadStats = 0;
  while (!stopTest) {
    sleep(params.statDuration);
    params.q->getAndResetStats(stats);

    // The normalized value of all the stats should be the same.
    double duration = stats.duration.count();
    ASSERT_GT(duration, 0);
    double normBytesPerSec = ((double)stats.perfStats[0].bytesProcessed) /
        stats.perfStats[0].principal.share / duration;

    bool badStats = false;
    for (const auto& stat : stats.perfStats) {
      double other =
          ((double)stat.bytesProcessed) / stat.principal.share / duration;
      if ((normBytesPerSec > other * 1.1) || (normBytesPerSec < other * 0.9)) {
        badStats = true;
      }
    }
    if (badStats) {
      numBadStats++;
    }
    ld_info("%s", stats.toString().c_str());
    // The stats could temporarily mismatch when the shares are reset.
    // But no more than 1 bad stat iterations.
    ASSERT_LT(badStats, 2);
  }
}

void workerThread(TestParams params) {
  int maxOutstanding = params.maxOutstanding;
  int maxEnqueues = params.maxEnqueues;
  int numIOPrincipals = params.numIOPrincipals;

  // First, just enqueue everything
  while (maxOutstanding) {
    int principal = folly::Random::rand32() % numIOPrincipals;
    uint64_t size = params.maxReqSize;
    assert(size);
    if (!params.isFixedReqSize) {
      // Produce a bias in request size so that Principals with larger
      // shares have a smaller request on the average. This will produce
      // the effect that we need to process a larger number of requests
      // -per-second for principals with higher shares. Then we can clearly
      // see that for the byte-based hcheduling the normalized
      // bytes-per-sec is the same across principals even when the normalized
      // reqs-per-sec are different.
      size = folly::Random::rand32() % (size - (numIOPrincipals * 40 * KB));
      size += ((numIOPrincipals - principal) * 30 * KB);
    }
    TestTask* task = new TestTask(size);
    task->principal_ = principal;
    params.q->enqueue(task, principal);
    maxOutstanding--;
  }

  // Then dequeue and enqueue in a loop.
  // Also avoid having to do malloc/free.
  while (maxEnqueues) {
    TestTask* task = params.q->blockingDequeue();
    // Sleep to simulate IO delay
    if (params.sleepMax) {
      usleep((params.sleepMin +
              (folly::Random::rand32() % (params.sleepMax - params.sleepMin))) *
             1000);
    }

    // Completed task can queue one more of its type.
    int principal = task->principal_;
    params.q->enqueue(task, principal);
    maxEnqueues--;
  }

  std::cout << "Worker thread exiting" << std::endl;
}

void printParams(TestParams& params) {
  std::cout << "==========================================" << std::endl;
  std::cout << "==========================================" << std::endl;
  std::cout << "quanta: " << params.quanta << std::endl;
  std::cout << "numThreads: " << params.numThreads << std::endl;
  std::cout << "isFixedReqSize: " << params.isFixedReqSize << std::endl;
  std::cout << "maxReqSize: " << params.maxReqSize << std::endl;
  std::cout << "maxEnqueues: " << params.maxEnqueues << std::endl;
  std::cout << "maxOutstanding: " << params.maxOutstanding << std::endl;
  std::cout << "statDuration: " << params.statDuration << std::endl;
  std::cout << "sleepMax: " << params.sleepMax << std::endl;
  std::cout << "sleepMin: " << params.sleepMin << std::endl;
  std::cout << "==========================================" << std::endl;
  std::cout << "==========================================" << std::endl;
}

// Test DRR share allocation
TEST_F(DRRIntegrationTest, RequestBasedFixedSize) {
  TestParams params;
  params.quanta = 1;
  params.isFixedReqSize = true;
  params.maxReqSize = 1;
  stopTest = false;

  // Build the shares
  std::vector<DRRPrincipal> shares;
  for (int i = 0; i < params.numIOPrincipals; i++) {
    DRRPrincipal principal;
    principal.name = "DRR-principalt-" + std::to_string(i);
    principal.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
    shares.push_back(principal);
  }

  printParams(params);

  DRRScheduler<TestTask, &TestTask::schedulerQHook_> ioq;
  ioq.initShares("unit-test-q", params.quanta, shares);

  params.q = &ioq;

  // Start the stats thread
  std::thread statsThread(printStatsThread, params);

  // Kick off the workers
  std::vector<std::thread> workers;
  for (int i = 0; i < params.numThreads; i++) {
    workers.push_back(std::thread(workerThread, params));
  }

  for (int i = 0; i < params.numThreads; i++) {
    workers[i].join();
  }

  stopTest = true;
  statsThread.join();

  // Drain the queues
  TestTask* task;
  while ((task = params.q->dequeue()))
    ;
}

// Test DRR share allocation
TEST_F(DRRIntegrationTest, RequestBasedFixedSizeResetShares) {
  TestParams params;
  // Bump maxEnqueues for this test
  params.maxEnqueues = 3000000;
  params.quanta = 1;
  params.isFixedReqSize = true;
  params.maxReqSize = 1;
  stopTest = false;

  // Build the shares
  std::vector<DRRPrincipal> shares;
  for (int i = 0; i < params.numIOPrincipals; i++) {
    DRRPrincipal principal;
    principal.name = "DRR-principalt-" + std::to_string(i);
    principal.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
    shares.push_back(principal);
  }

  printParams(params);

  DRRScheduler<TestTask, &TestTask::schedulerQHook_> ioq;
  ioq.initShares("unit-test-q", params.quanta, shares);

  params.q = &ioq;

  // Start the stats thread
  std::thread statsThread(printStatsThread, params);

  // Kick off the workers
  std::vector<std::thread> workers;
  for (int i = 0; i < params.numThreads; i++) {
    workers.push_back(std::thread(workerThread, params));
  }

  // Wait a little and then reset the shares
  sleep(2 * params.statDuration);
  for (auto& p : shares) {
    p.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
  }
  ioq.setShares(params.quanta, shares);

  for (int i = 0; i < params.numThreads; i++) {
    workers[i].join();
  }

  stopTest = true;
  statsThread.join();

  // Drain the queues
  TestTask* task;
  while ((task = params.q->dequeue()))
    ;
}

TEST_F(DRRIntegrationTest, ByteBasedFixedSize) {
  TestParams params;
  params.quanta = 1 * MB;
  params.isFixedReqSize = true;
  params.maxReqSize = 1 * MB;
  stopTest = false;

  // Build the shares
  std::vector<DRRPrincipal> shares;
  for (int i = 0; i < params.numIOPrincipals; i++) {
    DRRPrincipal principal;
    principal.name = "DRR-principalt-" + std::to_string(i);
    principal.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
    shares.push_back(principal);
  }

  printParams(params);

  DRRScheduler<TestTask, &TestTask::schedulerQHook_> ioq;
  ioq.initShares("unit-test-q", params.quanta, shares);

  params.q = &ioq;

  // Start the stats thread
  std::thread statsThread(printStatsThread, params);

  // Kick off the workers
  std::vector<std::thread> workers;
  for (int i = 0; i < params.numThreads; i++) {
    workers.push_back(std::thread(workerThread, params));
  }

  // Then wait
  for (int i = 0; i < params.numThreads; i++) {
    workers[i].join();
  }

  stopTest = true;
  statsThread.join();

  // Drain the queues
  TestTask* task;
  while ((task = params.q->dequeue()))
    ;
}

TEST_F(DRRIntegrationTest, ByteBasedVariableSize) {
  TestParams params;
  params.quanta = 1 * MB;
  params.isFixedReqSize = false;
  params.maxReqSize = 1 * MB;
  stopTest = false;

  // Build the shares
  std::vector<DRRPrincipal> shares;
  for (int i = 0; i < params.numIOPrincipals; i++) {
    DRRPrincipal principal;
    principal.name = "DRR-principalt-" + std::to_string(i);
    principal.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
    shares.push_back(principal);
  }

  printParams(params);

  DRRScheduler<TestTask, &TestTask::schedulerQHook_> ioq;
  ioq.initShares("unit-test-q", params.quanta, shares);

  params.q = &ioq;

  // Start the stats thread
  std::thread statsThread(printStatsThread, params);

  // Kick off the workers
  std::vector<std::thread> workers;
  for (int i = 0; i < params.numThreads; i++) {
    workers.push_back(std::thread(workerThread, params));
  }

  // Then wait
  for (int i = 0; i < params.numThreads; i++) {
    workers[i].join();
  }

  stopTest = true;
  statsThread.join();

  // Drain the queues
  TestTask* task;
  while ((task = params.q->dequeue()))
    ;
}

TEST_F(DRRIntegrationTest, ByteBasedSmallQuantaFixesSize) {
  TestParams params;
  params.quanta = 1 * MB;
  params.isFixedReqSize = true;
  params.maxReqSize = 4 * MB;
  stopTest = false;

  // Build the shares
  std::vector<DRRPrincipal> shares;
  for (int i = 0; i < params.numIOPrincipals; i++) {
    DRRPrincipal principal;
    principal.name = "DRR-principalt-" + std::to_string(i);
    principal.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
    shares.push_back(principal);
  }

  printParams(params);

  DRRScheduler<TestTask, &TestTask::schedulerQHook_> ioq;
  ioq.initShares("unit-test-q", params.quanta, shares);

  params.q = &ioq;

  // Start the stats thread
  std::thread statsThread(printStatsThread, params);

  // Kick off the workers
  std::vector<std::thread> workers;
  for (int i = 0; i < params.numThreads; i++) {
    workers.push_back(std::thread(workerThread, params));
  }

  // Then wait
  for (int i = 0; i < params.numThreads; i++) {
    workers[i].join();
  }

  stopTest = true;
  statsThread.join();

  // Drain the queues
  TestTask* task;
  while ((task = params.q->dequeue()))
    ;
}

TEST_F(DRRIntegrationTest, ByteBasedSmallQuantaVariableSize) {
  TestParams params;
  params.quanta = 1 * MB;
  params.isFixedReqSize = false;
  params.maxReqSize = 4 * MB;
  stopTest = false;

  // Build the shares
  std::vector<DRRPrincipal> shares;
  for (int i = 0; i < params.numIOPrincipals; i++) {
    DRRPrincipal principal;
    principal.name = "DRR-principalt-" + std::to_string(i);
    principal.share = 1 + folly::Random::rand32() % params.numIOPrincipals;
    shares.push_back(principal);
  }

  printParams(params);

  DRRScheduler<TestTask, &TestTask::schedulerQHook_> ioq;
  ioq.initShares("unit-test-q", params.quanta, shares);

  params.q = &ioq;

  // Start the stats thread
  std::thread statsThread(printStatsThread, params);

  // Kick off the workers
  std::vector<std::thread> workers;
  for (int i = 0; i < params.numThreads; i++) {
    workers.push_back(std::thread(workerThread, params));
  }

  // Then wait
  for (int i = 0; i < params.numThreads; i++) {
    workers[i].join();
  }

  stopTest = true;
  statsThread.join();

  // Drain the queues
  TestTask* task;
  while ((task = params.q->dequeue()))
    ;
}
