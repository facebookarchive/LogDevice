/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <memory>
#include <signal.h>

#include <gtest/gtest.h>

#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Err.h"

using namespace facebook::logdevice;

// Kill test process after this many seconds
const std::chrono::seconds TEST_TIMEOUT(10);

namespace {

struct CountingRequestTestState {
  Semaphore sem; // signalled after every batch of n_requests_total requests
  std::atomic<int> n_requests_executed{0};
  int n_requests_total;
};

struct ConditionRequestTestState {
  std::condition_variable cv;
  std::mutex cv_mutex;
  std::atomic<bool> requests_can_be_executed{false};

  void unblockRequests() {
    std::lock_guard<std::mutex> guard(cv_mutex);
    requests_can_be_executed.store(true);
    cv.notify_all();
  }
};

// a test request whose execute() method counts the number of times it
// was called
struct CountingRequest : public Request {
  explicit CountingRequest(CountingRequestTestState* state, int thread_affinity)
      : Request(RequestType::TEST_REQUEST_QUEUE_COUNTING_REQUEST),
        state_(state),
        thread_affinity_(thread_affinity) {}

  Execution execute() override {
    if (++state_->n_requests_executed == state_->n_requests_total) {
      state_->sem.post();
    }
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return thread_affinity_;
  }

  CountingRequestTestState* state_;
  int thread_affinity_;
};

// a test request whose execute() wait for semaphore to finish
struct ConditionRequest : public Request {
  explicit ConditionRequest(ConditionRequestTestState* state,
                            int thread_affinity)
      : Request(RequestType::TEST_REQUEST_QUEUE_CONDITION_REQUEST),
        state_(state),
        thread_affinity_(thread_affinity) {}

  Execution execute() override {
    std::unique_lock<std::mutex> crit(state_->cv_mutex);
    state_->cv.wait(
        crit, [this] { return state_->requests_can_be_executed.load(); });
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return thread_affinity_;
  }

  ConditionRequestTestState* state_;
  int thread_affinity_;
};

struct SimpleRequest : public Request {
  explicit SimpleRequest(int thread_affinity)
      : Request(RequestType::TEST_REQUEST_QUEUE_SIMPLE_REQUEST),
        thread_affinity_(thread_affinity) {}

  Execution execute() override {
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return thread_affinity_;
  }

  int thread_affinity_;
};

class MessagingPostImportantTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dbg::assertOnData = true;

    // In order for writes to closed pipes to return EPIPE (instead of bringing
    // down the process), which we rely on to detect shutdown, ignore SIGPIPE.
    struct sigaction sa;
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPIPE, &sa, &oldact);
  }

  void TearDown() override {
    sigaction(SIGPIPE, &oldact, nullptr);
  }

 private:
  struct sigaction oldact {};
  Alarm alarm_{TEST_TIMEOUT};
};
} // namespace

// 1) For all workers, post one conditional request so they will wait for
//    conditional variable signal to be executed.
// 2) Post simple requests to all workers until pipe is overloaded.
// 3) Post counting requests to all workers using postImportant(). They will
//    be queued.
// 4) Signal all workers conditional request to finish. Wait and check that all
//    of workers requests have finished.
TEST_F(MessagingPostImportantTest, Overflow) {
  Settings settings = create_default_settings<Settings>();
  settings.worker_request_pipe_capacity = 500;

  int num_overload_requests = 100;
  auto processor = make_test_processor(settings);
  int num_workers = processor->getWorkerCount(WorkerType::GENERAL);

  std::vector<CountingRequestTestState> counting_states(num_workers);
  ConditionRequestTestState cond_state;
  for (int i = 0; i < num_workers; ++i) {
    counting_states[i].n_requests_total = num_overload_requests;
  }

  for (int i = 0; i < num_workers; ++i) {
    std::unique_ptr<Request> rq =
        std::make_unique<ConditionRequest>(&cond_state, i);
    int rv = processor->postRequest(rq);
    ASSERT_EQ(0, rv);
    while (rv == 0) {
      std::unique_ptr<Request> req = std::make_unique<SimpleRequest>(i);
      rv = processor->postRequest(req);
    }
  }

  for (int j = 0; j < num_overload_requests; j++) {
    for (int worker = 0; worker < num_workers; worker++) {
      std::unique_ptr<Request> rq =
          std::make_unique<CountingRequest>(&counting_states[worker], worker);
      int rv = processor->postImportant(rq);
      ASSERT_EQ(0, rv);
    }
  }

  cond_state.unblockRequests();

  for (int i = 0; i < num_workers; i++) {
    counting_states[i].sem.wait();
    ASSERT_EQ(
        num_overload_requests, counting_states[i].n_requests_executed.load());
  }
}

// Overflow pipe with requests, shut down the Processor.
// Expect Processor to reject enqueuing new requests, important or not.
TEST_F(MessagingPostImportantTest, Shutdown) {
  Settings settings = create_default_settings<Settings>();
  settings.worker_request_pipe_capacity = 500;
  int num_overload_requests = 100;
  std::shared_ptr<Processor> processor = make_test_processor(settings);
  int num_workers = processor->getWorkerCount(WorkerType::GENERAL);

  std::vector<ConditionRequestTestState> cond_states(num_workers);

  for (int i = 0; i < num_workers; ++i) {
    std::unique_ptr<Request> rq =
        std::make_unique<ConditionRequest>(&cond_states[i], i);
    int rv = processor->postRequest(rq);
    ASSERT_EQ(0, rv);
    while (rv == 0) {
      std::unique_ptr<Request> req = std::make_unique<SimpleRequest>(i);
      rv = processor->postRequest(req);
    }
  }

  for (int i = 0; i < num_workers; i++) {
    cond_states[i].unblockRequests();
  }

  processor->shutdown();

  for (int j = 0; j < num_overload_requests; j++) {
    for (int worker = 0; worker < num_workers; worker++) {
      std::unique_ptr<Request> rq = std::make_unique<SimpleRequest>(worker);
      int rv = processor->postImportant(rq);
      EXPECT_EQ(-1, rv);
      EXPECT_EQ(E::SHUTDOWN, err);
    }
  }
}
