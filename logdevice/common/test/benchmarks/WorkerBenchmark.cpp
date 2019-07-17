/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Benchmark.h>
#include <folly/Singleton.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/EventLoopTaskQueue.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

constexpr static int kPosts = 5;
constexpr static int kNumWorkers = 5;

class BenchmarkRequest : public Request {
 public:
  BenchmarkRequest(Processor* p,
                   std::atomic<int>& pending,
                   size_t reposts,
                   bool use_post_important = false)
      : Request(),
        processor_(p),
        pending_(pending),
        reposts_(reposts),
        use_post_important_(use_post_important) {}

  Request::Execution execute() override {
    pending_.fetch_sub(1, std::memory_order_relaxed);
    if (reposts_ == 0) {
      return Request::Execution::COMPLETE;
    }

    buffer_[reposts_ - 1] =
        std::make_unique<BenchmarkRequest>(processor_, pending_, reposts_ - 1);

    int rv = use_post_important_
        ? processor_->postImportant(buffer_[reposts_ - 1])
        : processor_->postRequest(buffer_[reposts_ - 1]);
    ld_check(!rv);
    return Request::Execution::COMPLETE;
  }

 private:
  std::array<std::unique_ptr<Request>, kPosts - 1> buffer_;
  Processor* processor_;
  std::atomic<int>& pending_;
  const size_t reposts_;
  size_t executed_{0};
  bool use_post_important_{false};
};

BENCHMARK(WorkerBenchmarkpostImportantv2, n) {
  const int request_count = n / kPosts;

  std::atomic<int> pending{kPosts * request_count};
  std::vector<std::unique_ptr<Request>> requests;
  Settings settings = create_default_settings<Settings>();
  std::shared_ptr<Processor> processor;
  StatsHolder stats{StatsParams().setIsServer(true)};

  BENCHMARK_SUSPEND {
    settings.num_workers = kNumWorkers;
    processor = make_test_processor(settings, nullptr, &stats);

    for (int i = 0; i < request_count; ++i) {
      requests.emplace_back(std::make_unique<BenchmarkRequest>(
          processor.get(), pending, kPosts - 1, true));
    }
  }

  for (auto& request : requests) {
    processor->postImportant(request);
  }

  while (pending.load(std::memory_order_relaxed) != 0) {
  }
}

BENCHMARK(WorkerBenchmarkPostRequestv2, n) {
  const int request_count = n / kPosts;

  std::atomic<int> pending{kPosts * request_count};
  std::vector<std::unique_ptr<Request>> requests;
  Settings settings = create_default_settings<Settings>();
  std::shared_ptr<Processor> processor;
  StatsHolder stats{StatsParams().setIsServer(true)};

  BENCHMARK_SUSPEND {
    settings.num_workers = kNumWorkers;
    processor = make_test_processor(settings, nullptr, &stats);

    for (int i = 0; i < request_count; ++i) {
      requests.emplace_back(std::make_unique<BenchmarkRequest>(
          processor.get(), pending, kPosts - 1));
    }
  }

  for (auto& request : requests) {
    processor->postRequest(request);
  }

  while (pending.load(std::memory_order_relaxed) != 0) {
  }
}

class RequestPumpBenchmarkRequest : public Request {
 public:
  RequestPumpBenchmarkRequest(int postAfter,
                              std::atomic<int>& counter,
                              Semaphore& sem)
      : postAfter_(postAfter), counter_(counter), sem_(sem) {}

  Execution execute() override {
    int count = counter_.fetch_add(1, std::memory_order::memory_order_relaxed);
    if (count + 1 == postAfter_) {
      sem_.post();
    }
    return Execution::COMPLETE;
  }

 private:
  int postAfter_;
  std::atomic<int>& counter_;
  Semaphore& sem_;
};

BENCHMARK(RequestPumpFunctionBenchmarkOnEventLoop, n) {
  std::condition_variable producers_sem;
  std::mutex producers_sem_mtx;
  Semaphore sem;
  std::atomic<int> executed_count{0};
  std::unique_ptr<EventLoop> loop;
  constexpr int numProducers = kNumWorkers;
  std::array<std::thread, numProducers> requestsPerProducer;
  BENCHMARK_SUSPEND {
    // Create and init consumer
    loop = std::make_unique<EventLoop>();
    loop->getTaskQueue().add([&sem]() { sem.post(); });
    sem.wait();
    executed_count = 0;

    for (int i = 0; i < requestsPerProducer.size(); ++i) {
      requestsPerProducer[i] = std::thread(
          [&](int requestCount) {
            {
              std::unique_lock<std::mutex> _(producers_sem_mtx);
              sem.post();
              producers_sem.wait(_);
            }
            for (int j = 0; j < requestCount; ++j) {
              loop->getTaskQueue().add([&sem, &executed_count, n]() {
                int count = executed_count.fetch_add(
                    1, std::memory_order::memory_order_relaxed);
                if (count + 1 == n) {
                  sem.post();
                }
              });
            }
          },
          n / numProducers + (i < (n % numProducers)));
      sem.wait();
    }
  }
  {
    std::unique_lock<std::mutex> _(producers_sem_mtx);
    producers_sem.notify_all();
  }
  sem.wait();
  BENCHMARK_SUSPEND {
    for (auto& i : requestsPerProducer) {
      i.join();
    }
    loop.reset();
  }
}

BENCHMARK_RELATIVE(RequestPumpFutureBenchmark, n) {
  std::condition_variable producers_sem;
  std::mutex producers_sem_mtx;
  Semaphore sem;
  std::atomic<int> executed_count{0};
  std::unique_ptr<EventLoop> loop;
  constexpr int numProducers = kNumWorkers;
  auto& inlineExecutor = folly::InlineExecutor::instance();
  std::array<std::thread, numProducers> producers;
  BENCHMARK_SUSPEND {
    // Create and init consumer
    loop = std::make_unique<EventLoop>();
    loop->add([&sem]() { sem.post(); });
    sem.wait();
    executed_count = 0;

    for (int i = 0; i < producers.size(); ++i) {
      producers[i] = std::thread(
          [&](int requestCount) {
            {
              std::unique_lock<std::mutex> _(producers_sem_mtx);
              sem.post();
              producers_sem.wait(_);
            }
            for (int j = 0; j < requestCount; ++j) {
              folly::Promise<folly::Unit> promise;
              promise.getSemiFuture()
                  .via(&inlineExecutor)
                  .thenValue([&sem, &executed_count, n](folly::Unit&&) {
                    int count = executed_count.fetch_add(
                        1, std::memory_order::memory_order_relaxed);
                    if (count + 1 == n) {
                      sem.post();
                    }
                  });
              loop->add([p = std::move(promise)]() mutable { p.setValue(); });
            }
          },
          n / numProducers + (i < (n % numProducers)));
      sem.wait();
    }
  }
  {
    std::unique_lock<std::mutex> _(producers_sem_mtx);
    producers_sem.notify_all();
  }
  sem.wait();
  BENCHMARK_SUSPEND {
    for (auto& p : producers) {
      p.join();
    }
    loop.reset();
  }
}

BENCHMARK_RELATIVE(RequestPumpFunctionBenchmarkOnEventBase, n) {
  std::condition_variable producers_sem;
  std::mutex producers_sem_mtx;
  Semaphore sem;
  std::atomic<int> executed_count{0};
  constexpr int numProducers = 10;
  std::array<std::thread, numProducers> requestsPerProducer;
  std::unique_ptr<folly::EventBase> loop;
  std::unique_ptr<std::thread> consumer_ptr;
  BENCHMARK_SUSPEND {
    // Create and init consumer
    loop = std::make_unique<folly::EventBase>(false);
    consumer_ptr =
        std::make_unique<std::thread>([&loop] { loop->loopForever(); });
    loop->add([&sem]() { sem.post(); });

    sem.wait();
    executed_count = 0;

    for (int i = 0; i < requestsPerProducer.size(); ++i) {
      requestsPerProducer[i] = std::thread(
          [&](int requestCount) {
            {
              std::unique_lock<std::mutex> _(producers_sem_mtx);
              sem.post();
              producers_sem.wait(_);
            }
            for (int j = 0; j < requestCount; ++j) {
              folly::Func f = [&sem, &executed_count, n]() {
                int count = executed_count.fetch_add(
                    1, std::memory_order::memory_order_relaxed);
                if (count + 1 == n) {
                  sem.post();
                }
              };
              loop->add(std::move(f));
            }
          },
          n / numProducers + (i < (n % numProducers)));
      sem.wait();
    }
  }
  {
    std::unique_lock<std::mutex> _(producers_sem_mtx);
    producers_sem.notify_all();
  }
  sem.wait();
  BENCHMARK_SUSPEND {
    for (auto& i : requestsPerProducer) {
      i.join();
    }
    loop->terminateLoopSoon();
    consumer_ptr->join();
    loop = nullptr;
  }
}

BENCHMARK(RequestPumpFunctionBenchmarkSequential, n) {
  std::unique_ptr<EventLoop> loop;
  BENCHMARK_SUSPEND {
    // Create and init consumer
    loop = std::make_unique<EventLoop>();
    folly::Promise<folly::Unit> started;
    auto wait_for_start = started.getSemiFuture();
    loop->getTaskQueue().add(
        [p = std::move(started)]() mutable { p.setValue(); });
    wait_for_start.wait();
  }
  for (int i = 0; i < n; ++i) {
    folly::Baton baton;
    loop->getTaskQueue().add([&baton] { baton.post(); });
    baton.wait();
  }
  BENCHMARK_SUSPEND {
    loop.reset();
  }
}

}} // namespace facebook::logdevice

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
