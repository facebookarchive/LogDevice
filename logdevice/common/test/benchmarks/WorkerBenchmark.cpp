/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Benchmark.h>
#include <folly/Singleton.h>

#include "logdevice/common/EventLoopHandle.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

namespace {
class BenchmarkRequest : public Request {
 public:
  BenchmarkRequest(Processor* p, Semaphore& sem, size_t max_executions)
      : Request(), processor_(p), sem_(sem), limit_(max_executions) {}
  Request::Execution execute() override {
    if (++executed_ == limit_) {
      // done
      sem_.post();
      return Request::Execution::COMPLETE;
    }
    // post this same request again - with a new ID
    const_cast<request_id_t&>(id_) = Request::getNextRequestID();
    std::unique_ptr<Request> rq(this);
    int rv = processor_->postImportant(rq);
    ld_assert_eq(0, rv);
    return Request::Execution::CONTINUE;
  }

 private:
  Processor* processor_;
  Semaphore& sem_;
  const size_t limit_;
  size_t executed_{0};
};

class EmptyRequest : public Request {
 public:
  EmptyRequest() {}
  Request::Execution execute() override {
    return Request::Execution::COMPLETE;
  }
};
} // namespace

// Benchmark posting requests n times via the whole processor pipeline
BENCHMARK(WorkerBenchmark) {
  std::shared_ptr<Processor> processor;
  std::vector<std::unique_ptr<Request>> requests;
  Semaphore sem;
  size_t request_count = 4000;
  const size_t repost_request_count = 5;

  BENCHMARK_SUSPEND {
    Settings settings = create_default_settings<Settings>();
    settings.num_workers *= 2;
    processor = make_test_processor(settings);

    while (request_count > 0) {
      requests.emplace_back(
          new BenchmarkRequest(processor.get(), sem, repost_request_count));
      --request_count;
    }
  }
  auto start = SteadyTimestamp::now();
  for (auto& request : requests) {
    int rv = processor->postImportant(request);
    ld_assert_eq(0, rv);
  }
  for (size_t i = 0; i < requests.size(); ++i) {
    sem.wait();
    if (i == 0) {
      ld_info("First semaphore fired");
    } else if (i == requests.size() - 1) {
      ld_info("Last semaphore fired");
    }
  }
  ld_info("Completed iteration with %lu requests in %lu usec",
          requests.size(),
          usec_since(start));
}

BENCHMARK(RequestPumpPost) {
  size_t request_count = 30000;
  std::unique_ptr<EventLoop> ev_loop;
  std::shared_ptr<RequestPump> request_pump;
  std::vector<std::unique_ptr<Request>> requests;
  BENCHMARK_SUSPEND {
    ev_loop.reset(new EventLoop());
    Settings settings = create_default_settings<Settings>();
    request_pump =
        std::make_shared<RequestPump>(ev_loop->getEventBase(),
                                      settings.worker_request_pipe_capacity,
                                      settings.requests_per_iteration);
    for (int i = 0; i < request_count; ++i) {
      requests.emplace_back(new EmptyRequest());
    }
  }

  for (size_t i = 0; i < request_count; ++i) {
    int rv = request_pump->forcePost(requests[i]);
    ld_assert_eq(0, rv);
  }
}

BENCHMARK(RequestPumpFunctionPost) {
  size_t request_count = 30000;
  std::unique_ptr<EventLoop> ev_loop(new EventLoop());
  std::shared_ptr<RequestPump> request_pump;
  BENCHMARK_SUSPEND {
    Settings settings = create_default_settings<Settings>();
    request_pump =
        std::make_shared<RequestPump>(ev_loop->getEventBase(),
                                      settings.worker_request_pipe_capacity,
                                      settings.requests_per_iteration);
  }

  for (auto i = 0; i < request_count; ++i) {
    Func func = []() {};
    int rv = request_pump->add(std::move(func));
    ld_assert_eq(0, rv);
  }
}

// Execute tasks is private in EventLoopTaskQueue
BENCHMARK(RequestPumpPop) {
  size_t request_count = 30000;
  std::unique_ptr<EventLoop> ev_loop;
  std::shared_ptr<RequestPump> request_pump;
  Semaphore sem_start, sem_end;
  BENCHMARK_SUSPEND {
    ev_loop = std::make_unique<EventLoop>();
    request_pump =
        std::make_shared<RequestPump>(ev_loop->getEventBase(), 1000, 16);
    ev_loop->setRequestPump(request_pump);
    ev_loop->start();

    // Make sure the thread is started.
    std::unique_ptr<Request> first_request = std::make_unique<FuncRequest>(
        worker_id_t(0), WorkerType::GENERAL, RequestType::MISC, [&sem_start]() {
          sem_start.post();
        });
    int rv = request_pump->forcePost(first_request);
    ld_assert_eq(0, rv);
    sem_start.wait();
    // Wait for benchmark to start.
    first_request = std::make_unique<FuncRequest>(
        worker_id_t(0), WorkerType::GENERAL, RequestType::MISC, [&sem_start]() {
          sem_start.wait();
        });
    for (size_t i = 0; i < request_count; ++i) {
      std::unique_ptr<Request> req(new EmptyRequest());
      rv = request_pump->forcePost(req);
      ld_assert_eq(0, rv);
    }

    // Wait till the final request runs to completion.
    std::unique_ptr<Request> last_request = std::make_unique<FuncRequest>(
        worker_id_t(0), WorkerType::GENERAL, RequestType::MISC, [&sem_end]() {
          sem_end.post();
        });
    rv = request_pump->forcePost(last_request);
    ld_assert_eq(0, rv);
  }
  sem_start.post();
  sem_end.wait();
  BENCHMARK_SUSPEND {
    ev_loop.reset();
  }
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
