/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Benchmark.h>
#include <folly/Singleton.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Timestamp.h"
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

} // namespace

// Benchmark posting requests n times via the whole processor pipeline
BENCHMARK(WorkerBenchmark, n) {
  std::shared_ptr<Processor> processor;
  std::vector<std::unique_ptr<Request>> requests;
  Semaphore sem;
  const size_t request_count = 10000;

  BENCHMARK_SUSPEND {
    Settings settings = create_default_settings<Settings>();
    settings.num_workers *= 2;
    processor = make_test_processor(settings);

    size_t local_N = n;
    size_t local_request_count = request_count;
    while (local_request_count > 0) {
      size_t executions = local_N / local_request_count;
      if (executions != 0) {
        requests.emplace_back(
            new BenchmarkRequest(processor.get(), sem, executions));
        local_N -= executions;
      }
      --local_request_count;
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
  ld_info("Completed %u iterations with %lu requests in %lu usec",
          n,
          requests.size(),
          usec_since(start));
}

}} // namespace facebook::logdevice

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
