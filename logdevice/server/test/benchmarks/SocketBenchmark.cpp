/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Benchmark.h>
#include <folly/Singleton.h>

#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/TEST_Message.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

DEFINE_string(num_server_workers,
              "ncores",
              "number of worker threads for the node");
DEFINE_string(num_client_workers,
              "ncores",
              "number of worker threads for the client");
DEFINE_int32(
    max_sends_per_iteration,
    1000,
    "a cap on the number of messages to send in a single event loop iteration");

namespace facebook { namespace logdevice {

// Benchmark that sends N messages through the socket layer
BENCHMARK(SocketBenchmark, n) {
  folly::BenchmarkSuspender benchmark_suspender;

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--num-workers", FLAGS_num_server_workers.c_str())
          .create(1);
  std::unique_ptr<ClientSettings> client_settings{ClientSettings::create()};
  if (client_settings->set("num-workers", FLAGS_num_client_workers.c_str()) !=
      0) {
    ld_info("Unable to set client num-workers to %s",
            FLAGS_num_client_workers.c_str());
    exit(1);
  }
  if (client_settings->set("execute-requests", "1") != 0) {
    ld_info("Unable to set execute-requests");
    exit(1);
  }
  auto client = cluster->createClient(
      getDefaultTestTimeout(), std::move(client_settings));
  Processor* processor =
      &checked_downcast<ClientImpl*>(client.get())->getProcessor();

  // Count the number of workers
  auto worker_counter = run_on_all_workers(processor, [&]() { return 1; });
  size_t num_workers =
      std::accumulate(worker_counter.begin(), worker_counter.end(), 0);

  // Separate work among worker threads
  std::vector<size_t> per_worker_messages_to_send;
  size_t remaining_messages = n;
  size_t remaining_workers = num_workers;
  while (remaining_workers) {
    size_t wm = remaining_messages / remaining_workers;
    per_worker_messages_to_send.push_back(wm);
    remaining_messages -= wm;
    --remaining_workers;
  }
  ld_assert(remaining_messages == 0);

  // Body of the main request running on the workers. Posts itself to the
  // worker again from time to time to yield to the event loop for flushing
  Semaphore sem;
  std::function<int()> callback_fn;
  callback_fn = [&]() {
    auto w = Worker::onThisThread();
    size_t& to_send = per_worker_messages_to_send.at(w->idx_.val());
    size_t sent_in_this_iteration = 0;
    Address addr(NodeID(0));
    while (to_send) {
      auto msg = std::make_unique<TEST_Message>(TEST_Message_Header());
      int rv = w->sender().sendMessage(std::move(msg), addr);
      if (rv == 0) {
        --to_send;
        ++sent_in_this_iteration;
      }
      if (rv != 0 || sent_in_this_iteration >= FLAGS_max_sends_per_iteration) {
        // Either our send failed or we sent enough in this iteration - post
        // another request and yield to the event loop
        auto callback_copy = callback_fn;
        run_on_worker_nonblocking(processor,
                                  w->idx_,
                                  w->worker_type_,
                                  RequestType::ADMIN_CMD_UTIL_INTERNAL,
                                  std::move(callback_copy),
                                  true);
        return 0;
      }
    }
    sem.post();
    return 0;
  };

  auto start = SteadyTimestamp::now();
  benchmark_suspender.dismiss();

  run_on_all_workers(processor, callback_fn);
  for (size_t i = 0; i < num_workers; ++i) {
    sem.wait();
  }
  auto output = [&](const std::string& action) {
    auto usec = usec_since(start);
    ld_info("%s %d messages in %lu usec using %lu workers, %01.2fM/sec",
            action.c_str(),
            n,
            usec,
            num_workers,
            double(n) / usec);
  };
  output("Sent");
  wait_until("all messages received", [&]() {
    uint64_t rcvd = cluster->getNode(0).stats()["message_received.TEST"];
    ld_info("Received %lu", rcvd);
    return rcvd == n;
  });
  output("Sent and received");
}

}} // namespace facebook::logdevice

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
