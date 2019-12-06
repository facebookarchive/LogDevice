/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <signal.h>
#include <sstream>
#include <utility>

#include <folly/Singleton.h>

#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/Worker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::ldbench;

static std::atomic<bool> shutdown_requested;
static std::atomic<int> debug_info_requested{0};

static void stop_worker(int /* sig */) noexcept {
  // Kill the process on double control-C.
  std::signal(SIGUSR1, SIG_DFL);
  std::signal(SIGTERM, SIG_DFL);
  std::signal(SIGINT, SIG_DFL);

  shutdown_requested.store(true);
}

static void dump_debug_info(int /* sig */) noexcept {
  ++debug_info_requested;
}

int main(int argc, char* argv[]) {
  logdeviceInit();

  // Parse command-line options.
  auto client_settings_impl = std::make_unique<ClientSettingsImpl>();
  std::stringstream ss;
  auto exit_code = parse_commandline_options(*client_settings_impl,
                                             options,
                                             argc,
                                             const_cast<const char**&>(argv),
                                             ss);
  // updating global variable that is accessed from within LogDeviceClient, if
  // the benchmark is for LogDevice.
  client_settings = std::move(client_settings_impl);
  if (exit_code.hasValue()) {
    (exit_code.value() == 0 ? std::cout : std::cerr) << ss.str();
    return exit_code.value();
  }
  ld_check(ss.str().empty()); // don't expect output if there's no error

  // Look up benchmark worker factory by name.
  const auto& worker_factory_map = getWorkerFactoryMap();
  auto it = worker_factory_map.find(options.bench_name);
  ld_check(it != worker_factory_map.end());
  ld_check(options.write_bytes_increase_step == 0 ||
           options.write_bytes_increase_factor == 1.0);
  // Create benchmark worker.
  std::unique_ptr<Worker> worker;
  try {
    worker = (it->second.factory)();
  } catch (...) {
    ld_error("Failed to create Worker");
    return 1;
  }

  // Register signal handler. Allows benchmark worker to be stopped.
  std::signal(SIGUSR1, stop_worker);
  std::signal(SIGUSR2, dump_debug_info);
  std::signal(SIGTERM, stop_worker);
  std::signal(SIGINT, stop_worker);

  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  int rv;

  // Run benchmark worker in a separate thread.
  std::thread thread([&] {
    rv = worker->run();
    std::unique_lock<std::mutex> lock(mutex);
    done = true;
    cv.notify_all();
  });

  // Wait for the run to complete, and process signals. We can't use
  // condition_variable or Worker::requestDebugInfoDump() in signal handled
  // because they're not signal safe, so let's just poll an atomic every 10ms.
  {
    bool shutdown = false;
    int debug_info_processed = 0;
    std::unique_lock<std::mutex> lock(mutex);
    while (true) {
      cv.wait_for(lock, std::chrono::milliseconds(10), [&] {
        return done || (!shutdown && shutdown_requested.load()) ||
            debug_info_requested.load() > debug_info_processed;
      });
      if (done) {
        break;
      }
      if (!shutdown && shutdown_requested.load()) {
        worker->stop();
        shutdown = true;
      }
      int requested = debug_info_requested.load();
      while (debug_info_processed < requested) {
        worker->requestDebugInfoDump();
        ++debug_info_processed;
      }
    }
  }

  thread.join();
  return rv;
}
