/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <iostream>
#include <thread>

#include <folly/Benchmark.h>
#include <folly/Singleton.h>
#include <gflags/gflags.h>

#include "logdevice/common/debug.h"

using namespace facebook::logdevice;

// call ld_speww which is going to discard the message
// this allows measuring the impact of getModuleLogLevel
void callLoggingDiscard(int n) {
  for (int i = 0; i < n; ++i) {
    ld_spew("item #%d", i);
  }
}

BENCHMARK(LoggingNoOverrides, n) {
  callLoggingDiscard(n);
}

BENCHMARK(LoggingOverridesSame, n) {
  BENCHMARK_SUSPEND {
    dbg::setLogLevelOverrides(
        {{"DebugLoggingBenchmark.cpp", dbg::Level::DEBUG}});
  }
  callLoggingDiscard(n);
  BENCHMARK_SUSPEND {
    dbg::clearLogLevelOverrides();
  }
}

BENCHMARK(LoggingOverridesOther, n) {
  BENCHMARK_SUSPEND {
    dbg::setLogLevelOverrides({{"Other.cpp", dbg::Level::DEBUG}});
  }
  callLoggingDiscard(n);
  BENCHMARK_SUSPEND {
    dbg::clearLogLevelOverrides();
  }
}

BENCHMARK(LoggingOverridesMulti, n) {
  BENCHMARK_SUSPEND {
    dbg::LogLevelMap map;
    for (int i = 0; i < 50; ++i) {
      map[std::to_string(i) + "whatever.cpp"] = dbg::Level::DEBUG;
    }
    dbg::setLogLevelOverrides(map);
  }
  callLoggingDiscard(n);
  BENCHMARK_SUSPEND {
    dbg::clearLogLevelOverrides();
  }
}

#ifndef BENCHMARK_BUNDLE
int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
#endif
