#include <folly/Benchmark.h>
#include <gflags/gflags.h>
#include <folly/Singleton.h>

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
