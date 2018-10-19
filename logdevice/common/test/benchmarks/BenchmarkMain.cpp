#include <folly/Benchmark.h>
#include <folly/Singleton.h>
#include <gflags/gflags.h>

int main(int argc, char** argv) {
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
