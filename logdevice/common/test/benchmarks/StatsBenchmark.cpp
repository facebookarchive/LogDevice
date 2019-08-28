#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/ThreadCachedInt.h>
#include <folly/synchronization/Baton.h>
#include <gflags/gflags.h>

#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

DEFINE_int32(num_threads, 32, "Number of threads for benchmarks.");

namespace facebook { namespace logdevice {

struct ThreadsState {
  bool go{false};
  std::mutex mtx;
  std::condition_variable cond;
};

class MultiBaton {
 public:
  explicit MultiBaton(uint64_t callers) : callers_(callers) {}

  void post() {
    ++signals_;
    if (signals_ == callers_) {
      impl_.post();
    }
  }

  void wait() {
    impl_.wait();
    signals_ = 0;
  }

 private:
  std::atomic<uint64_t> signals_{0};
  const uint64_t callers_{0};
  folly::Baton<> impl_;
};

class Thread {
 public:
  Thread(ThreadsState& state, std::function<void()> work, MultiBaton& done)
      : state_(state), work_(std::move(work)), done_(done) {
    impl_ = std::thread([this]() {
      std::unique_lock<std::mutex> lock(state_.mtx);
      state_.cond.wait(lock, [&]() { return state_.go; });

      work_();
      done_.post();
    });
  }

  void join() {
    impl_.join();
  }

  ~Thread() {
    if (impl_.joinable()) {
      join();
    }
  }

 private:
  ThreadsState& state_;
  std::function<void()> work_;
  std::thread impl_;
  MultiBaton& done_;
};

static inline void
stats_benchmark(int nthreads,
                int niters,
                std::function<void()> thread_work,
                std::function<void()> main_work = std::function<void()>()) {
  std::vector<std::unique_ptr<Thread>> threads;
  ThreadsState state;
  MultiBaton done(nthreads);

  BENCHMARK_SUSPEND {
    for (int i = 0; i < nthreads; ++i) {
      auto work = [niters, thread_work]() {
        for (int j = 0; j < niters; ++j) {
          thread_work();
        }
      };
      threads.emplace_back(
          std::make_unique<Thread>(state, std::move(work), done));
    }

    std::unique_lock<std::mutex> lock(state.mtx);
    state.go = true;
    state.cond.notify_all();
  }

  if (main_work) {
    main_work();
  }
  done.wait();

  BENCHMARK_SUSPEND {
    threads.clear();
  }
}

// benchmarks for a few different stats implementations (only multithreaded
// increments are used)

BENCHMARK(BM_stats_thread_local, iters) {
  const int pt = iters / FLAGS_num_threads;
  CHECK_GT(pt, 0);
  StatsHolder stats(StatsParams().setIsServer(true));

  stats_benchmark(
      FLAGS_num_threads, pt, [&stats]() { ++stats.get().num_connections; });
}

BENCHMARK_RELATIVE(BM_stats_thread_cached_int, iters) {
  const int pt = iters / FLAGS_num_threads;
  CHECK_GT(pt, 0);
  folly::ThreadCachedInt<int64_t>* counter;

  stats_benchmark(FLAGS_num_threads, pt, [&counter]() { ++counter; });
}

BENCHMARK_RELATIVE(BM_stats_atomic_increment, iters) {
  const int pt = iters / FLAGS_num_threads;
  std::atomic<int64_t> counter{};

  stats_benchmark(FLAGS_num_threads, pt, [&counter]() { ++counter; });
}

BENCHMARK(BM_stats_aggregate, iters) {
  const int pt = iters / FLAGS_num_threads;
  CHECK_GT(pt, 0);
  StatsHolder stats(StatsParams().setIsServer(true));

  stats_benchmark(
      FLAGS_num_threads,
      pt,
      [&stats]() {
        ++stats.get().num_connections;
        ++stats.get().read_requests;
        ++stats.get().read_requests_to_storage;
        ++stats.get().epoch_offset_to_storage;
        ++stats.get().skipped_record_lsn_before_trim_point;
        ++stats.get().write_ops;
        ++stats.get().write_batches;
        ++stats.get().write_ops_stallable;
        ++stats.get().write_batches_stallable;
        ++stats.get().write_ops_sync_already_done;
        ++stats.get().non_blocking_reads;
        ++stats.get().non_blocking_reads_empty;
        ++stats.get().records_trimmed_removed;
        ++stats.get().metadata_log_records_trimmed_removed;
        ++stats.get().last_known_good_from_metadata_reads;
        ++stats.get().last_known_good_from_metadata_reads_to_storage;
        ++stats.get().last_known_good_from_record_reads;
        ++stats.get().last_known_good_from_record_reads_to_storage;
      },
      [&stats, pt]() {
        for (int i = 0; i < pt; ++i) {
          stats.aggregate();
        }
      });
}

}} // namespace facebook::logdevice

#ifndef BENCHMARK_BUNDLE
#include <folly/experimental/symbolizer/SignalHandler.h>

int main(int argc, char* argv[]) {
  folly::symbolizer::installFatalSignalHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  gflags::SetCommandLineOptionWithMode(
      "bm_min_iters", "100000", gflags::SET_FLAG_IF_DEFAULT);
  folly::runBenchmarks();
  return 0;
}
#endif
