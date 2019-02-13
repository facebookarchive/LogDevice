/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <queue>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/SlidingWindowSingleEpoch.h"
#include "logdevice/common/checks.h"

using namespace facebook::logdevice;

static int g_windowSize; // maximum # items in the window, must stay constant
                         // for the duration of each inidividual test

namespace {

struct Stats {
  // number of successful calls to grow() on this thread
  uint64_t n_insertions = 0;

  uint64_t n_retired = 0; // the number of calls to retire() on this thread
  uint64_t n_reaped = 0;  // the number of calls to deleter on this thread
  uint64_t n_reaped_reported = 0; // the number of reaped entries reported by
                                  // calls to retire() on this thread

  // maximum item id returned by grow() on this thread
  lsn_t right_edge_max = LSN_INVALID;

  uint64_t n_window_full = 0;  // number of times grow() found the window full
  uint64_t n_window_empty = 0; // number of times the window got empty
  uint64_t n_retire_ooo = 0;   // number of times retire() indicated that no
                               // items were reaped (out-of-order retirement)

  uint64_t n_etoobig = 0; // number of times grow() failed with E::TOOBIG

  void aggregate(const Stats& r) {
    n_insertions += r.n_insertions;
    n_retired += r.n_retired;
    n_reaped += r.n_reaped;
    n_reaped_reported += r.n_reaped_reported;
    right_edge_max = std::max(right_edge_max, r.right_edge_max);
    n_window_full += r.n_window_full;
    n_window_empty += r.n_window_empty;
    n_retire_ooo += r.n_retire_ooo;
    n_etoobig += r.n_etoobig;
  }

} __attribute__((__aligned__(64)));

// Window item.
class Item {
 public:
  explicit Item(uint32_t priority) : id_(LSN_INVALID), priority_(priority) {}

  bool operator==(const Item& other) const {
    return priority_ == other.priority_;
  }
  bool operator<(const Item& other) const {
    return priority_ < other.priority_;
  }
  bool operator<=(const Item& other) const {
    return priority_ <= other.priority_;
  }

  class Deleter {
   public:
    explicit Deleter(Stats* stats) : stats_(stats) {
      ld_check(stats);
    }

    void operator()(Item* it) {
      ASSERT_EQ(
          lsn_to_epoch(Item::Deleter::last_reaped), lsn_to_epoch(it->id_));
      ++Item::Deleter::last_reaped;
      ASSERT_EQ(Item::Deleter::last_reaped, it->id_);
      stats_->n_reaped++;
      delete it;
    }

    static void initLastReaped(lsn_t initial) {
      Item::Deleter::last_reaped = initial - 1;
    }

   private:
    Stats* stats_; // per-thread stats to update

    // id of last Item that retire() passed to a Deleter on any thread.
    // Must monotonically increase without gaps.
    static lsn_t last_reaped;
  };

  lsn_t id_;

 private:
  uint32_t priority_; // priority value that threads will use to decide
                      // which item to retire() next
} __attribute__((__aligned__(4)));

lsn_t Item::Deleter::last_reaped;

} // anonymous namespace

/**
 * Start multiple threads that will all operate on the same
 * SlidingWindow.  Each thread calls grow() a certain number of
 * times. After each grow() it may also call retire() on one of the
 * items it previously inserted and not yet retired, with some
 * probability. If grow() fails because the maximum window size is
 * reached, the thread calls retire() unconditionally. Deleter objects
 * verify that reaping is done in the strict ascending order with no
 * gaps. Some per-thread stats are maintained and logged to stdout at
 * the end of test.
 */

TEST(SlidingWindowTest, RandomRetirement) {
  const int n_threads = 24;          // number of threads to run
  const int n_attempts = 128 * 1024; // calls to grow() per thread
  const epoch_t initial_epoch(1);    // epoch where to start the window
  g_windowSize = 10001;              // maximum # items in the window

  SlidingWindowSingleEpoch<Item, Item::Deleter> window(
      initial_epoch, g_windowSize);
  std::vector<std::thread> g_threads;
  std::vector<Stats> g_stats(n_threads, Stats());

  lsn_t initial_lsn = compose_lsn(initial_epoch, ESN_MIN);
  Item::Deleter::initLastReaped(initial_lsn);

  for (int thread_id = 0; thread_id < n_threads; thread_id++) {
    g_threads.emplace_back(std::thread([=, &window, &g_stats] {
      struct drand48_data rnd;
      srand48_r(time(0), &rnd);

      // these constants indirectly control min and max number of retire()
      // calls per grow()
      const double retire_per_insertion_min = 32.0;
      const double retire_per_insertion_max = 256.0;

      // rate at which the number of retire() calls per grow() changes
      double retire_per_insertion_step = 0.0001;

      double retire_per_insertion = retire_per_insertion_min;
      Stats* stats = &g_stats[thread_id];

      // items that have not yet been retired
      std::priority_queue<Item> active;

      for (int i = 0; i < n_attempts || !active.empty(); i++) {
        double retire_points = retire_per_insertion;

        if (i < n_attempts) {
          long priority;
          lrand48_r(&rnd, &priority);
          Item* it = new Item((uint32_t)priority);
          lsn_t lsn = window.grow(it);

          if (lsn != LSN_INVALID) {
            ASSERT_GT(lsn, stats->right_edge_max);
            stats->n_insertions++;
            stats->right_edge_max = lsn;
            it->id_ = lsn;
            active.push(*it);
          } else {
            ASSERT_EQ(facebook::logdevice::E::NOBUFS, err);
            stats->n_window_full++;
            // window size limit reached. Start aggressively retiring items,
            // gradually reducing aggressiveness.
            retire_per_insertion = retire_per_insertion_max;
            if (retire_per_insertion_step > 0) {
              retire_per_insertion_step = -retire_per_insertion_step;
            }
            delete it;
          }
        } else {
          // done with inserts, retire all remaining items asap
          retire_points = g_windowSize;
        }

        for (;;) {
          long r;
          lrand48_r(&rnd, &r);

          double p = (double)r / INT_MAX;

          if (p > retire_points) { // not enough retire points left
            break;
          }

          retire_points -= p;

          if (!active.empty()) {
            const Item& retire_item = active.top();

            ld_check(retire_item.id_ != LSN_INVALID);

            stats->n_retired++;
            Item::Deleter deleter(stats);
            size_t n_reaped = window.retire(retire_item.id_, deleter);

            if (n_reaped == 0) {
              stats->n_retire_ooo++;
            } else {
              stats->n_reaped_reported += n_reaped;
            }

            active.pop();
          } else {
            // don't have any more items to retire. Drop retirement rate
            // to min and start slowly increasing it.
            retire_per_insertion = retire_per_insertion_min;
            if (retire_per_insertion_step < 0) {
              retire_per_insertion_step = -retire_per_insertion_step;
            }
          }

          if (window.size() == 0) {
            ASSERT_TRUE(active.empty());
            stats->n_window_empty++;
            break;
          }
        }

        retire_per_insertion += retire_per_insertion_step;

        if (retire_per_insertion < retire_per_insertion_min) {
          retire_per_insertion = retire_per_insertion_min;
        } else if (retire_per_insertion > retire_per_insertion_max) {
          retire_per_insertion = retire_per_insertion_max;
        }
      } // for i
    }));
  }

  for (std::thread& t : g_threads) {
    t.join();
  }

  Stats totals;

  for (const Stats& s : g_stats) {
    totals.aggregate(s);
  }

  EXPECT_EQ(n_attempts * n_threads, totals.n_insertions + totals.n_window_full);
  EXPECT_EQ(totals.n_insertions, totals.n_retired);
  EXPECT_EQ(totals.n_insertions, totals.n_reaped);
  EXPECT_EQ(totals.n_insertions, totals.n_reaped_reported);

  std::cout << "SUMMARY: inserted, retired and reaped " << totals.n_insertions
            << " items on " << n_threads << " threads." << std::endl
            << "  Window filled up " << totals.n_window_full << " time(s)"
            << std::endl
            << "  Window went empty " << totals.n_window_empty << " time(s)"
            << std::endl
            << "  The number of out-of-order retirements was "
            << totals.n_retire_ooo << std::endl;
}

/**
 * Creates a SlidingWindow with the max esn very close to ESN_MIN.
 * Start multiple threads that all operate on that SlidingWindow. Verifies
 * that grow() starts reporting E::TOOBIG in all threads after a certain
 * number of insertions. Verifies that all previously inserted items can
 * be retired and reaped.
 */

TEST(SlidingWindowTest, EsnExhaustion) {
  const int n_threads = 24;         // number of threads to run
  const int n_attempts = 10000;     // calls to grow() per thread
  const int esns_available = 15000; // number of ESNs remaining
  const esn_t initial_esn = ESN_MIN;
  const esn_t max_esn(esns_available);
  const lsn_t initial_lsn(compose_lsn(EPOCH_MIN, ESN_MIN));

  g_windowSize = 10230; // maximum # items in the window

  SlidingWindowSingleEpoch<Item, Item::Deleter> window(
      EPOCH_MIN, g_windowSize, max_esn);
  std::vector<std::thread> g_threads;
  std::vector<Stats> g_stats(n_threads, Stats());

  Item::Deleter::initLastReaped(initial_lsn);

  for (int thread_id = 0; thread_id < n_threads; thread_id++) {
    g_threads.emplace_back(std::thread([=, &window, &g_stats] {
      struct drand48_data rnd;
      srand48_r(time(0), &rnd);

      Stats* stats = &g_stats[thread_id];

      std::queue<Item> active;

      for (int i = 0; i < n_attempts || !active.empty(); i++) {
        int retire = 2;

        if (i < n_attempts) {
          Item* it = new Item(0);
          lsn_t lsn = window.grow(it);

          if (lsn != LSN_INVALID) {
            ASSERT_GT(lsn, stats->right_edge_max);
            ASSERT_LT(lsn_to_esn(lsn).val_, ESN_MAX.val_);
            ASSERT_GE(lsn_to_esn(lsn), initial_esn); // must not wrap around
            ASSERT_EQ(0, stats->n_etoobig); // no successes after E::TOOBIG
            stats->n_insertions++;
            stats->right_edge_max = lsn;
            it->id_ = lsn;
            active.push(*it);
          } else if (err == facebook::logdevice::E::NOBUFS) {
            stats->n_window_full++;
            retire++; // retire an extra item
            delete it;
          } else {
            ASSERT_EQ(facebook::logdevice::E::TOOBIG, err);
            stats->n_etoobig++;
            delete it;
          }
        } else {
          // Done with insertions. Retire all remaining items.
          retire = active.size();
        }

        // now retire some items
        for (int j = 0; j < retire && !active.empty(); j++) {
          const Item& retire_item = active.front();

          ld_check(retire_item.id_ != LSN_INVALID);

          stats->n_retired++;
          Item::Deleter deleter(stats);
          size_t n_reaped = window.retire(retire_item.id_, deleter);

          if (n_reaped == 0) {
            stats->n_retire_ooo++;
          } else {
            stats->n_reaped_reported += n_reaped;
          }

          active.pop();
        }
      }
    }));
  }

  for (std::thread& t : g_threads) {
    t.join();
  }

  Stats totals;

  for (const Stats& s : g_stats) {
    totals.aggregate(s);
  }

  EXPECT_EQ(n_attempts * n_threads,
            totals.n_insertions + totals.n_window_full + totals.n_etoobig);
  EXPECT_EQ(totals.n_insertions, totals.n_retired);
  EXPECT_EQ(totals.n_insertions, totals.n_reaped);
  EXPECT_EQ(esns_available, lsn_to_esn(totals.right_edge_max).val_);

  EXPECT_EQ(esns_available, totals.n_insertions);

  std::cout << "SUMMARY: inserted, retired and reaped " << totals.n_insertions
            << " items on " << n_threads << " threads." << std::endl
            << "  Window filled up " << totals.n_window_full << " time(s)"
            << std::endl
            << "  Window went empty " << totals.n_window_empty << " time(s)"
            << std::endl
            << "  The number of out-of-order retirements was "
            << totals.n_retire_ooo << std::endl
            << "  The number of insertions failed with E::TOOBIG was "
            << totals.n_etoobig << std::endl
            << "  Highest ESN assigned was "
            << lsn_to_esn(totals.right_edge_max).val_ << std::endl;
}

TEST(SlidingWindowTest, ConditionalInsert) {
  Stats stats;
  SlidingWindowSingleEpoch<Item, Item::Deleter> window(EPOCH_MIN, 1024);
  Item::Deleter::initLastReaped(compose_lsn(EPOCH_MIN, ESN_MIN));
  Item* it = new Item(0);
  lsn_t lsn = window.grow(it, LSN_INVALID);
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::COND_WRITE_NOT_READY, err);
  // the same for other lsns
  lsn = window.grow(it, compose_lsn(EPOCH_MIN, esn_t(1)));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::COND_WRITE_NOT_READY, err);
  // set the prev epoch tail but with wrong epoch
  int rv = window.set_prev_tail(compose_lsn(EPOCH_MIN, esn_t(1)));
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);
  // set the prev epoch tail in the right way
  rv = window.set_prev_tail(LSN_INVALID);
  ASSERT_EQ(0, rv);
  // conditional append for the wrong lsn
  lsn = window.grow(it, compose_lsn(epoch_t(0), esn_t(1)));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::COND_WRITE_FAILED, err);
  // conditional append for the right lsn
  lsn = window.grow(it, LSN_INVALID);
  ASSERT_EQ(compose_lsn(EPOCH_MIN, ESN_MIN), lsn);
  it->id_ = lsn;

  it = new Item(0);
  // perform a conditional insert with a wrong prev_lsn
  lsn = window.grow(it, compose_lsn(EPOCH_MIN, esn_t(2)));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::COND_WRITE_FAILED, err);
  // perform a conditional insert with the right prev_lsn
  lsn = window.grow(it, compose_lsn(EPOCH_MIN, esn_t(1)));
  ASSERT_EQ(compose_lsn(EPOCH_MIN, esn_t(2)), lsn);
  it->id_ = lsn;

  Item::Deleter deleter(&stats);
  size_t n_reaped = window.retire(compose_lsn(EPOCH_MIN, esn_t(1)), deleter);
  ASSERT_EQ(1, n_reaped);
  n_reaped = window.retire(compose_lsn(EPOCH_MIN, esn_t(2)), deleter);
  ASSERT_EQ(1, n_reaped);
}

// the window can perform a conditonal insert after successful unconditonal
// insert, even if the prev lsn remains unset
TEST(SlidingWindowTest, ConditionalInsert2) {
  Stats stats;
  SlidingWindowSingleEpoch<Item, Item::Deleter> window(epoch_t(5), 1024);
  Item::Deleter::initLastReaped(compose_lsn(epoch_t(5), ESN_MIN));
  Item* it = new Item(0);
  lsn_t lsn = window.grow(it, compose_lsn(epoch_t(4), esn_t(9)));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::COND_WRITE_NOT_READY, err);

  // perform an unconditional insert, should be successful
  lsn = window.grow(it);
  ASSERT_EQ(compose_lsn(epoch_t(5), ESN_MIN), lsn);
  it->id_ = lsn;

  it = new Item(0);
  // perform a conditional insert with a wrong prev_lsn
  lsn = window.grow(it, compose_lsn(epoch_t(5), esn_t(2)));
  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::COND_WRITE_FAILED, err);

  // perform a conditional insert with the right prev_lsn
  lsn = window.grow(it, compose_lsn(epoch_t(5), esn_t(1)));
  ASSERT_EQ(compose_lsn(epoch_t(5), esn_t(2)), lsn);
  it->id_ = lsn;

  Item::Deleter deleter(&stats);
  size_t n_reaped = window.retire(compose_lsn(epoch_t(5), esn_t(2)), deleter);
  ASSERT_EQ(0, n_reaped);
  n_reaped = window.retire(compose_lsn(epoch_t(5), esn_t(1)), deleter);
  ASSERT_EQ(2, n_reaped);
}
