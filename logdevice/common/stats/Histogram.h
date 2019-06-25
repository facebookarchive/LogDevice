/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <folly/Range.h>

namespace folly {
template <typename>
class Histogram;
}

namespace facebook { namespace logdevice {

/**
 * @file  Multiple kinds of histograms used for stats.
 *
 * HistogramInterface is a common interface for the histograms, allowing to
 * add values, merge/subtract histograms and get percentiles.
 * Two implementations of this interface are MultiScaleHistogram and
 * CompactHistogram; those define how the histogram actually works.
 *
 * MultiScaleHistogram is an older, fancier and heavyweight implementation
 * with round bucket boundaries and more precise percentiles.
 *
 * CompactHistogram is a simpler implementation that minimizes size of the
 * data structure. Use it when you have a lot of histograms or when you want
 * fewer buckets. Main caveat is that it's sometimes not responsive to small
 * changes in values, see comment starting with "IMPORTANT" below.
 *
 * Each of the two implementations has multiple subclasses for different units
 * of measurement. They define how the histograms are presented
 * (e.g. "1h" instead of "3600000000") and, for MultiScaleHistogram, what
 * the block boundaries are.
 */

class HistogramInterface {
 public:
  HistogramInterface() = default;
  HistogramInterface(HistogramInterface&) = delete;
  HistogramInterface& operator=(const HistogramInterface&) = delete;
  virtual ~HistogramInterface() = default;

  /**
   * Remove all data points from this histogram.
   *
   * Thread-safe.
   */
  virtual void clear() = 0;

  /**
   * Add a new value to the histogram.
   *
   * Thread-safe with respect to concurrent calls to other thread-safe
   * functions. Not thread safe with respect to concurrent calls to add();
   * i.e., only one thread at a time may call add().
   */
  virtual void add(int64_t value) = 0;

  /**
   * Copy another histogram into this one.
   * Using an explicit method instead of operator=() to emphasize that
   * `other` and `this` must be the same subclass of HistogramInterface.
   */
  virtual void assign(const HistogramInterface& other) = 0;

  /**
   * Merge another histogram into this histogram.
   * `other` must have the same type as `this`.
   *
   * Thread-safe.
   */
  virtual void merge(const HistogramInterface& other) = 0;

  /**
   * Subtracts another histogram from this histogram.
   * `other` must have the same type as `this`.
   *
   * Thread-safe.
   */
  virtual void subtract(const HistogramInterface& other) = 0;

  /**
   * Batched version of estimatePercentile()+getCountAndSum().
   * More efficient than multiple equivalent calls to estimatePercentile().
   *
   * @param percentiles   Array of input percentiles. Must be sorted. Caller
   *                      retains ownership of memory.
   * @param npercentiles  Number of input percentiles. Length of array pct.
   * @param samples_out   Array of estimated output samples, aligned with pct.
   *                      Array must have length of at least npct. Caller
   *                      retains ownership of memory.
   * @param count_out     If not null, total number of values in the histogram
   *                      is assigned here.
   * @param sum_out       If not null, approximate sum of values in the
   *                      histogram is assigned here.
   */
  virtual void estimatePercentiles(const double* percentiles,
                                   size_t npercentiles,
                                   int64_t* samples_out,
                                   uint64_t* count_out = nullptr,
                                   int64_t* sum_out = nullptr) const = 0;

  /**
   * Get total number and sum of values in histogram.
   *
   * Thread-safe.
   */
  virtual std::pair<uint64_t /* count */, int64_t /* sum */>
  getCountAndSum() const {
    uint64_t count;
    int64_t sum;
    estimatePercentiles(nullptr, 0, nullptr, &count, &sum);
    return std::make_pair(count, sum);
  }

  /**
   * Computes a sample value at the given percentile (must be between 0 and 1).
   * Because we don't keep individual samples but only counts in buckets,
   * we'll know the right bucket but make a linear estimate within it.
   * If histogram is empty, returns 0.
   *
   * Thread-safe.
   *
   * NOTE: This is a fairly expensive function. Prefer estimatePercentiles() to
   *       estimate sample values for a whole batch of percentiles.
   */
  virtual int64_t estimatePercentile(double percentile) const {
    int64_t sample;
    estimatePercentiles(&percentile, 1, &sample);
    return sample;
  }

  /**
   * Print this histogram into _out_. Empty buckets are
   * skipped. Non-empty buckets are printed one per line in ascending
   * order of their min values.  For each non-empty bucket min and max
   * value in appropriate units, and that bucket's count is
   * printed. Buckets that P50, P75, P95, and P99 latencies fall into
   * are labelled.
   *
   * Thread-safe.
   */
  virtual void print(std::ostream& out) const = 0;

  // Returns the unit of measurement, e.g. "B" for bytes, "us" for microseconds.
  virtual std::string getUnitName() const = 0;

  // Convert a value to a pretty string with appropriate units, the same way as
  // bucket boundaries are printed by print(). E.g. for latency histogram
  // 1234567 would be turned into something like "1.234 s"
  virtual std::string valueToString(int64_t value) const = 0;
};

// A mix of linear and exponential histograms: a collection of linear histograms
// at exponentially increasing bucket sizes.
class MultiScaleHistogram : public HistogramInterface {
 public:
  using LinearHistogram = folly::Histogram<int64_t>;

  /// Maximum number of values staged in staged_values_ before merging them
  /// into the linear histograms, in batch.
  static constexpr size_t STAGED_VALUE_SLOTS = 512;

  // This struct describes how to translate the values tracked
  // by a given histogram level into units.
  struct Scale {
    // how many elementary units (microseconds, bytes) are in each unit
    int64_t unit;
    // name of unit
    const char* unit_name;
  };

  virtual ~MultiScaleHistogram();

 protected:
  /**
   * Create empty histogram.
   */
  MultiScaleHistogram(std::vector<LinearHistogram>&& histograms,
                      const std::vector<Scale>* scale);

 public:
  /**
   * Copy constructor.
   *
   * Thread-safe.
   */
  MultiScaleHistogram(const MultiScaleHistogram& rhs);

  /**
   * Move constructor.
   *
   * Not thread-safe with respect to rhs.
   */
  MultiScaleHistogram(MultiScaleHistogram&& rhs) noexcept;

  /**
   * Copy-assignment and move-assignment operators.
   *
   * Thread-safe with respect to this. Not thread-safe with respect to rhs.
   *
   * Copyable even without knowing the exact type:
   *  LatencyHistogram x, y;
   *  MultiScaleHistogram &a = x, &b = y;
   *  a = b; // This works.
   *
   * However, copying into an instance of different type is illegal:
   *  LatencyHistogram x;
   *  SizeHistogram y;
   *  MultiScaleHistogram &a = x, &b = y;
   *  a = b; // Don't do this!
   */
  MultiScaleHistogram& operator=(const MultiScaleHistogram& rhs);
  MultiScaleHistogram& operator=(MultiScaleHistogram&& rhs) noexcept(false);

  // HistogramInterface implementation.
  void clear() override;
  void add(int64_t value) override;
  void assign(const HistogramInterface& other) override;
  void merge(const HistogramInterface& other) override;
  void subtract(const HistogramInterface& other) override;
  void estimatePercentiles(const double* percentiles,
                           size_t npercentiles,
                           int64_t* samples_out,
                           uint64_t* count_out = nullptr,
                           int64_t* sum_out = nullptr) const override;
  void print(std::ostream& out) const override;
  std::string getUnitName() const override;
  std::string valueToString(int64_t value) const override;

  /**
   * Get translation descriptors for levels of linear histograms.
   *
   * Thread-safe.
   */
  const std::vector<Scale>& getScale() const noexcept {
    return *scale_;
  }

 private:
  /**
   * This is a heavily write-optimized class. add() is lock-free on the
   * common path, but all reads need to lock mutex_.
   *
   * Thread-safety is guaranteed because:
   *
   * - add() is the only function that modifies staged_values_tail_idx_;
   * - any function that modifies staged_values_head_idx_ does it with mutex_
   *   locked, and only ever advances it to staged_values_tail_idx_;
   * - add() could advance staged_values_tail_idx_ at any time, but will lock
   *   mutex_ and call mergeStagedValues() if staged_values_tail_idx_ and
   *   staged_values_head_idx_ would become equal (buffer full).
   *
   * In other words, mutex_ protects staged values from removal. Staged
   * values can be added at any time, but only by add(), and they become
   * immediately protected by mutex_.
   */

  /**
   * Not thread-safe. Expects mutex_ to be locked.
   */
  std::string getLabel(int level, int bucket) const;

  /**
   * Merge staged values into linear histograms.
   *
   * Not thread-safe. Expects mutex_ to be locked.
   */
  void mergeStagedValues();

  /// Head and tail pointers for staged_values_ ring buffer. Equal means empty.
  std::atomic<size_t> staged_value_head_idx_{0};
  std::atomic<size_t> staged_value_tail_idx_{0};

  /// Ring buffer of values that have been added via add() but which have not
  /// been written to the linear histograms yet, to avoid excessive locking
  /// overhead. Staged values are merged into the linear histograms on read
  /// access, or when out of staging slots.
  /// @seealso STAGED_VALUE_SLOTS
  /// @seealso mergeStagedValues
  std::unique_ptr<int64_t[]> staged_values_;

  /// Mutex for thread-safety. Locked everywhere except the common path of
  /// add().
  mutable std::mutex mutex_;

  /// The linear histograms, in order of scale.
  std::vector<LinearHistogram> histograms_;

  /// Translation descriptors for levels in histograms_[] vector.
  /// If shorter than histograms_, higher levels contain values are translated
  /// using scales_.back().
  const std::vector<Scale>* scale_;

  /// Total number of values added to the histogram.
  uint64_t count_{0};

  /// Sum of all values added to the histogram.
  int64_t sum_{0};
};

// Histogram for tracking request latencies. Bucket sizes are
// 10usec, 100usec, 1ms, 10ms, 100ms, and 1, 10, and 100s.
// USEC_MAX is the maximum latency value in microseconds that the histogram
// object will accept.
class LatencyHistogram final : public MultiScaleHistogram {
 public:
  // maximum value in microseconds that histogram can track
  static const int64_t USEC_MAX;

  explicit LatencyHistogram(int64_t usec_max = USEC_MAX);

 private:
  static std::vector<LinearHistogram> createHistograms(int64_t usec_max);

  static const std::vector<Scale>* getScales();
};

// Histogram for tracking sizes. Bucket sizes are
// 1B, 10B, 100B, 1KiB, 10KiB, 100KiB, 1MiB, 10MiB, 100MiB, 1GiB, 10GiB, 100GiB.
// Note that for linear histograms like 100B..1KiB the last bucket is 24% bigger
// than the rest because is spans 900B..1023B which is 124B.
class SizeHistogram final : public MultiScaleHistogram {
 public:
  // maximum value in bytes that histogram can track
  static const int64_t BYTES_MAX;

  explicit SizeHistogram(int64_t bytes_max = BYTES_MAX);

 private:
  static std::vector<LinearHistogram> createHistograms(int64_t bytes_max);

  static const std::vector<Scale>* getScales();
};

// Histogram for trimmed record age in seconds. Bucket sizes are
// 1s, 10s, 100s, 1000s, 10000s, 100000s, 1000000s, 10000000s,
// 100000000s, 1000000000s.
class RecordAgeHistogram final : public MultiScaleHistogram {
 public:
  // maximum age in seconds that histogram can track
  static const int64_t AGE_MAX;

  explicit RecordAgeHistogram(int64_t age_max = AGE_MAX);

 private:
  static std::vector<LinearHistogram> createHistograms(int64_t age_max);

  static const std::vector<Scale>* getScales();
};

// Bucket sizes are
// 1, 10, 100, 1K, 10K, 100K, 1M, 10M, 100M, 1B, 10B, 100B, 1T, 10T, 100T.
class NoUnitHistogram final : public MultiScaleHistogram {
 public:
  // maximum value that histogram can track
  static const int64_t VALUE_MAX;

  explicit NoUnitHistogram(int64_t value_max = VALUE_MAX);

 private:
  static std::vector<LinearHistogram> createHistograms(int64_t value_max);

  static const std::vector<Scale>* getScales();
};

// Simple histogram with 60 buckets corresponding to powers of two.
// Just 60 uint64_t values, with memory_order_relaxed.
// All methods are thread-safe, including the ones that HistogramInterface
// doesn't require to be thread-safe.
//
// Compared to MultiScaleHistogram:
// Pros:
//  + simple
//  + small: 480 bytes vs many kilobytes
//  + no heap allocations or mutexes
//  + probably faster
// Cons:
//  - IMPORTANT: percentiles don't change when values change without moving
//    buckets. Example: suppose a histogram of request latencies has a bucket
//    [32.768 ms, 65.536 ms), and all requests take 35-40 ms; if there's
//    a regression that causes requests to take 55-60 ms instead,
//    CompactHistogram won't notice but MultiScaleHistogram will.
//    MultiScaleHistogram maintains the sum of values of each bucket, allowing
//    it to get an exact average in each bucket, which it incorporates in
//    percentile estimates. CompactHistogram chooses not to do it to keep
//    the size small.
//  - less precision: 3x fewer buckets
//  - bucket boundaries are not round (unless powers of two are considered
//    round, e.g. for sizes in KiB/MiB/etc)
class CompactHistogram : public HistogramInterface {
 public:
  CompactHistogram() = default;

  // Must be the same subclass.
  CompactHistogram(const CompactHistogram& rhs);
  CompactHistogram& operator=(const CompactHistogram& rhs);

  void add(int64_t value) override;
  void clear() override;
  void assign(const HistogramInterface& other) override;
  void merge(const HistogramInterface& other) override;
  void subtract(const HistogramInterface& other) override;
  void estimatePercentiles(const double* percentiles,
                           size_t npercentiles,
                           int64_t* samples_out,
                           uint64_t* count_out = nullptr,
                           int64_t* sum_out = nullptr) const override;
  void print(std::ostream& out) const override;

  std::string getUnitName() const override;
  std::string valueToString(int64_t value) const override;

  // A short string representation of the histogram. A comma-separated list of
  // pairs "<bucket_idx>:<value>", listing only nonempty buckets.
  // E.g.: "3:1234,5:33,13:100".
  // In particular, if histogram is empty, empty string is returned.
  std::string toShortString() const;

  // Parses the histogram from a string in format produced by toShortString().
  // If the string is not in the right format, returns false.
  bool fromShortString(folly::StringPiece s);

 protected:
  struct Unit {
    // What value constitutes one of this unit. E.g. 1<<20 for "MiB".
    int64_t unit;
    const char* name;
  };

  explicit CompactHistogram(const std::vector<Unit>* units);

 private:
  // buckets_[i] corresponds to values [1l<<(i-1), 1l<<i).
  // buckets_[0] is [-infinity, 0].
  std::array<std::atomic<uint64_t>, 60> buckets_{};
  const std::vector<Unit>* units_ = nullptr;

  const Unit& pickUnit(int64_t value) const;
};

class CompactLatencyHistogram : public CompactHistogram {
 public:
  CompactLatencyHistogram();
};

class CompactSizeHistogram : public CompactHistogram {
 public:
  CompactSizeHistogram();
};

class CompactNoUnitHistogram : public CompactHistogram {
 public:
  CompactNoUnitHistogram();
};

}} // namespace facebook::logdevice
