/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/stats/Histogram.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <folly/Conv.h>
#include <folly/Likely.h>
#include <folly/small_vector.h>
#include <folly/stats/Histogram.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

// ~30 years
const int64_t LatencyHistogram::USEC_MAX = 1000l * 1000 * 1000 * 1000 * 1000;
// 1 PiB
const int64_t SizeHistogram::BYTES_MAX = 1l << 50;
// ~30 years
const int64_t RecordAgeHistogram::AGE_MAX = 1000l * 1000 * 1000;
// 1000T (10^15)
const int64_t NoUnitHistogram::VALUE_MAX =
    1000l * 1000l * 1000l * 1000l * 1000l;

LatencyHistogram::LatencyHistogram(int64_t usec_max)
    : MultiScaleHistogram(createHistograms(usec_max), getScales()) {}

LatencyHistogram::LatencyHistogram(
    const std::map<std::string, std::string>& map,
    const std::string& prefix,
    int64_t usec_max)
    : MultiScaleHistogram(createHistograms(usec_max),
                          getScales(),
                          map,
                          prefix) {}

std::vector<MultiScaleHistogram::LinearHistogram>
LatencyHistogram::createHistograms(int64_t usec_max) {
  if (UNLIKELY(usec_max > USEC_MAX)) {
    throw std::invalid_argument("usec_max is too large");
  }

  int64_t resolution = 10; // smallest bucket is 10 usec
  size_t nhist = std::max<size_t>(1, std::ceil(std::log10(usec_max)) - 1);
  std::vector<LinearHistogram> histograms;
  histograms.reserve(nhist);
  do {
    histograms.emplace_back(resolution, resolution, resolution * 10);
    resolution *= 10;
  } while (resolution < usec_max);
  ld_check(histograms.size() == nhist);

  return histograms;
}

const std::vector<MultiScaleHistogram::Scale>* LatencyHistogram::getScales() {
  static const std::vector<MultiScaleHistogram::Scale> SCALE({{1, "usec"},
                                                              {1, "usec"},
                                                              {1000, "ms"},
                                                              {1000, "ms"},
                                                              {1000, "ms"},
                                                              {1000000, "s"}});
  return &SCALE;
};

SizeHistogram::SizeHistogram(int64_t bytes_max)
    : MultiScaleHistogram(createHistograms(bytes_max), getScales()) {}

SizeHistogram::SizeHistogram(const std::map<std::string, std::string>& map,
                             const std::string& prefix,
                             int64_t bytes_max)
    : MultiScaleHistogram(createHistograms(bytes_max),
                          getScales(),
                          map,
                          prefix) {}

std::vector<MultiScaleHistogram::LinearHistogram>
SizeHistogram::createHistograms(int64_t bytes_max) {
  if (UNLIKELY(bytes_max > BYTES_MAX)) {
    throw std::invalid_argument("bytes_max is too large");
  }

  std::vector<LinearHistogram> histograms;
  for (int64_t unit = 1; unit < bytes_max; unit <<= 10) {
    histograms.emplace_back(unit, unit, unit * 10);
    histograms.emplace_back(unit * 10, unit * 10, unit * 100);
    histograms.emplace_back(unit * 100, unit * 100, unit * 900);
    histograms.emplace_back(unit * 124, unit * 900, unit * 1024);
  }

  return histograms;
}

const std::vector<MultiScaleHistogram::Scale>* SizeHistogram::getScales() {
  static const std::vector<MultiScaleHistogram::Scale> SCALE(
      {{1, "B"},
       {1, "B"},
       {1, "B"},
       {1, "B"},
       {1 << 10, "KiB"},
       {1 << 10, "KiB"},
       {1 << 10, "KiB"},
       {1 << 10, "KiB"},
       {1 << 20, "MiB"},
       {1 << 20, "MiB"},
       {1 << 20, "MiB"},
       {1 << 20, "MiB"},
       {1 << 30, "GiB"},
       {1 << 30, "GiB"},
       {1 << 30, "GiB"},
       {1 << 30, "GiB"}});
  return &SCALE;
};

RecordAgeHistogram::RecordAgeHistogram(int64_t age_max)
    : MultiScaleHistogram(createHistograms(age_max), getScales()) {}

RecordAgeHistogram::RecordAgeHistogram(
    const std::map<std::string, std::string>& map,
    const std::string& prefix,
    int64_t age_max)
    : MultiScaleHistogram(createHistograms(age_max), getScales(), map, prefix) {
}

std::vector<MultiScaleHistogram::LinearHistogram>
RecordAgeHistogram::createHistograms(int64_t age_max) {
  if (UNLIKELY(age_max > AGE_MAX)) {
    throw std::invalid_argument("age_max is too large");
  }

  int64_t resolution = 10; // smallest bucket is 10 seconds
  size_t nhist = std::max<size_t>(1, std::ceil(std::log10(age_max)) - 1);
  std::vector<LinearHistogram> histograms;
  histograms.reserve(nhist);
  do {
    histograms.emplace_back(resolution, resolution, resolution * 10);
    resolution *= 10;
  } while (resolution < age_max);
  ld_check(histograms.size() == nhist);

  return histograms;
}

const std::vector<MultiScaleHistogram::Scale>* RecordAgeHistogram::getScales() {
  static const std::vector<MultiScaleHistogram::Scale> SCALE({{1, "s"}});
  return &SCALE;
};

NoUnitHistogram::NoUnitHistogram(int64_t value_max)
    : MultiScaleHistogram(createHistograms(value_max), getScales()) {}

NoUnitHistogram::NoUnitHistogram(const std::map<std::string, std::string>& map,
                                 const std::string& prefix,
                                 int64_t value_max)
    : MultiScaleHistogram(createHistograms(value_max),
                          getScales(),
                          map,
                          prefix) {}

std::vector<MultiScaleHistogram::LinearHistogram>
NoUnitHistogram::createHistograms(int64_t value_max) {
  if (UNLIKELY(value_max > VALUE_MAX)) {
    throw std::invalid_argument("value_max is too large");
  }

  int64_t resolution = 1;
  size_t nhist = std::max<size_t>(1, std::ceil(std::log10(value_max)));
  std::vector<LinearHistogram> histograms;
  histograms.reserve(nhist);
  do {
    histograms.emplace_back(resolution, resolution, resolution * 10);
    resolution *= 10;
  } while (resolution < value_max);
  ld_check(histograms.size() == nhist);

  return histograms;
}

const std::vector<MultiScaleHistogram::Scale>* NoUnitHistogram::getScales() {
  static const std::vector<MultiScaleHistogram::Scale> SCALE(
      {{1, ""},
       {1, ""},
       {1, ""},
       {1000, "K"},
       {1000, "K"},
       {1000, "K"},
       {1000000, "M"},
       {1000000, "M"},
       {1000000, "M"},
       {1000000000, "B"},
       {1000000000, "B"},
       {1000000000, "B"},
       {1000000000, "T"}});
  return &SCALE;
};

namespace {
// Level and bucket index where a given percentile falls.
struct Percentile {
  double pct; // percentile as a  (0 .. 1.0]
  int level;  // histogram_[] level where this percentile falls
  int bucket; // index of bucket at .level where this percentile falls
};

std::string percentilesInBucketStr(const std::vector<Percentile>& percentiles,
                                   int level,
                                   int bucket) {
  std::string out;
  bool first = true;

  for (const Percentile& p : percentiles) {
    if (p.level == level && p.bucket == bucket) {
      if (first) {
        out = " * ";
        first = false;
      }
      out += "P" + folly::to<std::string>((int)round(p.pct * 100)) + " ";
    }
  }

  return out;
}

// Could be replaced by std::scoped_lock<std::mutex, std::mutex> in C++17.
class LockGuardPair {
 public:
  LockGuardPair(std::mutex& m1, std::mutex& m2)
      : g1_(&m1 < &m2 ? m1 : m2), g2_(&m1 < &m2 ? m2 : m1) {}

 private:
  std::lock_guard<std::mutex> g1_, g2_;
};
} // namespace

MultiScaleHistogram::~MultiScaleHistogram() = default;

MultiScaleHistogram::MultiScaleHistogram(
    std::vector<LinearHistogram>&& histograms,
    const std::vector<Scale>* scale)
    : staged_values_(new int64_t[STAGED_VALUE_SLOTS]),
      histograms_(std::move(histograms)),
      scale_(scale) {
  ld_check(!histograms_.empty());
  // Assert that histograms_ cover some range without gaps and overlaps.
  for (size_t i = 0; i + 1 < histograms_.size(); ++i) {
    ld_check(histograms_[i].getMax() == histograms_[i + 1].getMin());
  }
}

MultiScaleHistogram::MultiScaleHistogram(
    std::vector<LinearHistogram>&& histograms,
    const std::vector<Scale>* scale,
    const std::map<std::string, std::string>& map,
    const std::string& prefix)
    : MultiScaleHistogram(std::move(histograms), scale) {
  for (int l = 0; l < histograms_.size(); l++) {
    LinearHistogram& h = histograms_[l];
    for (int i = 0; i < h.getNumBuckets(); i++) {
      std::string label = getLabel(l, i);
      if (label.empty()) {
        continue;
      }
      auto it = map.find(prefix + label);
      if (it == map.end()) {
        continue;
      }

      ld_check(h.getBucketByIndex(i).count == 0);

      // Accumulate count and sum of values.
      auto value = h.getBucketMin(i);
      auto count = folly::to<uint64_t>(it->second);
      count_ += count;
      sum_ += value * count;

      // Add values to histogram.
      ld_check(l == 0 || h.getMin() <= value);
      ld_check(l == histograms.size() - 1 || value < h.getMax());
      h.addRepeatedValue(value, count);
    }
  }
}

MultiScaleHistogram::MultiScaleHistogram(const MultiScaleHistogram& rhs)
    : staged_values_(new int64_t[STAGED_VALUE_SLOTS]) {
  *this = rhs;
}

MultiScaleHistogram::MultiScaleHistogram(MultiScaleHistogram&& rhs) noexcept
    : staged_value_head_idx_(
          rhs.staged_value_head_idx_.load(std::memory_order_relaxed)),
      staged_value_tail_idx_(
          rhs.staged_value_tail_idx_.load(std::memory_order_relaxed)),
      staged_values_(std::move(rhs.staged_values_)),
      histograms_(std::move(rhs.histograms_)),
      scale_(rhs.scale_),
      count_(rhs.count_),
      sum_(rhs.sum_) {}

MultiScaleHistogram& MultiScaleHistogram::
operator=(const MultiScaleHistogram& rhs) {
  if (this == &rhs) {
    return *this;
  }

  // Merge staged values in rhs to solve race conditions and avoid copying them
  // too.
  LockGuardPair lock(mutex_, rhs.mutex_);
  const_cast<MultiScaleHistogram&>(rhs).mergeStagedValues();

  // Clear our staged value buffer.
  staged_value_head_idx_.store(staged_value_tail_idx_.load());

  // Copy histograms and other state.
  histograms_ = rhs.histograms_;
  scale_ = rhs.scale_;
  count_ = rhs.count_;
  sum_ = rhs.sum_;
  return *this;
}

MultiScaleHistogram& MultiScaleHistogram::operator=(MultiScaleHistogram&& rhs) {
  if (this == &rhs) {
    return *this;
  }

  std::unique_lock<std::mutex> lock(mutex_);

  staged_value_head_idx_.store(
      rhs.staged_value_head_idx_.load(std::memory_order_relaxed));
  staged_value_tail_idx_.store(
      rhs.staged_value_tail_idx_.load(std::memory_order_relaxed));
  staged_values_ = std::move(rhs.staged_values_);
  histograms_ = std::move(rhs.histograms_);
  scale_ = rhs.scale_;
  count_ = rhs.count_;
  sum_ = rhs.sum_;
  return *this;
}

void MultiScaleHistogram::add(int64_t value) {
  for (;;) {
    auto head_idx = staged_value_head_idx_.load(std::memory_order_acquire);
    auto tail_idx = staged_value_tail_idx_.load(std::memory_order_acquire);

    static_assert(STAGED_VALUE_SLOTS > 1, "need more slots for ring buffer");
    auto next_tail_idx = (tail_idx + 1) % STAGED_VALUE_SLOTS;
    if (next_tail_idx != head_idx) {
      // staged_values_ array is not full (common case), write to tail.
      // Note that we do not lock the mutex but rely on memory ordering. The
      // memory fence ensures that the staged value becomes visible when
      // mergeStagedValues() performs an atomic load of staged_value_tail_idx_.
      staged_values_[tail_idx] = value;
      std::atomic_thread_fence(std::memory_order_release);
      staged_value_tail_idx_.store(next_tail_idx, std::memory_order_release);
      return;
    } else {
      // staged_values_ array is full (rare case), merge and try again.
      std::lock_guard<std::mutex> lock(mutex_);
      mergeStagedValues();
    }
  }
}

void MultiScaleHistogram::merge(const MultiScaleHistogram& other) {
  LockGuardPair lock(mutex_, other.mutex_);

  if (UNLIKELY(histograms_.size() != other.histograms_.size())) {
    throw std::invalid_argument("Cannot merge: different number of levels");
  }

  // Merge all staged values.
  mergeStagedValues();
  const_cast<MultiScaleHistogram&>(other).mergeStagedValues();

  // Merge corresponding LinearHistograms.
  for (int i = 0; i < histograms_.size(); i++) {
    histograms_[i].merge(other.histograms_[i]);
  }

  // Accumulate count and sum.
  count_ += other.count_;
  sum_ += other.sum_;
}

void MultiScaleHistogram::clear() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Clear histograms and other state.
  count_ = 0;
  sum_ = 0;
  for (auto& h : histograms_) {
    h.clear();
  }

  // Finally, clear staged values (there may have been concurrent calls to
  // add()).
  staged_value_head_idx_.store(staged_value_tail_idx_.load());
}

std::string MultiScaleHistogram::getLabel(int l, int i) const {
  const Scale& scale = (*scale_)[std::min(l, (int)scale_->size() - 1)];
  const LinearHistogram::ValueType unit_value = scale.unit;
  const LinearHistogram& h = histograms_[l];

  std::ostringstream label;

  if (l == 0 && i == 0) {
    label << "< " << histograms_[0].getMin() << " " << scale.unit_name;
  } else if (l == histograms_.size() - 1 && i == h.getNumBuckets() - 1) {
    label << ">= " << h.getMax() / unit_value << " " << scale.unit_name;
  } else if (i > 0 && i < h.getNumBuckets() - 1) {
    // skip buckets containing values <min and >=max for interior levels

    label << h.getBucketMin(i) / unit_value << ".."
          << h.getBucketMax(i) / unit_value << " " << scale.unit_name;
  }

  return label.str();
}

void MultiScaleHistogram::print(std::ostream& out) const {
  std::vector<Percentile> percentiles{
      {0.5, -1, -1}, {0.75, -1, -1}, {0.95, -1, -1}, {0.99, -1, -1}};

  std::lock_guard<std::mutex> lock(mutex_);
  const_cast<MultiScaleHistogram*>(this)->mergeStagedValues();

  // for each percentile find the level and bucket where it falls
  for (int n = 0; n < percentiles.size(); n++) {
    for (int l = 0; l < histograms_.size(); l++) {
      const LinearHistogram& h = histograms_[l];
      unsigned i = h.getPercentileBucketIdx(percentiles[n].pct);
      if ((i == 0 && l == 0) ||
          (i == h.getNumBuckets() - 1 && l == histograms_.size() - 1) ||
          (i > 0 && i < h.getNumBuckets() - 1)) {
        percentiles[n].level = l;
        percentiles[n].bucket = i;
        break;
      }
    }
    ld_check(percentiles[n].level >= 0);
    ld_check(percentiles[n].bucket >= 0);
  }

  for (int l = 0; l < histograms_.size(); l++) {
    const LinearHistogram& h = histograms_[l];
    for (int i = 0; i < h.getNumBuckets(); i++) {
      const LinearHistogram::Bucket& b = h.getBucketByIndex(i);

      if (b.count == 0) {
        continue;
      }

      std::string label = getLabel(l, i);
      if (!label.empty()) {
        out << std::setw(20) << std::right << label << std::setw(1) << " : "
            << std::setw(10) << std::left << b.count << std::setw(1)
            << percentilesInBucketStr(percentiles, l, i) << std::endl;
      }
    }
  }
}

std::map<std::string, std::string>
MultiScaleHistogram::toMap(const std::string& prefix) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const_cast<MultiScaleHistogram*>(this)->mergeStagedValues();

  std::map<std::string, std::string> map;
  for (int l = 0; l < histograms_.size(); l++) {
    const LinearHistogram& h = histograms_[l];
    for (int i = 0; i < h.getNumBuckets(); i++) {
      const LinearHistogram::Bucket& b = h.getBucketByIndex(i);

      if (b.count == 0) {
        continue;
      }

      std::string label = getLabel(l, i);
      if (!label.empty()) {
        map[prefix + label] = std::to_string(b.count);
      }
    }
  }
  return map;
}

void MultiScaleHistogram::subtract(const MultiScaleHistogram& other) {
  LockGuardPair lock(mutex_, other.mutex_);

  if (UNLIKELY(histograms_.size() != other.histograms_.size())) {
    throw std::invalid_argument("Cannot subtract: different number of levels");
  }

  // Merge all staged values.
  mergeStagedValues();
  const_cast<MultiScaleHistogram&>(other).mergeStagedValues();

  // Subtract corresponding LinearHistograms from one another.
  for (size_t hist_idx = 0; hist_idx < histograms_.size(); ++hist_idx) {
    auto& this_histogram = histograms_[hist_idx];
    const auto& other_histogram = other.histograms_[hist_idx];
    size_t num_buckets = this_histogram.getNumBuckets();
    ld_check(num_buckets == other_histogram.getNumBuckets());

    // Subtract corresponding buckets from one another.
    for (size_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx) {
      const auto& other_bucket = other_histogram.getBucketByIndex(bucket_idx);
      auto& this_bucket = const_cast<LinearHistogram::Bucket&>(
          this_histogram.getBucketByIndex(bucket_idx));
      this_bucket.sum -= std::min(other_bucket.sum, this_bucket.sum);
      this_bucket.count -= std::min(other_bucket.count, this_bucket.count);
    }
  }

  // Subtract overall count and sum.
  sum_ -= std::min(other.sum_, sum_);
  count_ -= std::min(other.count_, count_);
}

std::pair<uint64_t, int64_t> MultiScaleHistogram::getCountAndSum() const {
  // Merge all staged values to ensure count_ and sum_ are fresh.
  std::lock_guard<std::mutex> lock(mutex_);
  const_cast<MultiScaleHistogram*>(this)->mergeStagedValues();
  return std::make_pair(count_, sum_);
}

void MultiScaleHistogram::estimatePercentiles(const double* percentiles,
                                              size_t npercentiles,
                                              int64_t* samples_out) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const_cast<MultiScaleHistogram*>(this)->mergeStagedValues();

  // Input percentiles must be sorted and in valid range [0.0, 1.0].
  ld_check(std::is_sorted(percentiles, percentiles + npercentiles));
  ld_check(std::all_of(percentiles, percentiles + npercentiles, [](double p) {
    return p >= 0.0 && p <= 1.0;
  }));

  // While iterating below, track number of samples below minimum of current
  // LinearHistogram, and above maximum of current LinearHistogram. These counts
  // may become inaccurate during execution due to concurrent updates. We
  // cannot prevent that without locks or atomics, but calling
  // computeTotalCount() on all LinearHistograms here should give a fairly
  // accurate snapshot. We do not read any other mutable state from histograms_
  // for the remainder of this function, to rule out concurrency issues.
  folly::small_vector<uint64_t, 16> histogram_counts;
  histogram_counts.reserve(histograms_.size());
  std::transform(
      histograms_.begin(),
      histograms_.end(),
      std::back_inserter(histogram_counts),
      [](const LinearHistogram& h) { return h.computeTotalCount(); });
  ld_check(histograms_.size() == histogram_counts.size());
  const uint64_t ntotal = std::accumulate(
      histogram_counts.begin(), histogram_counts.end(), uint64_t(0));
  if (ntotal == 0) {
    // All histograms are empty. All output samples are 0.
    std::fill_n(samples_out, npercentiles, 0);
    return;
  }
  uint64_t ncurrent = histogram_counts.front();
  ld_check(ncurrent <= ntotal); /* (incomplete) check for overflow */
  uint64_t nbelow_min = 0;
  uint64_t nabove_max = ntotal - ncurrent;

  // Iterate over input percentiles. If the current input percentile is
  // covered by the current LinearHistogram, compute an estimated sample and
  // write it to the output array. Then go to next input percentile. If the
  // input percentile is beyond the range of percentiles covered by the current
  // histogram, go to the next histogram.
  for (size_t pct_idx = 0, histogram_idx = 0; pct_idx < npercentiles;) {
    // Skip empty LinearHistograms.
    if (ncurrent > 0) {
      // Current LinearHistogram and input percentile.
      const LinearHistogram& h = histograms_[histogram_idx];
      const double pct = percentiles[pct_idx];

      // Compute range of percentiles covered by current LinearHistogram. Note
      // that pct_low is guaranteed to be 0.0 for the first histogram, and
      // pct_high is guaranteed to be 1.0 for the last non-empty histogram.
      // Also, pct_high of histogram i is equal to pct_low of histogram i+1, so
      // pct is always >= pct_low (assuming percentiles array is ordered). It
      // follows that the if-branch below *must* eventually match for every
      // input percentile.
      double pct_low = static_cast<double>(nbelow_min) / ntotal;
      double pct_high = static_cast<double>(nbelow_min + ncurrent) / ntotal;
      ld_check(pct >= pct_low);
      if (pct <= pct_high) {
        // Input percentile is covered by current LinearHistogram. Localize the
        // input percentile with respect to the percentile range covered by the
        // current LinearHistogram.
        double pct_local = pct_low != pct_high
            ? (pct - pct_low) / (pct_high - pct_low)
            : 0.5; /* should only happen if ntotal >= DBL_RADIX^DBL_MANT_DIG */
        ld_check(pct_local >= 0.0 && pct_local <= 1.0);

        // Get percentile estimate from LinearHistogram. Finds the bucket
        // which covers the percentile and performs 3-point linear interpolation
        // using the lower bound, average value, and upper bound of that bucket.
        samples_out[pct_idx] = h.getPercentileEstimate(pct_local);

        // Go to next input percentile. Stay on current LinearHistogram.
        ++pct_idx;
        continue;
      }
    }

    // Current LinearHistogram is empty or percentile outside of covered range.
    // Go to next next histogram. Update number of samples below minimum and
    // above maximum of the histogram.
    ++histogram_idx;
    ld_check(histogram_idx < histogram_counts.size());
    nbelow_min += ncurrent;
    ld_check(nbelow_min >= ncurrent); /* check for overflow */
    ncurrent = histogram_counts[histogram_idx];
    ld_check(nabove_max >= ncurrent);
    nabove_max -= ncurrent;
  }
}

void MultiScaleHistogram::mergeStagedValues() {
  const size_t head_idx =
      staged_value_head_idx_.load(std::memory_order_acquire);
  const size_t tail_idx =
      staged_value_tail_idx_.load(std::memory_order_acquire);

  if (head_idx == tail_idx) {
    // No staged values. Bail out immediately.
    return;
  }

  // Exploit unsigned integer underflow semantics in two's-complement.
  const size_t nvalues = (tail_idx - head_idx) % STAGED_VALUE_SLOTS;
  int64_t values[nvalues];

  // Counterpart to memory fence in add(). Ensures that staged_values_ are
  // ready to be read.
  std::atomic_thread_fence(std::memory_order_acquire);

  // Copy staged values to stack-resident dynamic array.
  if (head_idx <= tail_idx) {
    std::memcpy(
        values, staged_values_.get() + head_idx, sizeof(*values) * nvalues);
  } else {
    std::memcpy(values, staged_values_.get(), sizeof(*values) * tail_idx);
    std::memcpy(values + tail_idx,
                staged_values_.get() + head_idx,
                sizeof(*values) * (STAGED_VALUE_SLOTS - head_idx));
  }

  // Clear staged value buffer.
  staged_value_head_idx_.store(tail_idx, std::memory_order_release);

  // Accumulate count and sum.
  int64_t* const values_end = values + nvalues;
  count_ += nvalues;
  sum_ += std::accumulate(values, values_end, int64_t(0));

  // Sort stack-resident values to allow linear merge (as opposed to per-value
  // binary search) into linear histograms. Also saves a lot of cache misses.
  std::sort(values, values_end);

  // Merge stack-resident values into histograms_.
  auto hist_it = histograms_.begin();
  std::for_each(values, values_end, [this, &hist_it](int64_t v) {
    // Iterate to lowest-level histogram whose maximum is greater than the
    // input value (max is exclusive). This is the most fine-grained histogram
    // which can hold the value(s), so this is where we want to add it.
    while (hist_it < histograms_.end() && hist_it->getMax() <= v) {
      ++hist_it;
    }

    if (hist_it != histograms_.end()) {
      ld_check(hist_it == histograms_.begin() || hist_it->getMin() <= v);
      ld_check(v < hist_it->getMax());
      hist_it->addValue(v);
    } else {
      ld_check(!histograms_.empty());
      histograms_.back().addValue(v);
    }
  });
}
}} // namespace facebook::logdevice
