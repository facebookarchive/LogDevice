/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/util.h"

#include <algorithm>

#include <folly/Random.h>
#include <folly/String.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace ldbench {

static constexpr double SPIKE_EPSILON = 1e-6;

// Returns either floor(x) or ceil(x), so that, on average, the return value
// is equal to x. `rnd` must be uniformly distributed in [0, 1].
static uint64_t randomRound(double x, double rnd) {
  if (x < 0) {
    ld_check(x >= -1e-9); // rounding error
    x = 0;
  }
  return (uint64_t)(x + rnd);
}

void Log2Histogram::fromBucketProbabilities(std::vector<double> buckets) {
  p = std::move(buckets);
  if (p.empty()) {
    return;
  }
  ld_check(p.size() <= 63);

  double sum = std::accumulate(p.begin(), p.end(), 0.0);
  // Divide by sum and calculate prefix sums.
  double acc = 0;
  for (double& x : p) {
    acc += x / sum;
    x = acc;
  }
  ld_check(fabs(p.back() - 1) < 1e-9);
}

std::vector<double> Log2Histogram::toBucketProbabilities() const {
  std::vector<double> b = p;
  double prev = 0;
  for (double& x : b) {
    double y = x;
    x -= prev;
    prev = y;
  }
  return b;
}

bool Log2Histogram::parse(const std::string& s) {
  p.clear();
  if (s == "constant") {
    return true;
  }
  std::vector<std::string> tokens;
  folly::split(',', s, tokens);
  for (std::string& t : tokens) {
    try {
      double x = folly::to<double>(t);
      if (x < 0) {
        return false;
      }
      p.push_back(x);
    } catch (std::range_error&) {
      return false;
    }
  }
  ld_check(!p.empty());
  if (p.size() > 63) {
    // It's supposed to be a distribution over ~64-bit integers.
    return false;
  }
  fromBucketProbabilities(p);
  return true;
}

double Log2Histogram::sample() const {
  return sample(folly::Random::randDouble01());
}

double Log2Histogram::sample(double x) const {
  if (p.empty()) {
    return 1;
  }

  ld_check(!p.empty());
  ld_check(x >= 0 && x <= 1);
  // Bucket index.
  size_t i = std::upper_bound(p.begin(), p.end(), x) - p.begin();
  i = std::min(i, p.size() - 1);
  // Boundaries of the bucket in the probability space.
  double p1 = i ? p[i - 1] : 0.;
  double p2 = p[i];
  // Boundaries of the bucket in the value space.
  double v1 = (double)(1ull << i);
  double v2 = (double)(1ull << (i + 1));
  // Position of x inside the bucket.
  double z = (p2 - p1 < 1e-9)
      ? 0.5 // nearly zero probability bucket, very unlikely
      : (x - p1) / (p2 - p1);
  ld_check(z >= -1e-9 && z <= 1 + 1e-9);
  // Lineraly interpolate in value space.
  return v1 + z * (v2 - v1);
}

double Log2Histogram::getMean() const {
  if (p.empty()) {
    return 1;
  }
  double prev = 0;
  double res = 0;
  ld_check(p[0] >= -1e-9);
  ld_check(fabs(p.back() - 1) <= 1e-9);
  for (size_t i = 0; i < p.size(); ++i) {
    double d = p[i] - prev;
    ld_check(d >= 0);
    res += d * (1ull << i) * 1.5;
    prev = p[i];
  }
  return res;
}

double Log2Histogram::getMeanInverse() const {
  if (p.empty()) {
    return 1;
  }
  double prev = 0;
  double res = 0;
  ld_check(p[0] >= -1e-9);
  ld_check(fabs(p.back() - 1) <= 1e-9);
  for (size_t i = 0; i < p.size(); ++i) {
    double d = p[i] - prev;
    ld_check(d >= 0);
    // Expected value of 1/x for x uniformly distributed in [1, 2].
    const double log2 = 0.6931471805599453;
    res += d / (1ull << i) * log2;
    prev = p[i];
  }
  return res;
}

Log2Histogram Log2Histogram::inverse() const {
  Log2Histogram h;
  if (p.empty()) {
    return h;
  }

  h.p.resize(p.size());
  for (size_t i = 0; i < p.size(); ++i) {
    h.p[i] = (i ? h.p[i - 1] : 0.) +
        (p[p.size() - 1 - i] - (i <= p.size() - 2 ? p[p.size() - 2 - i] : 0.0));
  }
  ld_check(h.p[0] >= -1e-9);
  ld_check(fabs(h.p.back() - 1) <= 1e-9);
  ld_check(std::is_sorted(h.p.begin(), h.p.end()));

  return h;
}

RoughProbabilityDistribution::RoughProbabilityDistribution()
    : target_mean(0), histogram_mean(1) {}

RoughProbabilityDistribution::RoughProbabilityDistribution(
    double _target_mean,
    Log2Histogram _histogram)
    : target_mean(_target_mean), histogram(std::move(_histogram)) {
  histogram_mean = histogram.getMean();
}

double RoughProbabilityDistribution::sampleFloat() const {
  return histogram.sample() / histogram_mean * target_mean;
}

double RoughProbabilityDistribution::sampleFloat(double x) const {
  return histogram.sample(x) / histogram_mean * target_mean;
}

uint64_t RoughProbabilityDistribution::sampleInteger() const {
  return randomRound(sampleFloat(), folly::Random::randDouble01());
}

uint64_t RoughProbabilityDistribution::sampleInteger(double x, double y) const {
  return randomRound(sampleFloat(x), y);
}

bool Spikiness::parse(const std::string& s, std::string* out_error) {
#define ERR(msg)        \
  do {                  \
    if (out_error) {    \
      *out_error = msg; \
    }                   \
    return false;       \
  } while (false)

  if (s == "none") {
    *this = Spikiness();
    return true;
  }

  std::vector<std::string> tokens;
  folly::split('/', s, tokens);
  aligned = false;
  if (tokens.size() == 4) {
    if (tokens.back() != "aligned") {
      ERR("unexpected last token");
    }
    aligned = true;
    tokens.pop_back();
  }
  if (tokens.size() != 3) {
    ERR("unexpected number of tokens");
  }

  // "14min"
  std::chrono::duration<double> d;
  int rv = parse_chrono_string(tokens.back(), &d);
  tokens.pop_back();
  if (rv != 0) {
    ERR("invalid duration");
  }
  period_sec = d.count();
  if (period_sec <= 0) {
    ERR("period must be positive");
  }

  // "30%/2%"
  std::vector<double> fractions;
  fractions.reserve(2);
  for (std::string& in : tokens) {
    if (in.size() < 2 || in.back() != '%') {
      ERR("'%' expected'");
    }
    in.pop_back(); // removing '%'
    try {
      double x = folly::to<double>(in);
      if (x < 0 || x > 100) {
        ERR("precentage out of range [0, 100]");
      }
      fractions.push_back(x / 100);
    } catch (std::range_error&) {
      ERR("bad precentage");
    }
  }
  spike_load_fraction = fractions[0];
  spike_time_fraction = fractions[1];

  // Don't allow inverse spikes.
  if (spike_load_fraction < spike_time_fraction) {
    ERR("first percentage (spike load) must be greater than second (spike "
        "duration)");
  }

  // Normalize some degenerate cases.

  // "Everything is a spike" is just another way of saying "no spikes".
  if (spike_time_fraction >= 1 - SPIKE_EPSILON) {
    ld_check(spike_load_fraction >= 1 - SPIKE_EPSILON * 2);
    spike_load_fraction = spike_time_fraction = 0;
  }

  // Keep denominators either exactly zero or decently far from zero.
  if (spike_load_fraction < SPIKE_EPSILON) {
    spike_load_fraction = 0;
  }
  if (spike_load_fraction > 1 - SPIKE_EPSILON) {
    spike_load_fraction = 1;
  }
  if (spike_time_fraction < SPIKE_EPSILON) {
    spike_time_fraction = 0;
  }

  return true;
#undef ERR
}

double Spikiness::transform(double x) const {
  if (spike_load_fraction == 0) {
    // No spikes.
    return x;
  }

  // Scale everything by period_sec to make period equal to 1.
  x /= period_sec;
  // Skip full periods.
  double res = floor(x);
  x -= res;
  if (x < spike_load_fraction || spike_load_fraction == 1) {
    // The answer is inside a spike.
    res += x / spike_load_fraction * spike_time_fraction;
  } else {
    // The answer is outside a spike.
    res += spike_time_fraction +
        (x - spike_load_fraction) / (1 - spike_load_fraction) *
            (1 - spike_time_fraction);
  }
  return res * period_sec;
}

double Spikiness::inverseTransform(double x) const {
  if (spike_load_fraction == 0) {
    // No spikes.
    return x;
  }

  x /= period_sec;
  double res = floor(x);
  x -= res;
  if (x < spike_time_fraction) {
    res += x / spike_time_fraction * spike_load_fraction;
  } else {
    res += spike_load_fraction +
        (x - spike_time_fraction) / (1 - spike_time_fraction) *
            (1 - spike_load_fraction);
  }
  return res * period_sec;
}

RandomEventSequence::RandomEventSequence() = default;
RandomEventSequence::RandomEventSequence(const Spikiness& _spikiness)
    : spikiness(_spikiness) {}

RandomEventSequence::State
RandomEventSequence::newState(double events_per_sec,
                              double initial_time,
                              double spikes_randomness) const {
  ld_check(events_per_sec >= -1e-9);
  State s;
  s.events_per_sec = events_per_sec;
  if (spikiness.aligned) {
    s.offset = 0;
  } else {
    if (spikes_randomness < 0) {
      spikes_randomness = folly::Random::randDouble01();
    }
    s.offset = spikes_randomness * spikiness.period_sec;
  }
  s.prev_virtual_time = spikiness.inverseTransform(initial_time - s.offset);
  return s;
}

double RandomEventSequence::nextEvent(State& state) const {
  ld_check(state.events_per_sec >= -1e-9);
  if (state.events_per_sec == 0) {
    // Use year 2070 as an approximation of "never".
    return 3600. * 24 * 365 * 100;
  }

  double x = folly::Random::randDouble01();
  x = std::max(1e-10, x);
  state.prev_virtual_time += -log(x) / state.events_per_sec;
  return spikiness.transform(state.prev_virtual_time) + state.offset;
}

}}} // namespace facebook::logdevice::ldbench
