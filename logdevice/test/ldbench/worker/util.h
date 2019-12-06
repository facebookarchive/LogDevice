/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

// See ldbench.md for more context about these data structures.

namespace facebook { namespace logdevice { namespace ldbench {

// An approximate, order-of-magnitude, probability distribution.
// Just a histogram with exponentially increasing bucket widths.
struct Log2Histogram {
  // p[i] is the probability that value is less than 2^(i+1)
  // 0 <= p[0] <= p[1] <= ... <= p.back() = 1.
  // If empty, the probability distribution is a constant 1.
  std::vector<double> p;

  // Initializes *this.
  // `buckets[i]` is the probability that value is in [2^i, 2^(i+1)).
  // May be unnormalized. Note that this is different from `p`: `p` are
  // cumulative sums of `buckets`.
  void fromBucketProbabilities(std::vector<double> buckets);

  std::vector<double> toBucketProbabilities() const;

  // Draw a sample from the distribution. Always >= 1.
  // Picks a bucket according to probabilities p[i], then uses uniform
  // distribution inside the bucket.
  double sample() const;

  // A deterministic version of sample(). Instead of generating a random number,
  // uses x. x should be a random number uniformly distributed in [0, 1).
  double sample(double x) const;

  // Expected value of sample().
  double getMean() const;

  // Expected value of 1/sample().
  double getMeanInverse() const;

  // Returns true on success, false on failure.
  bool parse(const std::string& s);

  // Returns this histogram with `p` reversed. Can be used to convert a
  // histogram of x into a similar histogram of 1/x, e.g. period to frequency.
  // Note that the conversion is only approximate: the distribution of
  // inverse().sample() is not the same as distribution of 1/sample(),
  // and inverse().getMean() != 1/getMean().
  Log2Histogram inverse() const;
};

// An average value and a Log2Histogram defining the variation around that
// average.
// See ldbench.md for details.
class RoughProbabilityDistribution {
 public:
  RoughProbabilityDistribution();
  RoughProbabilityDistribution(double target_mean, Log2Histogram histogram);
  RoughProbabilityDistribution(RoughProbabilityDistribution&&) = default;
  RoughProbabilityDistribution& operator=(RoughProbabilityDistribution&&) =
      default;

  double sampleFloat() const;
  double sampleFloat(double x) const;

  uint64_t sampleInteger() const;
  // For implementation reasons, this method needs two random numbers.
  uint64_t sampleInteger(double x, double y) const;

 private:
  // The mean value of sampleFloat() and sampleInteger().
  double target_mean;
  // Equal to histogram.getMean().
  double histogram_mean;

  Log2Histogram histogram;
};

// Description of how bursty a workload is.
// The graph of event rate over time is periodic and piecewise-constant:
//
// ^
// |    +---+             +---+             +---+
// |    |   |             |   |             |   |
// |    |   |             |   |             |   |
// |----+   +-------------+   +-------------+   +---
// +----------------------------------------------->
//      ^^^^^^^^^^^^^^^^^^^                 ^^^^^
//            period                        spike
//
// The rate is higher during first spike_time_fraction * period_sec out of each
// period_sec. spike_load_fraction tells what fraction of all events happen
// during spikes. The remaining 1-spike_load_fraction requests happen outside
// the spikes.
// Event rate during a spike:
// events_per_sec * spike_load_fraction / spike_time_fraction
// During the remaining period_sec * (1 - spike_time_fraction) of time:
// events_per_sec * (1 - spike_load_fraction) / (1 - spike_time_fraction).
struct Spikiness {
  double period_sec = 1;
  double spike_time_fraction = 0; // in [0, 1]
  double spike_load_fraction = 0; // in [0, 1]
  // If true, the spikes will happen at the time for everyone. Otherwise,
  // the positions of spikes will be randomly shifted for each worker/log/etc
  // depending on context.
  bool aligned = false;

  // Examples:
  //  "50%/10%/10s"
  //  "30%/2%/14min/aligned"
  // See ldbench.md for details.
  // Returns true on success, false on failure.
  bool parse(const std::string& s, std::string* out_error = nullptr);

  // Spikiness works by warping time: spike_load_fraction worth of virtual time
  // get compressed into spike_time_fraction of actual time, and the remaining
  // 1-spike_load_fraction get stretched to fill the remaining
  // 1-spike_time_fraction.
  // This function transforms from virtual to actual time.
  // Doesn't take `aligned` into account, spikes are always in the same places.
  double transform(double x) const;

  // Transforms from actual to virtual time. Note one actual time point may
  // correspond to many virtual time points if spikes are infinitely short.
  double inverseTransform(double t) const;
};

// Description of a random series of time points (aka "point process").
// Used for generating simulated events, e.g. appends, reader restarts.
// See ldbench.md for details.
struct RandomEventSequence {
  Spikiness spikiness;

  RandomEventSequence();
  explicit RandomEventSequence(const Spikiness& spikiness);

  struct State {
    double events_per_sec = -1;
    double offset; // in "actual" time
    double prev_virtual_time;
  };

  // Initializes a new sequence.
  // @param events_per_sec  Average rate of events.
  // @param initial_time    Time point (in "actual time") at which the event
  //                        sequence starts. Usually just current time.
  // @param spikes_randomness
  //   Value in [0, 1] that defines the shift of the locations of the spikes.
  //   If -1 and !spikiness.aligned, the locations of spikes will be shifted
  //   randomly.
  State newState(double events_per_sec,
                 double initial_time,
                 double spikes_randomness = -1) const;

  // Generate next event in the sequence.
  double nextEvent(State& state) const;
};

}}} // namespace facebook::logdevice::ldbench
