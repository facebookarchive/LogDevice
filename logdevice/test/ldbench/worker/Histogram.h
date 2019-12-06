/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <type_traits>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice { namespace ldbench {

/**
 * Compute an equi-depth (equi-height) histogram.
 *
 * Range of input samples must be sorted i.e. in non-decreasing order.
 *
 * @param samples_begin
 *  Input iterator to beginning of samples range.
 * @param samples_end
 *  Input iterator to end of samples range.
 * @param nbuckets
 *  Number of output buckets.
 * @param lower_bounds
 *  Lower bounds of each bucket. Minimum sample is written to first element.
 *  Maximum sample is written to (nbuckets + 1)th element.
 * @param counts
 *  Output iterator for count of each bucket.
 * @param start_percentile
 *  Percentile at which to start the histogram. Any samples below that
 *  percentile are ignored.
 */
template <typename SampleIt, typename SampleOutIt, typename CountOutIt>
void equi_depth_histogram(SampleIt samples_begin,
                          SampleIt samples_end,
                          size_t nbuckets,
                          SampleOutIt lower_bounds,
                          CountOutIt counts,
                          double /*start_percentile*/ = 0.0) {
  using SampleT = std::remove_reference_t<decltype(*samples_begin)>;

  // Sanity checks.
  ld_assert(std::is_sorted(samples_begin, samples_end));
  ld_check(nbuckets > 0);

  // Compute bucket size. It is a floating point number because the number of
  // samples may not divide evenly by the number of buckets.
  size_t nsamples = std::distance(samples_begin, samples_end);
  ld_check(nsamples >= nbuckets);
  double bucket_size = double(nsamples) / nbuckets;

  // Compute lower bound and count of each bucket.
  double next_bucket_rank = bucket_size;
  size_t sample_rank = 0;
  SampleT max_sample;
  for (size_t bucket_num = 0; bucket_num < nbuckets; ++bucket_num) {
    size_t count = 0;
    bool first_sample = true;
    while ((samples_begin != samples_end) &&
           ((sample_rank < next_bucket_rank) || (bucket_num == nbuckets - 1))) {
      ++count;
      auto sample = *samples_begin++;
      max_sample = sample;
      if (first_sample) {
        *lower_bounds++ = sample;
        first_sample = false;
      }
      ++sample_rank;
    }
    *counts++ = count;
    next_bucket_rank += bucket_size;
  }
  ld_check(sample_rank == nsamples);

  // Append max sample to lower bounds.
  *lower_bounds++ = max_sample;
}

}}} // namespace facebook::logdevice::ldbench
