/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WorkerLoadBalancing.h"

#include <folly/Random.h>

#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

worker_id_t WorkerLoadBalancing::selectWorker() {
  if (loads_.size() == 1) {
    return worker_id_t(0);
  }

  // On a high level, this randomly selects two workers and prefers the one
  // with smaller load (ref: "The Power of Two Random Choices" by Mitzenmacher
  // et al 1996).
  //
  // To avoid starvation of the worker with the highest load if load is
  // more-or-less even, we choose between the two workers probabilistically
  // based on the difference in load.  If this calculation becomes a hot path
  // (not expected at time of writing) we can bypass the nondeterminism and
  // always go for the less loaded worker.

  int index1;
  int index2;
  double coinflip;

  {
    const int n = loads_.size();
    // This selects a pair of random numbers in [0, n - 1].
    index1 = folly::Random::rand32(n);
    index2 = (index1 + folly::Random::rand32(1, n)) % n;
    ld_check(index1 != index2);
    // This will be used for a biased coin flip to decide between the two
    // workers.
    coinflip = folly::Random::randDouble01();
  }

  auto load1 = loads_[index1].val.load();
  auto load2 = loads_[index2].val.load();
  // Make sure load1 <= load2
  if (!(load1 <= load2)) {
    std::swap(index1, index2);
    std::swap(load1, load2);
  }

  // Calculate the relative difference in load.  The probability of choosing
  // the worker with *higher* load will start at 50% for no difference (same
  // load) and drop exponentially to 1% for 10% difference.  This means that
  // for non-trivial differences in load we will almost always choose the
  // less-loaded worker.
  double prob;
  if (load2 <= 0) {
    prob = 0.5;
  } else {
    const double TARGET_DIFF = 0.1;
    const double TARGET_PROB = 0.01;
    const double ALPHA = 1 / TARGET_DIFF * log(2 * TARGET_PROB);

    double rel_diff = double(load2 - load1) / load2;
    // Alpha was calculated so that when rel_diff == TARGET_DIFF, this
    // probability comes out to TARGET_PROB
    prob = 0.5 * exp(ALPHA * rel_diff);
  }
  return worker_id_t(coinflip < prob ? index2 : index1);
}

}} // namespace facebook::logdevice
