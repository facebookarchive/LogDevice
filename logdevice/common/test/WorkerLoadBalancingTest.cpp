/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WorkerLoadBalancing.h"

#include <gtest/gtest.h>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

static const worker_id_t W0(0), W1(1);

// With balanced load, expect balanced assignment
TEST(WorkerLoadBalancingTest, TwoWorkersBalanced) {
  WorkerLoadBalancing balancer(2);
  balancer.reportLoad(W0, 878);
  balancer.reportLoad(W1, 879);
  int count = 0;
  for (int i = 0; i < 1000; ++i) {
    count += balancer.selectWorker() == W0;
  }
  // Probability of failure is ~1.8e-10.
  ASSERT_GE(count, 400);
  ASSERT_LE(count, 600);
}

// With W0 much more loaded than W1, expect almost all work to be directed at
// W1
TEST(WorkerLoadBalancingTest, TwoWorkersUnbalanced) {
  WorkerLoadBalancing balancer(2);
  balancer.reportLoad(W0, 100);
  balancer.reportLoad(W1, 10);
  int count = 0;
  for (int i = 0; i < 100; ++i) {
    count += balancer.selectWorker() == W1;
  }
  ASSERT_GE(count, 99);
}

// With many workers and random loads, if we add as much work as is already
// there, we expect new work to be assigned so that load mostly balances out
// (this is without reporting updated load to the balancer).
TEST(WorkerLoadBalancingTest, NewWorkRebalances) {
  const int N = 100;
  std::mt19937 rng(0xbabababa241ef19e);
  std::vector<int> load(N);
  for (auto& x : load) {
    x = std::uniform_int_distribution<int>(1, 100)(rng);
  }
  double before_ratio = double(*std::max_element(load.begin(), load.end())) /
      *std::min_element(load.begin(), load.end());
  fprintf(stderr,
          "Before load balancing, most loaded worker had %.1fx the load "
          "of the least loaded\n",
          before_ratio);
  // Expect this to be pretty big
  ASSERT_GE(before_ratio, 10.0);

  WorkerLoadBalancing balancer(N);
  for (int i = 0; i < N; ++i) {
    balancer.reportLoad(worker_id_t(i), load[i]);
  }

  int nadd = std::accumulate(load.begin(), load.end(), 0);
  for (int i = 0; i < nadd; ++i) {
    ++load[balancer.selectWorker().val_];
  }

  double after_ratio = double(*std::max_element(load.begin(), load.end())) /
      *std::min_element(load.begin(), load.end());
  fprintf(stderr,
          "After load balancing, most loaded worker had %.1fx the load "
          "of the least loaded\n",
          after_ratio);
  // Expect this to be much smaller
  ASSERT_LT(after_ratio, before_ratio);
  ASSERT_LT(after_ratio, 2.2);
}

}} // namespace facebook::logdevice
