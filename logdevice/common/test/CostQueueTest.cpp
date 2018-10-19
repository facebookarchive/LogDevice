/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <logdevice/common/CostQueue.h>

#include <algorithm>
#include <cstdlib>
#include <iterator>
#include <vector>

#include <folly/Random.h>
#include <gtest/gtest.h>
#include <logdevice/common/PriorityQueue.h>
#include <logdevice/common/types_internal.h>

using namespace facebook::logdevice;
namespace {

class Item {
 public:
  explicit Item(size_t cost) : cost_(cost) {}

  folly::IntrusiveListHook item_links;
  size_t cost() const {
    return cost_;
  }

 private:
  size_t cost_;
};

using TestCostQ = CostQueue<Item, &Item::item_links>;

std::vector<Item*> AllocateItems(size_t* ptotal_cost = nullptr) {
  const int NUM_ITEMS = 1000;
  size_t total_cost = 0;
  std::vector<Item*> items;

  total_cost = 0;
  for (size_t i = 0; i < NUM_ITEMS; i++) {
    size_t cost = folly::Random::rand32() % MAX_PAYLOAD_SIZE_PUBLIC;
    auto* item = new Item(cost);
    total_cost += cost;
    items.push_back(item);
  }

  if (ptotal_cost != nullptr) {
    *ptotal_cost = total_cost;
  }
  return items;
}

TEST(CostQueueTest, CostAccounting) {
  size_t current_cost = 0;
  std::vector<Item*> items = AllocateItems(&current_cost);
  TestCostQ costq;

  for (auto item : items) {
    costq.push_back(*item);
  }

  EXPECT_EQ(costq.cost(), current_cost);
  while (!items.empty()) {
    auto it = items.begin();
    std::advance(it, folly::Random::rand32() % items.size());

    auto* item = *it;
    costq.erase(*item);
    items.erase(it);
    current_cost -= item->cost();
    delete item;

    EXPECT_EQ(costq.cost(), current_cost);
  }
}

} // namespace
