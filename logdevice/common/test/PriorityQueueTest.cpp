/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <logdevice/common/PriorityQueue.h>

#include <algorithm>
#include <iterator>
#include <vector>

#include <folly/Random.h>
#include <gtest/gtest.h>
#include <logdevice/common/CostQueue.h>
#include <logdevice/common/Priority.h>
#include <logdevice/common/types_internal.h>

using namespace facebook::logdevice;

namespace {

class Item {
 public:
  explicit Item(Priority p, size_t cost) : cost_(cost), p_(p) {}

  folly::IntrusiveListHook item_links;
  Priority priority() const {
    return p_;
  }
  size_t cost() const {
    return cost_;
  }

 private:
  size_t cost_;
  Priority p_;
};

using TestPriorityQ = PriorityQueue<Item, &Item::item_links>;

std::vector<Item*>
AllocateItems(size_t* ptotal_cost = nullptr,
              Priority num_priorities = Priority::NUM_PRIORITIES) {
  const int NUM_ITEMS = 1000;
  size_t total_cost = 0;
  std::vector<Item*> items;

  total_cost = 0;
  for (size_t i = 0; i < NUM_ITEMS; i++) {
    int p_idx = folly::Random::rand32() % asInt(num_priorities);
    size_t cost = folly::Random::rand32() % MAX_PAYLOAD_SIZE_PUBLIC;
    auto* item = new Item(static_cast<Priority>(p_idx), cost);
    total_cost += cost;
    items.push_back(item);
  }

  if (ptotal_cost != nullptr) {
    *ptotal_cost = total_cost;
  }
  return items;
}

void PriorityQueueInsert(TestPriorityQ& pq, std::vector<Item*>& items) {
  for (auto item : items) {
    pq.push(*item);
  }
}

size_t PriorityQueueConsume(TestPriorityQ& pq, bool delete_items = true) {
  Priority last_p = Priority::INVALID;
  size_t num_consumed = 0;

  while (!pq.empty()) {
    auto& item = pq.front();
    pq.pop();
    if (last_p != Priority::INVALID) {
      EXPECT_TRUE(asInt(last_p) <= asInt(item.priority()));
    }
    last_p = item.priority();
    if (delete_items) {
      delete &item;
    }
    num_consumed++;
  }

  return (num_consumed);
}

bool ItemPtrCompare(Item* lhs, Item* rhs) {
  return (asInt(lhs->priority()) < asInt(rhs->priority()));
}

TEST(PriorityQueueTest, InsertRandomOrder) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  PriorityQueueInsert(pq, items);
  EXPECT_EQ(items.size(), PriorityQueueConsume(pq));
}

TEST(PriorityQueueTest, InsertInOrder) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  std::sort(items.begin(), items.end(), ItemPtrCompare);
  PriorityQueueInsert(pq, items);
  EXPECT_EQ(items.size(), PriorityQueueConsume(pq));
}

TEST(PriorityQueueTest, InsertReverseOrder) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  std::sort(items.begin(), items.end(), ItemPtrCompare);
  std::reverse(items.begin(), items.end());
  PriorityQueueInsert(pq, items);
  EXPECT_EQ(items.size(), PriorityQueueConsume(pq));
}

TEST(PriorityQueueTest, TrimInvalidPriority) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  PriorityQueueInsert(pq, items);
  EXPECT_FALSE(pq.trim(Priority::INVALID, 1));
  EXPECT_EQ(items.size(), PriorityQueueConsume(pq));
}

TEST(PriorityQueueTest, TrimWithNothingToTrim) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems(nullptr, Priority::CLIENT_NORMAL);
  PriorityQueueInsert(pq, items);
  EXPECT_FALSE(pq.trim(Priority::CLIENT_LOW, 1));
  EXPECT_EQ(items.size(), PriorityQueueConsume(pq));
}

TEST(PriorityQueueTest, TrimWithNotEnoughToTrim) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  PriorityQueueInsert(pq, items);
  Priority cur_priority = priorityAbove(Priority::NUM_PRIORITIES);
  while (pq.cost(cur_priority) == 0) {
    cur_priority = priorityAbove(cur_priority);
  }
  EXPECT_TRUE(pq.cost(cur_priority) != 0);
  EXPECT_FALSE(pq.trim(cur_priority, pq.cost(cur_priority) + 1));
  EXPECT_EQ(items.size(), PriorityQueueConsume(pq));
}

TEST(PriorityQueueTest, TrimFromSinglePriority) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  PriorityQueueInsert(pq, items);
  Priority cur_priority = priorityAbove(Priority::NUM_PRIORITIES);
  while (pq.cost(cur_priority) == 0) {
    cur_priority = priorityAbove(cur_priority);
  }
  EXPECT_TRUE(pq.cost(cur_priority) != 0);
  EXPECT_TRUE(pq.trim(cur_priority, pq.cost(cur_priority) / 2));
  PriorityQueueConsume(pq, false);
  for (auto item : items) {
    delete item;
  }
}

TEST(PriorityQueueTest, TrimFromMultiplePriorities) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  PriorityQueueInsert(pq, items);
  Priority cur_priority = priorityAbove(Priority::NUM_PRIORITIES);
  uint64_t trim_amount = 0;
  while (cur_priority != Priority::INVALID && pq.cost(cur_priority) == 0) {
    cur_priority = priorityAbove(cur_priority);
  }
  EXPECT_TRUE(pq.cost(cur_priority) != 0);
  trim_amount = pq.cost(cur_priority);
  do {
    cur_priority = priorityAbove(cur_priority);
  } while (cur_priority != Priority::INVALID && pq.cost(cur_priority) == 0);
  if (cur_priority != Priority::INVALID) {
    trim_amount++;
  }
  EXPECT_TRUE(pq.trim(cur_priority, trim_amount));
  PriorityQueueConsume(pq, false);
  for (auto item : items) {
    delete item;
  }
}

TEST(PriorityQueueTest, DisablePriorityLevel) {
  TestPriorityQ pq;
  std::vector<Item*> items = AllocateItems();
  PriorityQueueInsert(pq, items);
  EXPECT_FALSE(pq.empty());
  Priority cur_priority = priorityAbove(Priority::NUM_PRIORITIES);
  while (cur_priority != Priority::INVALID && pq.cost(cur_priority) != 0) {
    pq.enable(cur_priority, false);
    cur_priority = priorityAbove(cur_priority);
  }
  EXPECT_FALSE(pq.empty());
  EXPECT_TRUE(pq.enabledEmpty());

  cur_priority = priorityAbove(Priority::NUM_PRIORITIES);
  while (cur_priority != Priority::INVALID && pq.cost(cur_priority) != 0) {
    pq.enable(cur_priority);
    cur_priority = priorityAbove(cur_priority);
  }
  EXPECT_FALSE(pq.empty());
  EXPECT_FALSE(pq.enabledEmpty());

  cur_priority = priorityAbove(Priority::MAX);
  while (cur_priority != Priority::INVALID && pq.cost(cur_priority) != 0) {
    if (!pq.empty(cur_priority)) {
      EXPECT_EQ(&pq.enabledFront(), &pq.front(cur_priority));
      pq.enable(cur_priority, false);
      if (!pq.enabledEmpty()) {
        EXPECT_NE(&pq.enabledFront(), &pq.front(cur_priority));
      }
    }
    cur_priority = priorityBelow(cur_priority);
  }
}

} // namespace
