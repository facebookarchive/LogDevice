/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/UnorderedMapWithLRU.h"

#include <gtest/gtest.h>

#include "logdevice/include/types.h"

using namespace facebook::logdevice;

using namespace std::literals::string_literals;

struct Identity {
  size_t operator()(const uint64_t key) const {
    return key;
  }
};

void expectContents(
    UnorderedMapWithLRU<uint64_t, std::string, Identity>& map,
    const std::vector<std::pair<uint64_t, std::string>>& items_in_lru_order) {
  for (const auto& item : items_in_lru_order) {
    EXPECT_NE(item.first, map.emptyKey());
    EXPECT_NE(item.first, map.deletedKey());

    EXPECT_EQ(item.first, map.removeLRU());
  }
  EXPECT_EQ(map.emptyKey(), map.removeLRU());
}

void expectContents(UnorderedSetWithLRU<uint64_t, Identity>& set,
                    const std::vector<uint64_t>& items_in_lru_order) {
  for (const auto& item : items_in_lru_order) {
    EXPECT_NE(item, set.emptyKey());
    EXPECT_NE(item, set.deletedKey());

    const uint64_t key{set.getLRU()};
    EXPECT_EQ(item, key);
    set.erase(set.getWithoutPromotion(key));
  }
  EXPECT_EQ(set.emptyKey(), set.getLRU());
}

TEST(UnorderedMapWithLRUTest, Basic) {
  UnorderedMapWithLRU<uint64_t, std::string, Identity> map(0, ~uint64_t{0});

  EXPECT_EQ(nullptr, map.get(1).first);
  EXPECT_EQ(nullptr, map.get(2).first);

  map.verifyIntegrity(0);
  map.add(1, "hello"s);
  map.verifyIntegrity(1);
  map.add(2, "goodbye"s);
  map.verifyIntegrity(2);

  EXPECT_EQ("hello"s, *map.get(1).first);
  map.verifyIntegrity(2);
  EXPECT_EQ("goodbye"s, *map.get(2).first);
  map.verifyIntegrity(2);

  expectContents(map, {{1, "hello"s}, {2, "goodbye"s}});
}

TEST(UnorderedMapWithLRUTest, LRUExists) {
  UnorderedMapWithLRU<uint64_t, std::string, Identity> map(0, ~uint64_t{0});

  map.add(1, "hello"s);
  map.add(2, "goodbye"s);

  EXPECT_EQ("hello"s, *map.get(1).first);

  expectContents(map, {{2, "goodbye"s}, {1, "hello"s}});
}

TEST(UnorderedMapWithLRUTest, LRUAdd) {
  UnorderedMapWithLRU<uint64_t, std::string, Identity> map(0, ~uint64_t{0});

  map.add(1, "hello"s);
  map.add(2, "goodbye"s);

  map.add(1, "bonjour"s);

  expectContents(map, {{2, "goodbye"s}, {1, "bonjour"s}});
}

// Actual methods used in production:
// getLRU(), get(), erase(), getOrAddWithoutPromotion(), empty().

TEST(UnorderedMapWithLRUTest, Basic2) {
  UnorderedMapWithLRU<uint64_t, std::string, Identity> map(0, ~uint64_t{0});

  EXPECT_TRUE(map.empty());
  EXPECT_EQ(nullptr, map.get(1).first);
  EXPECT_EQ(nullptr, map.get(2).first);

  map.verifyIntegrity(0);
  map.getOrAddWithoutPromotion(1) = "hello"s;
  EXPECT_FALSE(map.empty());
  map.verifyIntegrity(1);
  map.getOrAddWithoutPromotion(2) = "goodbye"s;
  map.verifyIntegrity(2);
  EXPECT_FALSE(map.empty());

  auto value_and_iter{map.get(1)};
  EXPECT_EQ("hello"s, *value_and_iter.first);
  EXPECT_FALSE(map.empty());
  map.verifyIntegrity(2);

  map.erase(value_and_iter.second);
  EXPECT_FALSE(map.empty());
  map.verifyIntegrity(1);

  value_and_iter = map.get(2);
  EXPECT_EQ("goodbye"s, *value_and_iter.first);
  EXPECT_FALSE(map.empty());
  map.verifyIntegrity(1);

  map.erase(value_and_iter.second);
  EXPECT_TRUE(map.empty());
  map.verifyIntegrity(0);
}

TEST(UnorderedWithLRUTest, BasicSet) {
  UnorderedSetWithLRU<uint64_t, Identity> set(0, ~uint64_t{0});

  EXPECT_TRUE(set.get(1) == set.end());
  EXPECT_TRUE(set.get(2) == set.end());

  set.verifyIntegrity(0);
  set.add(1);
  set.verifyIntegrity(1);
  set.add(2);
  set.verifyIntegrity(2);

  EXPECT_FALSE(set.get(1) == set.end());
  set.verifyIntegrity(2);
  EXPECT_FALSE(set.get(2) == set.end());
  set.verifyIntegrity(2);

  expectContents(set, {1, 2});
}
