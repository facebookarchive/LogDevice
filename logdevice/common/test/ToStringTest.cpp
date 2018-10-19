/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/toString.h"

#include <array>
#include <deque>
#include <forward_list>
#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/SmallMap.h"
#include "logdevice/common/Timestamp.h"

using namespace facebook::logdevice;

class ToStringTest : public ::testing::Test {};

struct Ostreamable {
  int x;
  explicit Ostreamable(int x) : x(x) {}
};

std::ostream& operator<<(std::ostream& o, const Ostreamable& x) {
  return o << "O" << x.x;
}

struct Tostr {
  int x;
  explicit Tostr(int x) : x(x) {}
  std::string toString() const {
    return "T" + std::to_string(x);
  }
};

struct Both {
  int x;
  explicit Both(int x) : x(x) {}
  std::string toString() const {
    return "B" + std::to_string(x);
  }
};
std::ostream& operator<<(std::ostream& o, const Both& /*x*/) {
  return o << "OH NOES!";
}

TEST_F(ToStringTest, Simple) {
  EXPECT_EQ("42", toString(42));
  EXPECT_EQ("42", toString("42"));
  EXPECT_EQ("42", toString(std::string("42")));
  EXPECT_EQ("(foo, 42)", toString(std::make_pair("foo", 42)));
  EXPECT_EQ("(foo, 42, bar, baz)",
            toString(std::make_tuple("foo", 42, std::string("bar"), "baz")));
  EXPECT_EQ("()", toString(std::make_tuple()));

  EXPECT_EQ("O42", toString(Ostreamable(42)));
  EXPECT_EQ("T42", toString(Tostr(42)));
  EXPECT_EQ("B42", toString(Both(42)));

  EXPECT_EQ("[1, 2, 3]", toString(std::vector<long long>({1, 2, 3})));
  EXPECT_EQ("[1, 2, 3]", toString(std::deque<unsigned int>({1, 2, 3})));
  EXPECT_EQ("[1, 2, 3]", toString(std::list<int>({1, 2, 3})));
  EXPECT_EQ("[1, 2, 3]", toString(std::forward_list<int>({1, 2, 3})));
  EXPECT_EQ("[1, 2, 3]", toString(std::array<int, 3>({1, 2, 3})));

  EXPECT_EQ("{1, 2, 3}", toString(std::set<int>({1, 2, 3})));
  EXPECT_EQ("{1, 1, 2, 2, 3}", toString(std::multiset<int>({1, 2, 3, 1, 2})));
  EXPECT_EQ("{(1, 2), (3, 4)}", toString(std::map<int, int>({{1, 2}, {3, 4}})));
  EXPECT_EQ("{(1, 3), (1, 2), (3, 4)}",
            toString(std::multimap<int, int>({{1, 3}, {3, 4}, {1, 2}})));

  EXPECT_EQ(
      strlen("{1, 2, 3}"), toString(std::unordered_set<int>({1, 2, 3})).size());
  EXPECT_EQ(strlen("{1, 1, 2, 2, 3}"),
            toString(std::unordered_multiset<int>({1, 2, 3, 1, 2})).size());
  EXPECT_EQ(strlen("{(1, 2), (3, 4)}"),
            toString(std::unordered_map<int, int>({{1, 2}, {3, 4}})).size());
  EXPECT_EQ(
      strlen("{(1, 3), (1, 2), (3, 4)}"),
      toString(std::unordered_multimap<int, int>({{1, 3}, {3, 4}, {1, 2}}))
          .size());

  SmallOrderedMap<std::string, std::string> m;
  m["foo"] = "bar";
  m["1"] = "2";
  EXPECT_EQ("{(1, 2), (foo, bar)}", toString(m));
}

TEST_F(ToStringTest, Nested) {
  EXPECT_EQ("{([1], {2})}",
            toString(std::map<std::vector<int>, std::set<int>>({{{1}, {2}}})));
  EXPECT_EQ("[[1, 2], [3, 4]]",
            toString(std::vector<std::vector<std::string>>(
                {{"1", "2"}, {"3", "4"}})));
  EXPECT_EQ(
      "[{(, (B1, O2, T3)), (foo, (B10, O20, T30))}, {}]",
      toString(std::vector<
               std::map<std::string, std::tuple<Both, Ostreamable, Tostr>>>(
          {{{"", std::make_tuple(Both(1), Ostreamable(2), Tostr(3))},
            {"foo", std::make_tuple(Both(10), Ostreamable(20), Tostr(30))}},
           {}})));
}

TEST_F(ToStringTest, Timestamps) {
  std::string s = toString(SteadyTimestamp::now());
  EXPECT_EQ(" ago", s.substr(s.size() - 4));
  EXPECT_EQ(strlen("[0:00:00.000 ago, 0:00:00.000 ago]"),
            toString(std::vector<SteadyTimestamp>(
                         {SteadyTimestamp::now(), SteadyTimestamp::now()}))
                .size());
}
