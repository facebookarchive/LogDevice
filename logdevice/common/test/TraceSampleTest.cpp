/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/TraceSample.h"

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

TEST(TraceSampleTest, testSettersGetters) {
  TraceSample sample;
  sample.addIntValue("num", 10);
  sample.addNormalValue("str", "val");
  sample.addNormVectorValue("vec", {"elem"});
  sample.addMapValue("map", {{"itm", "val"}});
  sample.addSetValue("set", {"itm"});

  // Ints
  EXPECT_EQ(10, sample.getIntValue("num"));
  EXPECT_EQ(10, sample.getOptionalIntValue("num").value());
  EXPECT_TRUE(sample.isIntValueSet("num"));
  EXPECT_EQ(0, sample.getIntValue("not_found"));
  EXPECT_EQ(folly::none, sample.getOptionalIntValue("not_found"));
  EXPECT_FALSE(sample.isIntValueSet("not_found"));

  // Strings
  EXPECT_EQ("val", sample.getNormalValue("str"));
  EXPECT_EQ("val", sample.getOptionalNormalValue("str").value());
  EXPECT_TRUE(sample.isNormalValueSet("str"));
  EXPECT_EQ("", sample.getNormalValue("not_found"));
  EXPECT_EQ(folly::none, sample.getOptionalNormalValue("not_found"));
  EXPECT_FALSE(sample.isNormalValueSet("not_found"));

  // Vectors
  EXPECT_EQ(
      (std::vector<std::string>{"elem"}), sample.getNormVectorValue("vec"));
  EXPECT_EQ((std::vector<std::string>{"elem"}),
            sample.getOptionalNormVectorValue("vec").value());
  EXPECT_TRUE(sample.isNormVectorValueSet("vec"));
  EXPECT_EQ(
      (std::vector<std::string>{}), sample.getNormVectorValue("not_found"));
  EXPECT_EQ(folly::none, sample.getOptionalNormVectorValue("not_found"));
  EXPECT_FALSE(sample.isNormVectorValueSet("not_found"));

  // Maps
  EXPECT_EQ((std::map<std::string, std::string>{{"itm", "val"}}),
            sample.getMapValue("map"));
  EXPECT_EQ((std::map<std::string, std::string>{{"itm", "val"}}),
            sample.getOptionalMapValue("map").value());
  EXPECT_TRUE(sample.isMapValueSet("map"));
  EXPECT_EQ(
      (std::map<std::string, std::string>{}), sample.getMapValue("not_found"));
  EXPECT_EQ(folly::none, sample.getOptionalMapValue("not_found"));
  EXPECT_FALSE(sample.isMapValueSet("not_found"));

  // Sets
  EXPECT_EQ(std::set<std::string>{"itm"}, sample.getSetValue("set"));
  EXPECT_EQ(
      std::set<std::string>{"itm"}, sample.getOptionalSetValue("set").value());
  EXPECT_TRUE(sample.isSetValueSet("set"));
  EXPECT_EQ(std::set<std::string>{}, sample.getSetValue("not_found"));
  EXPECT_EQ(folly::none, sample.getOptionalSetValue("not_found"));
  EXPECT_FALSE(sample.isSetValueSet("not_found"));
}

TEST(TraceSampleTest, testReset) {
  TraceSample sample;
  sample.addIntValue("num", 10);
  sample.addNormalValue("str", "val");
  sample.addNormVectorValue("vec", {"elem"});
  sample.addMapValue("map", {{"itm", "val"}});
  sample.addSetValue("set", {"itm"});

  sample.reset();

  EXPECT_FALSE(sample.isIntValueSet("num"));
  EXPECT_FALSE(sample.isNormalValueSet("str"));
  EXPECT_FALSE(sample.isNormVectorValueSet("vec"));
  EXPECT_FALSE(sample.isMapValueSet("map"));
  EXPECT_FALSE(sample.isSetValueSet("set"));
}

}} // namespace facebook::logdevice
