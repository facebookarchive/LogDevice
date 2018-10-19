/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/util.h"

#include <gtest/gtest.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"

using namespace facebook::logdevice;

TEST(UtilTest, LogIDToString) {
  EXPECT_EQ("LOGID_INVALID", toString(LOGID_INVALID));
  EXPECT_EQ("LOGID_INVALID2", toString(LOGID_INVALID2));
  EXPECT_EQ("L123", toString(MetaDataLog::dataLogID(logid_t{123})));
  EXPECT_EQ("M4567", toString(MetaDataLog::metaDataLogID(logid_t{4567})));
}

TEST(UtilTest, WorkerName) {
  EXPECT_EQ("WG0", Worker::getName(WorkerType::GENERAL, worker_id_t{0}));
  EXPECT_EQ(
      "WF1", Worker::getName(WorkerType::FAILURE_DETECTOR, worker_id_t{1}));
  EXPECT_EQ("WB23", Worker::getName(WorkerType::BACKGROUND, worker_id_t{23}));
  EXPECT_EQ("W?-1", Worker::getName(WorkerType::MAX, worker_id_t{-1}));
}

// Test a LOGDEVICE_STRONG_TYPEDEF that hasn't been overridden.

LOGDEVICE_STRONG_TYPEDEF(uint32_t, test_typedef_t);

TEST(UtilTest, DefaultStrongTypedefToString) {
  EXPECT_EQ("0", toString(test_typedef_t{0}));
  EXPECT_EQ("1", toString(test_typedef_t{1}));
  EXPECT_EQ("23", toString(test_typedef_t{23}));
  EXPECT_EQ("987654", toString(test_typedef_t{987654}));
}

TEST(UtilTest, SetIfNotNull) {
  {
    int* p = nullptr;
    set_if_not_null(p, 42);
    EXPECT_EQ(nullptr, p);

    int x = 0;
    p = &x;
    set_if_not_null(p, 42);
    EXPECT_NE(nullptr, p);
    EXPECT_EQ(42, x);

    // const
    const int y = 43;
    set_if_not_null(p, y);
    EXPECT_EQ(y, x);

    int* const q = &x;
    set_if_not_null(q, 123);
    EXPECT_EQ(123, x);
  }

  {
    std::string s;
    std::string t{"bar"};
    auto p = &s;
    set_if_not_null(p, std::move(t));
    EXPECT_EQ("bar", s);
  }

  {
    auto p = std::make_shared<int>(123);
    auto q = std::make_shared<int>(456);
    std::shared_ptr<int>* p_ptr = &p;
    set_if_not_null(p_ptr, std::move(p)); // self-assignment
    EXPECT_NE(nullptr, p_ptr);
    EXPECT_NE(nullptr, *p_ptr);
    EXPECT_EQ(123, **p_ptr);

    set_if_not_null(p_ptr, q);
    EXPECT_NE(nullptr, p_ptr);
    EXPECT_NE(nullptr, *p_ptr);
    EXPECT_EQ(456, **p_ptr);
  }
}
