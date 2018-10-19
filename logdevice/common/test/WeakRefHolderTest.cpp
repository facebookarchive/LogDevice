/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WeakRefHolder.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

TEST(WeakRefHolder, Simple) {
  struct Object {
    Object() : holder(this) {}
    WeakRefHolder<Object> holder;
  };
  auto object = std::make_unique<Object>();
  {
    WeakRefHolder<Object>::Ref ref = object->holder.ref();
    ASSERT_TRUE(ref);
    WeakRefHolder<Object>::Ref ref2 = std::move(ref);
    ASSERT_TRUE(ref2);
    WeakRefHolder<Object>::Ref ref3 = ref2;
    ASSERT_TRUE(ref3);
  }
  {
    WeakRefHolder<Object>::Ref ref = object->holder.ref();
    ASSERT_TRUE(ref);
    object.reset();
    ASSERT_FALSE(ref);
    ASSERT_EQ(nullptr, ref.get());
  }
  {
    WeakRefHolder<Object>::Ref ref;
    ASSERT_FALSE(ref);
    ASSERT_EQ(nullptr, ref.get());
  }

  // Default constructor of Ref should create an invalid ref.
  WeakRefHolder<Object>::Ref ref;
  ASSERT_FALSE(ref);
}

}} // namespace facebook::logdevice
