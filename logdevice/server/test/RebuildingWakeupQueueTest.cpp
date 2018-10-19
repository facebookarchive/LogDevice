/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingWakeupQueue.h"

#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {

typedef std::chrono::milliseconds ms;

TEST(RebuildingWakeupQueueTest, Basic) {
  std::vector<logid_t> res;

  RebuildingWakeupQueue queue;
  queue.push(logid_t{1}, RecordTimestamp::from(ms{10042}));
  queue.push(logid_t{5}, RecordTimestamp::from(ms{10043}));
  queue.push(logid_t{3}, RecordTimestamp::from(ms{10342}));
  queue.push(logid_t{4}, RecordTimestamp::from(ms{10000}));

  // Initially, the queue's local window end is RecordTimestamp::min().
  // So calling pop() will return an empty array.
  ASSERT_TRUE(queue.sizeInsideWindow() == 0);
  ASSERT_TRUE(queue.sizeOutsideWindow() == 4);
  ASSERT_TRUE(queue.pop(10).empty());

  // Now let's move the window a little bit, but still not far enough.
  queue.advanceWindow(RecordTimestamp::from(ms{9999}));
  ASSERT_TRUE(queue.sizeInsideWindow() == 0);
  ASSERT_TRUE(queue.sizeOutsideWindow() == 4);
  ASSERT_TRUE(queue.pop(10).empty());
  ASSERT_EQ(RecordTimestamp::from(ms{10000}), queue.nextTimestamp());

  // Now, far enough so that we can wake up only one log (log 4).
  queue.advanceWindow(RecordTimestamp::from(ms{10000}));
  ASSERT_TRUE(queue.sizeInsideWindow() == 1);
  ASSERT_TRUE(queue.sizeOutsideWindow() == 3);
  res = queue.pop(10);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(logid_t{4}, res[0]);
  ASSERT_EQ(RecordTimestamp::from(ms{10042}), queue.nextTimestamp());

  // Now move the window a bit further so that log 1 can be woken up.
  queue.advanceWindow(RecordTimestamp::from(ms{10050}));

  // Schedule some more logs.
  queue.push(logid_t{6}, RecordTimestamp::from(ms{10045}));
  queue.push(logid_t{2}, RecordTimestamp::from(ms{10049}));
  queue.push(logid_t{7}, RecordTimestamp::from(ms{10044}));
  queue.push(logid_t{8}, RecordTimestamp::from(ms{10051}));
  queue.push(logid_t{9}, RecordTimestamp::from(ms{10046}));

  // We have 6 logs within the window (logs 1, 5, 7, 6, 9, 2).
  // Remove 9,8 and pick 3 logs to pick
  std::unordered_set<logid_t, logid_t::Hash> logs_to_remove;
  logs_to_remove.insert(logid_t(9));
  logs_to_remove.insert(logid_t(8));
  queue.remove(logs_to_remove);
  res = queue.pop(3);
  ASSERT_EQ(3, res.size());

  // The three woken up logs should be logs 1, 2 and 5. If the queue was only
  // considering the timestamp for the order with which logs are woken up, we
  // would expect logs 1, 5, 7 to be woken up. However, this queue tries to spit
  // logs with ids close together.
  ASSERT_EQ(logid_t{1}, res[0]);
  ASSERT_EQ(logid_t{2}, res[1]);
  ASSERT_EQ(logid_t{5}, res[2]);

  // Wake up the remaining two logs.
  ASSERT_TRUE(queue.sizeInsideWindow() == 2);
  ASSERT_TRUE(queue.sizeOutsideWindow() == 1);
  res = queue.pop(2);
  ASSERT_EQ(2, res.size());
  ASSERT_EQ(logid_t{6}, res[0]);
  ASSERT_EQ(logid_t{7}, res[1]);
  ASSERT_EQ(RecordTimestamp::from(ms{10342}), queue.nextTimestamp());

  // Move the window far enough to be able to wake up log 3 as well.
  queue.advanceWindow(RecordTimestamp::from(ms{10400}));
  ASSERT_TRUE(queue.sizeInsideWindow() == 1);
  ASSERT_TRUE(queue.sizeOutsideWindow() == 0);
  res = queue.pop(10);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(logid_t{3}, res[0]);
  ASSERT_TRUE(queue.empty());
}

}} // namespace facebook::logdevice
