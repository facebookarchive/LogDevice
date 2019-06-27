// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/PayloadHolder.h"

#include <event2/buffer.h>
#include <folly/container/Array.h>
#include <gtest/gtest.h>

#include "logdevice/common/BatchedBufferDisposer.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/libevent/compat.h"
using namespace ::testing;

using namespace facebook::logdevice;

TEST(PayloadHolderTest, Simple) {
  auto ev_loop = std::make_unique<EventLoop>(
      "",
      ThreadID::Type::UNKNOWN_EVENT_LOOP,
      /* capacity */ 2000,
      /* enable_priority_queues */ true,
      /* requests per iteration */ folly::make_array<uint32_t>(1, 1, 1));

  Semaphore sem0, sem1;
  ev_loop->add([&sem0] {
    ThreadID::set(ThreadID::UNKNOWN_WORKER, "");
    sem0.wait();
    for (size_t i = 0; i < 1000; ++i) {
      auto evbuffer = LD_EV(evbuffer_new)();
      ld_check(evbuffer);
      auto holder = std::make_shared<PayloadHolder>(evbuffer);
    }
    // Should post 1000 buffers into the same eventloop for deletion.
  });

  ev_loop->add([&ev_loop, &sem1] {
    auto& disposer = ev_loop->disposer();
    EXPECT_TRUE(disposer.scheduledCleanup());
    EXPECT_EQ(1000, disposer.numBuffers());
    // Post a event which will be executed after all buffers are deleted.
    ev_loop->add([&sem1] { sem1.post(); });
  });

  sem0.post();
  sem1.wait();
  auto& disposer = ev_loop->disposer();
  EXPECT_FALSE(disposer.scheduledCleanup());
  EXPECT_EQ(0, disposer.numBuffers());
}
