// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/PayloadHolder.h"

#include <event2/buffer.h>
#include <gtest/gtest.h>

#include "logdevice/common/BatchedBufferDisposer.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/EventLoopHandle.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/libevent/compat.h"
using namespace ::testing;

using namespace facebook::logdevice;

TEST(PayloadHolderTest, Simple) {
  EventLoopHandle handle(new EventLoop("",
                                       ThreadID::Type::UNKNOWN_EVENT_LOOP,
                                       /* capacity */ 2000,
                                       /* requests per iteration */ 1));
  auto ev_loop = handle.get();
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
