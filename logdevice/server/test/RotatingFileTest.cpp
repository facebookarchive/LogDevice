/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/RotatingFile.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"

using namespace facebook::logdevice;
using namespace testing;

/**
 * Mocking system calls adding ability to block threads inside the calls
 */
class RotatingFileSysMock : public RotatingFile {
 public:
  std::mutex open_mutex;
  Semaphore write_wait_semaphore;
  Semaphore write_enter_semaphore;
  bool waiting_open{false};
  std::atomic<int> waiting_writers{0};
  ~RotatingFileSysMock() override {}
  MOCK_METHOD3(open_mock, int(const char*, int, mode_t));
  MOCK_METHOD3(write_mock, ssize_t(int, const void*, size_t));

 protected:
  int open_(const char* path, int flags, mode_t mode) override {
    std::lock_guard<std::mutex> _(open_mutex);
    waiting_open = false;
    return open_mock(path, flags, mode);
  }
  ssize_t write_(int fd, const void* buf, size_t size) override {
    waiting_writers.fetch_add(1);
    write_enter_semaphore.post();
    write_wait_semaphore.wait();
    waiting_writers.fetch_add(-1);
    return write_mock(fd, buf, size);
  }
};

/**
 * Testing basic functionality of open/write/reopen from a single thread
 */
TEST(RotatingFileTest, Basic) {
  RotatingFileSysMock file;
  const char* path = "test_path";
  std::string test_string("Testing log write");
  // Expect nothing writen before file open
  EXPECT_EQ(-1, file.write(test_string.c_str(), test_string.size()));
  int flags = 1;
  int descriptor = -1;
  int mode = 0;
  // Expect call to ::open expect descriptor (error) propagated correctly
  EXPECT_CALL(file, open_mock(StrEq(path), flags, mode))
      .Times(Exactly(1))
      .WillOnce(Return(descriptor));
  EXPECT_EQ(descriptor, file.open(path, flags, mode));

  // Expect nothing writen since open failed (negative descriptor)
  EXPECT_EQ(-1, file.write(test_string.c_str(), test_string.size()));
  descriptor = 15;

  // Expect call to ::open expect descriptor propagated correctly
  EXPECT_CALL(file, open_mock(StrEq(path), flags, mode))
      .Times(Exactly(1))
      .WillOnce(Return(descriptor));
  EXPECT_EQ(descriptor, file.open(path, flags, mode));

  // Expect call to ::write and correct ammount of data writen
  EXPECT_CALL(
      file, write_mock(descriptor, test_string.c_str(), test_string.size()))
      .Times(Exactly(1))
      .WillOnce(Return(test_string.size()));
  file.write_wait_semaphore.post();
  EXPECT_EQ(
      test_string.size(), file.write(test_string.c_str(), test_string.size()));
}
/*
 * Testing that starting a reopen operation does not affect writes.
 */
TEST(RotatingFileTest, RotateNotBlockingWrites) {
  RotatingFileSysMock file;
  const char* path = "test_path";
  std::string test_string("Testing log write");
  int flags = O_APPEND;
  int descriptor = 15;
  int new_descriptor = 16;
  int mode = 0;
  EXPECT_CALL(file, open_mock(StrEq(path), flags, mode))
      .Times(Exactly(2))
      .WillOnce(Return(descriptor))
      .WillOnce(Return(new_descriptor));
  EXPECT_EQ(descriptor, file.open(path, flags, mode));
  std::thread reopen_thread;
  {
    std::lock_guard<std::mutex> _(file.open_mutex);
    file.waiting_open = true;
    auto reopen_fun = [&]() { EXPECT_EQ(new_descriptor, file.reopen()); };
    std::thread t(reopen_fun);
    reopen_thread.swap(t);
    const int WRITE_COUNT = 10;
    EXPECT_CALL(
        file, write_mock(descriptor, test_string.c_str(), test_string.size()))
        .Times(Exactly(WRITE_COUNT))
        .WillRepeatedly(Return(test_string.size()));
    for (auto i = 0; i < WRITE_COUNT; ++i) {
      file.write_wait_semaphore.post();
      EXPECT_EQ(test_string.size(),
                file.write(test_string.c_str(), test_string.size()));
    }
    ASSERT_TRUE(file.waiting_open);
  }
  reopen_thread.join();
  ASSERT_FALSE(file.waiting_open);
}
/**
 * Testing that multiple writes can concurrently end up inside write system
 * call and that this does not affect reopen operation.
 */
TEST(RotatingFileTest, ConcurentWrites) {
  RotatingFileSysMock file;
  const char* path = "test_path";
  std::string test_string("Testing log write");
  int flags = O_APPEND;
  int descriptor = 15;
  int new_descriptor = 16;
  int mode = 0;
  EXPECT_CALL(file, open_mock(StrEq(path), flags, mode))
      .Times(Exactly(2))
      .WillOnce(Return(descriptor))
      .WillOnce(Return(new_descriptor));
  EXPECT_EQ(descriptor, file.open(path, flags, mode));
  const int WRITE_COUNT = 10;
  std::thread reopen_thread;
  std::vector<std::thread> write_threads;
  {
    std::lock_guard<std::mutex> _(file.open_mutex);
    file.waiting_open = true;
    auto reopen_fun = [&]() { EXPECT_EQ(new_descriptor, file.reopen()); };

    EXPECT_CALL(
        file, write_mock(descriptor, test_string.c_str(), test_string.size()))
        .Times(Exactly(WRITE_COUNT))
        .WillRepeatedly(Return(test_string.size()));
    auto write_function = [&]() {
      EXPECT_EQ(test_string.size(),
                file.write(test_string.c_str(), test_string.size()));
    };
    while (write_threads.size() < WRITE_COUNT) {
      write_threads.emplace_back(write_function);
    }
    for (auto i = 0; i < WRITE_COUNT; ++i) {
      file.write_enter_semaphore.wait();
    }
    EXPECT_EQ(WRITE_COUNT, file.waiting_writers.load());
    std::thread t(reopen_fun);
    reopen_thread.swap(t);
    ASSERT_TRUE(file.waiting_open);
  }

  reopen_thread.join();
  ASSERT_FALSE(file.waiting_open);
  EXPECT_EQ(WRITE_COUNT, file.waiting_writers.load());
  for (auto i = 0; i < WRITE_COUNT; ++i) {
    file.write_wait_semaphore.post();
  }
  for (auto& th : write_threads) {
    th.join();
  }
  EXPECT_EQ(0, file.waiting_writers.load());
}
