/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"

#include <condition_variable>
#include <mutex>
#include <ostream>

#include <folly/Conv.h>
#include <folly/experimental/TestUtil.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/FileBasedNodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ZookeeperNodesConfigurationStore.h"
#include "logdevice/common/test/InMemNodesConfigurationStore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/test/ZookeeperClientInMemory.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership;

using version_t = NodesConfigurationStore::version_t;

namespace {
std::unique_ptr<NodesConfigurationStore> store{nullptr};

const std::string kConfigKey{"/foo"};

class NodesConfigurationStoreTest : public ::testing::Test {
 public:
  // temporary dir for file baesd store test
  std::unique_ptr<folly::test::TemporaryDirectory> temp_dir_;

  explicit NodesConfigurationStoreTest()
      : temp_dir_(createTemporaryDir("NodesConfigurationStoreTest",
                                     /*keep_data*/ false)) {
    ld_check(temp_dir_ != nullptr);
  }

  std::unique_ptr<NodesConfigurationStore>
  createFileBasedStore(NodesConfigurationStore::extract_version_fn f) {
    return std::make_unique<FileBasedNodesConfigurationStore>(
        kConfigKey, temp_dir_->path().string(), std::move(f));
  }
};

void checkAndResetBaton(folly::Baton<>& b) {
  using namespace std::chrono_literals;
  // The baton should have be "posted" way sooner. We do this so that if the
  // baton is not posted, and the program hangs, we get a clear timeout message
  // instead of waiting for the test to time out.
  EXPECT_TRUE(b.try_wait_for(1s));
  b.reset();
}

void runBasicTests(std::unique_ptr<NodesConfigurationStore> store,
                   bool initialWrite = true) {
  folly::Baton<> b;
  std::string value_out{};

  if (initialWrite) {
    // no config stored yet
    store->getConfig([&b](Status status, std::string) {
      EXPECT_EQ(Status::NOTFOUND, status);
      b.post();
    });
    checkAndResetBaton(b);

    store->getConfig(
        [&b](Status status, std::string) {
          EXPECT_EQ(Status::NOTFOUND, status);
          b.post();
        },
        /*base_version*/ version_t(100));
    checkAndResetBaton(b);

    EXPECT_EQ(Status::NOTFOUND, store->getConfigSync(&value_out));

    // initial write
    EXPECT_EQ(Status::OK,
              store->updateConfigSync(
                  TestEntry{10, "foo123"}.serialize(), folly::none));

    EXPECT_EQ(Status::OK, store->getConfigSync(&value_out));
    EXPECT_EQ(TestEntry(10, "foo123"), TestEntry::fromSerialized(value_out));

    store->getConfig(
        [&b](Status status, std::string) {
          EXPECT_EQ(Status::UPTODATE, status);
          b.post();
        },
        /*base_version*/ version_t(10));
    checkAndResetBaton(b);

    store->getConfig(
        [&b](Status status, std::string str) {
          EXPECT_EQ(Status::OK, status);
          EXPECT_EQ(TestEntry(10, "foo123"), TestEntry::fromSerialized(str));
          b.post();
        },
        /*base_version*/ version_t(9));
    checkAndResetBaton(b);
  }

  // update: blind overwrite
  EXPECT_EQ(Status::OK,
            store->updateConfigSync(
                TestEntry{12, "foo456"}.serialize(), folly::none));
  EXPECT_EQ(Status::OK, store->getConfigSync(&value_out));
  auto e = TestEntry::fromSerialized(value_out);
  EXPECT_EQ(TestEntry(12, "foo456"), e);

  // update: conditional update
  MembershipVersion::Type prev_version{e.version().val() - 1};
  MembershipVersion::Type curr_version{e.version().val()};
  MembershipVersion::Type next_version{e.version().val() + 1};
  version_t version_out = MembershipVersion::EMPTY_VERSION;
  value_out = "";
  EXPECT_EQ(
      Status::VERSION_MISMATCH,
      store->updateConfigSync(TestEntry{next_version, "foo789"}.serialize(),
                              prev_version,
                              &version_out,
                              &value_out));
  EXPECT_EQ(curr_version, version_out);
  EXPECT_EQ(TestEntry(12, "foo456"), TestEntry::fromSerialized(value_out));
  store->updateConfig(

      TestEntry{next_version, "foo789"}.serialize(),
      next_version,
      [&b, curr_version](Status status, version_t version, std::string value) {
        EXPECT_EQ(Status::VERSION_MISMATCH, status);
        EXPECT_EQ(curr_version, version);
        EXPECT_EQ(TestEntry(12, "foo456"), TestEntry::fromSerialized(value));
        b.post();
      });
  checkAndResetBaton(b);

  EXPECT_EQ(Status::OK,
            store->updateConfigSync(
                TestEntry{next_version, "foo789"}.serialize(), curr_version));
  store->getConfig([&b, next_version](Status status, std::string value) {
    EXPECT_EQ(Status::OK, status);
    EXPECT_EQ(TestEntry(next_version, "foo789"),
              TestEntry::fromSerialized(std::move(value)));
    b.post();
  });
  checkAndResetBaton(b);

  store->getLatestConfig([&b, next_version](Status status, std::string value) {
    EXPECT_EQ(Status::OK, status);
    EXPECT_EQ(TestEntry(next_version, "foo789"),
              TestEntry::fromSerialized(std::move(value)));
    b.post();
  });
  checkAndResetBaton(b);
}

void runMultiThreadedTests(std::unique_ptr<NodesConfigurationStore> store) {
  constexpr size_t kNumThreads = 5;
  constexpr size_t kIter = 30;
  std::array<std::thread, kNumThreads> threads;
  std::atomic<uint64_t> successCnt{0};

  std::condition_variable cv;
  std::mutex m;
  bool start = false;

  auto f = [&](int thread_idx) {
    {
      std::unique_lock<std::mutex> lk{m};
      cv.wait(lk, [&]() { return start; });
    }

    LOG(INFO) << folly::sformat("thread {} started...", thread_idx);
    folly::Baton<> b;
    for (uint64_t k = 0; k < kIter; ++k) {
      version_t base_version{k};
      version_t next_version{k + 1};
      store->updateConfig(

          TestEntry{next_version, "foo" + folly::to<std::string>(k + 1)}
              .serialize(),
          base_version,
          [&b, &base_version, &successCnt](
              Status status, version_t new_version, std::string value) {
            if (status == Status::OK) {
              successCnt++;
            } else {
              EXPECT_EQ(Status::VERSION_MISMATCH, status);
              if (new_version != MembershipVersion::EMPTY_VERSION) {
                auto entry = TestEntry::fromSerialized(value);
                EXPECT_GT(entry.version(), base_version);
                EXPECT_GT(new_version, base_version);
              }
            }
            b.post();
          });
      // Note: if the check fails here, the thread dies but the test would
      // keep hanging unfortunately.
      checkAndResetBaton(b);
    }
  };
  for (auto i = 0; i < kNumThreads; ++i) {
    threads[i] = std::thread(std::bind(f, i));
  }

  // write version 0 (i.e., ~provision)
  ASSERT_EQ(
      Status::OK,
      store->updateConfigSync(TestEntry{0, "foobar"}.serialize(), folly::none));

  {
    std::lock_guard<std::mutex> g{m};
    start = true;
  }
  cv.notify_all();
  for (auto& t : threads) {
    t.join();
  }

  std::string value_out;
  EXPECT_EQ(Status::OK, store->getConfigSync(&value_out));
  EXPECT_EQ(TestEntry(kIter, "foo" + folly::to<std::string>(kIter)),
            TestEntry::fromSerialized(std::move(value_out)));
  EXPECT_EQ(kIter, successCnt.load());
}
} // namespace

TEST(NodesConfigurationStore, basic) {
  runBasicTests(std::make_unique<InMemNodesConfigurationStore>(
      kConfigKey, TestEntry::extractVersionFn));
}

TEST(NodesConfigurationStore, basicMT) {
  runMultiThreadedTests(std::make_unique<InMemNodesConfigurationStore>(
      kConfigKey, TestEntry::extractVersionFn));
}

TEST(NodesConfigurationStore, zk_basic) {
  auto z = std::make_unique<ZookeeperClientInMemory>(
      "unused quorum", ZookeeperClientInMemory::state_map_t{});
  runBasicTests(std::make_unique<ZookeeperNodesConfigurationStore>(
                    kConfigKey, TestEntry::extractVersionFn, std::move(z)),
                /* initialWrite = */ false);
}

TEST(NodesConfigurationStore, zk_basicMT) {
  auto z = std::make_unique<ZookeeperClientInMemory>(
      "unused quorum",
      ZookeeperClientInMemory::state_map_t{
          {kConfigKey,
           {TestEntry{0, "initValue"}.serialize(), zk::Stat{.version_ = 4}}}});
  runMultiThreadedTests(std::make_unique<ZookeeperNodesConfigurationStore>(
      kConfigKey, TestEntry::extractVersionFn, std::move(z)));
}

TEST_F(NodesConfigurationStoreTest, file_basic) {
  runBasicTests(createFileBasedStore(TestEntry::extractVersionFn));
}

TEST_F(NodesConfigurationStoreTest, file_basicMT) {
  runMultiThreadedTests(createFileBasedStore(TestEntry::extractVersionFn));
}
