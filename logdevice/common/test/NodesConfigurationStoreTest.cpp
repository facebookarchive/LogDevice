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
using mutation_callback_t = FileBasedVersionedConfigStore::mutation_callback_t;

using version_t = NodesConfigurationStore::version_t;

namespace {
std::unique_ptr<NodesConfigurationStore> store{nullptr};

const std::string kConfigKey{"/foo"};

class NodesConfigurationStoreTest : public ::testing::Test {
 public:
  // temporary dir for file baesd store test
  std::unique_ptr<TemporaryDirectory> temp_dir_;

  explicit NodesConfigurationStoreTest()
      : temp_dir_(std::make_unique<TemporaryDirectory>(
            "NodesConfigurationStoreTest")) {
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

  // TODO: Add tests for empty value configuration.(T52914072)

  if (initialWrite) {
    // Read modify write using readModifyWriteConfig : CANCEL after initial
    // read.
    auto mutation_func = [](folly::Optional<std::string> current_value)
        -> std::pair<Status, std::string> {
      EXPECT_EQ(current_value, folly::none);
      return std::make_pair(Status::CANCELLED, "");
    };
    store->readModifyWriteConfig(
        mutation_func,
        [&b](Status status, version_t /* version */, std::string /* value */) {
          EXPECT_EQ(Status::CANCELLED, status);
          b.post();
        });
    checkAndResetBaton(b);

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
                  TestEntry{10, "foo123"}.serialize(),
                  NodesConfigurationStore::Condition::createIfNotExists()));

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
  EXPECT_EQ(
      Status::OK,
      store->updateConfigSync(TestEntry{12, "foo456"}.serialize(),
                              NodesConfigurationStore::Condition::overwrite()));

  // A write with a createIfNotExists on an object that exists should fail with
  // a VERSION_MISMATCH
  EXPECT_EQ(Status::VERSION_MISMATCH,
            store->updateConfigSync(
                TestEntry{13, "foo456"}.serialize(),
                NodesConfigurationStore::Condition::createIfNotExists()));

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

  // Test Read-Modify-Write using readModifyWriteConfig.
  curr_version = next_version;
  next_version.val_ = curr_version.val_ + 1;
  auto mutation_func =
      [curr_version, next_version](folly::Optional<std::string> current_value) {
        EXPECT_EQ(TestEntry(curr_version, "foo789"),
                  TestEntry::fromSerialized(*current_value));
        return std::make_pair(
            Status::OK, TestEntry(next_version, "foo890").serialize());
      };

  store->readModifyWriteConfig(
      mutation_func,
      [&b, next_version](
          Status status, version_t version, std::string /* value */) {
        EXPECT_EQ(Status::OK, status);
        EXPECT_EQ(next_version, version);
        b.post();
      });
  checkAndResetBaton(b);

  store->getLatestConfig([&b, next_version](Status status, std::string value) {
    EXPECT_EQ(Status::OK, status);
    EXPECT_EQ(TestEntry(next_version, "foo890"),
              TestEntry::fromSerialized(std::move(value)));
    b.post();
  });
  checkAndResetBaton(b);

  curr_version = next_version;
  // Read modify write : Cancel after read.
  auto mutation_func2 =
      [curr_version](folly::Optional<std::string> current_value)
      -> std::pair<Status, std::string> {
    EXPECT_EQ(TestEntry(curr_version, "foo890"),
              TestEntry::fromSerialized(*current_value));
    return std::make_pair(Status::CANCELLED, "");
  };

  store->readModifyWriteConfig(
      mutation_func2,
      [&b](Status status, version_t /* version */, std::string /* value */) {
        EXPECT_EQ(Status::CANCELLED, status);
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
      store->updateConfigSync(TestEntry{0, "foobar"}.serialize(),
                              NodesConfigurationStore::Condition::overwrite()));

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

void runReadModifyWriteCreateIfNotExistTest(
    std::unique_ptr<NodesConfigurationStore> store) {
  folly::Baton<> value_read, value_written;
  ASSERT_EQ(E::NOTFOUND, store->getConfigSync(nullptr));

  std::thread t([&]() {
    store->readModifyWriteConfig(
        [&](auto) {
          value_read.post();
          value_written.wait();
          return std::make_pair<Status, std::string>(
              E::OK, TestEntry{0, "foobar"}.serialize());
        },
        [](auto st, auto, auto) { EXPECT_EQ(E::VERSION_MISMATCH, st); });
  });
  value_read.wait();

  EXPECT_EQ(
      Status::OK,
      store->updateConfigSync(TestEntry{0, "foobar2"}.serialize(),
                              NodesConfigurationStore::Condition::overwrite()));
  value_written.post();
  t.join();

  std::string value_out;
  EXPECT_EQ(Status::OK, store->getConfigSync(&value_out));
  EXPECT_EQ(
      TestEntry(0, "foobar2"), TestEntry::fromSerialized(std::move(value_out)));
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

TEST(NodesConfigurationStore, basicRMWCreateIfNotExist) {
  runReadModifyWriteCreateIfNotExistTest(
      std::make_unique<InMemNodesConfigurationStore>(
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

TEST(NodesConfigurationStore, zk_basicRMWCreateIfNotExist) {
  auto z = std::make_unique<ZookeeperClientInMemory>(
      "unused quorum", ZookeeperClientInMemory::state_map_t{});
  runReadModifyWriteCreateIfNotExistTest(
      std::make_unique<ZookeeperNodesConfigurationStore>(
          kConfigKey, TestEntry::extractVersionFn, std::move(z)));
}

TEST_F(NodesConfigurationStoreTest, file_basic) {
  runBasicTests(createFileBasedStore(TestEntry::extractVersionFn));
}

TEST_F(NodesConfigurationStoreTest, file_basicMT) {
  runMultiThreadedTests(createFileBasedStore(TestEntry::extractVersionFn));
}

TEST_F(NodesConfigurationStoreTest, file_basicRMWCreateIfNotExist) {
  runReadModifyWriteCreateIfNotExistTest(
      createFileBasedStore(TestEntry::extractVersionFn));
}
