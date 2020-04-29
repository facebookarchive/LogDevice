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
#include "logdevice/common/test/MockZookeeperClient.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/test/ZookeeperClientInMemory.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership;
using mutation_callback_t = FileBasedVersionedConfigStore::mutation_callback_t;

using version_t = NodesConfigurationStore::version_t;

using testing::_;

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
                    kConfigKey,
                    TestEntry::extractVersionFn,
                    std::move(z),
                    /* max_retries= */ 3),
                /* initialWrite = */ false);
}

TEST(NodesConfigurationStore, zk_basicMT) {
  auto z = std::make_unique<ZookeeperClientInMemory>(
      "unused quorum",
      ZookeeperClientInMemory::state_map_t{
          {kConfigKey,
           {TestEntry{0, "initValue"}.serialize(), zk::Stat{.version_ = 4}}}});
  runMultiThreadedTests(std::make_unique<ZookeeperNodesConfigurationStore>(
      kConfigKey,
      TestEntry::extractVersionFn,
      std::move(z),
      /* max_retries= */ 3));
}

TEST(NodesConfigurationStore, zk_basicRMWCreateIfNotExist) {
  auto z = std::make_unique<ZookeeperClientInMemory>(
      "unused quorum", ZookeeperClientInMemory::state_map_t{});
  runReadModifyWriteCreateIfNotExistTest(
      std::make_unique<ZookeeperNodesConfigurationStore>(
          kConfigKey,
          TestEntry::extractVersionFn,
          std::move(z),
          /* max_retries= */ 3));
}

// A test that mocks all of the interactions between the ZKVCS and ZK API and
// injects retryable errors on every interaction and makes sure that the VCS
// reties the operations.
TEST(NodesConfigurationStore, zk_retryTransientErrors) {
  // Utilities to build mock callbacks responding with a certain status
  const auto build_read_cb =
      [](int rc, std::string data = "", zk::Stat stat = zk::Stat{}) {
        return [=](std::string, ZookeeperClientBase::data_callback_t cb) {
          cb(rc, data, stat);
        };
      };
  const auto build_sync_cb = [](int rc) {
    return [=](ZookeeperClientBase::sync_callback_t cb) { cb(rc); };
  };
  const auto build_create_cb = [](int rc) {
    return [=](std::string path,
               std::string,
               ZookeeperClientBase::create_callback_t cb,
               std::vector<zk::ACL>,
               int32_t) { cb(rc, path); };
  };

  const auto build_stat_cb = [](int rc) {
    return [=](std::string path,
               std::string,
               ZookeeperClientBase::stat_callback_t cb,
               zk::version_t) { cb(rc, zk::Stat{}); };
  };

  static const std::string data1 = TestEntry{10, "foo123"}.serialize();
  static const std::string data2 = TestEntry{11, "foo345"}.serialize();

  {
    ld_info("getConfig tests");

    auto z = std::make_unique<MockZookeeperClient>();
    EXPECT_CALL(*z, getData(kConfigKey, _))
        .WillOnce(testing::Invoke(build_read_cb(ZCONNECTIONLOSS)))
        .WillOnce(testing::Invoke(build_read_cb(ZOPERATIONTIMEOUT)))
        .WillOnce(testing::Invoke(build_read_cb(ZSESSIONEXPIRED)))
        .WillOnce(testing::Invoke(build_read_cb(ZOK, data1)));

    auto vcs = std::make_unique<ZookeeperNodesConfigurationStore>(
        kConfigKey,
        TestEntry::extractVersionFn,
        std::move(z),
        /* max_retries= */ 3);
    std::string ret;
    EXPECT_EQ(Status::OK, vcs->getConfigSync(&ret));
    EXPECT_EQ(data1, ret);
  }

  {
    ld_info("getLatestConfig tests");

    auto z = std::make_unique<MockZookeeperClient>();

    testing::InSequence seq;
    EXPECT_CALL(*z, sync(_))
        .WillOnce(testing::Invoke(build_sync_cb(ZCONNECTIONLOSS)))
        .WillOnce(testing::Invoke(build_sync_cb(ZOK)));

    EXPECT_CALL(*z, getData(kConfigKey, _))
        .WillOnce(testing::Invoke(build_read_cb(ZCONNECTIONLOSS)));

    EXPECT_CALL(*z, sync(_)).WillOnce(testing::Invoke(build_sync_cb(ZOK)));

    EXPECT_CALL(*z, getData(kConfigKey, _))
        .WillOnce(testing::Invoke(build_read_cb(ZOK, data1)));

    auto vcs = std::make_unique<ZookeeperNodesConfigurationStore>(
        kConfigKey,
        TestEntry::extractVersionFn,
        std::move(z),
        /* max_retries= */ 3);
    folly::Baton<> b;
    vcs->getLatestConfig([&b](auto st, auto data) {
      EXPECT_EQ(Status::OK, st);
      EXPECT_EQ(data1, data);
      b.post();
    });
    b.wait();
  }

  {
    ld_info("updateConfig tests");

    auto z = std::make_unique<MockZookeeperClient>();

    testing::InSequence seq;

    EXPECT_CALL(*z, getData(kConfigKey, _))
        .WillOnce(testing::Invoke(build_read_cb(ZCONNECTIONLOSS)))
        .WillOnce(testing::Invoke(build_read_cb(ZOK, data1, zk::Stat{10})));

    EXPECT_CALL(*z, setData(kConfigKey, data2, _, 10))
        .WillOnce(testing::Invoke(build_stat_cb(ZCONNECTIONLOSS)))
        .WillOnce(testing::Invoke(build_stat_cb(ZOK)));

    auto vcs = std::make_unique<ZookeeperNodesConfigurationStore>(
        kConfigKey,
        TestEntry::extractVersionFn,
        std::move(z),
        /* max_retries= */ 3);
    EXPECT_EQ(
        Status::OK,
        vcs->updateConfigSync(data2,
                              VersionedConfigStore::Condition::overwrite(),
                              nullptr,
                              nullptr));
  }

  {
    ld_info("readModifyWrite tests");

    auto z = std::make_unique<MockZookeeperClient>();

    testing::InSequence seq;

    EXPECT_CALL(*z, getData(kConfigKey, _))
        .WillOnce(testing::Invoke(build_read_cb(ZCONNECTIONLOSS)))
        .WillOnce(testing::Invoke(build_read_cb(ZNONODE, data1)));

    EXPECT_CALL(*z, createWithAncestors(kConfigKey, data2, _, _, _))
        .WillOnce(testing::Invoke(build_create_cb(ZCONNECTIONLOSS)))
        .WillOnce(testing::Invoke(build_create_cb(ZOK)));

    auto vcs = std::make_unique<ZookeeperNodesConfigurationStore>(
        kConfigKey,
        TestEntry::extractVersionFn,
        std::move(z),
        /* max_retries= */ 3);
    folly::Baton<> b;
    vcs->readModifyWriteConfig(
        [](auto val_opt) { return std::make_pair(Status::OK, data2); },
        [&b](Status st, version_t, std::string) {
          EXPECT_EQ(Status::OK, st);
          b.post();
        });
    b.wait();
  }
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
