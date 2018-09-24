/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <condition_variable>
#include <mutex>

#include <folly/Conv.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/NodesConfigStore.h"
#include "logdevice/common/test/InMemNodesConfigStore.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::membership;

using version_t = NodesConfigStore::version_t;

namespace {
const std::string kFoo{"/foo"};
const std::string kBar{"/bar"};
std::unique_ptr<NodesConfigStore> store{nullptr};

const std::string kValue{"value"};
const std::string kVersion{"version"};

struct TestEntry {
  explicit TestEntry(version_t version, std::string value)
      : version_(version), value_(std::move(value)) {}

  explicit TestEntry(version_t::raw_type version, std::string value)
      : version_(version), value_(std::move(value)) {}

  static TestEntry fromSerialized(folly::StringPiece buf) {
    auto d = folly::parseJson(buf);
    version_t version{
        folly::to<typename version_t::raw_type>(d.at(kVersion).getString())};
    return TestEntry{version, d.at(kValue).getString()};
  }

  std::string serialize() const {
    folly::dynamic d = folly::dynamic::object(kValue, value_)(
        kVersion, folly::to<std::string>(version_.val()));
    return folly::toJson(d);
  }

  version_t version() const {
    return version_;
  }

  std::string value() const {
    return value_;
  }

 private:
  version_t version_;
  std::string value_;

  friend bool operator==(const TestEntry& lhs, const TestEntry& rhs);
};

bool operator==(const TestEntry& lhs, const TestEntry& rhs) {
  return lhs.version_ == rhs.version_ && lhs.value_ == rhs.value_;
}

version_t extractVersionFn(folly::StringPiece buf) {
  return TestEntry::fromSerialized(buf).version();
}

void runBasicTests(std::unique_ptr<NodesConfigStore> store) {
  Status status_out;
  std::string value_out{};

  // no config stored yet
  EXPECT_NE(0, store->getConfig(kFoo, [](Status status, std::string) {
    EXPECT_EQ(Status::NOTFOUND, status);
  }));
  EXPECT_NE(0, store->getConfigSync(kFoo, &status_out, &value_out));
  EXPECT_EQ(Status::NOTFOUND, status_out);

  // initial write
  EXPECT_EQ(
      0,
      store->updateConfigSync(
          kFoo, &status_out, TestEntry{10, "foo123"}.serialize(), folly::none));
  EXPECT_EQ(Status::OK, status_out);

  EXPECT_EQ(0, store->getConfigSync(kFoo, &status_out, &value_out));
  EXPECT_EQ(Status::OK, status_out);
  EXPECT_EQ(TestEntry(10, "foo123"), TestEntry::fromSerialized(value_out));

  EXPECT_NE(0, store->getConfigSync(kBar, &status_out, &value_out));
  EXPECT_EQ(Status::NOTFOUND, status_out);

  // update: blind overwrite
  EXPECT_EQ(
      0,
      store->updateConfigSync(
          kFoo, &status_out, TestEntry{12, "foo456"}.serialize(), folly::none));
  EXPECT_EQ(Status::OK, status_out);
  EXPECT_EQ(0, store->getConfigSync(kFoo, &status_out, &value_out));
  EXPECT_EQ(Status::OK, status_out);
  auto e = TestEntry::fromSerialized(value_out);
  EXPECT_EQ(TestEntry(12, "foo456"), e);

  // update: conditional update
  MembershipVersion::Type prev_version{e.version().val() - 1};
  MembershipVersion::Type curr_version{e.version().val()};
  MembershipVersion::Type next_version{e.version().val() + 1};
  version_t version_out = MembershipVersion::EMPTY_VERSION;
  value_out = "";
  EXPECT_NE(
      0,
      store->updateConfigSync(kFoo,
                              &status_out,
                              TestEntry{next_version, "foo789"}.serialize(),
                              prev_version,
                              &version_out,
                              &value_out));
  EXPECT_EQ(Status::VERSION_MISMATCH, status_out);
  EXPECT_EQ(curr_version, version_out);
  EXPECT_EQ(TestEntry(12, "foo456"), TestEntry::fromSerialized(value_out));
  EXPECT_NE(
      0,
      store->updateConfig(
          kFoo,
          TestEntry{next_version, "foo789"}.serialize(),
          next_version,
          [curr_version](Status status, version_t version, std::string value) {
            EXPECT_EQ(Status::VERSION_MISMATCH, status);
            EXPECT_EQ(curr_version, version);
            EXPECT_EQ(
                TestEntry(12, "foo456"), TestEntry::fromSerialized(value));
          }));

  EXPECT_EQ(
      0,
      store->updateConfigSync(kFoo,
                              &status_out,
                              TestEntry{next_version, "foo789"}.serialize(),
                              curr_version));
  EXPECT_EQ(Status::OK, status_out);
  EXPECT_EQ(0, store->getConfig(kFoo, [&](Status status, std::string value) {
    EXPECT_EQ(Status::OK, status);
    EXPECT_EQ(TestEntry(next_version, "foo789"),
              TestEntry::fromSerialized(std::move(value)));
  }));
}

void runMultiThreadedTests(std::unique_ptr<NodesConfigStore> store) {
  constexpr size_t kNumThreads = 10;
  constexpr size_t kIter = 1000;
  std::array<std::thread, kNumThreads> threads;
  std::atomic<uint64_t> successCnt{0};

  std::condition_variable cv;
  std::mutex m;
  bool start = false;

  auto f = [&]() {
    std::unique_lock<std::mutex> lk{m};
    cv.wait(lk, [&]() { return start; });

    version_t base_version{0};
    for (uint64_t k = 0; k < kIter; ++k) {
      version_t next_version{base_version.val() + 1};
      store->updateConfig(
          kFoo,
          TestEntry{next_version, "foo" + folly::to<std::string>(k + 1)}
              .serialize(),
          base_version,
          [&base_version, &successCnt](
              Status status, version_t new_version, std::string value) {
            if (status == Status::OK) {
              successCnt++;
            } else {
              EXPECT_EQ(Status::VERSION_MISMATCH, status);
              auto entry = TestEntry::fromSerialized(value);
              EXPECT_GT(entry.version(), base_version);
            }
            base_version = new_version;
          });
    }
  };
  for (auto i = 0; i < kNumThreads; ++i) {
    threads[i] = std::thread(f);
  }

  // write version 0 (i.e., ~provision)
  Status status_out;
  store->updateConfigSync(
      kFoo, &status_out, TestEntry{0, "foobar"}.serialize(), folly::none);
  ASSERT_EQ(Status::OK, status_out);

  {
    std::lock_guard<std::mutex> g{m};
    start = true;
  }
  cv.notify_all();
  for (auto& t : threads) {
    t.join();
  }

  std::string value_out;
  store->getConfigSync(kFoo, &status_out, &value_out);
  EXPECT_EQ(TestEntry(kIter, "foo" + folly::to<std::string>(kIter)),
            TestEntry::fromSerialized(std::move(value_out)));
  EXPECT_EQ(kIter, successCnt.load());
}
} // namespace

TEST(NodesConfigStore, basic) {
  runBasicTests(std::make_unique<InMemNodesConfigStore>(extractVersionFn));
}

TEST(NodesConfigStore, basicMT) {
  runMultiThreadedTests(
      std::make_unique<InMemNodesConfigStore>(extractVersionFn));
}
