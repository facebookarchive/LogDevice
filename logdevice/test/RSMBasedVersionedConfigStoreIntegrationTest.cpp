/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/RSMBasedVersionedConfigStore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class RSMBasedVersionedConfigStoreIntegrationTest : public ::testing::Test {
 public:
  void SetUp() override {
    cluster_ = IntegrationTestUtils::ClusterFactory().setNumLogs(1).create(4);
    client_ = cluster_->createClient();
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;
  std::unique_ptr<RSMBasedVersionedConfigStore> vcs_;
};

TEST_F(RSMBasedVersionedConfigStoreIntegrationTest,
       UpdatesAndGetsAfterRestart) {
  cluster_->waitForRecovery();

  ClientImpl* client = dynamic_cast<ClientImpl*>(client_.get());

  auto extract_function = [](folly::StringPiece s) {
    return VersionedConfigStore::version_t(s.back());
  };

  vcs_ = std::make_unique<RSMBasedVersionedConfigStore>(
      logid_t(1),
      extract_function,
      &client->getProcessor(),
      std::chrono::milliseconds(100));

  std::string value;
  Status status;
  do {
    status = vcs_->getConfigSync("key", &value);
    /* sleep override */
    usleep(5000);
  } while (status == Status::AGAIN);
  EXPECT_EQ(Status::NOTFOUND, status);

  status = vcs_->updateConfigSync(
      "key1", "value1", VersionedConfigStore::Condition::overwrite());
  EXPECT_EQ(Status::OK, status);
  status = vcs_->getConfigSync("key1", &value);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value1", value);

  status = vcs_->updateConfigSync(
      "key2", "value2", VersionedConfigStore::Condition::overwrite());
  EXPECT_EQ(Status::OK, status);

  status = vcs_->updateConfigSync(
      "key2", "value2", VersionedConfigStore::Condition::overwrite());
  // The version is the last character of the value. In this case, we try
  // to write the same version, which causes version mismatch.
  EXPECT_EQ(Status::VERSION_MISMATCH, status);

  status = vcs_->getConfigSync("key2", &value);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value2", value);

  folly::Baton<> cb_baton;
  auto cb = [&cb_baton](Status status, auto, auto) {
    EXPECT_EQ(Status::OK, status);
    cb_baton.post();
  };
  vcs_->updateConfig("key1",
                     "value3",
                     VersionedConfigStore::Condition::overwrite(),
                     std::move(cb));
  cb_baton.wait();

  folly::Baton<> get_cb_baton;
  auto get_cb = [&get_cb_baton](Status status, std::string value) {
    ASSERT_EQ(Status::OK, status);
    EXPECT_EQ("value3", value);
    get_cb_baton.post();
  };
  vcs_->getLatestConfig("key1", std::move(get_cb));
  get_cb_baton.wait();

  vcs_ = std::make_unique<RSMBasedVersionedConfigStore>(
      logid_t(1),
      extract_function,
      &client->getProcessor(),
      std::chrono::milliseconds(100));

  do {
    status = vcs_->getConfigSync("key1", &value);
  } while (status == Status::AGAIN);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value3", value);

  status = vcs_->getConfigSync("key2", &value);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value2", value);

  status = vcs_->updateConfigSync(
      "key3", "value4", VersionedConfigStore::Condition::overwrite());
  EXPECT_EQ(Status::OK, status);
  auto mcb = [](folly::Optional<std::string> value) {
    EXPECT_TRUE(value.hasValue());
    return std::make_pair(Status::OK, value.value() + "4");
  };
  folly::Baton<> call_baton;
  auto ucb = [&call_baton](Status status, auto, auto) {
    EXPECT_EQ(Status::OK, status);
    call_baton.post();
  };
  vcs_->readModifyWriteConfig("key2", std::move(mcb), std::move(ucb));
  call_baton.wait();
  status = vcs_->updateConfigSync(
      "key4", "value6", VersionedConfigStore::Condition::overwrite());
  EXPECT_EQ(Status::OK, status);

  status = vcs_->getConfigSync("key3", &value);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value4", value);
  status = vcs_->getConfigSync("key2", &value);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value24", value);
  status = vcs_->getConfigSync("key4", &value);
  ASSERT_EQ(Status::OK, status);
  EXPECT_EQ("value6", value);
}
