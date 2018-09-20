/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

namespace {
class AppendErrorInjectorIntegrationTest : public IntegrationTestBase {
 public:
  void initializeCluster(int log_count = 1) {
    cluster =
        IntegrationTestUtils::ClusterFactory{}.setNumLogs(log_count).create(1);
    wait_until(
        "Sequencer activation", [log_count, client = cluster->createClient()] {
          auto is_activated{true};
          for (int i = 1; i <= log_count; ++i) {
            is_activated = is_activated &&
                client->appendSync(logid_t(i), ".") != LSN_INVALID;
          }
          return is_activated;
        });
  }

  std::shared_ptr<Client>
  createClient(Status status, std::unordered_map<logid_t, double> fail_ratios) {
    auto client = cluster->createClient();
    ClientImpl* impl = static_cast<ClientImpl*>(client.get());
    impl->setAppendErrorInjector(
        AppendErrorInjector{status, std::move(fail_ratios)});

    return client;
  }

  std::unique_ptr<Cluster> cluster;
};
} // namespace

TEST_F(AppendErrorInjectorIntegrationTest, SyncedAppend) {
  logid_t log{1};
  initializeCluster();
  auto client = createClient(Status::SEQNOBUFS, {{log, 1.0}});

  EXPECT_EQ(LSN_INVALID, client->appendSync(log, "."));
  EXPECT_EQ(Status::SEQNOBUFS, err);
}

TEST_F(AppendErrorInjectorIntegrationTest, AsyncAppend) {
  logid_t log{1};
  initializeCluster();
  auto client = createClient(Status::BADMSG, {{log, 1.0}});

  Semaphore sem;
  client->append(
      logid_t(1), ".", [&sem](Status status, const DataRecord& /*record*/) {
        EXPECT_EQ(Status::BADMSG, status);
        sem.post();
      });
  sem.wait();
}

TEST_F(AppendErrorInjectorIntegrationTest, DifferentLogs) {
  initializeCluster(3);
  logid_t log1{1}, log2{2}, log3{3};
  auto client = createClient(Status::SEQNOBUFS, {{log1, 1.0}, {log2, 1.0}});

  EXPECT_EQ(LSN_INVALID, client->appendSync(log1, "."));
  EXPECT_EQ(Status::SEQNOBUFS, err);

  EXPECT_EQ(LSN_INVALID, client->appendSync(log2, "."));
  EXPECT_EQ(Status::SEQNOBUFS, err);

  EXPECT_NE(LSN_INVALID, client->appendSync(log3, "."));
}
