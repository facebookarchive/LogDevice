/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace IntegrationTestUtils;
using namespace testing;

class InternalLogsIntegrationTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /*rsm_trim_up_to_read_ptr*/> {
 public:
  static const size_t NNODES = 3;

  void
  buildClusterAndClient(bool rsm_include_read_pointer_in_snapshot = false) {
    auto factory = IntegrationTestUtils::ClusterFactory()
                       .enableLogsConfigManager()
                       .allowExistingMetaData()
                       .doPreProvisionEpochMetaData();

    if (rsm_include_read_pointer_in_snapshot) {
      factory.setParam("--rsm-include-read-pointer-in-snapshot", "true");
    }

    cluster = factory.create(NNODES);

    std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
    ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
    client = cluster->createIndependentClient(
        DEFAULT_TEST_TIMEOUT, std::move(client_settings));

    // cast the Client object back to ClientImpl and enable internal log writes
    client_impl = std::dynamic_pointer_cast<ClientImpl>(client);
    ASSERT_NE(nullptr, client);

    cluster->waitForRecovery();
  }

  lsn_t getTrimPointFor(logid_t log) {
    auto head_attr = client->getHeadAttributesSync(
        configuration::InternalLogs::CONFIG_LOG_DELTAS);
    ld_check(head_attr);
    return head_attr->trim_point;
  }

  Status trimLogsconfig(bool trim_everything) {
    Semaphore sem;
    Status res;

    auto cb = [&](Status st) {
      res = st;
      sem.post();
    };

    auto cur_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch());
    logid_t delta_log_id = configuration::InternalLogs::CONFIG_LOG_DELTAS;
    logid_t snapshot_log_id = configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS;

    std::unique_ptr<Request> rq =
        std::make_unique<TrimRSMRequest>(delta_log_id,
                                         snapshot_log_id,
                                         std::chrono::milliseconds::max(),
                                         cb,
                                         worker_id_t{0},
                                         WorkerType::GENERAL,
                                         RSMType::LOGS_CONFIG_STATE_MACHINE,
                                         trim_everything,
                                         client_impl->getTimeout(),
                                         client_impl->getTimeout());

    client_impl->getProcessor().postWithRetrying(rq);

    sem.wait();
    return res;
  }

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  std::shared_ptr<ClientImpl> client_impl;
};

INSTANTIATE_TEST_CASE_P(InternalLogsIntegrationTest,
                        InternalLogsIntegrationTest,
                        ::testing::Bool());

// test that Client cannot directly write to internal logs
TEST_F(InternalLogsIntegrationTest, ClientCannotWriteInternalLog) {
  buildClusterAndClient();

  // Create arbitrary payload. This is safe since we only append to deltas logs
  size_t dataSize = 512;
  std::string data(dataSize, 'x');

  lsn_t lsn = client->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                                 Payload((void*)data.c_str(), dataSize));

  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::INVALID_PARAM, err);
}

// test that Client can write to internal logs with the right flags
TEST_F(InternalLogsIntegrationTest, ClientWriteInternalLog) {
  buildClusterAndClient();

  client_impl->allowWriteInternalLog();

  // Create garbage payload. This is safe since we only append to deltas logs
  size_t dataSize = 512;
  std::string data(dataSize, 'x');

  lsn_t lsn =
      client_impl->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                              Payload((void*)data.c_str(), dataSize));

  ASSERT_NE(LSN_INVALID, lsn);
  ASSERT_NE(E::INVALID_PARAM, err);

  // verify we can read it back
  auto reader = client->createReader(1);

  reader->setTimeout(std::chrono::seconds(5));
  auto rv =
      reader->startReading(configuration::InternalLogs::CONFIG_LOG_DELTAS, lsn);
  ASSERT_EQ(0, rv);

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;
  auto count = reader->read(1, &records, &gap);
  ASSERT_GT(count, 0);
  ASSERT_EQ(records[0]->payload.toString(), data);
}

/**
 * If the `rsm-include-read-pointer-in-snapshot` setting is enabled, our
 * snapshots should allow us to trim up to the delta log read pointer.
 */
TEST_P(InternalLogsIntegrationTest, TrimmingUpToDeltaLogReadPointer) {
  const bool rsm_include_read_pointer_in_snapshot = GetParam();
  buildClusterAndClient(rsm_include_read_pointer_in_snapshot);

  /* write something to the delta log */
  auto dir = client->makeDirectorySync("/facebok_sneks", true);
  lsn_t last_delta = dir->version();

  /* check we never trimmed */
  ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
            LSN_INVALID);

  /* bump epoch a number of times */
  const size_t NUM_EPOCHS_TO_BUMP = 5;
  auto& seq = cluster->getSequencerNode();
  for (size_t t = 0; t < NUM_EPOCHS_TO_BUMP; ++t) {
    seq.kill();
    seq.start();
    seq.waitUntilStarted();
    cluster->waitForRecovery();
  }

  /* double check that we bumped those epochs */
  auto tail_lsn =
      client->getTailLSNSync(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_EQ(epoch_t(lsn_to_epoch(last_delta).val_ + NUM_EPOCHS_TO_BUMP),
            lsn_to_epoch(tail_lsn));

  /* sync logsconfig everywhere */
  std::vector<node_index_t> all_nodes(NNODES);
  std::iota(all_nodes.begin(), all_nodes.end(), 0);
  cluster->waitUntilLogsConfigSynced(tail_lsn, all_nodes);

  /* take snapshot */
  auto result =
      cluster->getNode(0).sendCommand("rsm write-snapshot logsconfig");
  EXPECT_THAT(result, HasSubstr("Successfully created logsconfig snapshot"));

  ASSERT_EQ(trimLogsconfig(/*trim_everything=*/false), E::OK);

  if (rsm_include_read_pointer_in_snapshot) {
    // then we should have trimmed up to delta log read ptr
    ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
              tail_lsn - 1);
  } else {
    // then we should have trimmed up to last applied delta
    ASSERT_EQ(getTrimPointFor(configuration::InternalLogs::CONFIG_LOG_DELTAS),
              last_delta);
  }

  /* now check logsconfig read availability */
  seq.kill();
  seq.start();
  seq.waitUntilStarted();
  seq.waitUntilLogsConfigSynced(tail_lsn);
}
