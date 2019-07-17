/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/StaticSequencerLocator.h"
#include "logdevice/common/metadata_log/TrimMetaDataLogRequest.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)

using namespace facebook::logdevice;

class MetaDataLogsIntegrationTest : public IntegrationTestBase {};

// test that Client cannot directly write to metadata logs
TEST_F(MetaDataLogsIntegrationTest, ClientCannotWriteMetaDataLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  auto client = cluster->createClient();
  for (size_t id = 1; id <= 2; ++id) {
    const logid_t logid(id);
    const logid_t meta_logid = MetaDataLog::metaDataLogID(logid);
    EpochMetaData metadata(StorageSet{N3, N4, N5, N6},
                           ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                           epoch_t(10),
                           epoch_t(6));
    std::string data = metadata.toStringPayload();
    lsn_t lsn =
        client->appendSync(meta_logid, Payload(data.data(), data.size()));
    ASSERT_EQ(LSN_INVALID, lsn);
    ASSERT_EQ(E::INVALID_PARAM, err);
  }
}

TEST_F(MetaDataLogsIntegrationTest, WriteMetaDataLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .doNotLetSequencersProvisionEpochMetaData()
                     .create(4);
  auto client_orig = cluster->createClient();

  // cast the Client object back to ClientImpl and enable metadata log
  // writes
  auto client = std::dynamic_pointer_cast<ClientImpl>(client_orig);
  ASSERT_NE(nullptr, client);
  client->allowWriteMetaDataLog();

  EpochMetaData metadata(StorageSet{N3, N4, N5, N6},
                         ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                         epoch_t(3),
                         epoch_t(2));

  ASSERT_TRUE(metadata.isValid());
  const logid_t logid(2);
  const logid_t meta_logid = MetaDataLog::metaDataLogID(logid);

  auto do_write = [&]() {
    std::string data = metadata.toStringPayload();
    lsn_t record_lsn;
    // retry until the write can complete
    wait_until([&]() {
      lsn_t lsn =
          client->appendSync(meta_logid, Payload(data.data(), data.size()));
      if (lsn == LSN_INVALID) {
        // expect to E::SEQNOBUFS when there is a write in-flight
        EXPECT_EQ(E::SEQNOBUFS, err);
        return false;
      }
      record_lsn = lsn;
      return true;
    });
    return record_lsn;
  };

  std::vector<lsn_t> lsn_written;
  // write 10 records
  while (lsn_written.size() < 10) {
    lsn_t lsn = do_write();
    ASSERT_NE(LSN_INVALID, lsn);
    lsn_written.push_back(lsn);
  }

  // read records written and examine them
  auto it = lsn_written.cbegin();
  Semaphore sem;

  auto gap_cb = [&](const GapRecord& gap) {
    EXPECT_EQ(GapType::BRIDGE, gap.type);
    if (it == lsn_written.cend()) {
      return true;
    }
    EXPECT_NE(lsn_written.cbegin(), it);

    // expect two bridge gaps for each epoch with record:
    if (lsn_to_esn(gap.lo) == ESN_INVALID) {
      EXPECT_EQ(*it - 1, gap.lo);
      EXPECT_EQ(*it - 1, gap.hi);
    } else {
      EXPECT_EQ(*std::prev(it, 1) + 1, gap.lo);
      // *it has esn == 1, -2 goes to ESN_MAX of the previous epoch
      EXPECT_EQ(*it - 2, gap.hi);
    }

    return true;
  };

  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    EXPECT_EQ(meta_logid, r->logid);
    EXPECT_NE(lsn_written.cend(), it);
    EXPECT_EQ(*it, r->attrs.lsn);
    EpochMetaData metadata_read;
    int rv = metadata_read.fromPayload(
        r->payload, logid, *cluster->getConfig()->getNodesConfiguration());
    EXPECT_EQ(0, rv);
    EXPECT_EQ(metadata, metadata_read);
    if (++it == lsn_written.cend()) {
      sem.post();
    }
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader(-1));
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(meta_logid, lsn_written.front());
  sem.wait();

  Semaphore stop_sem;
  int rv = reader->stopReading(meta_logid, [&]() { stop_sem.post(); });
  ASSERT_EQ(0, rv);
  stop_sem.wait();

  for (int i = 0; i < lsn_written.size(); ++i) {
    // each record should have ESN to be 1 and higher epoch than the
    // previous one
    ASSERT_EQ(ESN_MIN, lsn_to_esn(lsn_written[i]));
    if (i > 0) {
      ASSERT_GT(lsn_to_epoch(lsn_written[i]), lsn_to_epoch(lsn_written[i - 1]));
    }
  }

  // try to write an invalid epoch metadata, wait until we got E::BADPAYLOAD
  std::string data = metadata.toStringPayload();
  data.pop_back();
  wait_until([&]() {
    lsn_t lsn =
        client->appendSync(meta_logid, Payload(data.data(), data.size()));
    EXPECT_EQ(LSN_INVALID, lsn);
    if (err == E::SEQNOBUFS) {
      return false;
    }
    EXPECT_EQ(E::BADPAYLOAD, err);
    return true;
  });
}

// Tests that the client retries reading the metadata on failure
TEST_F(MetaDataLogsIntegrationTest, CorruptedMetaDataWithOnDemandLogsConfig) {
  dbg::currentLevel = dbg::Level::SPEW;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .doNotLetSequencersProvisionEpochMetaData()
                     .deferStart()
                     .create(2);
  // Writing corrupted metadata
  auto metadata_provisioner = cluster->createMetaDataProvisioner();
  std::vector<std::pair<epoch_t, std::string>> corrupted_metadata;
  corrupted_metadata.emplace_back(epoch_t(1), "corrupted_metadata");
  metadata_provisioner->prepopulateMetaDataLog(
      logid_t(1), std::move(corrupted_metadata));
  metadata_provisioner.reset();

  // Starting cluster
  cluster->start();
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("on-demand-logs-config", true));
  auto client_orig = ClientFactory()
                         .setTimeout(testTimeout())
                         .setClientSettings(std::move(client_settings))
                         .create(cluster->getConfigPath());

  // Writing data
  std::string data("data_payload");
  err = E::OK;
  lsn_t lsn =
      client_orig->appendSync(logid_t(1), Payload(data.data(), data.size()));
  ld_check(lsn != LSN_INVALID);

  // Attempting to read it
  auto reader = client_orig->createReader(1);

  reader->setTimeout(std::chrono::seconds(1));
  int rv = reader->startReading(logid_t(1), lsn_t(1));
  ASSERT_EQ(0, rv);

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;

  ssize_t count = reader->read(1, &records, &gap);
  ASSERT_EQ(-1, count);
  ASSERT_EQ(err, E::GAP);

  // Waiting upto 1 second for the record to surface
  count = reader->read(1, &records, &gap);
  ASSERT_EQ(0, count);
  auto client = std::dynamic_pointer_cast<ClientImpl>(client_orig);
  ASSERT_GT(
      client->stats()->aggregate().metadata_log_read_failed_corruption, 0);

  // Reading metadata from epoch store
  cluster->stop();
  std::vector<std::unique_ptr<EpochMetaData>> correct_metadata;
  auto epoch_store = cluster->createEpochStore();
  auto fetch_only_updater = [](logid_t log_id,
                               std::unique_ptr<EpochMetaData>& info) {
    ld_check(log_id.val() == 1);
    ld_check(info && !info->isEmpty() && info->isValid());
    return EpochMetaData::UpdateResult::UNCHANGED;
  };

  rv = epoch_store->createOrUpdateMetaData(
      logid_t(1),
      std::make_shared<SimpleEpochMetaDataUpdater>(
          std::move(fetch_only_updater)),
      [&correct_metadata](Status /*st*/,
                          logid_t /*log_id*/,
                          std::unique_ptr<EpochMetaData> metadata,
                          std::unique_ptr<EpochStoreMetaProperties>) {
        correct_metadata.push_back(std::move(metadata));
      },
      MetaDataTracer());
  ASSERT_EQ(0, rv);
  epoch_store.reset();
  ASSERT_EQ(1, correct_metadata.size());

  // Writing correct metadata at epoch 1
  correct_metadata.back()->h.epoch = epoch_t(1);
  metadata_provisioner = cluster->createMetaDataProvisioner();
  rv = metadata_provisioner->prepopulateMetaDataLog(
      logid_t(1), correct_metadata);
  ASSERT_EQ(0, rv);
  metadata_provisioner.reset();

  // Starting the cluster
  cluster->start();

  // The client should be able to read the record at this point
  reader->setTimeout(std::chrono::seconds(20));
  do {
    count = reader->read(1, &records, &gap);
    if (count == -1) {
      ASSERT_EQ(E::GAP, err);
    }
  } while (count == -1);
  ASSERT_EQ(1, count);
  ASSERT_EQ(count, records.size());
  ASSERT_TRUE((bool)records[0]);
  ASSERT_EQ(data, records[0]->payload.toString());
}

// Tests that the client retries reading the metadata if it encounters a record
// with an empty nodeset
TEST_F(MetaDataLogsIntegrationTest, EmptyNodeSetMetaData) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .doNotLetSequencersProvisionEpochMetaData()
                     .deferStart()
                     .create(2);
  // Reading metadata from epoch store
  std::vector<std::unique_ptr<EpochMetaData>> metadata_records;
  auto epoch_store = cluster->createEpochStore();
  auto fetch_only_updater = [](logid_t log_id,
                               std::unique_ptr<EpochMetaData>& info) {
    ld_check(log_id.val() == 1);
    ld_check(info && !info->isEmpty() && info->isValid());
    return EpochMetaData::UpdateResult::UNCHANGED;
  };

  int rv = epoch_store->createOrUpdateMetaData(
      logid_t(1),
      std::make_shared<SimpleEpochMetaDataUpdater>(
          std::move(fetch_only_updater)),
      [&metadata_records](Status /*st*/,
                          logid_t /*log_id*/,
                          std::unique_ptr<EpochMetaData> metadata,
                          std::unique_ptr<EpochStoreMetaProperties>) {
        metadata_records.push_back(std::move(metadata));
      },
      MetaDataTracer());
  ASSERT_EQ(0, rv);
  epoch_store.reset();
  ASSERT_EQ(1, metadata_records.size());

  // Modifying the nodeset and writing it back
  metadata_records.back()->setShards(StorageSet{ShardID(0x7fff, 0)});
  auto metadata_provisioner = cluster->createMetaDataProvisioner();
  rv = metadata_provisioner->prepopulateMetaDataLog(
      logid_t(1), metadata_records);
  ASSERT_EQ(0, rv);
  metadata_provisioner.reset();

  // Starting cluster
  cluster->start();
  auto client = cluster->createClient();

  // Writing data
  std::string data("data_payload1");
  lsn_t lsn = client->appendSync(logid_t(1), Payload(data.data(), data.size()));
  ld_check(lsn != LSN_INVALID);

  // Attempting to read it
  auto reader = client->createReader(1);

  reader->setTimeout(std::chrono::seconds(1));
  rv = reader->startReading(logid_t(1), lsn_t(1));
  ASSERT_EQ(0, rv);

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;

  // Waiting upto 1 second for the record to surface
  int count = 0;
  do {
    count = reader->read(1, &records, &gap);
    if (count == -1) {
      ASSERT_EQ(E::GAP, err);
      ASSERT_NE(GapType::DATALOSS, gap.type);
    }
  } while (count == -1);

  // expecting no records
  ASSERT_EQ(0, count);
  auto client_impl = std::dynamic_pointer_cast<ClientImpl>(client);
  ASSERT_EQ(
      0, client_impl->stats()->aggregate().metadata_log_read_failed_corruption);

  // Writing another metadata record
  cluster->stop();
  metadata_provisioner = cluster->createMetaDataProvisioner();
  std::shared_ptr<NodeSetSelector> selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::SELECT_ALL);
  rv = metadata_provisioner->provisionEpochMetaDataForLog(
      logid_t(1),
      selector,
      true /* use_storage_set_format */,
      false /* provision_if_empty */,
      true /* update_if_exists */,
      true /* write_metadata_log */,
      true /* force_update */);
  ASSERT_EQ(0, rv);
  metadata_provisioner.reset();

  // Starting the cluster
  cluster->start();

  // Appending another payload
  std::string data2("data_payload2");
  lsn = client->appendSync(logid_t(1), Payload(data2.data(), data2.size()));
  ld_check(lsn != LSN_INVALID);
  ld_info("Appended LSN %s", lsn_to_string(lsn).c_str());

  // The client should issue a data loss gap for the affected epoch and read
  // a subsequently appended record
  reader->setTimeout(reader->MAX_TIMEOUT);

  bool seen_dataloss = false;
  ld_info("Reading records");
  do {
    count = reader->read(1, &records, &gap);
    if (count == -1) {
      ASSERT_EQ(E::GAP, err);
      // TODO 16227919: temporarily hide such dataloss with BRIDGE gaps
      if (gap.type == GapType::BRIDGE) {
        seen_dataloss = true;
      }
      ld_info("Read a gap");
    }
  } while (count == -1);
  ASSERT_TRUE(seen_dataloss);
  ASSERT_EQ(1, count);
  ASSERT_EQ(count, records.size());
  ASSERT_TRUE((bool)records[0]);
  ASSERT_EQ(data2, records[0]->payload.toString());
}

TEST_F(MetaDataLogsIntegrationTest, MetaDataLogAppendWithStaleSequencerEpoch) {
  const logid_t LOG_ID(1);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .doNotLetSequencersProvisionEpochMetaData()
                     .create(4);
  auto client_orig = cluster->createClient();

  // cast the Client object back to ClientImpl and enable metadata log
  // writes
  auto client = std::dynamic_pointer_cast<ClientImpl>(client_orig);
  ASSERT_NE(nullptr, client);
  client->allowWriteMetaDataLog();

  cluster->waitForRecovery();

  // after the cluster is up bump the next epoch number in the epoch store
  // to 5
  auto epoch_store = cluster->createEpochStore();
  Semaphore semaphore;
  std::unique_ptr<EpochMetaData> metadata;

  // starting epoch is 2, see explanation in doc block in
  // ClusterFactory::create()
  for (epoch_t e = epoch_t(EPOCH_MIN.val_ + 2); e < epoch_t(5); ++e.val_) {
    epoch_store->createOrUpdateMetaData(
        LOG_ID,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
        [&](Status status,
            logid_t log_id,
            std::unique_ptr<EpochMetaData> info,
            std::unique_ptr<EpochStoreMetaProperties>) {
          ASSERT_EQ(E::OK, status);
          ASSERT_EQ(LOG_ID, log_id);
          ASSERT_NE(nullptr, info);
          ASSERT_EQ(e.val() + 1, info->h.epoch.val());
          metadata = std::move(info);
          semaphore.post();
        },
        MetaDataTracer());
    semaphore.wait();
  }

  ASSERT_TRUE(metadata->isValid());

  // at this time the data sequencer is still running at epoch 2
  // but the metadata to be written has its epoch of epoch 4 in payload
  ASSERT_EQ(epoch_t(5), metadata->h.epoch);
  --metadata->h.epoch.val_;

  // now append the metadata record, it should cause the data sequencer to
  // reactivate to epoch 5 before appending the metadata log record
  const logid_t meta_logid = MetaDataLog::metaDataLogID(LOG_ID);

  // clear the written flag before writing to the metadata log
  metadata->h.flags &= ~MetaDataLogRecordHeader::WRITTEN_IN_METADATALOG;
  std::string data = metadata->toStringPayload();
  lsn_t record_lsn = LSN_INVALID;
  // retry until the write can complete
  wait_until([&]() {
    lsn_t lsn =
        client->appendSync(meta_logid, Payload(data.data(), data.size()));
    if (lsn == LSN_INVALID) {
      // expect to E::SEQNOBUFS when there is a write in-flight
      EXPECT_EQ(E::SEQNOBUFS, err);
      return false;
    }
    record_lsn = lsn;
    return true;
  });

  ASSERT_NE(LSN_INVALID, record_lsn);
  // the first successful appended lsn must be with epoch 5
  ASSERT_EQ(epoch_t(5), lsn_to_epoch(record_lsn));
  // try append a data log record, verify its lsn epoch >= 5
  lsn_t lsn = client->appendSync(LOG_ID, "dummy1");
  ASSERT_LE(epoch_t(5), lsn_to_epoch(lsn));
}

// Test to ensure sequencer periodically updates the metadata_map_ by reading
// the metadata log.
TEST_F(MetaDataLogsIntegrationTest, SequencerReadHistoricMetadata) {
  const logid_t LOG_ID(1);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--update-metadata-map-interval",
                    "1s",
                    IntegrationTestUtils::ParamScope::SEQUENCER)
          .setParam("--get-trimpoint-interval",
                    "1s",
                    IntegrationTestUtils::ParamScope::SEQUENCER)
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .create(4);
  auto client_orig = cluster->createClient();
  std::shared_ptr<Client> client = cluster->createClient();
  auto* client_impl = static_cast<ClientImpl*>(client.get());
  Settings settings = create_default_settings<Settings>();
  auto processor =
      Processor::create(cluster->getConfig(),
                        std::make_shared<NoopTraceLogger>(cluster->getConfig()),
                        UpdateableSettings<Settings>(settings),
                        nullptr, /* stats*/

                        make_test_plugin_registry());

  std::string data(1024, 'x');
  // write one record
  lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
  ASSERT_NE(LSN_INVALID, lsn);
  auto first_epoch = lsn_to_epoch(lsn);
  auto result = client_impl->getHistoricalMetaDataSync(LOG_ID);

  // epoch metadata should be valid
  auto epoch_metadata_first = result->getEpochMetaData(first_epoch);
  ld_info("epoch_metadata_first: %s", epoch_metadata_first->toString().c_str());
  ASSERT_NE(nullptr, epoch_metadata_first);

  // change the nodes config and force two metadata log entries
  for (auto i = 0; i < 2; i++) {
    Semaphore sem;
    auto handle = client->subscribeToConfigUpdates([&]() { sem.post(); });
    auto org_res = client_impl->getHistoricalMetaDataSync(LOG_ID);
    auto org_epoch_metadata = org_res->getLastEpochMetaData();
    ASSERT_NE(nullptr, org_epoch_metadata);

    // Get the nodes in the server config
    auto current_config = cluster->getConfig()->get();
    auto nodes = current_config->serverConfig()->getNodes();
    nodes[1].storage_attributes->exclude_from_nodesets =
        nodes[1].storage_attributes->exclude_from_nodesets ? false : true;

    Configuration::NodesConfig nodes_config(std::move(nodes));
    // create a new config with the modified node config
    Configuration config(
        current_config->serverConfig()->withNodes(nodes_config),
        current_config->logsConfig());
    // Write it out to the logdevice.conf file
    cluster->writeConfig(config);

    ld_info("Waiting for Client to pick up the config changes");
    sem.wait();

    // verify new metadata log entry; there is no good way to do this, so
    // get the last epoch metadata and make sure this is not the same as the
    // first (which by definition means a new entry was added ;))
    wait_until([&]() {
      auto res = client_impl->getHistoricalMetaDataSync(LOG_ID);
      auto epoch_metadata_second = res->getLastEpochMetaData();
      ld_spew("epoch_metadata_second: %s",
              epoch_metadata_second->toString().c_str());
      EXPECT_NE(nullptr, epoch_metadata_second);
      if (org_epoch_metadata->h.epoch != epoch_metadata_second->h.epoch) {
        return true;
      }
      return false;
    });
  }

  result = client_impl->getHistoricalMetaDataSync(LOG_ID);
  auto epoch_metadata_second = result->getLastEpochMetaData();
  ld_info("epoch_metadata_full: %s", result->toString().c_str());
  ld_info(
      "epoch_metadata_second: %s", epoch_metadata_second->toString().c_str());
  ASSERT_NE(nullptr, epoch_metadata_second);
  ASSERT_NE(epoch_metadata_first->h.epoch, epoch_metadata_second->h.epoch);

  // at this point, the metadata log should have something like this:
  // E:1 S:1 Until: 3 (has one record)
  // E:3 S:3 Until: 5
  // E:5 S:5 Until: 6
  // write two more records: first_lsn and second_lsn. these should have
  // epoch >=5. trim first_lsn this should make <E:1 S:1 Until: 3> stale.
  // if first_lsn epoch is 6 then <E:3 S:3 Until:5> is also stale. (this is
  // why we force two metadata log entries above)
  auto first_lsn =
      client->appendSync(LOG_ID, Payload(data.data(), data.size()));
  ASSERT_NE(LSN_INVALID, first_lsn);
  auto second_lsn =
      client->appendSync(LOG_ID, Payload(data.data(), data.size()));
  ASSERT_NE(LSN_INVALID, second_lsn);

  auto rv = client->trimSync(LOG_ID, first_lsn);
  ld_info("Trimming lsn: %lu", first_lsn);
  EXPECT_EQ(0, rv);

  // trim metadata logs needs an updated trim point. the test sets the trim
  // point interval to 1s, so wait slightly longer: 5 secs
  // wait for the records to expire
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // trim metadata log. this should trim the stale entry <E:1 S:1 Until:3>
  // this could also trim <E: 3 S:3 Until: 5> (see comment above).
  // the sucess of this depends on the sequencer having an up to date trim point
  // so do this in a wait_until loop.
  wait_until([&]() {
    auto trim_status = E::UPTODATE;
    std::unique_ptr<Request> request = std::make_unique<TrimMetaDataLogRequest>(
        LOG_ID,
        cluster->getConfig()->get(),
        processor,
        std::chrono::milliseconds(30000),
        std::chrono::milliseconds(30000),
        /*do_trim_data_log=*/false,
        [&trim_status](Status st, logid_t /* unused */) { trim_status = st; },
        std::chrono::seconds(3600 * 24),
        false);
    rv = processor->blockingRequest(request);
    EXPECT_EQ(0, rv);
    if (trim_status == E::OK) {
      return true;
    }
    return false;
  });

  // verify that the stale entry is gone from the sequencer's metadata_map_
  // since the sequencer reads metadata log based on a timer that the test
  // sets to 1 sec, wait slightly longer: 5 secs
  auto start_time = std::chrono::system_clock::now();
  auto cur_time = start_time;
  std::unique_ptr<EpochMetaData> epoch_metadata;
  do {
    result = client_impl->getHistoricalMetaDataSync(LOG_ID);
    epoch_metadata = result->getEpochMetaData(first_epoch);
    ld_spew("epoch_metadata_third: %s", epoch_metadata->toString().c_str());
    if (epoch_metadata->h.epoch != epoch_metadata_first->h.epoch) {
      break;
    }
    cur_time = std::chrono::system_clock::now();
  } while (cur_time - start_time < std::chrono::seconds(5));

  ld_info("epoch_metadata_full: %s", result->toString().c_str());
  ld_info("epoch_metadata_third: %s", epoch_metadata->toString().c_str());
  // manually verify; we cannot strcmp since the FLAGS are different
  ASSERT_NE(epoch_metadata->h.epoch, epoch_metadata_first->h.epoch);
  ASSERT_NE(epoch_metadata->h.effective_since,
            epoch_metadata_first->h.effective_since);
}
