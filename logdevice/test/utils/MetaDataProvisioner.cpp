/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/MetaDataProvisioner.h"

#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/WeightedCopySetSelector.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/CopySetSelectorTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

MetaDataProvisioner::MetaDataProvisioner(
    std::unique_ptr<EpochStore> epoch_store,
    std::shared_ptr<UpdateableConfig> config,
    LogStoreFactory log_store_factory)
    : epoch_store_(std::move(epoch_store)),
      config_(std::move(config)),
      log_store_factory_(std::move(log_store_factory)) {}

MetaDataProvisioner::~MetaDataProvisioner() {
  finalize();
}

int MetaDataProvisioner::provisionEpochMetaDataForLog(
    logid_t log_id,
    std::shared_ptr<NodeSetSelector> selector,
    bool use_storage_set_format,
    bool provision_if_empty,
    bool update_if_exists,
    bool write_metadata_log,
    bool force_update) {
  ld_check(selector != nullptr);
  std::shared_ptr<Configuration> config = config_->get();
  ld_check(config != nullptr);

  return provisionEpochMetaDataForLog(
      log_id,
      std::make_shared<CustomEpochMetaDataUpdater>(
          config,
          config->getNodesConfigurationFromServerConfigSource(),
          std::move(selector),
          use_storage_set_format,
          provision_if_empty,
          update_if_exists,
          force_update),
      write_metadata_log);
}

int MetaDataProvisioner::provisionEpochMetaDataForLog(
    logid_t log_id,
    std::shared_ptr<EpochMetaData::Updater> provisioner,
    bool write_metadata_log) {
  ld_check(log_id != LOGID_INVALID);

  // provision epoch metadata in epochstore
  FileEpochStore* file_store = static_cast<FileEpochStore*>(epoch_store_.get());
  int rv = file_store->provisionMetaDataLog(log_id, std::move(provisioner));

  if (rv != 0 && err != E::UPTODATE) {
    ld_error("Cannot provision epoch metadata for log %lu in epoch store.",
             log_id.val_);
    return -1;
  }

  Semaphore sem;
  std::unique_ptr<EpochMetaData> metadata;

  // call createOrUpdateMetaData() to (1) fetch the current epoch metadata, and
  // (2) bump up the metadata epoch so that the next sequencer will start with a
  // higher epoch than the metadata epoch in metadata logs
  file_store->createOrUpdateMetaData(
      log_id,
      std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
      [log_id, &sem, &metadata](Status st,
                                logid_t logid,
                                std::unique_ptr<EpochMetaData> info,
                                std::unique_ptr<EpochStoreMetaProperties>) {
        ld_check(logid == log_id);
        if (st == E::OK) {
          metadata = std::move(info);
        }
        sem.post();
      },
      MetaDataTracer());

  sem.wait();

  if (metadata == nullptr) {
    ld_error(
        "Cannot fetch epoch metadata for log %lu in epoch store.", log_id.val_);
    return -1;
  }
  ld_check(metadata->isValid());

  if (!write_metadata_log) {
    // metadata log will be written by the sequencer
    return 0;
  }

  // write the metadata into meta storage nodes
  std::vector<std::unique_ptr<EpochMetaData>> epoch_metas;
  epoch_metas.push_back(std::move(metadata));

  if (prepopulateMetaDataLog(log_id, epoch_metas) != 0) {
    ld_error("Failed to prepopulate metadata log for log %lu in"
             "local log store of metadata storage nodes",
             log_id.val_);
    return -1;
  }

  bool mark_written = false;
  rv = file_store->createOrUpdateMetaData(
      log_id,
      std::make_shared<EpochMetaDataUpdateToWritten>(),
      [log_id, &sem, &mark_written](Status st,
                                    logid_t logid,
                                    std::unique_ptr<EpochMetaData> /*info*/,
                                    std::unique_ptr<EpochStoreMetaProperties>) {
        ld_check(logid == log_id);
        if (st == E::OK || st == E::UPTODATE) {
          mark_written = true;
        } else {
          ld_error("Failed to mark metadata log for log %lu as "
                   "written in epoch store: %s",
                   log_id.val_,
                   error_description(st));
        }
        sem.post();
      },
      MetaDataTracer());

  sem.wait();
  return mark_written ? 0 : -1;
}

int MetaDataProvisioner::prepopulateMetaDataLog(
    logid_t log_id,
    const std::vector<std::pair<epoch_t, std::string>>& metadata_payloads) {
  std::shared_ptr<Configuration> config = config_->get();
  ld_check(config != nullptr);

  const logid_t meta_logid(MetaDataLog::metaDataLogID(log_id));

  // get the metadata storage nodes and replication factor from the config
  const auto& nodes_configuration =
      config->serverConfig()->getNodesConfigurationFromServerConfigSource();

  auto meta_storage_set = EpochMetaData::nodesetToStorageSet(
      nodes_configuration->getStorageMembership()->getMetaDataNodeIndices(),
      meta_logid,
      *nodes_configuration);

  auto replication = nodes_configuration->getMetaDataLogsReplication()
                         ->getReplicationProperty();

  lsn_t record_lsn = LSN_INVALID;
  // records to be written to each node
  std::unordered_map<ShardID, std::vector<TestRecord>, ShardID::Hash>
      node_records;
  for (auto& kv : metadata_payloads) {
    // prepare the metadata log record
    // use the metadata epoch as the record epoch in its metadata log lsn
    auto epoch = kv.first;
    auto& payload = kv.second;

    if (epoch <= lsn_to_epoch(record_lsn)) {
      ld_error("Epochs of given metadata must be monotonically increasing. "
               "Current: %u, previous: %u",
               epoch.val(),
               lsn_to_epoch(record_lsn).val_);
      return -1;
    }
    record_lsn = compose_lsn(epoch, esn_t(1));

    TestRecord record(meta_logid, record_lsn, ESN_INVALID);
    record.payload(Payload(payload.data(), payload.size()));
    record.copyset(genCopySet(meta_storage_set, replication));
    // there is a known issue where purging deletes records that gets surfaced
    // in rebuilding tests, which is why we set the LNG of these records to
    // their LSN - to avoid them being purged. See t13850978
    record.last_known_good_ = lsn_to_esn(record_lsn);

    for (auto shard : record.copyset_) {
      node_records[shard].push_back(record);
    }
  }

  // write records into local log stores in each node
  for (auto it = node_records.cbegin(); it != node_records.cend(); ++it) {
    if (populateMetaDataRecordsOnNode(log_id, it->first.node(), it->second)) {
      ld_error("Failed to prepopulate metadata log for log %lu in"
               "local log store of Node %d",
               log_id.val_,
               it->first.node());
      return -1;
    }
  }

  return 0;
}

int MetaDataProvisioner::prepopulateMetaDataLog(
    logid_t log_id,
    const std::vector<std::unique_ptr<EpochMetaData>>& metadata) {
  std::vector<std::pair<epoch_t, std::string>> payloads; // holds payload data

  // reserve the space upfront so that payload won't get relocated as the
  // vector grows
  payloads.reserve(metadata.size());

  for (auto& meta : metadata) {
    if (meta == nullptr || !meta->isValid()) {
      ld_error("At least on of the given metadata is not valid.");
      return -1;
    }
    // epoch field should contain the same value as `effective_since` in the
    // metadata log record
    EpochMetaData meta_copy = *meta;
    meta_copy.h.epoch = meta_copy.h.effective_since;
    meta_copy.h.flags |= MetaDataLogRecordHeader::HAS_STORAGE_SET;
    payloads.push_back(
        std::make_pair(meta_copy.h.epoch, meta_copy.toStringPayload()));
    const std::string& str_payload = payloads.back().second;
    ld_check(!str_payload.empty());
  }

  ld_check(payloads.size() == metadata.size());
  return prepopulateMetaDataLog(log_id, std::move(payloads));
}

int MetaDataProvisioner::populateMetaDataRecordsOnNode(
    logid_t log_id,
    node_index_t node,
    const std::vector<TestRecord>& records) {
  ld_check(!records.empty());
  lsn_t last_lsn = records.back().lsn_;
  ld_check(last_lsn != LSN_INVALID);

  const logid_t meta_logid(MetaDataLog::metaDataLogID(log_id));

  ShardedLocalLogStore* sharded_store = createOrGet(node);
  ld_check(sharded_store);
  const shard_index_t shard_idx =
      getLegacyShardIndexForLog(meta_logid, sharded_store->numShards());
  LocalLogStore* store = sharded_store->getByIndex(shard_idx);
  ld_check(store);

  // write all records into the local log store
  store_fill(*store, records);

  // release records we just wrote and mark the epoch clean in store metadata
  int rv = store->writeLogMetadata(meta_logid,
                                   LastReleasedMetadata{last_lsn},
                                   LocalLogStore::WriteOptions());
  if (rv != 0) {
    ld_error("Cannot write last released metadata for log %lu to"
             "local log store of Node %d",
             log_id.val_,
             node);
    return -1;
  }

  rv = store->writeLogMetadata(meta_logid,
                               LastCleanMetadata{lsn_to_epoch(last_lsn)},
                               LocalLogStore::WriteOptions());
  if (rv != 0) {
    ld_error("Cannot write last clean metadata for log %lu to"
             "local log store of Node %d",
             log_id.val_,
             node);
    return -1;
  }

  return 0;
}

copyset_t MetaDataProvisioner::genCopySet(const StorageSet& storage_set,
                                          ReplicationProperty replication) {
  ld_check(replication.isValid());

  TestCopySetSelectorDeps deps;
  WeightedCopySetSelector selector(
      LOGID_INVALID,
      EpochMetaData(storage_set, replication),
      nullptr,
      config_->get()
          ->serverConfig()
          ->getNodesConfigurationFromServerConfigSource(),
      folly::none /* my_node_id */,
      nullptr /* log_attrs */,
      false /* locality */,
      nullptr /* stats */,
      DefaultRNG::get(),
      true /* print_bias_warnings */,
      &deps);

  std::vector<StoreChainLink> cs(selector.getReplicationFactor());
  copyset_size_t cs_sz;
  auto rv = selector.select(0 /* extras */, cs.data(), &cs_sz);
  ld_check(rv == CopySetSelector::Result::SUCCESS);

  copyset_t copyset;
  for (auto c : cs) {
    copyset.push_back(c.destination);
  }
  return copyset;
}

ShardedLocalLogStore* MetaDataProvisioner::createOrGet(node_index_t node_idx) {
  auto it = node_store_map_.find(node_idx);
  if (it != node_store_map_.end()) {
    // Store must be valid for an existing entry
    ld_check(it->second != nullptr);
    return it->second.get();
  }

  // entry does not exist, create the sharded local logstore instance
  auto result = node_store_map_.insert(
      std::make_pair(node_idx, log_store_factory_(node_idx)));

  ld_check(result.second);
  ld_check(result.first->second != nullptr);
  return result.first->second.get();
}

int MetaDataProvisioner::provisionEpochMetaData(
    std::shared_ptr<NodeSetSelector> selector,
    bool allow_existing_metadata,
    bool use_storage_set_format) {
  std::shared_ptr<Configuration> config = config_->get();
  ld_check(config != nullptr);
  ld_check(config->logsConfig()->isLocal());
  auto& logs_config = config->localLogsConfig();
  for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd(); ++it) {
    const logid_t log_id(it->first);
    if (provisionEpochMetaDataForLog(log_id,
                                     selector,
                                     use_storage_set_format,
                                     true /* create_if_empty */,
                                     allow_existing_metadata,
                                     true /*write_metadata_logs */)) {
      // error logged
      return -1;
    }
  }

  return 0;
}

void MetaDataProvisioner::finalize() {
  // destory all open ShardedLocalLogStore instances
  node_store_map_.clear();
}

}}} // namespace facebook::logdevice::IntegrationTestUtils
