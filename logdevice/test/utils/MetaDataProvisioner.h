/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

class NodeSetSelector;
class TestRecord;

namespace IntegrationTestUtils {

using LogStoreFactory =
    std::function<std::shared_ptr<ShardedLocalLogStore>(node_index_t nid)>;

/**
 * MetaDataProvisioner is a utility class for IntegrationTests. It performs the
 * initial provision of epoch metadata in epoch store. It can also prepopulate
 * designated metadata log records on metadata storage nodes before they start.
 *
 * MetaDataProvisioner opens local logstores on storage nodes and keeps them
 * open throughout its life time to avoid the cost of reopen logstores for
 * subsequent operations. All open local logstores will be closed when the
 * MetaDataProvisioner object is destroyed.
 */
class MetaDataProvisioner {
 public:
  MetaDataProvisioner(std::unique_ptr<EpochStore> epoch_store,
                      std::shared_ptr<UpdateableConfig> config,
                      LogStoreFactory log_store_factory);

  ~MetaDataProvisioner();

  /**
   * Provision epoch metadata for a single log with the given nodeset
   * selector.
   */
  int provisionEpochMetaDataForLog(logid_t log_id,
                                   std::shared_ptr<NodeSetSelector> selector,
                                   bool use_storage_set_format,
                                   bool provision_if_empty = true,
                                   bool update_if_exists = false,
                                   bool write_metadata_log = true,
                                   bool force_update = false);

  // overload that uses a customized provisioner to provision epoch metadata
  int provisionEpochMetaDataForLog(
      logid_t log_id,
      std::shared_ptr<EpochMetaData::Updater> provisioner,
      bool write_metadata_log);

  /**
   * convenient function that provisions epoch metadata for all logs in the
   * cluster config
   */
  int provisionEpochMetaData(std::shared_ptr<NodeSetSelector> selector,
                             bool allow_existing_metadata,
                             bool use_storage_set_format);

  /**
   * Prepopulate metadata log records on storage nodes that store metadata logs.
   * The set of metadata storage nodes and replication factor are taken from
   * the cluster configuration. Copyset for these records are selected randomly
   * within the metadata nodeset.
   * Should be called before nodes start or when nodes are stopped.
   */
  int prepopulateMetaDataLog(
      logid_t log_id,
      const std::vector<std::unique_ptr<EpochMetaData>>& metadata);

  /**
   * Same as above, but with raw metadata log record payload strings
   */
  int prepopulateMetaDataLog(
      logid_t log_id,
      const std::vector<std::pair<epoch_t, std::string>>& metadata_payloads);

  // close all open logstores so that storage nodes can run logdeviced on them
  void finalize();

 private:
  using NodeLogStoreMap =
      std::unordered_map<node_index_t, std::shared_ptr<ShardedLocalLogStore>>;

  // helper routine that stores metadata records on a storage node. It also
  // writes metadata (e.g., to releases these records stored)
  int populateMetaDataRecordsOnNode(logid_t log_id,
                                    node_index_t node,
                                    const std::vector<TestRecord>& records);

  // helper routine to randomly generate a copyset
  copyset_t genCopySet(const StorageSet& nodeset,
                       ReplicationProperty replication);

  // get the ShardedLocalLogStore for the node specified by @node_idx,
  // if the object does not exist, create the ShardedLocalLogStore for the
  // node
  ShardedLocalLogStore* createOrGet(node_index_t node_idx);

  // a map of current open log stores for storage nodes
  NodeLogStoreMap node_store_map_;

  std::unique_ptr<EpochStore> epoch_store_;

  std::shared_ptr<UpdateableConfig> config_;

  LogStoreFactory log_store_factory_;
};

} // namespace IntegrationTestUtils

}} // namespace facebook::logdevice
