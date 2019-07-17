/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/EpochStore.h"

#include <string>

#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "logdevice/admin/safety/LogMetaDataFetcher.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/ops/ldquery/Errors.h"
#include "logdevice/server/ZookeeperEpochStore.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns EpochStore::getColumns() const {
  return {
      {"log_id", DataType::LOGID, "Id of the log."},
      {"status",
       DataType::TEXT,
       "\"OK\" if the query to the epoch store succeeded for that log id.  If "
       "the log could not be found (which only happens if the user provided "
       "query constraints on the \"log_id\" column), set to NOTFOUND.  If we "
       "failed to contact the epoch store, set to one of NOTCONN, ACCESS, "
       "SYSLIMIT, FAILED."},
      {"since",
       DataType::BIGINT,
       "Epoch since which the metadata (\"replication\", \"storage_set\", "
       "\"flags\") are in effect."},
      {"epoch", DataType::BIGINT, "Next epoch to be assigned to a sequencer."},
      {"replication",
       DataType::TEXT,
       "Current replication property of the log."},
      {"storage_set_size",
       DataType::BIGINT,
       "Number of shards in storage_set."},
      {"storage_set",
       DataType::TEXT,
       "Set of shards that may have data records for the log in epochs "
       "[\"since\", \"epoch\" - 1]."},
      {"flags",
       DataType::TEXT,
       "Internal flags.  See \"logdevice/common/EpochMetaData.h\" for the "
       "description of each flag."},
      {"nodeset_signature",
       DataType::BIGINT,
       "Hash of the parts of config that potentially affect the nodeset."},
      {"target_nodeset_size",
       DataType::BIGINT,
       "Storage set size that was requested from NodeSetSelector. Can be "
       "different from storage_set_size for various reasons, see "
       "EpochMetaData.h"},
      {"nodeset_seed",
       DataType::BIGINT,
       "Random seed used when selecting nodeset."},
      {"lce",
       DataType::BIGINT,
       "Last epoch considered clean for this log.  Under normal conditions, "
       "this is equal to \"epoch\" - 2.   If this value is smaller, this means "
       "that the current sequencer needs to run the Log Recovery procedure on "
       "epochs [\"lce\" + 1, \"epoch\" - 2] and readers will be unable to read "
       "data in these epochs until they are cleaned."},
      {"meta_lce",
       DataType::BIGINT,
       "Same as \"lce\" but for the metadata log of this data log."},
      {"written_by",
       DataType::TEXT,
       "Id of the last node in the cluster that updated the epoch store for "
       "that log."},
      {"tail_record",
       DataType::TEXT,
       "Human readable string that describes tail record"},
  };
}

std::shared_ptr<TableData> EpochStore::getData(QueryContext& ctx) {
  auto result = std::make_shared<TableData>();
  auto full_client = ld_ctx_->getFullClient();

  ld_check(full_client);
  ClientImpl* client_impl = static_cast<ClientImpl*>(full_client.get());
  auto config = client_impl->getConfig()->get();

  std::shared_ptr<logdevice::EpochStore> epoch_store;

  auto zookeeper_quorum = config->zookeeperConfig()->getQuorumString();
  if (zookeeper_quorum.empty()) {
    // There is no zookeeper quorum.
    folly::StringPiece config_path{getContext().config_path};
    constexpr folly::StringPiece kPrefix = "file:";
    constexpr folly::StringPiece kSuffix = "/logdevice.conf";
    if (!config_path.startsWith(kPrefix) || !config_path.endsWith(kSuffix)) {
      std::string error = folly::sformat(
          "Invalid config_path %s. Must be of form 'file:.*/logdevice\\.conf'",
          config_path);
      ld_error("%s", error.c_str());
      throw LDQueryError(std::move(error));
    }
    config_path.advance(kPrefix.size());
    config_path.subtract(kSuffix.size());

    std::string epoch_store_path = config_path.str() + "/epoch_store";
    epoch_store = std::make_shared<FileEpochStore>(
        epoch_store_path,
        &(client_impl->getProcessor()),
        client_impl->getConfig()->updateableNodesConfiguration());
  } else {
    try {
      auto upd_config = client_impl->getConfig();
      auto& processor = client_impl->getProcessor();
      std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory =
          processor.getPluginRegistry()
              ->getSinglePlugin<ZookeeperClientFactory>(
                  PluginType::ZOOKEEPER_CLIENT_FACTORY);
      epoch_store = std::make_shared<ZookeeperEpochStore>(
          config->serverConfig()->getClusterName(),
          &processor,
          upd_config->updateableZookeeperConfig(),
          upd_config->updateableNodesConfiguration(),
          processor.updateableSettings(),
          zookeeper_client_factory);
    } catch (const ConstructorFailed&) {
      std::string error =
          folly::format("Failed to construct a Zookeeper client for [{}]: {}",
                        zookeeper_quorum.c_str(),
                        error_description(err))
              .str();
      ld_error("%s", error.c_str());
      throw LDQueryError(error);
    }
  }

  auto add_row = [&](Status status,
                     logid_t logid,
                     EpochMetaData* m,
                     epoch_t lce,
                     epoch_t meta_lce,
                     EpochStoreMetaProperties* meta_props,
                     TailRecord tail_record) {
    result->newRow();
    result->set("log_id", s(logid.val_));
    result->set("status", s(error_name(status)));
    if (m) {
      result->set("since", s(m->h.effective_since.val_));
      result->set("epoch", s(m->h.epoch.val_));
      result->set("replication", m->replication.toString());
      result->set("storage_set_size", s(m->shards.size()));
      result->set("storage_set", toString(m->shards));
      result->set("flags", EpochMetaData::flagsToString(m->h.flags));
      result->set("nodeset_signature", s(m->nodeset_params.signature));
      result->set(
          "target_nodeset_size", s(m->nodeset_params.target_nodeset_size));
      result->set("nodeset_seed", s(m->nodeset_params.seed));
    }
    result->set("lce", s(lce.val_));
    result->set("meta_lce", s(meta_lce.val_));
    result->set("tail_record", tail_record.toString());
    if (meta_props && meta_props->last_writer_node_id.hasValue()) {
      result->set("written_by", meta_props->last_writer_node_id->toString());
    }
  };

  Semaphore sem;
  auto callback = [&](LogMetaDataFetcher::Results results) {
    for (const auto& r : results) {
      if (r.second.epoch_store_status != E::OK) {
        add_row(r.second.epoch_store_status,
                r.first,
                nullptr,
                EPOCH_INVALID,
                EPOCH_INVALID,
                nullptr,
                TailRecord());
      } else {
        add_row(E::OK,
                r.first,
                r.second.epoch_store_metadata.get(),
                r.second.lce,
                r.second.metadatalog_lce,
                r.second.epoch_store_metadata_meta_props.get(),
                r.second.tail_record);
      }
    }
    sem.post();
  };

  std::vector<logid_t> logs;

  // If the query contains a constraint on the log id, make sure we only fetch
  // data given the constraint.
  std::string expr;
  if (columnHasEqualityConstraint(0, ctx, expr)) {
    const logid_t logid = logid_t(folly::to<logid_t::raw_type>(expr));
    logs.push_back(logid);
  } else {
    auto logs_config = config->localLogsConfig();
    ld_check(logs_config);
    for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd();
         ++it) {
      logs.push_back(logid_t(it->first));
    }
  }

  LogMetaDataFetcher fetcher(
      epoch_store, logs, callback, LogMetaDataFetcher::Type::EPOCH_STORE_ONLY);
  fetcher.setMaxInFlight(10000);
  fetcher.start(&client_impl->getProcessor());
  sem.wait();

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
