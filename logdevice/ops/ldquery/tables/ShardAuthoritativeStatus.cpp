/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/ShardAuthoritativeStatus.h"

#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ops/EventLogUtils.h"
#include "logdevice/ops/ldquery/Errors.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns ShardAuthoritativeStatus::getColumns() const {
  TableColumns res({
      {"node_id", DataType::BIGINT, "Id of the node."},
      {"shard", DataType::BIGINT, "Id of the shard."},
      {"rebuilding_version",
       DataType::TEXT,
       "Rebuilding version: the LSN of the last SHARD_NEEDS_REBUILD delta from "
       "the event log."},
      {"authoritative_status",
       DataType::TEXT,
       "Authoritative status of the shard."},
      {"donors_remaining",
       DataType::TEXT,
       "If authoritative status is UNDERREPLICATION, list of donors that have "
       "not finished rebuilding the under-replicated data."},
      {"drain",
       DataType::INTEGER,
       "Whether the shard is being drained or has been drained."},
      {"dirty_ranges",
       DataType::TEXT,
       "Time ranges where this shard may be missing data.  This happens if the "
       "LogDevice process on this storage node crashed before committing data "
       "to disk."},
      {"rebuilding_is_authoritative",
       DataType::INTEGER,
       "Whether rebuilding is authoritative.  A non authoritative rebuilding "
       "means that too many shards lost data such that all copies of some "
       "records may be unavailable.  Some readers may stall when this happens "
       "and there are some shards that are still marked as recoverable."},
      {"data_is_recoverable",
       DataType::INTEGER,
       "Indicates whether the shard's data has been marked as unrecoverable "
       "using `ldshell mark-unrecoverable`. If all shards in the rebuilding "
       "set are marked unrecoverable, shards for which rebuilding completed "
       "will transition to AUTHORITATIVE_EMPTY status even if that rebuilding "
       "is non authoritative. Note that if logdeviced is started on a shard "
       "whose corresponding disk has been wiped by a remediation, the shard's "
       "data will automatically be considered unrecoverable."},
      {"source",
       DataType::TEXT,
       "Entity that triggered rebuilding for this shard."},
      {"details", DataType::TEXT, "Reason for rebuilding this shard."},
      {"rebuilding_started_ts", DataType::TEXT, "When rebuilding was started."},
      {"rebuilding_completed_ts",
       DataType::TEXT,
       "When the shard transitioned to AUTHORITATIVE_EMPTY."},
  });

  if (verbose_ >= Verbose::VERBOSE) {
    res.push_back(
        TableColumn{"mode",
                    DataType::TEXT,
                    "Whether the shard participates in its own rebuilding"});
    res.push_back(TableColumn{
        "acked",
        DataType::INTEGER,
        "Whether the node acked the rebuilding. (Why would such nodes remain "
        "in the rebuilding set at all? No one remembers now.)"});
    res.push_back(
        TableColumn{"ack_lsn",
                    DataType::TEXT,
                    "LSN of the SHARD_ACK_REBUILT written by this shard."});
    res.push_back(TableColumn{"ack_version",
                              DataType::TEXT,
                              "Version of the rebuilding that was acked."});
  }

  if (verbose_ >= Verbose::SPEW) {
    res.push_back(TableColumn{"donors_complete", DataType::TEXT, ""});
    res.push_back(
        TableColumn{"donors_complete_authoritatively", DataType::TEXT, ""});
  }

  return res;
}

void ShardAuthoritativeStatus::newQuery() {
  cached_.reset();
}

std::shared_ptr<TableData>
ShardAuthoritativeStatus::getData(QueryContext& /*ctx*/) {
  if (cached_) {
    return cached_;
  }

  cached_ = std::make_shared<TableData>();
  auto ld_client = ld_ctx_->getClient();

  EventLogRebuildingSet set;
  const int rv = EventLogUtils::getRebuildingSet(*ld_client, set);
  if (rv != 0) {
    std::string failure_reason = "Cannot read event log: ";
    failure_reason += error_description(err);
    throw LDQueryError(std::move(failure_reason));
  }

  const auto& nodes_configuration = dynamic_cast<ClientImpl*>(ld_client.get())
                                        ->getConfig()
                                        ->getNodesConfiguration();

  for (auto& shard : set.getRebuildingShards()) {
    for (auto& node : shard.second.nodes_) {
      if (verbose_ == Verbose::NORMAL &&
          !nodes_configuration->isNodeInServiceDiscoveryConfig(node.first)) {
        // ignore nodes that are not in config
        continue;
      }

      std::vector<node_index_t> donors_remaining;
      auto status = set.getShardAuthoritativeStatus(
          node.first, shard.first, donors_remaining);
      if (verbose_ == Verbose::NORMAL &&
          status == AuthoritativeStatus::FULLY_AUTHORITATIVE &&
          donors_remaining.empty() && node.second.dc_dirty_ranges.empty()) {
        continue;
      }
      std::sort(donors_remaining.begin(), donors_remaining.end());
      cached_->cols["node_id"].push_back(s(node.first));
      cached_->cols["shard"].push_back(s(shard.first));
      cached_->cols["rebuilding_version"].push_back(
          lsn_to_string(node.second.version));
      cached_->cols["authoritative_status"].push_back(
          logdevice::toString(status));
      cached_->cols["donors_remaining"].push_back(
          folly::join(",", donors_remaining));
      cached_->cols["drain"].push_back(s(node.second.drain));
      cached_->cols["dirty_ranges"].push_back(
          logdevice::toString(node.second.dc_dirty_ranges));
      cached_->cols["rebuilding_is_authoritative"].push_back(
          s(!node.second.rebuildingIsNonAuthoritative()));
      cached_->cols["data_is_recoverable"].push_back(
          s(node.second.recoverable));
      cached_->cols["source"].push_back(node.second.source);
      cached_->cols["details"].push_back(node.second.details);
      cached_->cols["rebuilding_started_ts"].push_back(
          format_time(node.second.rebuilding_started_ts));
      cached_->cols["rebuilding_completed_ts"].push_back(
          format_time(node.second.rebuilding_completed_ts));

      if (verbose_ >= Verbose::VERBOSE) {
        std::string mode_name = "???";
        switch (node.second.mode) {
          case RebuildingMode::RELOCATE:
            mode_name = "RELOCATE";
            break;
          case RebuildingMode::RESTORE:
            mode_name = "RESTORE";
            break;
          case RebuildingMode::INVALID:
            mode_name = "INVALID";
            break;
        }
        cached_->cols["mode"].push_back(mode_name);
        cached_->cols["acked"].push_back(s(node.second.acked));
        cached_->cols["ack_lsn"].push_back(lsn_to_string(node.second.ack_lsn));
        cached_->cols["ack_version"].push_back(
            lsn_to_string(node.second.ack_version));
      }

      if (verbose_ >= Verbose::SPEW) {
        cached_->cols["donors_complete"].push_back(
            toString(node.second.donors_complete));
        cached_->cols["donors_complete_authoritatively"].push_back(
            toString(node.second.donors_complete_authoritatively));
      }
    }
  }

  return cached_;
}

}}}} // namespace facebook::logdevice::ldquery::tables
