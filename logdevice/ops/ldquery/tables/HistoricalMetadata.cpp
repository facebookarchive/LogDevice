/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/HistoricalMetadata.h"

#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "logdevice/admin/safety/LogMetaDataFetcher.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/ops/ldquery/Errors.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns HistoricalMetadataTableBase::getColumns() const {
  return {
      {"log_id", DataType::LOGID, "Id of the log"},
      {"status",
       DataType::TEXT,
       "\"OK\" if the query to retrieve historical metadata succeeded for that "
       "log id.  If the log is not in the config (which only happens if the "
       "user provided query constraints on the \"log_id\" column), set to "
       "INVALID_PARAM.  If we failed to read the log's historical metadata, "
       "set to one of TIMEDOUT, ACCESS, FAILED."},
      {"since",
       DataType::BIGINT,
       "Epoch since which the metadata (\"replication\", \"storage_set\", "
       "\"flags\") are in effect."},
      {"epoch",
       DataType::BIGINT,
       "Epoch up to which the metadata is in effect."},
      {"replication",
       DataType::TEXT,
       "Replication property for records in epochs [\"since\", \"epoch\"]."},
      {"storage_set_size",
       DataType::BIGINT,
       "Number of shards in storage_set."},
      {"storage_set",
       DataType::TEXT,
       "Set of shards that may have data records for the log in epochs "
       "[\"since\", \"epoch\"]."},
      {"flags",
       DataType::TEXT,
       "Internal flags.  See \"logdevice/common/EpochMetaData.h\" for the "
       "description of each flag."},
  };
}

TableColumns HistoricalMetadataLegacy::getColumns() const {
  TableColumns cols = HistoricalMetadataTableBase::getColumns();
  TableColumns additional = {
      {"lsn",
       DataType::LSN,
       "LSN of the metadata log record that contains this metadata"},
      {"timestamp",
       DataType::BIGINT,
       "Timestamp of the metadata log record that contains this metadata"},
  };
  cols.reserve(cols.size() + additional.size());
  std::copy(additional.begin(), additional.end(), back_inserter(cols));
  return cols;
}

std::shared_ptr<TableData>
HistoricalMetadataTableBase::getDataImpl(QueryContext& ctx, bool legacy) {
  auto result = std::make_shared<TableData>();
  auto full_client = ld_ctx_->getFullClient();

  ld_check(full_client);
  ClientImpl* client_impl = static_cast<ClientImpl*>(full_client.get());
  auto config = client_impl->getConfig()->get();

  auto add_row = [&](Status status,
                     logid_t logid,
                     const EpochMetaData* m,
                     lsn_t lsn,
                     std::chrono::milliseconds timestamp) {
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
    }
    if (legacy) {
      result->set("lsn", s(lsn));
      result->set("timestamp", s(timestamp.count()));
    }
  };

  Semaphore sem;
  auto callback = [&](LogMetaDataFetcher::Results results) {
    for (const auto& r : results) {
      if (r.second.historical_metadata_status != E::OK) {
        add_row(r.second.historical_metadata_status,
                r.first,
                nullptr,
                LSN_INVALID,
                std::chrono::milliseconds{0});
      } else {
        const auto& result = r.second;
        ld_check(result.historical_metadata);
        for (const auto& interval : *result.historical_metadata) {
          lsn_t lsn = LSN_INVALID;
          std::chrono::milliseconds timestamp = std::chrono::milliseconds{0};
          // populate extras info if found
          auto it =
              result.metadata_extras.find(interval.second.h.effective_since);
          if (it != result.metadata_extras.end()) {
            lsn = it->second.lsn;
            timestamp = it->second.timestamp;
          }
          add_row(E::OK, r.first, &interval.second, lsn, timestamp);
        }
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
      nullptr,
      logs,
      callback,
      LogMetaDataFetcher::Type::HISTORICAL_METADATA_ONLY);

  if (legacy) {
    // in legacy mode, only read historical metadata from metadata logs
    fetcher.readHistoricalMetaDataOnlyFromMetaDataLog();
  }

  // we allow more parallelism for getting metadata from sequencer since
  // it costs less
  fetcher.setMaxInFlight(legacy ? 1000 : 10000);

  fetcher.start(&client_impl->getProcessor());
  sem.wait();

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
