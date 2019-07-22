/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/LDQuery.h"

#include <cctype>
#include <chrono>

#include "logdevice/common/debug.h"
#include "logdevice/ops/ldquery/Context.h"
#include "logdevice/ops/ldquery/Errors.h"
#include "logdevice/ops/ldquery/Table.h"
#include "logdevice/ops/ldquery/TableRegistry.h"
#include "logdevice/ops/ldquery/VirtualTable.h"
#include "tables/AppendOutliers.h"
#include "tables/AppendThroughput.h"
#include "tables/CatchupQueues.h"
#include "tables/ChunkRebuildings.h"
#include "tables/ClientReadStreams.h"
#include "tables/ClusterStateTable.h"
#include "tables/EpochStore.h"
#include "tables/EventLog.h"
#include "tables/Graylist.h"
#include "tables/HistoricalMetadata.h"
#include "tables/Info.h"
#include "tables/InfoConfig.h"
#include "tables/IsLogEmpty.h"
#include "tables/Iterators.h"
#include "tables/LogGroups.h"
#include "tables/LogRebuildings.h"
#include "tables/LogStorageState.h"
#include "tables/LogsConfigRsm.h"
#include "tables/LogsDBDirectory.h"
#include "tables/LogsDBMetadata.h"
#include "tables/Nodes.h"
#include "tables/Partitions.h"
#include "tables/Purges.h"
#include "tables/Readers.h"
#include "tables/Record.h"
#include "tables/RecordCache.h"
#include "tables/Recoveries.h"
#include "tables/Sequencers.h"
#include "tables/Settings.h"
#include "tables/ShardAuthoritativeStatus.h"
#include "tables/ShardRebuildings.h"
#include "tables/Shards.h"
#include "tables/Sockets.h"
#include "tables/Stats.h"
#include "tables/StatsRocksdb.h"
#include "tables/StorageTasks.h"
#include "tables/StoredLogs.h"
#include "tables/SyncSequencerRequests.h"

using facebook::logdevice::err;
using facebook::logdevice::errorStrings;

namespace facebook { namespace logdevice { namespace ldquery {

bool LDQuery::QueryResult::operator==(const LDQuery::QueryResult& other) const {
  return headers == other.headers && rows == other.rows;
}

static void getCacheTTLStatic(sqlite3_context* context,
                              int argc,
                              sqlite3_value** argv) {
  ld_check(0 == argc);
  (void)(argc);
  (void)(argv);
  const LDQuery* ldquery = (LDQuery*)sqlite3_user_data(context);
  sqlite3_result_int(context, ldquery->getCacheTTL().count());
}

static void setCacheTTLStatic(sqlite3_context* context,
                              int argc,
                              sqlite3_value** argv) {
  if (argc == 1) {
    const int new_ttl = sqlite3_value_int(argv[0]);
    LDQuery* ldquery = (LDQuery*)sqlite3_user_data(context);
    ldquery->setCacheTTL(std::chrono::seconds(new_ttl));
    sqlite3_result_int(context, new_ttl);
  } else {
    sqlite3_result_null(context);
  }
}

static void lsnToStringStatic(sqlite3_context* context,
                              int argc,
                              sqlite3_value** argv) {
  if (argc == 1) {
    const int64_t lsn = sqlite3_value_int64(argv[0]);
    const std::string res = lsn_to_string(lsn_t(lsn));
    sqlite3_result_text(context, res.c_str(), res.size(), SQLITE_TRANSIENT);
  } else {
    sqlite3_result_null(context);
  }
}

static void stringToLsnStatic(sqlite3_context* context,
                              int argc,
                              sqlite3_value** argv) {
  if (argc == 1) {
    const std::string str((const char*)sqlite3_value_text(argv[0]));
    lsn_t lsn = lsn_t(-1);
    if (!string_to_lsn(str, lsn)) {
      sqlite3_result_int64(context, (int64_t)lsn);
      return;
    }
  }
  sqlite3_result_null(context);
}

static void lsnToEpochStatic(sqlite3_context* context,
                             int argc,
                             sqlite3_value** argv) {
  if (argc == 1) {
    const int64_t lsn = sqlite3_value_int64(argv[0]);
    const int64_t epoch = lsn_to_epoch(lsn).val_;
    sqlite3_result_int64(context, epoch);
  } else {
    sqlite3_result_null(context);
  }
}

static void log2Static(sqlite3_context* context,
                       int argc,
                       sqlite3_value** argv) {
  if (argc == 1) {
    sqlite3_result_double(context, log2(sqlite3_value_double(argv[0])));
  } else {
    sqlite3_result_null(context);
  }
}

LDQuery::LDQuery(std::string config_path,
                 std::chrono::milliseconds command_timeout,
                 bool use_ssl)
    : config_path_(std::move(config_path)), command_timeout_(command_timeout) {
  ctx_ = std::make_shared<Context>();
  ctx_->commandTimeout = command_timeout_;
  ctx_->config_path = config_path_;
  ctx_->use_ssl = use_ssl;

  table_registry_.registerTable<tables::AppendOutliers>(ctx_);
  table_registry_.registerTable<tables::AppendThroughput>(ctx_);
  table_registry_.registerTable<tables::CatchupQueues>(ctx_);
  table_registry_.registerTable<tables::ChunkRebuildings>(ctx_);
  table_registry_.registerTable<tables::ClientReadStreams>(ctx_);
  table_registry_.registerTable<tables::ClusterStateTable>(ctx_);
  table_registry_.registerTable<tables::LogsDBDirectory>(ctx_);
  table_registry_.registerTable<tables::EpochStore>(ctx_);
  table_registry_.registerTable<tables::EventLog>(ctx_);
  table_registry_.registerTable<tables::Graylist>(ctx_);
  table_registry_.registerTable<tables::HistoricalMetadata>(ctx_);
  table_registry_.registerTable<tables::HistoricalMetadataLegacy>(ctx_);
  table_registry_.registerTable<tables::Info>(ctx_);
  table_registry_.registerTable<tables::InfoConfig>(ctx_);
  table_registry_.registerTable<tables::IsLogEmpty>(ctx_);
  table_registry_.registerTable<tables::Iterators>(ctx_);
  table_registry_.registerTable<tables::LogGroups>(ctx_);
  table_registry_.registerTable<tables::LogRebuildings>(ctx_);
  table_registry_.registerTable<tables::LogStorageState>(ctx_);
  table_registry_.registerTable<tables::LogsConfigRsm>(ctx_);
  table_registry_.registerTable<tables::LogsDBMetadata>(ctx_);
  table_registry_.registerTable<tables::Nodes>(ctx_);
  table_registry_.registerTable<tables::Partitions>(ctx_);
  table_registry_.registerTable<tables::Purges>(ctx_);
  table_registry_.registerTable<tables::Readers>(ctx_);
  table_registry_.registerTable<tables::Record<tables::RecordQueryMode::CSI>>(ctx_);
  table_registry_.registerTable<tables::Record<tables::RecordQueryMode::DATA>>(ctx_);
  table_registry_.registerTable<tables::RecordCache>(ctx_);
  table_registry_.registerTable<tables::Recoveries>(ctx_);
  table_registry_.registerTable<tables::Sequencers>(ctx_);
  table_registry_.registerTable<tables::Settings>(ctx_);
  table_registry_.registerTable<tables::ShardAuthoritativeStatus>(
      ctx_, tables::ShardAuthoritativeStatus::Verbose::NORMAL);
  table_registry_.registerTable<tables::ShardAuthoritativeStatus>(
      ctx_, tables::ShardAuthoritativeStatus::Verbose::VERBOSE);
  table_registry_.registerTable<tables::ShardAuthoritativeStatus>(
      ctx_, tables::ShardAuthoritativeStatus::Verbose::SPEW);
  table_registry_.registerTable<tables::ShardRebuildings>(ctx_);
  table_registry_.registerTable<tables::Shards>(ctx_);
  table_registry_.registerTable<tables::Sockets>(ctx_);
  table_registry_.registerTable<tables::Stats>(ctx_);
  table_registry_.registerTable<tables::StatsRocksdb>(ctx_);
  table_registry_.registerTable<tables::StorageTasks>(ctx_);
  table_registry_.registerTable<tables::StoredLogs>(ctx_);
  table_registry_.registerTable<tables::SyncSequencerRequests>(ctx_);

  const int rc = sqlite3_open(":memory:", &db_);

  if (rc != 0) {
    ld_error("Can't open database: %s\n", sqlite3_errmsg(db_));
    throw ConstructorFailed();
  }

  if (table_registry_.attachTables(db_) != 0) {
    throw ConstructorFailed();
  }

  setCacheTTL(cache_ttl_);

  // A function for retrieving the current ttl configured for table cache.
  sqlite3_create_function(db_,
                          "get_cache_ttl",
                          0,
                          SQLITE_UTF8,
                          (void*)this,
                          getCacheTTLStatic,
                          0,
                          0);
  // A function for changing the ttl configured for table cache.
  sqlite3_create_function(db_,
                          "set_cache_ttl",
                          1,
                          SQLITE_UTF8,
                          (void*)this,
                          setCacheTTLStatic,
                          0,
                          0);
  // A function for converting an integer to a human readable lsn.
  sqlite3_create_function(db_,
                          "lsn_to_string",
                          1,
                          SQLITE_UTF8,
                          (void*)this,
                          lsnToStringStatic,
                          0,
                          0);
  // A function for converting a human readable lsn to an integer.
  sqlite3_create_function(db_,
                          "string_to_lsn",
                          1,
                          SQLITE_UTF8,
                          (void*)this,
                          stringToLsnStatic,
                          0,
                          0);
  // A function for converting an lsn integer to an epoch.
  sqlite3_create_function(
      db_, "lsn_to_epoch", 1, SQLITE_UTF8, (void*)this, lsnToEpochStatic, 0, 0);
  // Logarithm in base 2.
  sqlite3_create_function(
      db_, "log2", 1, SQLITE_UTF8, (void*)this, log2Static, 0, 0);
}

LDQuery::~LDQuery() {
  if (db_) {
    sqlite3_close(db_);
  }
}

LDQuery::QueryResult LDQuery::executeNextStmt(sqlite3_stmt* pStmt) {
  QueryResult res;
  ctx_->resetActiveQuery();

  const int ncols = sqlite3_column_count(pStmt);
  for (int i = 0; i < ncols; ++i) {
    res.headers.push_back(std::string(sqlite3_column_name(pStmt, i)));
    res.cols_max_size.push_back(res.headers.back().size());
  }

  while (sqlite3_step(pStmt) == SQLITE_ROW) {
    Row row;
    for (int i = 0; i < ncols; ++i) {
      char* v = (char*)sqlite3_column_text(pStmt, i);
      if (v) {
        row.emplace_back(v);
        res.cols_max_size[i] =
            std::max(res.cols_max_size[i], row.back().size());
      } else {
        // TODO(#7646110): Row should be
        // std::vector<folly::Optional<std::string>> in order to not confuse
        // empty string and null values. Python bindings would then convert
        // null values to None.
        row.push_back("");
      }
    }
    res.rows.push_back(std::move(row));
  }

  return res;
}

LDQuery::QueryResults LDQuery::query(const std::string& query) {
  QueryResults results;

  sqlite3_stmt* pStmt = nullptr;
  const char* leftover = query.c_str();

  // This tells each virtual table that the next time we fetch data from them
  // they should refill their cache depending on their ttl.
  table_registry_.notifyNewQuery();

  while (leftover[0]) {
    int rc = sqlite3_prepare_v2(db_, leftover, -1, &pStmt, &leftover);
    if (rc != SQLITE_OK) {
      ld_error("Error in statement %s", sqlite3_errmsg(db_));
      throw StatementError(sqlite3_errmsg(db_));
    }

    if (!pStmt) {
      // This happens for a comment or a whitespace.
      while (isspace(leftover[0])) {
        ++leftover;
      }
      continue;
    }

    std::chrono::steady_clock::time_point tstart =
        std::chrono::steady_clock::now();
    QueryResult res = executeNextStmt(pStmt);
    std::chrono::steady_clock::time_point tend =
        std::chrono::steady_clock::now();
    res.metadata = ctx_->activeQueryMetadata;
    sqlite3_finalize(pStmt);
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(tend - tstart)
            .count();
    res.metadata.latency = duration;
    results.push_back(std::move(res));
  }

  return results;
}

std::vector<TableMetadata> LDQuery::getTables() const {
  auto tables = table_registry_.getTables();
  return tables;
}

void LDQuery::setCacheTTL(std::chrono::seconds ttl) {
  cache_ttl_ = ttl;
  table_registry_.setCacheTTL(ttl);
}

void LDQuery::enableServerSideFiltering(bool val) {
  server_side_filtering_enabled_ = val;
  table_registry_.enableServerSideFiltering(val);
}

bool LDQuery::serverSideFilteringEnabled() const {
  return server_side_filtering_enabled_;
}

void LDQuery::setPrettyOutput(bool val) {
  ctx_->pretty_output = val;
}

bool LDQuery::getPrettyOutput() const {
  return ctx_->pretty_output;
}

}}} // namespace facebook::logdevice::ldquery
