/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <queue>

#include <folly/ThreadLocal.h>
#include <rocksdb/listener.h>
#include <rocksdb/version.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * RocksDBListener gets notified by rocksdb about flushes and compactions.
 * We use it to collect stats about table files.
 * RocksDBTablePropertiesCollector collects:
 *  - the histogram of the amount of data each log has in table file,
 *  - the amount of data for each backlog duration in each table file; this
 *    is used by RocksDBLocalLogStore to estimate the amount of space a
 *    compaction can reclaim.
 * Note that rocksdb allows registering multiple listeners, and RocksDBListener
 * is not the only one used by logdevice.
 */

class RocksDBListener : public rocksdb::EventListener {
 public:
  RocksDBListener(StatsHolder* stats, size_t shard)
      : stats_(stats), shard_(shard) {}

  void OnTableFileCreated(const rocksdb::TableFileCreationInfo& info) override;

 private:
  StatsHolder* stats_;
  const size_t shard_;
};

class RocksDBTablePropertiesCollector
    : public rocksdb::TablePropertiesCollector {
 public:
  // Backlog duration -> total size of records.
  using RetentionSizeMap = std::map<std::chrono::seconds, uint64_t>;

  explicit RocksDBTablePropertiesCollector(
      std::shared_ptr<Configuration> config,
      StatsHolder* stats)
      : config_(config), stats_(stats) {}

  rocksdb::Status AddUserKey(const rocksdb::Slice& key,
                             const rocksdb::Slice& value,
                             rocksdb::EntryType type,
                             rocksdb::SequenceNumber seq,
                             uint64_t file_size) override;
  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override;
  rocksdb::UserCollectedProperties GetReadableProperties() const override;
  const char* Name() const override;

  // Parses properties of the form "ld.bytes_with_retention.86400s" => "12345"
  // and adds them to the given map.
  static void extractRetentionSizeMap(
      const std::map<std::string, std::string>& table_properties,
      RetentionSizeMap& inout_map);

 private:
  enum class DataKind {
    PAYLOAD = 0,
    RECORD_HEADER,
    CSI,
    INDEX,

    OTHER,

    MAX,
  };

  using DataKindNamesEnumMap = EnumMap<DataKind, std::string, DataKind::MAX>;
  friend class EnumMap<DataKind, std::string, DataKind::MAX>;

  std::shared_ptr<Configuration> config_;
  StatsHolder* stats_;

  logid_t current_log_ = LOGID_INVALID;
  uint64_t current_size_ = 0;
  SizeHistogram log_size_histogram_;
  RetentionSizeMap backlog_sizes_;

  // Approximate number of bytes used for various types of data.
  std::array<size_t, (int)DataKind::MAX> data_size_per_kind_{};

  static DataKindNamesEnumMap& dataKindNames();

  void flushCurrentLog();
};

class RocksDBTablePropertiesCollectorFactory
    : public rocksdb::TablePropertiesCollectorFactory {
 public:
  explicit RocksDBTablePropertiesCollectorFactory(
      std::shared_ptr<UpdateableConfig> updateable_config,
      StatsHolder* stats)
      : updateable_config_(updateable_config), stats_(stats) {}

#if defined(ROCKSDB_MAJOR) && \
    (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 2))
  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override;
#else
  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector() override;
#endif
  const char* Name() const override;

 private:
  std::shared_ptr<UpdateableConfig> updateable_config_;
  StatsHolder* stats_;
};

}} // namespace facebook::logdevice
