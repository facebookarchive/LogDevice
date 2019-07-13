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

class IOTracing;
class RocksDBEnv;

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
  RocksDBListener(StatsHolder* stats,
                  size_t shard,
                  RocksDBEnv* env,
                  IOTracing* io_tracing)
      : stats_(stats), shard_(shard), env_(env), io_tracing_(io_tracing) {}

  // Bumps stats.
  void OnTableFileCreated(const rocksdb::TableFileCreationInfo& info) override;

  // These provide context to IO tracing.
  void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo&) override;
  void OnCompactionBegin(rocksdb::DB*,
                         const rocksdb::CompactionJobInfo&) override;

 private:
  StatsHolder* stats_;
  const size_t shard_;
  RocksDBEnv* env_;
  IOTracing* io_tracing_;
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

  virtual void BlockAdd(uint64_t blockRawBytes,
                        uint64_t blockCompressedBytesFast,
                        uint64_t blockCompressedBytesSlow) override;

  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override;
  rocksdb::UserCollectedProperties GetReadableProperties() const override;
  const char* Name() const override;

  // Parses "bytes_by_retention" section.
  // The map is not cleared; new values are added to the existing ones.
  // Returns false if format's not right or the section is missing.
  static bool extractRetentionSizeMap(
      const std::map<std::string, std::string>& table_properties,
      RetentionSizeMap& inout_map);

  // Parses "logs_by_size_log2" or "bytes_by_log" section.
  // Returns false if format's not right or the section is missing.
  static bool extractLogSizeHistogram(
      const std::map<std::string, std::string>& table_properties,
      CompactSizeHistogram& out_histogram);

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

  std::vector<std::pair<logid_t, size_t>> size_by_log_;

  // Approximate number of bytes used for various types of data.
  std::array<size_t, (int)DataKind::MAX> size_by_kind_{};

  // Assigned by Finish()/GetReadableProperties(), which is presumably called
  // only after all keys are added.
  mutable std::map<std::string, std::string> finished_properties_;
  mutable bool finished_ = false;

  static DataKindNamesEnumMap& dataKindNames();
};

class RocksDBTablePropertiesCollectorFactory
    : public rocksdb::TablePropertiesCollectorFactory {
 public:
  explicit RocksDBTablePropertiesCollectorFactory(
      std::shared_ptr<UpdateableConfig> updateable_config,
      StatsHolder* stats)
      : updateable_config_(updateable_config), stats_(stats) {}

  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override;
  const char* Name() const override;

 private:
  std::shared_ptr<UpdateableConfig> updateable_config_;
  StatsHolder* stats_;
};

}} // namespace facebook::logdevice
