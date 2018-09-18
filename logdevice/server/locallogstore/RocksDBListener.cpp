/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "RocksDBListener.h"

#include <set>

#include <folly/Conv.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"

namespace facebook { namespace logdevice {

using namespace RocksDBKeyFormat;

static const char* LOGS_OF_SIZE_PREFIX = "ld.logs_of_size.";
static const char* BYTES_WITH_RETENTION_PREFIX = "ld.bytes_with_retention.";

void RocksDBListener::OnFlushCompleted(
    rocksdb::DB* /*db*/,
    const rocksdb::FlushJobInfo& flush_job_info) {
  ld_check(stats_);
  PER_SHARD_STAT_INCR(stats_, num_memtable_flush_completed, shard_);
  if (!isDataCF(flush_job_info.cf_name)) {
    PER_SHARD_STAT_INCR(stats_, num_metadata_memtable_flush_completed, shard_);
    return;
  }
  ld_check(stats_);
  onJobCompleted(flush_job_info.job_id,
                 {flush_job_info.file_path},
                 stats_->get().per_shard_histograms->flushed_file_size,
                 stats_->get().per_shard_histograms->flushed_log_run_length);
}

void RocksDBListener::OnCompactionCompleted(
    rocksdb::DB* /*db*/,
    const rocksdb::CompactionJobInfo& ci) {
  if (!isDataCF(ci.cf_name)) {
    return;
  }
  if (!ci.status.ok()) {
    return;
  }
  ld_check(stats_);
  onJobCompleted(ci.job_id,
                 ci.output_files,
                 stats_->get().per_shard_histograms->compacted_file_size,
                 stats_->get().per_shard_histograms->compacted_log_run_length);
}

bool RocksDBListener::isDataCF(const std::string& cf_name) {
  return cf_name != "metadata";
}

void RocksDBListener::onJobCompleted(
    int job_id,
    const std::vector<std::string>& paths,
    PerShardHistograms::size_histogram_t& file_size_hist,
    PerShardHistograms::size_histogram_t& log_run_length_hist) {
  std::set<std::string> path_set(paths.begin(), paths.end());
  for (; !recently_created_files_->empty(); recently_created_files_->pop()) {
    const auto& info = recently_created_files_->front();
    if (info.job_id != job_id) {
      continue;
    }
    ld_check(path_set.count(info.file_path));
    path_set.erase(info.file_path);
    file_size_hist.add(shard_, info.file_size);
    if (auto out_hist = log_run_length_hist.get(shard_)) {
      out_hist->merge(
          SizeHistogram(info.table_properties.user_collected_properties,
                        LOGS_OF_SIZE_PREFIX));
    }
  }
}

void RocksDBListener::OnTableFileCreated(
    const rocksdb::TableFileCreationInfo& info) {
  if (info.file_path == "(nil)") {
    // This happens when compaction's output is empty, so compaction doesn't
    // produce a file. In this case rocksdb calls OnTableFileCreated() anyway,
    // but the corresponding OnCompactionCompleted() call has empty list of
    // output files.
    return;
  }
  recently_created_files_->push(info);
  if (recently_created_files_->size() > 1000) {
    recently_created_files_->pop();
  }
}

template <>
/* static */
const std::string&
RocksDBTablePropertiesCollector::DataKindNamesEnumMap::invalidValue() {
  static std::string s = "<invalid>";
  return s;
}

template <>
void RocksDBTablePropertiesCollector::DataKindNamesEnumMap::setValues() {
  static_assert((int)RocksDBTablePropertiesCollector::DataKind::MAX == 5,
                "Did you add a DataKind? Please add its name here.");
  set(RocksDBTablePropertiesCollector::DataKind::PAYLOAD, "payload");
  set(RocksDBTablePropertiesCollector::DataKind::RECORD_HEADER,
      "record_header");
  set(RocksDBTablePropertiesCollector::DataKind::CSI, "csi");
  set(RocksDBTablePropertiesCollector::DataKind::INDEX, "index");
  set(RocksDBTablePropertiesCollector::DataKind::OTHER, "other");
}

RocksDBTablePropertiesCollector::DataKindNamesEnumMap&
RocksDBTablePropertiesCollector::dataKindNames() {
  static DataKindNamesEnumMap x;
  return x;
}

rocksdb::Status
RocksDBTablePropertiesCollector::AddUserKey(const rocksdb::Slice& key,
                                            const rocksdb::Slice& value,
                                            rocksdb::EntryType type,
                                            rocksdb::SequenceNumber /*seq*/,
                                            uint64_t /*file_size*/) {
  size_t key_value_size = key.size() + value.size();

  if (IndexKey::valid(key.data(), key.size())) {
    data_size_per_kind_[(int)DataKind::INDEX] += key_value_size;
    return rocksdb::Status::OK();
  }

  if (CopySetIndexKey::valid(key.data(), key.size())) {
    data_size_per_kind_[(int)DataKind::CSI] += key_value_size;
    return rocksdb::Status::OK();
  }

  if (!DataKey::valid(key.data(), key.size())) {
    data_size_per_kind_[(int)DataKind::OTHER] += key_value_size;
    return rocksdb::Status::OK();
  }

  if (type == rocksdb::EntryType::kEntryPut ||
      type == rocksdb::EntryType::kEntryMerge) {
    Slice value_slice(value.data(), value.size());
    if (type == rocksdb::EntryType::kEntryMerge && value_slice.size > 0) {
      // Remove the 'd' byte prepended to merge operands by RocksDBWriter.
      value_slice.data = reinterpret_cast<const char*>(value_slice.data) + 1;
      --value_slice.size;
    }

    Payload payload;
    int rv = LocalLogStoreRecordFormat::parse(value_slice,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              0,
                                              nullptr,
                                              nullptr,
                                              &payload,
                                              -1 /* unused */);

    size_t payload_size = 0;
    if (rv == 0) {
      payload_size = payload.size();
    } else {
      payload_size = 0;
    }
    ld_check(payload_size <= key_value_size);

    data_size_per_kind_[(int)DataKind::PAYLOAD] += payload_size;
    data_size_per_kind_[(int)DataKind::RECORD_HEADER] +=
        key_value_size - payload_size;
  } else {
    // A delete.
    data_size_per_kind_[(int)DataKind::OTHER] += key_value_size;
  }

  logid_t log = DataKey::getLogID(key.data());
  if (log == current_log_) {
    current_size_ += key_value_size;
    return rocksdb::Status::OK();
  }
  // Assume that all records for the same log are consecutive.
  flushCurrentLog();
  current_log_ = log;
  current_size_ = key_value_size;
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDBTablePropertiesCollector::Finish(
    rocksdb::UserCollectedProperties* properties) {
  flushCurrentLog();
  *properties = GetReadableProperties();

  // Bump stats, unless this file appears to belong to metadata column family.
  bool has_interesting_data = false;
  for (int kind = 0; kind < (int)DataKind::MAX; ++kind) {
    if (kind != (int)DataKind::OTHER && data_size_per_kind_[kind] != 0) {
      has_interesting_data = true;
      break;
    }
  }
  if (has_interesting_data) {
    static_assert(
        (int)DataKind::MAX == 5,
        "Added a new DataKind? Please add a corresponding stat and bump it "
        "here.");
    STAT_ADD(
        stats_, sst_bytes_payload, data_size_per_kind_[(int)DataKind::PAYLOAD]);
    STAT_ADD(stats_,
             sst_bytes_record_header,
             data_size_per_kind_[(int)DataKind::RECORD_HEADER]);
    STAT_ADD(stats_, sst_bytes_csi, data_size_per_kind_[(int)DataKind::CSI]);
    STAT_ADD(
        stats_, sst_bytes_index, data_size_per_kind_[(int)DataKind::INDEX]);
    STAT_ADD(
        stats_, sst_bytes_other, data_size_per_kind_[(int)DataKind::OTHER]);
  }

  return rocksdb::Status::OK();
}

void RocksDBTablePropertiesCollector::flushCurrentLog() {
  if (current_size_ == 0) {
    return;
  }

  log_size_histogram_.add(current_size_);

  if (config_) {
    std::chrono::seconds backlog;
    const LogsConfig::LogGroupNode* log_config =
        config_->getLogGroupByIDRaw(current_log_);
    if (!log_config) {
      backlog = std::chrono::seconds(0);
    } else if (log_config->attrs().backlogDuration().value()) {
      backlog = log_config->attrs().backlogDuration().value().value();
    } else {
      backlog = std::chrono::seconds::max();
    }
    backlog_sizes_[backlog] += current_size_;
  }
}

rocksdb::UserCollectedProperties
RocksDBTablePropertiesCollector::GetReadableProperties() const {
  auto res = log_size_histogram_.toMap(LOGS_OF_SIZE_PREFIX);
  for (auto it : backlog_sizes_) {
    res[BYTES_WITH_RETENTION_PREFIX + chrono_string(it.first)] =
        std::to_string(it.second);
  }

  for (int kind = 0; kind < (int)DataKind::MAX; ++kind) {
    res["bytes_" + dataKindNames()[(DataKind)kind]] =
        std::to_string(data_size_per_kind_[kind]);
  }

  return res;
}

void RocksDBTablePropertiesCollector::extractRetentionSizeMap(
    const std::map<std::string, std::string>& table_properties,
    RetentionSizeMap& inout_map) {
  std::string prefix = BYTES_WITH_RETENTION_PREFIX;
  auto it_begin = table_properties.lower_bound(BYTES_WITH_RETENTION_PREFIX);
  std::string end_str = prefix;
  ++end_str[end_str.size() - 1];
  auto it_end = table_properties.lower_bound(end_str);

  for (auto it = it_begin; it != it_end; ++it) {
    std::chrono::seconds duration;
    uint64_t bytes;

    ld_check(it->first.substr(0, prefix.size()) == prefix);
    int rv = parse_chrono_string(it->first.substr(prefix.size()), &duration);
    if (rv != 0) {
      ld_warning("Invalid backlog duration in table properties: %s (value: %s)",
                 it->first.c_str(),
                 it->second.c_str());
      continue;
    }

    try {
      bytes = folly::to<uint64_t>(it->second);
    } catch (std::range_error&) {
      ld_warning("Invalid value in table properties for backlog duration %s: "
                 "%s",
                 it->first.c_str(),
                 it->second.c_str());
      continue;
    }
    inout_map[duration] += bytes;
  }
}

const char* RocksDBTablePropertiesCollector::Name() const {
  return "facebook::logdevice::TablePropertiesCollector";
}

rocksdb::TablePropertiesCollector*
#if defined(ROCKSDB_MAJOR) && \
    (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 2))
RocksDBTablePropertiesCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /*context*/) {
#else
RocksDBTablePropertiesCollectorFactory::CreateTablePropertiesCollector() {
#endif
  return new RocksDBTablePropertiesCollector(
      updateable_config_ ? updateable_config_->get() : nullptr, stats_);
}

const char* RocksDBTablePropertiesCollectorFactory::Name() const {
  return "facebook::logdevice::RocksDBTablePropertiesCollectorFactory";
}

}} // namespace facebook::logdevice
