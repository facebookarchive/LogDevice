/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBListener.h"

#include <set>

#include <folly/Conv.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/locallogstore/IOTracing.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"

namespace facebook { namespace logdevice {

using namespace RocksDBKeyFormat;

void RocksDBListener::OnTableFileCreated(
    const rocksdb::TableFileCreationInfo& info) {
  ld_check(stats_);

  if (info.file_path == "(nil)") {
    // This happens when compaction's output is empty, so compaction doesn't
    // produce a file. In this case rocksdb calls OnTableFileCreated() anyway,
    // but the corresponding OnCompactionCompleted() call has empty list of
    // output files.
    // Let's ignore such compactions.
    return;
  }
  if (info.cf_name == "metadata") {
    if (info.reason == rocksdb::TableFileCreationReason::kFlush) {
      PER_SHARD_STAT_INCR(
          stats_, num_metadata_memtable_flush_completed, shard_);
    }
    // We're not interested in other stats for metadata flushes/compactions
    // for now.
    return;
  }

  PerShardHistograms::compact_size_histogram_t* file_size_hist = nullptr;
  PerShardHistograms::compact_size_histogram_t* log_run_length_hist = nullptr;

  if (info.reason == rocksdb::TableFileCreationReason::kFlush) {
    PER_SHARD_STAT_INCR(stats_, num_memtable_flush_completed, shard_);

    file_size_hist = &stats_->get().per_shard_histograms->flushed_file_size;
    log_run_length_hist =
        &stats_->get().per_shard_histograms->flushed_log_run_length;
  } else if (info.reason == rocksdb::TableFileCreationReason::kCompaction) {
    file_size_hist = &stats_->get().per_shard_histograms->compacted_file_size;
    log_run_length_hist =
        &stats_->get().per_shard_histograms->compacted_log_run_length;
  }

  if (file_size_hist != nullptr) {
    file_size_hist->add(shard_, info.file_size);
    if (auto out_hist = log_run_length_hist->get(shard_)) {
      CompactSizeHistogram log_size_histogram;
      if (RocksDBTablePropertiesCollector::extractLogSizeHistogram(
              info.table_properties.user_collected_properties,
              log_size_histogram)) {
        out_hist->merge(log_size_histogram);
      } else {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            2,
            "Failed to parse retention distribution from table properties that "
            "were supposed to be produced by us just now. Something about "
            "table properties is broken, please investigate. Properties: %s",
            toString(info.table_properties.user_collected_properties).c_str());
      }
    }
  }
}

void RocksDBListener::OnFlushBegin(rocksdb::DB*,
                                   const rocksdb::FlushJobInfo& info) {
  IOTracing::AddContext* io_tracing_context =
      env_->backgroundJobContextOfThisThread();
  if (io_tracing_context && io_tracing_ && io_tracing_->isEnabled()) {
    io_tracing_context->assign(io_tracing_, "flush|cf:{}", info.cf_name);
  }
}
void RocksDBListener::OnCompactionBegin(
    rocksdb::DB*,
    const rocksdb::CompactionJobInfo& info) {
  IOTracing::AddContext* io_tracing_context =
      env_->backgroundJobContextOfThisThread();
  if (io_tracing_context && io_tracing_ && io_tracing_->isEnabled()) {
    io_tracing_context->assign(io_tracing_, "compact|cf:{}", info.cf_name);
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
  set(RocksDBTablePropertiesCollector::DataKind::RECORD_HEADER, "hdr");
  set(RocksDBTablePropertiesCollector::DataKind::CSI, "csi");
  set(RocksDBTablePropertiesCollector::DataKind::INDEX, "idx");
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
  if (finished_) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "RocksDB called AddUserKey() after Finalize()/GetReadableProperties(). "
        "This is weird, you may want to investigate that.");
    finished_ = false;
  }

  size_t key_value_size = key.size() + value.size();

  if (IndexKey::valid(key.data(), key.size())) {
    size_by_kind_[(int)DataKind::INDEX] += key_value_size;
    return rocksdb::Status::OK();
  }

  if (CopySetIndexKey::valid(key.data(), key.size())) {
    size_by_kind_[(int)DataKind::CSI] += key_value_size;
    return rocksdb::Status::OK();
  }

  if (!DataKey::valid(key.data(), key.size())) {
    size_by_kind_[(int)DataKind::OTHER] += key_value_size;
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

    size_by_kind_[(int)DataKind::PAYLOAD] += payload_size;
    size_by_kind_[(int)DataKind::RECORD_HEADER] +=
        key_value_size - payload_size;
  } else {
    // A delete.
    size_by_kind_[(int)DataKind::OTHER] += key_value_size;
  }

  // Assume that all records for the same log are consecutive.
  logid_t log = DataKey::getLogID(key.data());
  if (!size_by_log_.empty() && log == size_by_log_.back().first) {
    size_by_log_.back().second += key_value_size;
  } else {
    size_by_log_.emplace_back(log, key_value_size);
  }

  return rocksdb::Status::OK();
}

// We get called for every block cut by RocksDB. But not every block is selected
// for compression samplng. The blockCompressedBytesFast and
// blockCompressedBytesSlow are non=zero only if that block was selected for
// compression sampling.
void RocksDBTablePropertiesCollector::BlockAdd(
    uint64_t blockRawBytes,
    uint64_t blockCompressedBytesFast,
    uint64_t blockCompressedBytesSlow) {
  if (blockCompressedBytesFast) {
    STAT_ADD(stats_, sampled_blocks_raw_bytes_fast, blockRawBytes);
    STAT_ADD(
        stats_, sampled_blocks_compressed_bytes_fast, blockCompressedBytesFast);
  }

  if (blockCompressedBytesSlow) {
    STAT_ADD(stats_, sampled_blocks_raw_bytes_slow, blockRawBytes);
    STAT_ADD(
        stats_, sampled_blocks_compressed_bytes_slow, blockCompressedBytesSlow);
  }

  if (blockRawBytes >= 1024 * 1024) {
    STAT_ADD(stats_, num_sst_blocks_GT_1024KB, blockRawBytes);
  } else if (blockRawBytes >= 512 * 1024) {
    STAT_ADD(stats_, num_sst_blocks_GT_512KB, blockRawBytes);
  } else if (blockRawBytes >= 256 * 1024) {
    STAT_ADD(stats_, num_sst_blocks_GT_256KB, blockRawBytes);
  } else if (blockRawBytes >= 128 * 1024) {
    STAT_ADD(stats_, num_sst_blocks_GT_128KB, blockRawBytes);
  } else if (blockRawBytes >= 64 * 1024) {
    STAT_ADD(stats_, num_sst_blocks_GT_64KB, blockRawBytes);
  } else if (blockRawBytes >= 32 * 1024) {
    STAT_ADD(stats_, num_sst_blocks_GT_32KB, blockRawBytes);
  } else {
    STAT_ADD(stats_, num_sst_blocks_LT_32KB, blockRawBytes);
  }

  return;
}

rocksdb::Status RocksDBTablePropertiesCollector::Finish(
    rocksdb::UserCollectedProperties* properties) {
  *properties = GetReadableProperties();

  // Bump stats, unless this file appears to belong to metadata column family.
  bool has_interesting_data = false;
  for (int kind = 0; kind < (int)DataKind::MAX; ++kind) {
    if (kind != (int)DataKind::OTHER && size_by_kind_[kind] != 0) {
      has_interesting_data = true;
      break;
    }
  }
  if (has_interesting_data) {
    static_assert(
        (int)DataKind::MAX == 5,
        "Added a new DataKind? Please add a corresponding stat and bump it "
        "here.");
    STAT_ADD(stats_, sst_bytes_payload, size_by_kind_[(int)DataKind::PAYLOAD]);
    STAT_ADD(stats_,
             sst_bytes_record_header,
             size_by_kind_[(int)DataKind::RECORD_HEADER]);
    STAT_ADD(stats_, sst_bytes_csi, size_by_kind_[(int)DataKind::CSI]);
    STAT_ADD(stats_, sst_bytes_index, size_by_kind_[(int)DataKind::INDEX]);
    STAT_ADD(stats_, sst_bytes_other, size_by_kind_[(int)DataKind::OTHER]);
  }

  return rocksdb::Status::OK();
}

rocksdb::UserCollectedProperties
RocksDBTablePropertiesCollector::GetReadableProperties() const {
  if (finished_) {
    return finished_properties_;
  }

  // If we have few logs, just list them all in the properties.
  // If we have many, summarize their sizes in a histogram.
  // Histogram is usually shorter, but log IDs sometimes come in handy for
  // investigations, especially if there's only one.
  bool use_histogram = size_by_log_.size() > 5;
  CompactSizeHistogram log_size_histogram;
  RetentionSizeMap size_by_retention;
  for (const auto& p : size_by_log_) {
    if (use_histogram) {
      log_size_histogram.add(static_cast<int64_t>(p.second));
    }

    if (config_) {
      std::chrono::seconds backlog;
      const std::shared_ptr<LogsConfig::LogGroupNode> log_config =
          config_->getLogGroupByIDShared(p.first);
      if (!log_config) {
        backlog = std::chrono::seconds(0);
      } else if (log_config->attrs().backlogDuration().value()) {
        backlog = log_config->attrs().backlogDuration().value().value();
      } else {
        backlog = std::chrono::seconds::max();
      }
      size_by_retention[backlog] += p.second;
    }
  }

  // Pack everything into one string because rocksdb has significant memory
  // overhead per key in table properties map.
  std::stringstream ss;

  // example:
  // bytes_by_kind:{csi:10404,idx:7514,other:0,hdr:16762,payload:78918865},
  ss << "bytes_by_kind:{";
  bool first = true;
  for (int kind = 0; kind < (int)DataKind::MAX; ++kind) {
    size_t x = size_by_kind_[kind];
    if (x == 0) {
      continue;
    }
    if (!first) {
      ss << ",";
    }
    first = false;
    ss << dataKindNames()[(DataKind)kind] << ":" << x;
  }
  ss << "},";

  // example: bytes_by_retention:{3d:64241969,5d:14695191,7d:3282},
  ss << "bytes_by_retention:{";
  first = true;
  for (const auto& p : size_by_retention) {
    if (!first) {
      ss << ",";
    }
    first = false;
    ss << format_chrono_string(p.first) << ":" << p.second;
  }
  ss << "},";

  if (use_histogram) {
    // example: logs_by_size_log2:{3:9,5:9,19:42}
    ss << "logs_by_size_log2:{" << log_size_histogram.toShortString() << "}";
  } else {
    // example: bytes_by_log:{6628374678234:1234567}
    ss << "bytes_by_log:{";
    first = true;
    for (const auto& p : size_by_log_) {
      if (!first) {
        ss << ",";
      }
      first = false;
      ss << p.first.val() << ":" << p.second;
    }
    ss << "}";
  }

  finished_properties_ = {{"logdevice", ss.str()}};
  finished_ = true;
  return finished_properties_;
}

// Find substring "<section_name>:{...}" and return the "..." part.
static bool
findSection(const std::map<std::string, std::string>& table_properties,
            std::string section_name,
            folly::StringPiece& out) {
  auto it = table_properties.find("logdevice");
  if (it == table_properties.end()) {
    return false;
  }
  section_name += ":{";
  const char* begin = strstr(it->second.c_str(), section_name.c_str());
  if (begin == nullptr) {
    return false;
  }
  begin += section_name.size();
  const char* end = strchr(begin, '}');
  if (end == nullptr) {
    return false;
  }
  out = folly::StringPiece(begin, end);
  return true;
}

bool RocksDBTablePropertiesCollector::extractRetentionSizeMap(
    const std::map<std::string, std::string>& table_properties,
    RetentionSizeMap& inout_map) {
  folly::StringPiece section;
  if (!findSection(table_properties, "bytes_by_retention", section)) {
    return false;
  }
  std::vector<std::string> tokens;
  folly::split(',', section, tokens);
  for (const std::string& tok : tokens) {
    std::string key;
    size_t value;
    try {
      if (!folly::split(':', tok, key, value)) {
        return false;
      }
    } catch (std::range_error&) {
      return false;
    }
    std::chrono::seconds retention;
    if (parse_chrono_string(key, &retention) != 0) {
      return false;
    }
    inout_map[retention] += value;
  }

  return true;
}

bool RocksDBTablePropertiesCollector::extractLogSizeHistogram(
    const std::map<std::string, std::string>& table_properties,
    CompactSizeHistogram& out_histogram) {
  folly::StringPiece histogram_section;
  folly::StringPiece list_section;
  bool have_histogram =
      findSection(table_properties, "logs_by_size_log2", histogram_section);
  bool have_list = findSection(table_properties, "bytes_by_log", list_section);
  if (have_histogram == have_list) {
    return false;
  }

  if (have_histogram) {
    return out_histogram.fromShortString(histogram_section);
  }

  out_histogram.clear();
  std::vector<std::string> tokens;
  folly::split(',', list_section, tokens);
  for (const std::string& tok : tokens) {
    logid_t::raw_type log;
    size_t value;
    try {
      if (!folly::split(':', tok, log, value)) {
        return false;
      }
    } catch (std::range_error&) {
      return false;
    }
    out_histogram.add(static_cast<int64_t>(value));
  }

  return true;
}

const char* RocksDBTablePropertiesCollector::Name() const {
  return "facebook::logdevice::TablePropertiesCollector";
}

rocksdb::TablePropertiesCollector*
RocksDBTablePropertiesCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /*context*/) {
  return new RocksDBTablePropertiesCollector(
      updateable_config_ ? updateable_config_->get() : nullptr, stats_);
}

const char* RocksDBTablePropertiesCollectorFactory::Name() const {
  return "facebook::logdevice::RocksDBTablePropertiesCollectorFactory";
}

}} // namespace facebook::logdevice
