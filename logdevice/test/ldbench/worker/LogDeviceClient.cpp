/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogDeviceClient.h"

#include <functional>
#include <thread>

#include <boost/program_options.hpp>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"
#include "logdevice/test/ldbench/worker/LogDeviceReader.h"

namespace facebook { namespace logdevice { namespace ldbench {
LogDeviceClient::LogDeviceClient(const Options& options) {
  options_ = options;
  // Create client object.
  client_ = logdevice::ClientFactory()
                .setCredentials(options.credentials)
                .setTimeout(options.client_timeout)
                .setClusterName(options.cluster_name)
                .setClientSettings(std::move(client_settings))
                .create(options_.config_path);
  if (client_ == nullptr) {
    throw ConstructorFailed();
  }
}

LogDeviceClient::~LogDeviceClient() {}

std::unique_ptr<LogStoreReader> LogDeviceClient::createReader() {
  return std::make_unique<LogDeviceReader>(this);
}

void LogDeviceClient::createBufferedWriter() {
  auto buffered_writer_options = options_.buffered_writer_options;
  // default time trigger is 1 seconds, unless overridden by a commandline
  // specification. If time_trigger is -1, then batch is built until max payload
  // size is reached.
  writer_callback_ = std::make_unique<BufferedWriterCallback>(this);
  buffered_writer_ = BufferedWriter::create(
      client_, writer_callback_.get(), buffered_writer_options);
}

bool LogDeviceClient::append(LogIDType id,
                             std::string payload,
                             Context context) {
  logid_t logid(id);
  uint64_t res = 0;
  if (options_.use_buffered_writer) {
    folly::call_once(
        create_writer_flag_, &LogDeviceClient::createBufferedWriter, this);
    // 0 means no errors
    res = buffered_writer_->append(logid, std::move(payload), context);
  } else {
    auto cb = [this, context, payload](Status status, const DataRecord& rec) {
      if (writer_stats_update_cb_) {
        ContextSet contexts;
        contexts.emplace_back(context, payload);
        writer_stats_update_cb_(status == E::OK,
                                rec.logid.val(),
                                rec.attrs.lsn,
                                std::move(contexts));
      }
    };
    res = client_->append(logid, std::move(payload), std::move(cb));
  }
  return res == 0;
}

bool LogDeviceClient::getLogs(std::vector<LogIDType>* all_logs_res) {
  std::map<std::string, logid_range_t> ranges;
  if (!options_.log_range_names.empty()) {
    for (auto name : options_.log_range_names) {
      auto range = client_->getLogRangeByName(name);
      if (range.first == LOGID_INVALID) {
        ld_error("Failed to get log range \"%s\": %s",
                 name.c_str(),
                 error_name(err));
        return false;
      }
      ranges.emplace(name, range);
    }
  } else {
    ranges = client_->getLogRangesByNamespace("");
    if (ranges.empty() && err != E::OK) {
      ld_error("Failed to get all log ranges: %s", error_name(err));
      return false;
    }
  }

  for (const auto& kv : ranges) {
    auto& r = kv.second;
    for (uint64_t id = r.first.val_; id <= r.second.val_; ++id) {
      all_logs_res->emplace_back(id);
    }
  }
  return true;
}

std::shared_ptr<void> LogDeviceClient::getRawClient() {
  return std::static_pointer_cast<void>(client_);
}

void BufferedWriterCallback::onSuccess(logid_t log_id,
                                       ContextSet contexts,
                                       const DataRecordAttributes& attr) {
  if (owner_ && owner_->writer_stats_update_cb_) {
    owner_->writer_stats_update_cb_(
        true, log_id.val(), attr.lsn, std::move(contexts));
  }
}

void BufferedWriterCallback::onFailure(logid_t log_id,
                                       ContextSet contexts,
                                       Status /*status*/) {
  if (owner_ && owner_->writer_stats_update_cb_) {
    owner_->writer_stats_update_cb_(
        false, log_id.val(), 0, std::move(contexts));
  }
}

}}} // namespace facebook::logdevice::ldbench
