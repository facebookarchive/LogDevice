/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/settings/Settings.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"

namespace facebook { namespace logdevice { namespace test {

// The purpose of this class is to provide an intuitive format for a call to
// LocalLogStoreReader::read with some nice defaults.
class LocalLogStoreTestReader {
 public:
  LocalLogStoreTestReader()
      : filter_(std::make_shared<LocalLogStoreReadFilter>()),
        settings_(create_default_settings<Settings>()) {}
  Status process(const TemporaryRocksDBStore* store,
                 std::vector<RawRecord>& records,
                 LocalLogStoreReader::ReadPointer* read_ptr = nullptr);

  LocalLogStoreTestReader& logID(logid_t log_id) {
    log_id_ = log_id;
    return *this;
  }
  LocalLogStoreTestReader& from(LocalLogStoreReader::ReadPointer v) {
    read_ptr_ = v;
    return *this;
  }
  LocalLogStoreTestReader& until_lsn(lsn_t until) {
    until_lsn_ = until;
    return *this;
  }
  LocalLogStoreTestReader& window_high(lsn_t high) {
    window_high_ = high;
    return *this;
  }
  LocalLogStoreTestReader& last_released(lsn_t r) {
    last_released_ = r;
    return *this;
  }
  LocalLogStoreTestReader& first_record_any_size(bool v) {
    first_record_any_size_ = v;
    return *this;
  }
  LocalLogStoreTestReader& max_bytes_all_records(size_t v) {
    max_bytes_all_records_ = v;
    return *this;
  }
  LocalLogStoreTestReader& max_bytes_to_read(size_t v) {
    settings_.max_record_bytes_read_at_once = v;
    return *this;
  }
  LocalLogStoreTestReader& max_execution_time(std::chrono::milliseconds v) {
    settings_.max_record_read_execution_time = v;
    return *this;
  }
  LocalLogStoreTestReader& filter(std::shared_ptr<LocalLogStoreReadFilter> f) {
    filter_ = std::move(f);
    return *this;
  }
  LocalLogStoreTestReader& use_csi(bool v) {
    use_csi_ = v;
    return *this;
  }
  LocalLogStoreTestReader& csi_only() {
    csi_only_ = true;
    return *this;
  }
  LocalLogStoreTestReader& fail_after(int v) {
    fail_after_ = v;
    return *this;
  }

 private:
  logid_t log_id_{1};
  lsn_t until_lsn_{LSN_MAX};
  lsn_t window_high_{LSN_MAX};
  lsn_t last_released_{LSN_MAX};
  bool first_record_any_size_{false};
  size_t max_bytes_all_records_{1000000};
  std::shared_ptr<LocalLogStoreReadFilter> filter_;
  LocalLogStoreReader::ReadPointer read_ptr_{lsn_t{1}};
  int fail_after_ = -1;
  bool use_csi_ = false;
  bool csi_only_ = false;
  Settings settings_;
};

}}} // namespace facebook::logdevice::test
