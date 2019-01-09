/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/test/LocalLogStoreTestReader.h"

namespace facebook { namespace logdevice { namespace test {

namespace {
class TestCallback : public LocalLogStoreReader::Callback {
 public:
  /**
   * @param fail_after processRecord() will return -1 when called after
   *                   `fail_after` times.
   */
  explicit TestCallback(int fail_after = -1) : fail_after_(fail_after) {}
  ~TestCallback() override {}
  int processRecord(const RawRecord& record) override {
    if (fail_after_ == 0) {
      return -1;
    }
    --fail_after_;

    void* blob_copy = malloc(record.blob.size);
    if (blob_copy == nullptr) {
      throw std::bad_alloc();
    }
    memcpy(blob_copy, record.blob.data, record.blob.size);
    records_.emplace_back(record.lsn, Slice(blob_copy, record.blob.size), true);

    return 0;
  }
  std::vector<RawRecord>& getRecords() {
    return records_;
  };

 private:
  int fail_after_;
  std::vector<RawRecord> records_;
};
} // namespace

Status
LocalLogStoreTestReader::process(const TemporaryRocksDBStore* store,
                                 std::vector<RawRecord>& records,
                                 LocalLogStoreReader::ReadPointer* read_ptr) {
  TestCallback cb(fail_after_);
  LocalLogStore::ReadOptions read_options("TestReader");
  read_options.allow_copyset_index = use_csi_;
  read_options.csi_data_only = csi_only_;
  std::unique_ptr<LocalLogStore::ReadIterator> it =
      store->read(log_id_, read_options);

  LocalLogStoreReader::ReadContext ctx(log_id_,
                                       read_ptr_,
                                       until_lsn_,
                                       window_high_,
                                       std::chrono::milliseconds::max(),
                                       last_released_,
                                       max_bytes_all_records_,
                                       first_record_any_size_,
                                       false, // is_rebuilding
                                       filter_,
                                       CatchupEventTrigger::OTHER);

  Status st = LocalLogStoreReader::read(*it, cb, &ctx, nullptr, settings_);
  records = std::move(cb.getRecords());
  read_ptr_ = ctx.read_ptr_;

  if (read_ptr != nullptr) {
    *read_ptr = read_ptr_;
  }

  return st;
}

}}} // namespace facebook::logdevice::test
