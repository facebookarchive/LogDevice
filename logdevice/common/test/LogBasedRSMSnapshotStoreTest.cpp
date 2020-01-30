/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/replicated_state_machine/LogBasedRSMSnapshotStore.h"

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockLogBasedStore : public LogBasedRSMSnapshotStore {
 public:
  explicit MockLogBasedStore(std::string key,
                             logid_t snapshot_log,
                             Processor* p,
                             bool allow_snapshotting /* writable */)
      : LogBasedRSMSnapshotStore(key, snapshot_log, p, allow_snapshotting) {}
  int extractVersion(const std::string& /* unused */,
                     RSMSnapshotHeader& header_out) const override {
    header_out.base_version = fake_hdr_.base_version;
    return 0;
  }
  int getLastReleasedLsn(
      std::function<void(Status st, lsn_t last_release_lsn)> cb) override {
    cb(fake_get_last_released_status_, fake_get_last_released_lsn_);
    return 0;
  }

  read_stream_id_t createBasicReadStream(
      lsn_t /* unused */,
      lsn_t /* unused */,
      ClientReadStreamDependencies::record_cb_t on_record,
      ClientReadStreamDependencies::gap_cb_t on_gap,
      ClientReadStreamDependencies::health_cb_t /* unused */) override {
    if (is_record_) {
      on_record(fake_record_);
    } else {
      on_gap(fake_gap_);
    }
    return (read_stream_id_t)0;
  }

  void setFakeRecord(std::unique_ptr<DataRecord> record) {
    is_record_ = true;
    fake_record_ = std::move(record);
  }
  void setFakeGap(GapRecord gap) {
    is_record_ = false;
    fake_gap_ = gap;
  }
  void setFakeSnapshotHeader(RSMSnapshotHeader hdr) {
    fake_hdr_ = hdr;
  }
  void updateCache(lsn_t last_released_real_lsn,
                   std::string snapshot_blob,
                   SnapshotAttributes snapshot_attrs) {
    setCachedItems(last_released_real_lsn, snapshot_blob, snapshot_attrs);
  }

  uint32_t getNumCacheAccesses() {
    return used_cache_;
  }

  void setFakeGetLastReleasedLsnParams(Status st, lsn_t lsn) {
    fake_get_last_released_status_ = st;
    fake_get_last_released_lsn_ = lsn;
  }

 private:
  Status fake_get_last_released_status_{E::FAILED};
  lsn_t fake_get_last_released_lsn_{LSN_INVALID};
  std::unique_ptr<DataRecord> fake_record_;
  GapRecord fake_gap_;
  RSMSnapshotHeader fake_hdr_;
  bool is_record_{
      true}; // by default return records, if set to false, return GAP
};

static std::unique_ptr<DataRecordOwnsPayload> make_record(logid_t log_id,
                                                          lsn_t lsn,
                                                          std::string payload) {
  static std::atomic<int64_t> timestamp{1};
  return std::make_unique<DataRecordOwnsPayload>(
      log_id,
      PayloadHolder::copyString(payload),
      lsn,
      std::chrono::milliseconds(timestamp++),
      (RECORD_flags_t)0);
}

class LogBasedRsmTest : public ::testing::Test {
 public:
  void init(logid_t snapshot_log, logid_t delta_log) {
    Settings settings = create_default_settings<Settings>();
    test_processor_ = make_test_processor(settings, nullptr);
    snapshot_store_ = std::make_unique<MockLogBasedStore>(
        folly::to<std::string>(delta_log.val_),
        snapshot_log,
        test_processor_.get(),
        true /* writable */);
  }

  bool callback_called_{false};
  std::unique_ptr<MockLogBasedStore> snapshot_store_{nullptr};
  std::shared_ptr<Processor> test_processor_;
};

TEST_F(LogBasedRsmTest, GetLastReleasedLsnAtOrBehindCachedVersion) {
  init(logid_t(1), logid_t(2));
  lsn_t impl_ver = compose_lsn(epoch_t(2), esn_t(2));
  lsn_t base_ver = compose_lsn(epoch_t(10), esn_t(5));
  Semaphore sem;
  LogBasedRSMSnapshotStore::SnapshotAttributes fake_attrs(
      base_ver, std::chrono::milliseconds(1));
  snapshot_store_->updateCache(
      impl_ver /* last released */, "abc" /* snapshot blob */, fake_attrs);
  // setting last released lsn return params behind cached items
  // Verify that the store returns cached items
  uint32_t cached_lookups = 0;
  for (lsn_t cached_ver = impl_ver - 1; cached_ver <= impl_ver; cached_ver++) {
    snapshot_store_->setFakeGetLastReleasedLsnParams(E::OK, cached_ver);
    auto snapshot_cb = [&](Status st,
                           std::string snapshot_blob_out,
                           RSMSnapshotStore::SnapshotAttributes attrs) {
      ASSERT_EQ(st, E::OK);
      ASSERT_EQ(snapshot_blob_out, "abc");
      ASSERT_EQ(attrs.base_version, base_ver);
      ASSERT_EQ(attrs.timestamp, std::chrono::milliseconds(1));
      sem.post();
    };
    snapshot_store_->getSnapshot(LSN_OLDEST, std::move(snapshot_cb));
    sem.wait();
    ASSERT_EQ(snapshot_store_->getNumCacheAccesses(), ++cached_lookups);
  }
}

TEST_F(LogBasedRsmTest, GetLastReleasedLsnAheadOfCachedVersion) {
  init(logid_t(1), logid_t(2));
  lsn_t impl_ver = compose_lsn(epoch_t(2), esn_t(2));
  lsn_t base_ver = compose_lsn(epoch_t(10), esn_t(5));
  Semaphore sem;
  LogBasedRSMSnapshotStore::SnapshotAttributes fake_attrs(
      base_ver, std::chrono::milliseconds(1));
  snapshot_store_->updateCache(
      impl_ver - 1 /* last released */, "abc" /* snapshot blob */, fake_attrs);
  snapshot_store_->setFakeGetLastReleasedLsnParams(E::OK, impl_ver);
  RSMSnapshotHeader fake_hdr;
  auto fake_rec = make_record(logid_t(1), impl_ver, "def");
  fake_hdr.base_version = base_ver;
  snapshot_store_->setFakeRecord(std::move(fake_rec));
  snapshot_store_->setFakeSnapshotHeader(std::move(fake_hdr));

  auto snapshot_cb = [&](Status st,
                         std::string snapshot_blob_out,
                         RSMSnapshotStore::SnapshotAttributes attrs) {
    ASSERT_EQ(st, E::OK);
    ASSERT_EQ(snapshot_blob_out, "def");
    ASSERT_EQ(attrs.base_version, base_ver);
    ASSERT_EQ(attrs.timestamp, std::chrono::milliseconds(1));
    sem.post();
  };
  snapshot_store_->getSnapshot(LSN_OLDEST, snapshot_cb);
  sem.wait();
  ASSERT_EQ(snapshot_store_->getNumCacheAccesses(), 0);

  // verify that cache is updated
  snapshot_store_->getSnapshot(LSN_OLDEST, snapshot_cb);
  sem.wait();
  ASSERT_EQ(snapshot_store_->getNumCacheAccesses(), 1);
}

TEST_F(LogBasedRsmTest, GetVersion) {
  init(logid_t(1), logid_t(2));
  lsn_t impl_ver = compose_lsn(epoch_t(2), esn_t(2));
  lsn_t base_ver = compose_lsn(epoch_t(10), esn_t(5));
  Semaphore sem;
  RSMSnapshotHeader fake_hdr;
  auto fake_rec = make_record(logid_t(1), impl_ver, "abc");
  fake_hdr.base_version = base_ver;
  snapshot_store_->setFakeRecord(std::move(fake_rec));
  snapshot_store_->setFakeSnapshotHeader(std::move(fake_hdr));
  snapshot_store_->setFakeGetLastReleasedLsnParams(E::OK, impl_ver);

  auto ver_cb = [&](Status st, lsn_t snapshot_ver_out) {
    ASSERT_EQ(snapshot_ver_out, base_ver);
    ASSERT_EQ(st, E::OK);
    sem.post();
  };
  snapshot_store_->getVersion(ver_cb);
  sem.wait();
  ASSERT_EQ(snapshot_store_->getNumCacheAccesses(), 0);

  // getDurableVersion should be exactly same
  snapshot_store_->getDurableVersion(ver_cb);
  sem.wait();
  ASSERT_EQ(snapshot_store_->getNumCacheAccesses(), 1);
}

TEST_F(LogBasedRsmTest, OnGap) {
  init(logid_t(1), logid_t(2));
  lsn_t impl_ver = compose_lsn(epoch_t(2), esn_t(2));
  Semaphore sem;
  GapRecord fake_gap(logid_t(1), GapType::DATALOSS, impl_ver, impl_ver);
  snapshot_store_->setFakeGap(std::move(fake_gap));
  snapshot_store_->setFakeGetLastReleasedLsnParams(E::OK, impl_ver);

  auto snapshot_cb = [&](Status st,
                         std::string /* unused */,
                         RSMSnapshotStore::SnapshotAttributes attrs) {
    ASSERT_EQ(st, E::NOTFOUND);
    ASSERT_EQ(attrs.base_version, LSN_INVALID);
    sem.post();
  };
  snapshot_store_->getSnapshot(LSN_OLDEST, snapshot_cb);
  sem.wait();
}

TEST_F(LogBasedRsmTest, StaleReply) {
  init(logid_t(1), logid_t(2));
  lsn_t impl_ver = compose_lsn(epoch_t(2), esn_t(2));
  lsn_t base_ver = compose_lsn(epoch_t(10), esn_t(5));
  Semaphore sem;
  RSMSnapshotHeader fake_hdr;
  auto fake_rec = make_record(logid_t(1), impl_ver, "abc");
  fake_hdr.base_version = base_ver;
  snapshot_store_->setFakeRecord(std::move(fake_rec));
  snapshot_store_->setFakeSnapshotHeader(std::move(fake_hdr));
  snapshot_store_->setFakeGetLastReleasedLsnParams(E::OK, impl_ver);
  auto snapshot_cb = [&](Status st,
                         std::string snapshot_blob_out,
                         RSMSnapshotStore::SnapshotAttributes attrs) {
    ASSERT_EQ(st, E::OK);
    ASSERT_EQ(attrs.base_version, base_ver);
    ASSERT_EQ(snapshot_blob_out, "abc");
    sem.post();
  };
  snapshot_store_->getSnapshot(base_ver, std::move(snapshot_cb));
  sem.wait();

  auto snapshot_cb2 = [&](Status st,
                          std::string /* unused */,
                          RSMSnapshotStore::SnapshotAttributes attrs) {
    ASSERT_EQ(st, E::STALE);
    ASSERT_EQ(attrs.base_version, base_ver);
    sem.post();
  };
  snapshot_store_->getSnapshot(base_ver + 1, std::move(snapshot_cb2));
  sem.wait();
}

}} // namespace facebook::logdevice
