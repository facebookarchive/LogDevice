/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/metadata_log/TrimDataLogRequest.h"

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

namespace {

// Structure to represent a fake sequence of records/gaps
struct RecordOrGap {
  // GapType::MAX denotes a record instead of a gap
  GapType gap_type;
  lsn_t lsn;
  lsn_t gap_hi{LSN_INVALID};
};

class MockTrimDataLogRequest : public TrimDataLogRequest {
 public:
  explicit MockTrimDataLogRequest(bool do_trim)
      : TrimDataLogRequest(logid_t(1),
                           worker_id_t(0),
                           std::chrono::seconds(1),
                           std::chrono::seconds(1),
                           do_trim,
                           [](Status, lsn_t) {
                             // shouldn't actually run because there's an
                             // override of finalize()
                             ld_check(false);
                           }) {}

  // These variables should be set to set up the test before running execute()
  Status ssr_status_;
  lsn_t ssr_next_lsn_;
  std::vector<RecordOrGap> records_and_gaps_;
  folly::Optional<Status> trim_status_;

  Status getReadStatus() {
    return read_status_;
  }

  folly::Optional<lsn_t> getIssuedTrim() {
    return issued_trim_for_;
  }

  bool callbackCalled() {
    return callback_called_;
  }

  Status callbackStatus() {
    return callback_status_;
  }

  lsn_t callbackTrimPoint() {
    return callback_trim_point_;
  }

 protected: // mock stuff that communicates externally
  void getTailLSN(SyncSequencerRequest::Callback cb) override {
    cb(ssr_status_,
       NodeID(0, 1),
       ssr_next_lsn_,
       nullptr,
       nullptr,
       nullptr,
       folly::none);

    if (ssr_status_ != E::OK) {
      ld_check(reading_finalized_);
      // emulating firing of the timer
      onDestroyReadStreamTimer(read_status_);
    }
  }

  void startReader(lsn_t from_lsn, lsn_t until_lsn) override {
    lsn_t max_lsn_seen = LSN_INVALID;
    for (auto& rg : records_and_gaps_) {
      ld_check(rg.lsn >= from_lsn);
      if (rg.gap_type == GapType::MAX) {
        // record
        auto rec = std::make_unique<DataRecord>();
        rec->logid = logid_t(1);
        rec->attrs.lsn = rg.lsn;
        ld_check(rg.lsn = max_lsn_seen + 1);
        onDataRecord(rec);
        max_lsn_seen = rg.lsn;
      } else {
        // gap
        ld_check(rg.lsn = max_lsn_seen + 1);
        ld_check(rg.gap_hi >= rg.lsn);
        GapRecord gap(logid_t(1), rg.gap_type, rg.lsn, rg.gap_hi);
        onGapRecord(gap);
        max_lsn_seen = rg.gap_hi;
      }
    }
    if (max_lsn_seen >= until_lsn) {
      onDoneReading(logid_t(1));
    }
    ld_check(reading_finalized_);

    // emulating firing of the timer
    onDestroyReadStreamTimer(read_status_);
  }

  void finalizeReading(Status st) override {
    read_status_ = st;
    reading_finalized_ = true;
  }

  void stopReading() override {
    // no-op
  }

  void runTrim() override {
    issued_trim_for_ = new_trim_point_;
    ld_check(trim_status_.hasValue());
    onTrimComplete(trim_status_.value());
  }

  void finalize(Status st, lsn_t trim_point) override {
    callback_called_ = true;
    callback_status_ = st;
    callback_trim_point_ = trim_point;
  }

  void verifyWorker() override {
    // no-op
  }

  // set in finalizeReading()
  Status read_status_;

  // set in runTrim()
  folly::Optional<lsn_t> issued_trim_for_;

  bool callback_called_ = false;
  Status callback_status_;
  lsn_t callback_trim_point_;
};

constexpr lsn_t make_lsn(uint32_t e, uint32_t o) {
  return compose_lsn(epoch_t(e), esn_t(o));
}

TEST(TrimDataLogRequestTest, NoTrim) {
  MockTrimDataLogRequest req(/*do_trim=*/false);
  req.ssr_status_ = E::OK;
  req.ssr_next_lsn_ = make_lsn(1, 3);
  req.records_and_gaps_ = {{GapType::MAX /* record */, make_lsn(1, 1)},
                           {GapType::MAX /* record */, make_lsn(1, 2)}};
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::OK, req.callbackStatus());
  ASSERT_EQ(LSN_INVALID, req.callbackTrimPoint());
  ASSERT_EQ(folly::none, req.getIssuedTrim());
}

TEST(TrimDataLogRequestTest, NoTrimButDoTrim) {
  MockTrimDataLogRequest req(/*do_trim=*/true);
  req.ssr_status_ = E::OK;
  req.ssr_next_lsn_ = make_lsn(1, 3);
  req.trim_status_ = E::OK;
  req.records_and_gaps_ = {{GapType::MAX /* record */, make_lsn(1, 1)},
                           {GapType::MAX /* record */, make_lsn(1, 2)}};
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::OK, req.callbackStatus());
  ASSERT_EQ(make_lsn(1, 0), req.callbackTrimPoint());
  ASSERT_EQ(make_lsn(1, 0), req.getIssuedTrim());
}

TEST(TrimDataLogRequestTest, GetTrimPoint) {
  MockTrimDataLogRequest req(/*do_trim=*/false);
  req.ssr_status_ = E::OK;
  req.ssr_next_lsn_ = make_lsn(2, 3);
  req.records_and_gaps_ = {{GapType::TRIM, make_lsn(1, 1), make_lsn(1, 2)},
                           {GapType::BRIDGE, make_lsn(1, 3), make_lsn(2, 0)},
                           {GapType::MAX /* record */, make_lsn(2, 1)},
                           {GapType::MAX /* record */, make_lsn(2, 2)}};
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::OK, req.callbackStatus());
  ASSERT_EQ(make_lsn(1, 2), req.callbackTrimPoint());
  ASSERT_EQ(folly::none, req.getIssuedTrim());
}

TEST(TrimDataLogRequestTest, MoveTrimPointToFirstRecord) {
  MockTrimDataLogRequest req(/*do_trim=*/true);
  req.ssr_status_ = E::OK;
  req.ssr_next_lsn_ = make_lsn(2, 3);
  req.trim_status_ = E::OK;
  req.records_and_gaps_ = {{GapType::TRIM, make_lsn(1, 1), make_lsn(1, 2)},
                           {GapType::BRIDGE, make_lsn(1, 3), make_lsn(2, 0)},
                           {GapType::MAX /* record */, make_lsn(2, 1)},
                           {GapType::MAX /* record */, make_lsn(2, 2)}};
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::OK, req.callbackStatus());
  ASSERT_EQ(make_lsn(2, 0), req.callbackTrimPoint());
  ASSERT_EQ(make_lsn(2, 0), req.getIssuedTrim());
}

TEST(TrimDataLogRequestTest, MoveTrimPointToTail) {
  MockTrimDataLogRequest req(/*do_trim=*/true);
  req.ssr_status_ = E::OK;
  req.ssr_next_lsn_ = make_lsn(2, 3);
  req.trim_status_ = E::OK;
  req.records_and_gaps_ = {
      {GapType::TRIM, make_lsn(1, 1), make_lsn(1, 2)},
      {GapType::BRIDGE, make_lsn(1, 3), make_lsn(2, 0)},
      {GapType::BRIDGE, make_lsn(2, 0), make_lsn(2, 2)},
  };
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::OK, req.callbackStatus());
  ASSERT_EQ(make_lsn(2, 2), req.callbackTrimPoint());
  ASSERT_EQ(make_lsn(2, 2), req.getIssuedTrim());
}

TEST(TrimDataLogRequestTest, TailLSNFailure) {
  MockTrimDataLogRequest req(/*do_trim=*/true);
  req.ssr_status_ = E::FAILED;
  req.ssr_next_lsn_ = LSN_INVALID;
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::FAILED, req.callbackStatus());
}

TEST(TrimDataLogRequestTest, TrimFailure) {
  MockTrimDataLogRequest req(/*do_trim=*/true);
  req.ssr_status_ = E::OK;
  req.ssr_next_lsn_ = make_lsn(2, 3);
  req.trim_status_ = E::FAILED;
  req.records_and_gaps_ = {{GapType::TRIM, make_lsn(1, 1), make_lsn(1, 2)},
                           {GapType::BRIDGE, make_lsn(1, 3), make_lsn(2, 0)},
                           {GapType::MAX /* record */, make_lsn(2, 1)},
                           {GapType::MAX /* record */, make_lsn(2, 2)}};
  ASSERT_EQ(Request::Execution::CONTINUE, req.execute());
  ASSERT_TRUE(req.callbackCalled());
  ASSERT_EQ(E::FAILED, req.callbackStatus());
  ASSERT_EQ(make_lsn(2, 0), req.getIssuedTrim());
}

} // namespace
