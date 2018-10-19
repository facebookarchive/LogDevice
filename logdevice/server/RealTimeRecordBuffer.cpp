/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RealTimeRecordBuffer.h"

#include <folly/Format.h>

#include "logdevice/common/ZeroCopiedRecord.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

ReleasedRecords::~ReleasedRecords() {
  if (buffer_) {
    buffer_->deletedReleasedRecords(this);
  }
}

size_t ReleasedRecords::computeBytesEstimate(const ZeroCopiedRecord* entries) {
  // Assuming the control block consists of a reference count and pointer to
  // deleter.
  size_t size = sizeof(ReleasedRecords) + sizeof(size_t) + sizeof(void*);
  for (; entries != nullptr; entries = entries->next_.get()) {
    size += entries->getBytesEstimate();
  }

  return size;
}

std::string ReleasedRecords::toString() const {
  return folly::sformat("{} [{}-{}], bytes {}",
                        facebook::logdevice::toString(logid_),
                        lsn_to_string(begin_lsn_),
                        lsn_to_string(end_lsn_),
                        bytes_estimate_);
}

bool RealTimeRecordBuffer::appendReleasedRecords(
    std::unique_ptr<ReleasedRecords> records) {
  // NOTE: called on the thread that's doing the release, NOT on the Worker
  // where this object "lives".
  ld_check(records);
  ld_assert(!shutdown_.load());

  // We need to INSERT before we ADD TO released_records_bytes_.  That's
  // because, when we check for eviction, we first look at r_r_bytes_, and if
  // that's too big, we need to be able to find the records to evict.

  // If this would put us over the max, don't add.
  const auto size_of_these_records = records->getBytesEstimate();
  if (released_records_bytes_.load() + size_of_these_records > max_bytes_) {
    // We destroy the ReleasedRecords here.
    return true;
  }

  released_records_.insertHead(records.release());

  auto new_size = released_records_bytes_.fetch_add(size_of_these_records);

  return new_size > eviction_threshold_bytes_;
}

size_t RealTimeRecordBuffer::shutdown() {
  shutdown_.store(true);
  size_t count = 0;
  sweep([&count](std::unique_ptr<ReleasedRecords> /* recs */) {
    ++count;
    // The ReleasedRecords will be deleted when recs goes out of scope.
  });
  return count;
}

RealTimeRecordBuffer::~RealTimeRecordBuffer() {
  ld_check(shutdown_.load());
  ld_check(logids_.empty());
}

}} // namespace facebook::logdevice
