/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/IteratorTracker.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

void TrackableIterator::registerTracking(std::string column_family,
                                         logid_t log_id,
                                         bool tailing,
                                         bool blocking,
                                         IteratorType type,
                                         TrackingContext context) {
  imm_info_.column_family_name = column_family;
  imm_info_.log_id = log_id;
  imm_info_.high_level_id = context.high_level_id;
  imm_info_.type = type;
  imm_info_.tailing = tailing;
  imm_info_.blocking = blocking;
  imm_info_.created_by_rebuilding = context.created_by_rebuilding;

  mut_info_.last_seek_lsn = LSN_INVALID;
  mut_info_.last_seek_time = std::chrono::milliseconds(0);
  mut_info_.more_context = context.more_context;

  ld_check(!parent_);
  IteratorTracker::get()->registerIterator(this);
  ld_check(parent_);
}

void TrackableIterator::trackSeek(lsn_t lsn, uint64_t version) {
  std::lock_guard<std::mutex> lock(tracking_mutex_);
  mut_info_.last_seek_lsn = lsn;
  mut_info_.last_seek_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  mut_info_.last_seek_version = version;
}

void TrackableIterator::trackIteratorRelease() {
  trackSeek(LSN_INVALID, 0);
}

TrackableIterator::MutableTrackingInfo
TrackableIterator::getMutableTrackingInfo() const {
  std::lock_guard<std::mutex> lock_guard(tracking_mutex_);
  return mut_info_;
}

TrackableIterator::TrackingInfo TrackableIterator::getDebugInfo() const {
  TrackingInfo info;
  info.imm = imm_info_;
  {
    std::lock_guard<std::mutex> lock_guard(tracking_mutex_);
    info.mut = mut_info_;
  }
  return info;
}

TrackableIterator::~TrackableIterator() {
  if (parent_) {
    // auto-unlink is not good enough, since we need to lock the mutex
    parent_->unregisterIterator(this);
  }
  ld_check(!parent_);
}

void TrackableIterator::setContextString(const char* str) {
  std::lock_guard<std::mutex> lock(tracking_mutex_);
  mut_info_.more_context = str;
}

IteratorTracker* IteratorTracker::get() {
  // This singleton leaks, and it's okay
  static IteratorTracker* inst = new IteratorTracker();
  return inst;
}

void IteratorTracker::registerIterator(TrackableIterator* it) {
  ld_check(it);
  std::lock_guard<std::mutex> lock(mutex_);
  auto list_it = list_.insert(list_.end(), it);
  it->parent_ = this;
  it->list_iterator_ = std::move(list_it);
}

void IteratorTracker::unregisterIterator(TrackableIterator* it) {
  ld_check(it);
  ld_check(it->parent_ == this);
  std::lock_guard<std::mutex> lock(mutex_);
  list_.erase(it->list_iterator_);
  it->parent_ = nullptr;
}

std::vector<TrackableIterator::TrackingInfo> IteratorTracker::getDebugInfo() {
  std::vector<TrackableIterator::TrackingInfo> res;
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    res.push_back((*it)->getDebugInfo());
  }
  return res;
}

TrackableIterator::TrackingContext::TrackingContext(bool rebuilding,
                                                    const char* more_ctx)
    : created_by_rebuilding(rebuilding), more_context(std::move(more_ctx)) {
  static std::atomic<uint64_t> last_id(0);
  high_level_id = last_id.fetch_add(1);
}

}} // namespace facebook::logdevice
