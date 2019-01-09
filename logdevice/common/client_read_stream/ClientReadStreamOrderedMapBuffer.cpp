/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/ClientReadStreamOrderedMapBuffer.h"

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using RecordState = ClientReadStreamRecordState;

RecordState* ClientReadStreamOrderedMapBuffer::createOrGet(lsn_t lsn) {
  // lsn must be with in the range of
  // [buffer_head, buffer_head + capacity() - 1]
  if (!LSNInBuffer(lsn)) {
    return nullptr;
  }
  return &map_[lsn];
}

RecordState* ClientReadStreamOrderedMapBuffer::find(lsn_t lsn) {
  if (map_.empty()) {
    return nullptr;
  }

  if (lsn < map_.begin()->first) {
    return nullptr;
  }

  auto it = map_.find(lsn);
  if (it == map_.end()) {
    return nullptr;
  }

  ld_assert(LSNInBuffer(it->first));
  return &it->second;
}

std::pair<ClientReadStreamRecordState*, lsn_t>
ClientReadStreamOrderedMapBuffer::findFirstMarker() {
  if (map_.empty()) {
    return std::make_pair(nullptr, LSN_INVALID);
  }

  auto it = map_.begin();
  ld_assert(LSNInBuffer(it->first));
  return std::make_pair(&it->second, it->first);
}

ClientReadStreamRecordState* ClientReadStreamOrderedMapBuffer::front() {
  if (map_.empty()) {
    return nullptr;
  }

  auto it = map_.begin();
  if (it->first > buffer_head_) {
    return nullptr;
  }

  ld_check(it->first == buffer_head_);
  return &it->second;
}

void ClientReadStreamOrderedMapBuffer::popFront() {
  // record and list, if exist, must be already consumed
  if (map_.empty()) {
    return;
  }

  auto it = map_.begin();
  if (it->first == buffer_head_) {
    ld_check(!it->second.record && !it->second.filtered_out);
    ld_check(it->second.list.empty());
    map_.erase(it);
  }
}

void ClientReadStreamOrderedMapBuffer::advanceBufferHead(size_t offset) {
  // caller needs to ensure that there must not be any marker
  // in the buffer slots that get advanced. assert this below.
  ld_check(map_.empty() || map_.begin()->first >= buffer_head_ + offset);
  buffer_head_ += offset;
}

void ClientReadStreamOrderedMapBuffer::clear() {
  map_.clear();
}

void ClientReadStreamOrderedMapBuffer::forEach(
    lsn_t from,
    lsn_t to,
    std::function<bool(lsn_t, ClientReadStreamRecordState& record)> cb) {
  const bool reverse = from > to;

  auto it = map_.find(from);
  if (it == map_.end()) {
    it = map_.lower_bound(from);
    if (reverse) {
      if (it != map_.begin()) {
        --it;
      } else {
        it = map_.end();
      }
    }
  }

  bool done = false;
  while (!done && it != map_.end()) {
    if ((reverse && it->first < to) || (!reverse && it->first > to)) {
      break;
    }

    auto cur = it;
    if (reverse) {
      if (it == map_.begin()) {
        it = map_.end();
      } else {
        --it;
      }
    } else {
      ++it;
    }

    done = !cb(cur->first, cur->second);
    if (!cur->second.record && !cur->second.gap && !cur->second.filtered_out) {
      ld_check(cur->second.list.empty());
      map_.erase(cur);
    }
  }
}

void ClientReadStreamOrderedMapBuffer::forEachUpto(
    lsn_t to,
    std::function<void(lsn_t, RecordState& record)> callback) {
  if (map_.empty()) {
    return;
  }
  auto from = map_.begin()->first;
  if (from > to) {
    return;
  }
  forEach(from, to, [cb = std::move(callback)](lsn_t lsn, RecordState& rstate) {
    cb(lsn, rstate);
    return true;
  });
}

}} // namespace facebook::logdevice
