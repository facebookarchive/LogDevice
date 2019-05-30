/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/ClientReadStreamCircularBuffer.h"

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using RecordState = ClientReadStreamRecordState;

RecordState* ClientReadStreamCircularBuffer::createOrGet(lsn_t lsn) {
  // lsn must be with in the range of
  // [buffer_head, buffer_head + capacity() - 1]
  if (!LSNInBuffer(lsn)) {
    return nullptr;
  }
  return &buffer_[getIndex(lsn)];
}

RecordState* ClientReadStreamCircularBuffer::find(lsn_t lsn) {
  if (!LSNInBuffer(lsn)) {
    return nullptr;
  }

  RecordState& state = buffer_[getIndex(lsn)];
  if (!state.record && !state.gap && !state.filtered_out) {
    // this is an empty placeholder RecordState, treat it as not
    // exist
    ld_check(state.list.empty());
    return nullptr;
  }

  return &state;
}

std::pair<ClientReadStreamRecordState*, lsn_t>
ClientReadStreamCircularBuffer::findFirstMarker() {
  // avoid searching beyond buffer capacity
  // avoid searching beyond LSN_MAX
  size_t limit =
      std::min(capacity(), LSN_MAX - std::max(buffer_head_, 1lu) + 1);

  for (size_t i = 0; i < limit; ++i) {
    if (buffer_[i].gap || buffer_[i].record || buffer_[i].filtered_out) {
      return std::make_pair(&buffer_[i], getLSN(i));
    }
    // for slot that is not a record/gap marker, its list must be
    // empty
    ld_check(buffer_[i].list.empty());
  }

  // no gap/record marker in buffer
  return std::make_pair(nullptr, LSN_INVALID);
}

ClientReadStreamRecordState* ClientReadStreamCircularBuffer::front() {
  if (buffer_.front().record || buffer_.front().gap ||
      buffer_.front().filtered_out) {
    return &buffer_.front();
  }

  // the descriptor is a placeholder, return nullptr
  ld_check(buffer_.front().list.empty());
  return nullptr;
}

void ClientReadStreamCircularBuffer::popFront() {
  // record and list, if exist, must be already consumed
  ld_check(!buffer_.front().record && !buffer_.front().filtered_out);
  ld_check(buffer_.front().list.empty());
  buffer_.front().reset();
}

void ClientReadStreamCircularBuffer::advanceBufferHead(size_t offset) {
  // Important: caller needs to ensure that there must not be any marker
  // in the buffer slots that get advanced. Assert in the following statements.

  if (folly::kIsDebug) {
    size_t limit =
        std::min(std::min(offset, capacity()), LSN_MAX - buffer_head_ + 1);
    for (size_t i = 0; i < limit; ++i) {
      ld_check(!buffer_[i].gap && !buffer_[i].record &&
               !buffer_[i].filtered_out);
      ld_check(buffer_[i].list.empty());
    }
  }

  buffer_.rotate(offset);
  buffer_head_ += offset;
}

void ClientReadStreamCircularBuffer::clear() {
  for (size_t i = 0; i < buffer_.size(); ++i) {
    buffer_[i].reset();
  }
}

void ClientReadStreamCircularBuffer::forEachUpto(
    lsn_t to,
    std::function<void(lsn_t, RecordState& record)> callback) {
  ld_check(to >= buffer_head_);
  forEach(buffer_head_,
          to,
          [cb = std::move(callback)](lsn_t lsn, RecordState& rstate) {
            cb(lsn, rstate);
            return true;
          });
}

void ClientReadStreamCircularBuffer::forEach(
    lsn_t from,
    lsn_t to,
    std::function<bool(lsn_t, ClientReadStreamRecordState& record)> cb) {
  const bool reverse = from > to;
  size_t count = (reverse) ? from - to + 1 : to - from + 1;
  size_t limit = std::min(count, capacity());
  for (size_t i = 0; i < limit; i++) {
    if (!cb(from, buffer_[getIndex(from)])) {
      break;
    }
    from += (reverse) ? -1 : 1;
  }
}

}} // namespace facebook::logdevice
