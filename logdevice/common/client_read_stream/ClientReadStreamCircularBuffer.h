/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/CircularBuffer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBuffer.h"

namespace facebook { namespace logdevice {

/**
 * @file ClientReadStreamCircularBuffer is an implementatin of
 *       ClientReadStreamBuffer using a random access circular buffer. Each
 *       RecordState descriptor for LSNs in the buffer is preallocated as
 *       placeholders. Advancing the buffer head is implemented by simply
 *       rotating the circular buffer.
 */

class ClientReadStreamCircularBuffer : public ClientReadStreamBuffer {
 public:
  ClientReadStreamCircularBuffer(size_t capacity, lsn_t buffer_head)
      : buffer_(capacity), buffer_head_(buffer_head) {}

  // see ClientReadStreamBuffer::createOrGet()
  // complexity O(1)
  ClientReadStreamRecordState* createOrGet(lsn_t lsn) override;

  // see ClientReadStreamBuffer::find()
  // complexity O(1)
  ClientReadStreamRecordState* find(lsn_t lsn) override;

  // see ClientReadStreamBuffer::findFirstMarker()
  // complexity O(n) in which n is the number of descriptor slots in the buffer
  std::pair<ClientReadStreamRecordState*, lsn_t> findFirstMarker() override;

  // see ClientReadStreamBuffer::front()
  // complexity O(1)
  ClientReadStreamRecordState* front() override;

  // see ClientReadStreamBuffer::popFront()
  // complexity O(1)
  void popFront() override;

  // see ClientReadStreamBuffer::advanceBufferHead()
  // complexity O(1)
  void advanceBufferHead(size_t offset = 1) override;

  // see ClientReadStreamBuffer::capacity()
  size_t capacity() const override {
    return buffer_.size();
  }

  // see ClientReadStreamBuffer::clear()
  // complexity O(n)
  void clear() override;

  // see ClientReadStreamBuffer::forEachUpto()
  // complexity O(min(n, to - buffer_head_))
  void forEachUpto(
      lsn_t to,
      std::function<void(lsn_t, ClientReadStreamRecordState& record)> cb)
      override;

  // see ClientReadStreamBuffer::forEach()
  // complexity O(min(n, abs(to - from)))
  void forEach(lsn_t from,
               lsn_t to,
               std::function<bool(lsn_t, ClientReadStreamRecordState& record)>
                   cb) override;

  // see ClientReadStreamBuffer::getBufferHead()
  lsn_t getBufferHead() const override {
    return buffer_head_;
  }

 private:
  // get the index for the given lsn. lsn must fit in the current buffer
  size_t getIndex(lsn_t lsn) const {
    ld_assert(LSNInBuffer(lsn));
    return lsn - buffer_head_;
  }

  // get the LSN for the given index. index must be valid
  lsn_t getLSN(size_t index) const {
    ld_check(index < capacity());
    ld_check(buffer_head_ <= LSN_MAX);
    ld_check(index <= LSN_MAX - buffer_head_);
    return buffer_head_ + index;
  }

  // circular buffer that holds all descriptors
  CircularBuffer<ClientReadStreamRecordState> buffer_;
  // tracks the buffer head
  lsn_t buffer_head_;
};

}} // namespace facebook::logdevice
