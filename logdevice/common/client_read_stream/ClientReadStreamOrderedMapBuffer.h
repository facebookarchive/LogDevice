/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include "logdevice/common/client_read_stream/ClientReadStreamBuffer.h"

namespace facebook { namespace logdevice {

/**
 * @file ClientReadStreamOrderedMapBuffer is an implementation of
 *       ClientReadStreamBuffer using a std::map. The map only stores
 *       <LSN, RecordState descriptor> pair for records and gap markers.
 *       No placeholder or preallocation is needed for LSNs that are not
 *       associated with a record or gap marker. This makes it desireable for
 *       reading logs that have sparse LSNs for their records (e.g., metadata
 *       logs).
 */

class ClientReadStreamOrderedMapBuffer : public ClientReadStreamBuffer {
 public:
  ClientReadStreamOrderedMapBuffer(size_t capacity, lsn_t buffer_head)
      : capacity_(capacity), buffer_head_(buffer_head) {}

  // see ClientReadStreamBuffer::createOrGet()
  // complexity O(logN)
  ClientReadStreamRecordState* createOrGet(lsn_t lsn) override;

  // see ClientReadStreamBuffer::find()
  // complexity O(logN)
  ClientReadStreamRecordState* find(lsn_t lsn) override;

  // see ClientReadStreamBuffer::findFirstMarker()
  // complexity O(1)
  std::pair<ClientReadStreamRecordState*, lsn_t> findFirstMarker() override;

  // see ClientReadStreamBuffer::front()
  // complexity O(1)
  ClientReadStreamRecordState* front() override;

  // see ClientReadStreamBuffer::popFront()
  // complexity O(logN)
  void popFront() override;

  // see ClientReadStreamBuffer::advanceBufferHead()
  // complexity O(1)
  void advanceBufferHead(size_t offset = 1) override;

  // see ClientReadStreamBuffer::capacity()
  size_t capacity() const override {
    return capacity_;
  }

  // see ClientReadStreamBuffer::clear()
  void clear() override;

  // see ClientReadStreamBuffer::forEachUpto()
  // complexity O(logN + (to - buffer_head_))
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
  // determine the maximum LSN to accept in the buffer
  // note: this is ususally much larger that the number of descriptors in
  //       the buffer if the LSNs are sparse
  size_t capacity_;
  // tracks the buffer head
  lsn_t buffer_head_;
  // map that holds all RecordState descriptors
  std::map<lsn_t, ClientReadStreamRecordState> map_;
};

}} // namespace facebook::logdevice
