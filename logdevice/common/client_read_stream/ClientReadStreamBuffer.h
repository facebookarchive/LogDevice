/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <utility>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file ClientReadStreamBuffer declares the interface for the buffer
 *       structure used in ClientReadStream. It is used to buffer record
 *       and gap markers received from storage nodes. Each record/gap marker
 *       is associated with a lsn, and the buffer can only hold record/gap
 *       markers within a certain range of LSNs depending on its current
 *       capacity. ClientReadStreamBuffer has a `buffer head' which is usually
 *       the next LSN to be delivered. Therefore, the lsn range can be
 *       buffered is: [buffer_head, buffer_head + capacity - 1].
 */

struct ClientReadStreamRecordState;

class ClientReadStreamBuffer {
 public:
  /**
   * Get the RecordState descriptor of a particular lsn, if the descriptor
   * cannot be found, create one for the given lsn and initialize it to the
   * default state.
   *
   * @return   the pointer of the RecordState descriptor in the buffer
   *           nullptr if the lsn cannot fit into the buffer
   */
  virtual ClientReadStreamRecordState* createOrGet(lsn_t lsn) = 0;

  /**
   * Search for the RecordState descriptor of a particular lsn.
   *
   *  @return  the pointer of the RecordState descriptor if found
   *           nullptr if the decriptor is not found, or the given
   *                   lsn cannot fit into the buffer
   */
  virtual ClientReadStreamRecordState* find(lsn_t lsn) = 0;

  /**
   * Search the buffer for the first valid RecordState descriptor with
   * a marker (i.e., record/gap marker).
   *
   * @return   a pair of <ClientReadStreamRecordState *, lsn_t> denoting
   *           the pointer and lsn associated to marker;
   *           if no marker is found within the buffer, returns a pair of
   *           <nullptr, LSN_INVALID>
   */
  virtual std::pair<ClientReadStreamRecordState*, lsn_t> findFirstMarker() = 0;

  /**
   * Access the front slot of the buffer, which always corresponds to lsn
   * getBufferHead().
   *
   * @return   if the slot contains a valid record/gap marker, return a pointer
   *           to RecordState descripor stored in the slot. Otherwise return
   *           nullptr.
   */
  virtual ClientReadStreamRecordState* front() = 0;

  /**
   * If exist, delete the RecordState descriptor associated with the first lsn
   * in the buffer. Otherwise, the function does nothing.
   *
   * Noted that the caller needs to make sure that the descriptor is properly
   * processed (e.g., record is delivered, list is consumed) before calling
   * this function.
   */
  virtual void popFront() = 0;

  /**
   * Return the current buffer head of the buffer.
   */
  virtual lsn_t getBufferHead() const = 0;

  /**
   * Advance the buffer head by the given offset. Note that the caller needs
   * to ensure there must not be any record/gap marker in the buffer slots that
   * get advanced.
   */
  virtual void advanceBufferHead(size_t offset = 1) = 0;

  /**
   * Return the current capacity of the buffer.
   */
  virtual size_t capacity() const = 0;

  /**
   * Clear the entire buffer despite all descriptors in it. Caller needs to be
   * aware that all buffered RecordStates are lost after the call. This is
   * often used when ClientReadStream needs to reset its internal state.
   */
  virtual void clear() = 0;

  /**
   * Invoke the supplied callback for each RecordState instance up to and
   * including the specified LSN.  The callback may modify the instance.
   */
  virtual void forEachUpto(
      lsn_t to,
      std::function<void(lsn_t, ClientReadStreamRecordState& record)> cb) = 0;

  /**
   * Invokes the supplied callback for each RecordState instance between from
   * and to (inclusive). The callback may modify the instance and may also
   * terminate the processing early by returning false.
   * Note that from may be greater than to, in which case the iteration will
   * run backwards.
   */
  virtual void forEach(
      lsn_t from,
      lsn_t to,
      std::function<bool(lsn_t, ClientReadStreamRecordState& record)> cb) = 0;

  // TODO: add interface to support dynamic scaling of buffer capacity

  ////// utility functions

  // Find the largest LSN we can fit into the buffer, taking care to avoid
  // integer overflow.
  static lsn_t maxLSNToAccept(lsn_t buffer_head, size_t capacity) {
    return buffer_head < LSN_MAX - capacity + 1 ? buffer_head + capacity - 1
                                                : LSN_MAX;
  }

  lsn_t maxLSNToAccept() const {
    return maxLSNToAccept(getBufferHead(), capacity());
  }

  // Checks if an LSN is within the lsn range of the buffer
  bool LSNInBuffer(lsn_t lsn) const {
    return lsn >= getBufferHead() && lsn <= maxLSNToAccept();
  }

  virtual ~ClientReadStreamBuffer(){};
};

}} // namespace facebook::logdevice
