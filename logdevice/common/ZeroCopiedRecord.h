/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include <folly/AtomicIntrusiveLinkedList.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file  ZeroCopiedRecord is an internal representation of Logdevice record
 *        originated from messages recevied from the network. In other words,
 *        the payload payload can be libevent evbuffer. The primary goal for
 *        ZeroCopiedRecord is to support efficient multi-cast in thread-safe
 *        manner, in specific:
 *         - shared pointer of ZeroCopiedRecord can be passed around among
 *           thread without worrying out evbuffer thread safety issues.
 *
 *         - payload can be written to evbuffers in multiple worker threads for
 *           multi-casting without copy using evbuffer_add_reference (or
 *           ProtocolWriter::writeWithoutCopy).
 *
 *         - once the refcount of ZeroCopiedRecord dropped to zero, payload is
 *           guaranteed to be destroyed on the original eventloop thread that
 *           allocated the payload. This ensures the thread safety of evbuffer
 *           contained in the payload.
 *
 *        Important Note:
 *
 *         - TODO T20422519: Currently it is required that the payload used to
 *           construct ZeroCopiedRecord must be linear, which means that for
 *           evbuffer payload one must call evbuffer_pullup
 *           (i.e., PayloadHolder::getPayload). evbuffer_pullup might be
 *           expensive and can cause extra copy, in the future we should enforce
 *           libevent to always allocate contiguous buffer when receiving
 *           payloads from network sockets.
 */

class PayloadHolder;

class ZeroCopiedRecord {
 public:
  lsn_t lsn;
  STORE_flags_t flags; // flags from the original STORE_Message
  uint64_t timestamp;
  esn_t last_known_good;
  uint32_t wave_or_recovery_epoch;
  copyset_t copyset;
  OffsetMap offsets_within_epoch;
  std::map<KeyType, std::string> keys;

  // Slice of the linearized payload data
  Slice payload_raw;

  // Linked list used by realtime reads.
  std::shared_ptr<ZeroCopiedRecord> next_;

  virtual ~ZeroCopiedRecord() {}

  virtual void onDisposedOf() {}
  virtual size_t getBytesEstimate() const;

  /**
   * Get an estimate of the amount of bytes used for the record.
   * it is an estimate since it estimates the size of the control block
   * of std::shared_ptr(s)
   */
  static size_t getBytesEstimate(Slice payload_raw);

  ZeroCopiedRecord(const ZeroCopiedRecord&) = delete;
  ZeroCopiedRecord& operator=(const ZeroCopiedRecord&) = delete;

  ZeroCopiedRecord();
  ZeroCopiedRecord(lsn_t lsn,
                   STORE_flags_t flags,
                   uint64_t timestamp,
                   esn_t last_known_good,
                   uint32_t wave_or_recovery_epoch,
                   const copyset_t& copyset,
                   OffsetMap offsets_within_epoch,
                   std::map<KeyType, std::string>&& keys,
                   Slice payload_raw,
                   std::shared_ptr<PayloadHolder> payload_holder);

 protected:
  // Hold a reference to the actual payload.
  // Note: destruction of the entry object does not necessarily
  // destroy the payload since there may be other references.
  // However, record cache makes sure that entry is destroyed on the
  // same Worker thread that allocates the payload. Given that other
  // references of payload are all owned by the same worker, the payload
  // should be ultimatedly freed on the same worker thread.
  std::shared_ptr<PayloadHolder> payload_holder_;
};
}} // namespace facebook::logdevice
