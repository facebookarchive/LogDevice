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
 *         - once the refcount of ZeroCopiedRecord dropped to zero, it is
 *           guaranteed to be destroyed on the original worker thread that
 *           allocated the payload. This ensures the thread safety of evbuffer.
 *
 *        Important Note:
 *         - In practice, ZeroCopiedRecord (and its derived classes) must only
 *           be constructed using the factory method create() of each class.
 *           This ensures that ZeroCopiedRecord is ref counted using shared_ptr
 *           with a custom Deleter that disposes of the object in the correct
 *           thread.
 *
 *         - TODO T20422519: Currently it is required that the payload used to
 *           construct ZeroCopiedRecord must be linear, which means that for
 *           evbuffer payload one must call evbuffer_pullup
 *           (i.e., PayloadHolder::getPayload). evbuffer_pullup might be
 *           expensive and can cause extra copy, in the future we should enforce
 *           libevent to always allocate contiguous buffer when receiving
 *           payloads from network sockets.
 */

class ZeroCopiedRecordDisposal;
class PayloadHolder;

class ZeroCopiedRecord {
 public:
  lsn_t lsn;
  STORE_flags_t flags; // flags from the original STORE_Message
  uint64_t timestamp;
  esn_t last_known_good;
  uint32_t wave_or_recovery_epoch;
  copyset_t copyset;
  // TODO(T33977412) : Remove offset_within_epoch
  uint64_t offset_within_epoch;
  OffsetMap offsets_within_epoch;
  std::map<KeyType, std::string> keys;

  // Slice of the linearized payload data
  Slice payload_raw;

  // Linked list used by realtime reads.
  std::shared_ptr<ZeroCopiedRecord> next_;

  // atomic list hook for disposal purpose
  folly::AtomicIntrusiveLinkedListHook<ZeroCopiedRecord> hook;

  virtual ~ZeroCopiedRecord() {}

  virtual void onDisposedOf() {}
  virtual size_t getBytesEstimate() const;

  /**
   * Get an estimate of the amount of bytes used for the record.
   * it is an estimate since it estimates the size of the control block
   * of std::shared_ptr(s)
   */
  static size_t getBytesEstimate(Slice payload_raw);

  // return if the entry is in the disposal list waiting to be freed
  bool isLinked() const {
    return hook.next != nullptr;
  }

  /**
   * @return  the index of the worker thread which is responsible for freeing
   *          the payload, -1 if the payload can be freed on any thread
   */
  worker_id_t getDisposalThread() const;

  WorkerType getDisposalWorkerType() const;

  ZeroCopiedRecord(const ZeroCopiedRecord&) = delete;
  ZeroCopiedRecord& operator=(const ZeroCopiedRecord&) = delete;

  // functor class invoked when reference count of entry drops to zero
  class Disposer;

  /**
   * Factory function for creating a ZeroCopiedRecord object. Should be the
   * only way an entry created. Ensures that entry is always reference counted
   * with a custom deleter function.
   */
  template <typename Derived, typename Disposer, typename... Args>
  static std::shared_ptr<Derived> create(Disposer disposer, Args&&... args) {
    return std::shared_ptr<Derived>(
        new Derived(std::forward<Args>(args)...), std::move(disposer));
  }

 protected:
  ZeroCopiedRecord();
  // TODO(T33977412) : Change type of offset_within_epoch to OffsetMap
  ZeroCopiedRecord(lsn_t lsn,
                   STORE_flags_t flags,
                   uint64_t timestamp,
                   esn_t last_known_good,
                   uint32_t wave_or_recovery_epoch,
                   const copyset_t& copyset,
                   uint64_t offset_within_epoch,
                   std::map<KeyType, std::string>&& keys,
                   Slice payload_raw,
                   std::shared_ptr<PayloadHolder> payload_holder);

  // Hold a reference to the actual payload.
  // Note: destruction of the entry object does not necessarily
  // destroy the payload since there may be other references.
  // However, record cache makes sure that entry is destroyed on the
  // same Worker thread that allocates the payload. Given that other
  // references of payload are all owned by the same worker, the payload
  // should be ultimatedly freed on the same worker thread.
  std::shared_ptr<PayloadHolder> payload_holder_;
};

class ZeroCopiedRecord::Disposer {
 public:
  explicit Disposer(ZeroCopiedRecordDisposal* const disposal)
      : disposal_(disposal) {}
  void operator()(ZeroCopiedRecord* record);

 private:
  ZeroCopiedRecordDisposal* const disposal_;
};

}} // namespace facebook::logdevice
