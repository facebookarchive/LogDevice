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
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file  ZeroCopiedRecord is an internal representation of Logdevice record
 *        originated from messages recevied from the network.
 *
 * This class's original purpose was dealing with thread safe destruction of
 * libevent buffers. This purpose is gone because we now use folly::IOBuf, whose
 * destruction is already thread safe, and we're doing zero-copying without
 * jumping through these hoops. The class is still here because it has all the
 * other stuff apart from payload. TODO: Rename, refactor, or remove.
 */

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

  PayloadHolder payload;

  // Linked list used by realtime reads.
  std::shared_ptr<ZeroCopiedRecord> next_;

  virtual ~ZeroCopiedRecord() {}

  virtual void onDisposedOf() {}
  virtual size_t getBytesEstimate() const;

  /**
   * Get an estimate of the amount of bytes used for the record.
   * It is an estimate since it estimates the size of the control block
   * of folly::IOBuf.
   */
  static size_t getBytesEstimate(Payload payload_raw);

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
                   const PayloadHolder& payload_holder);
};
}} // namespace facebook::logdevice
