/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

#include <folly/AtomicIntrusiveLinkedList.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/server/EpochRecordCacheEntry.h"
#include "logdevice/server/RecordCacheDependencies.h"

namespace facebook { namespace logdevice {

class ServerProcessor;

class RecordCacheDisposal : public RecordCacheDependencies {
 public:
  explicit RecordCacheDisposal(ServerProcessor* processor);

  // see RecordCacheDependencies for docs of the following methods
  void
  disposeOfCacheEntry(std::unique_ptr<EpochRecordCacheEntry> entry) override;

  void onRecordsReleased(const EpochRecordCache&,
                         lsn_t begin,
                         lsn_t end,
                         const ReleasedVector& entries) override;

  folly::Optional<Seal> getSeal(logid_t logid,
                                shard_index_t shard,
                                bool soft = false) const override;
  int getHighestInsertedLSN(logid_t log_id,
                            shard_index_t shard,
                            lsn_t* highest_lsn) const override;

  size_t getEpochRecordCacheSize(logid_t logid) const override;

  bool tailOptimized(logid_t logid) const override;

  StatsHolder* getStatsHolder() const override;

 private:
  ServerProcessor* const processor_;
};

}} // namespace facebook::logdevice
