/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <vector>

#include <folly/Conv.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file  A task responsible for updating seal for a log in the local log store
 *        and the state map, getting all LNG values and offset within epoch
 *        for epochs in the interval <last_clean_epoch, seal_epoch], and sending
 *        a SEALED reply to the sequencer.
 */

class LocalLogStore;
class LogStorageStateMap;
class LogStorageState;
class PurgeUncleanEpochs;

class SealStorageTask : public StorageTask {
 public:
  // Identify the purpose of the storage task
  enum class Context : uint8_t { RECOVERY = 0, PURGING };

  struct EpochInfo {
    esn_t lng;
    esn_t last_record;
    OffsetMap epoch_offset_map;
    uint64_t last_timestamp;

    bool operator==(const EpochInfo& rhs) const {
      return lng == rhs.lng && last_record == rhs.last_record &&
          epoch_offset_map == rhs.epoch_offset_map &&
          last_timestamp == rhs.last_timestamp;
    }

    std::string toString() const {
      return "[lng: " + folly::to<std::string>(lng.val()) +
          ", last_record: " + folly::to<std::string>(last_record.val()) +
          ", epoch_offset_map: " + epoch_offset_map.toString() +
          ", last_timestamp: " + folly::to<std::string>(last_timestamp) + "]";
    }
  };

  using EpochInfoMap = std::map<epoch_t::raw_type, EpochInfo>;

  enum class EpochInfoSource : uint8_t {
    INVALID = 0,
    CACHE,
    LOCAL_LOG_STORE,
    MAX
  };

  static EnumMap<EpochInfoSource, std::string>& EpochInfoSourceNames();

  // constuctor for recovery context
  SealStorageTask(logid_t log_id,
                  epoch_t last_clean,
                  Seal seal,
                  const Address& reply_to,
                  bool tail_optimized);

  // constuctor for purging context
  SealStorageTask(logid_t log_id,
                  epoch_t last_clean,
                  Seal seal,
                  WeakRef<PurgeUncleanEpochs> driver);

  // see StorageTask.h
  void execute() override;

  Durability durability() const override {
    return durability_;
  }

  void onDone() override;
  void onDropped() override;

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::HIGH;
  }

  // Public for tests
  Status executeImpl(LocalLogStore& store,
                     LogStorageStateMap& state_map,
                     StatsHolder* stats = nullptr);

  // expose seal_. used for testing
  Seal getSeal() const {
    return seal_;
  }

  // get LNG values for all epochs in [last_clean_ + 1, seal_epoch_],
  // used to reply the SEAL message
  void getAllEpochInfo(std::vector<lsn_t>& epoch_lng,
                       std::vector<OffsetMap>& epoch_offset_map,
                       std::vector<uint64_t>& last_timestamp,
                       std::vector<lsn_t>& max_seen_lsn) const;

  // stores EpochInfo of all **non-empty** epoch in the range
  // [last_clean_ + 1, seal_epoch_]
  std::unique_ptr<EpochInfoMap> epoch_info_;

  // tail records for each epoch that has a valid per-epoch released
  // record. used in the Seal context
  std::vector<TailRecord> tail_records_;

 protected:
  virtual shard_index_t getShardIdx() const;

 private:
  const logid_t log_id_;
  const epoch_t last_clean_;
  const epoch_t seal_epoch_;
  const Context context_;
  const Address reply_to_;
  const bool tail_optimized_;

  Seal seal_;
  Status status_;

  // by default no sync is needed. Sync is only required if the seal metadata
  // in local log store changed value
  Durability durability_{Durability::INVALID};

  WeakRef<PurgeUncleanEpochs> purge_driver_;

  EpochInfoSource epoch_info_source_{EpochInfoSource::INVALID};

  // helper method that checks soft seals for preemption, may read metadata
  // from local logstore. updates seal_ if soft seal has a higher Seal record
  // for preemption
  Status checkSoftSeals(LocalLogStore& store, LogStorageState* log_state);
  // helper method that recovers soft seal metadata from local log store if
  // it is absent in memory
  Status recoverSoftSeals(LocalLogStore& store, LogStorageState* log_state);

  // Get the list of last known good ESNs, corresponding epoch sizes and last
  // record esn for each epoch in [last_clean_ + 1, seal_epoch].
  Status getEpochInfo(LocalLogStore& store,
                      LogStorageState* log_state,
                      StatsHolder* stats);

  // Used by getEpochInfo(), try to read RecordCache to avoid reading from local
  // log store.
  // @return   true if cache hit, epoch_info_ is populated from the cache.
  //           false if record cache is not available or cache miss
  bool getEpochInfoFromCache(LocalLogStore& store,
                             LogStorageState* log_state,
                             StatsHolder* stats);

  // actual routine performed when context_ is Context::PURGING
  Status sealForPurging(LocalLogStore& store,
                        LogStorageState* log_state,
                        StatsHolder* stats);
};

}} // namespace facebook::logdevice
