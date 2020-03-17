/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"
#include "logdevice/common/replicated_state_machine/RsmSnapshotStoreFactory.h"
#include "logdevice/server/LocalRSMSnapshotStoreImpl.h"

/**
 * @file A mixed implementation composed of LocalRSMSnapshotStore,
 *       and one or more fallback stores
 */

namespace facebook { namespace logdevice {
class LocalSnapshotStoreWithFallbacks : public RSMSnapshotStore {
 public:
  explicit LocalSnapshotStoreWithFallbacks(Processor* processor,
                                           std::string key,
                                           bool writable)
      : RSMSnapshotStore(key, writable) {
    local_store_ = std::make_unique<LocalRSMSnapshotStoreImpl>(key, writable);
    msg_fallback_store_ = RsmSnapshotStoreFactory::create(
        processor, SnapshotStoreType::MESSAGE, key, true /* is_server */);
    log_based_store_ = RsmSnapshotStoreFactory::create(
        processor, SnapshotStoreType::LOG, key, true /* is_server */);
  }

  lsn_t getMaxRsmVersionInCluster();
  void getSnapshotImpl(lsn_t min_ver, snapshot_cb_t cb);
  void getSnapshot(lsn_t min_ver, snapshot_cb_t) override;

  void writeSnapshot(lsn_t snapshot_ver,
                     std::string snapshot_blob,
                     completion_cb_t cb) override;

  void getVersion(snapshot_ver_cb_t cb) override;

  void getDurableVersion(snapshot_ver_cb_t cb) override {
    local_store_->getDurableVersion(std::move(cb));
  }

 private:
  // last known version in local store
  lsn_t last_known_version_{LSN_INVALID};
  snapshot_cb_t read_cb_;
  completion_cb_t write_cb_;
  std::unique_ptr<RSMSnapshotStore> local_store_;
  // This is for retrieving snapshots via message based store
  std::unique_ptr<RSMSnapshotStore> msg_fallback_store_;
  // This is for dual writing the snapshots to a log based store
  // until local store is stable in prod. Without it there's no
  // way to rollback to a log based store safely.
  std::unique_ptr<RSMSnapshotStore> log_based_store_;
};
}} // namespace facebook::logdevice
