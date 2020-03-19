/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/LocalSnapshotStoreWithFallbacks.h"

#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {
lsn_t LocalSnapshotStoreWithFallbacks::getMaxRsmVersionInCluster() {
  lsn_t max_ver_in_cluster{LSN_INVALID};
  FailureDetector* fd{nullptr};
  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    ServerProcessor* processor = w->processor_;
    fd = processor->failure_detector_.get();
  }
  if (fd) {
    std::multimap<lsn_t, node_index_t, std::greater<lsn_t>> all_rsm_versions;
    auto delta_log_id = logid_t(folly::to<logid_t::raw_type>(key_));
    auto st = fd->getAllRSMVersionsInCluster(
        delta_log_id, RsmVersionType::IN_MEMORY, all_rsm_versions);
    if (st != E::OK || !all_rsm_versions.size()) {
      return LSN_INVALID;
    }
    auto begin_it = all_rsm_versions.begin();
    max_ver_in_cluster = begin_it->first;
  }
  return max_ver_in_cluster;
}

void LocalSnapshotStoreWithFallbacks::getVersion(snapshot_ver_cb_t cb) {
  auto local_store_cb = [this, cb](Status st, lsn_t snapshot_ver_out) {
    if (st == E::OK) {
      last_known_version_ = snapshot_ver_out;
    }
    cb(st, snapshot_ver_out);
  };
  local_store_->getVersion(std::move(local_store_cb));
}

void LocalSnapshotStoreWithFallbacks::getSnapshotImpl(lsn_t min_ver,
                                                      snapshot_cb_t cb) {
  read_cb_ = std::move(cb);
  auto local_store_cb =
      [this, min_ver](Status st,
                      std::string snapshot_blob_out,
                      RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
        last_known_version_ = snapshot_attrs.base_version;
        if (st == E::OK || st == E::DROPPED || st == E::INPROGRESS) {
          read_cb_(st == E::DROPPED ? E::FAILED : st,
                   std::move(snapshot_blob_out),
                   snapshot_attrs);
        } else if (st == E::STALE || st == E::NOTFOUND ||
                   st == E::LOCAL_LOG_STORE_READ) {
          // Even for rw storage nodes, it's possible that no value was found
          // in the local store. One scenario is when the store is being used
          // for the very first time (in which case there is no snapshot written
          // to the store yet)
          ld_info("Trying fallback store's getSnapshot() for key:%s. st:%s",
                  key_.c_str(),
                  error_name(st));
          auto snapshot_cb =
              [this, min_ver](
                  Status st_from_fallback,
                  std::string snapshot_blob_out,
                  RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
                Status st_to_forward = st_from_fallback;
                // If the whole cluster is starting at the same time and none
                // of the cluster nodes has any snapshots, LogsConfig RSM will
                // not be able to start.
                if (min_ver <= LSN_OLDEST && st_from_fallback == E::STALE) {
                  ld_info("The whole cluster likely doesn't have any "
                          "snapshots(min_ver:%s, base_ver:%s), returning "
                          "E::EMPTY. key:%s",
                          lsn_to_string(min_ver).c_str(),
                          lsn_to_string(snapshot_attrs.base_version).c_str(),
                          key_.c_str());
                  st_to_forward = E::EMPTY;
                }
                read_cb_(st_to_forward,
                         std::move(snapshot_blob_out),
                         snapshot_attrs);
              };
          msg_fallback_store_->getSnapshot(min_ver, std::move(snapshot_cb));
        } else {
          ld_error("Invalid status returned:%s", error_name(st));
          read_cb_(E::FAILED,
                   "",
                   RSMSnapshotStore::SnapshotAttributes(
                       LSN_INVALID, std::chrono::milliseconds(0)));
        }
      };

  local_store_->getSnapshot(min_ver, std::move(local_store_cb));
}

void LocalSnapshotStoreWithFallbacks::getSnapshot(lsn_t min_ver,
                                                  snapshot_cb_t cb) {
  // First check if local store is even capable of holding snapshots
  if (!local_store_->isWritable()) {
    msg_fallback_store_->getSnapshot(min_ver, std::move(cb));
    return;
  }

  lsn_t max_ver_in_cluster = getMaxRsmVersionInCluster();
  ld_info("key:%s, max_ver_in_cluster:%s, min_ver:%s",
          key_.c_str(),
          lsn_to_string(max_ver_in_cluster).c_str(),
          lsn_to_string(min_ver).c_str());
  if (max_ver_in_cluster > LSN_OLDEST) {
    if (max_ver_in_cluster < min_ver) {
      cb(E::STALE,
         "",
         RSMSnapshotStore::SnapshotAttributes(
             LSN_INVALID, std::chrono::milliseconds(0)));
    } else if (max_ver_in_cluster == min_ver) {
      // Since min_ver here captures at-least current version_ on this node's
      // RSM, we can skip making an unnecessary call to local store if the
      // cluster doesn't have a better version than it.
      cb(E::UPTODATE,
         "",
         RSMSnapshotStore::SnapshotAttributes(
             LSN_INVALID, std::chrono::milliseconds(0)));
    }
  }

  // If the local store doesn't have the most upto-date version, use
  // MessageBased store to fetch it from other cluster nodes.
  // In other words, max_ver_in_cluster > version on this node
  if (max_ver_in_cluster > min_ver && min_ver > LSN_OLDEST &&
      min_ver > last_known_version_) {
    ld_info("Calling fallback store for key:%s, min_ver:%s, "
            "last_known_version_:%s",
            key_.c_str(),
            lsn_to_string(min_ver).c_str(),
            lsn_to_string(last_known_version_).c_str());
    msg_fallback_store_->getSnapshot(min_ver, std::move(cb));
    return;
  }

  getSnapshotImpl(min_ver, std::move(cb));
}

void LocalSnapshotStoreWithFallbacks::writeSnapshot(lsn_t snapshot_ver,
                                                    std::string snapshot_blob,
                                                    completion_cb_t cb) {
  auto double_write_cb = [=](Status st, lsn_t lsn) {
    ld_info("Double wrote snapshot, lsn:%s, st:%s",
            lsn_to_string(lsn).c_str(),
            error_name(st));
  };
  auto settings = Worker::settings();
  if (settings.rsm_snapshot_enable_dual_writes &&
      log_based_store_->isWritable()) {
    log_based_store_->writeSnapshot(
        snapshot_ver, snapshot_blob, std::move(double_write_cb));
  }

  write_cb_ = std::move(cb);
  auto local_store_cb = [this](Status st, lsn_t snapshot_ver_out) {
    if (st == E::OK) {
      last_known_version_ = snapshot_ver_out;
    } else if (st != E::INPROGRESS) {
      last_known_version_ = LSN_INVALID;
    }
    write_cb_(st, snapshot_ver_out);
  };
  local_store_->writeSnapshot(
      snapshot_ver, snapshot_blob, std::move(local_store_cb));
}

}} // namespace facebook::logdevice
