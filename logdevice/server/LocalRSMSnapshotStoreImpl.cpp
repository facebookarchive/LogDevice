/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/LocalRSMSnapshotStoreImpl.h"

#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {
void WriteRsmSnapshotStorageTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  RsmSnapshotMetadata metadata{
      snapshot_ver_,
      std::move(snapshot_blob_),
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())};
  LocalLogStore::WriteOptions options;
  int rv = store.writeLogMetadata(log_id_, metadata, options);
  if (!rv) {
    status_ = E::OK;
    ld_debug("Successfully wrote RsmSnapshotMetadata(%s) for log:%lu",
             metadata.toString().c_str(),
             log_id_.val_);
  } else {
    status_ = E::FAILED;
    ld_error("Could not write RsmSnapshotMetadata for log %lu: %s",
             log_id_.val_,
             error_description(err));
  }
}

void WriteRsmSnapshotStorageTask::onDone() {
  ld_debug("key_:%lu, status_:%s, snapshot_ver_:%s",
           log_id_.val_,
           error_name(status_),
           lsn_to_string(snapshot_ver_).c_str());
  cb_(status_, status_ == E::OK ? snapshot_ver_ : LSN_INVALID);
}

void WriteRsmSnapshotStorageTask::onDropped() {
  ld_error("StorageTask for key:%lu dropped.", log_id_.val_);
  cb_(E::FAILED, LSN_INVALID);
}

void ReadRsmSnapshotStorageTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  int rv = store.readLogMetadata(log_id_, &metadata_out_);
  if (rv == 0) {
    status_ = E::OK;
  } else {
    status_ = err;
    ld_info("Failed to read RsmSnapshotMetadata for log %lu, err:%s",
            log_id_.val_,
            error_description(err));
  }
}

void ReadRsmSnapshotStorageTask::onDone() {
  if (status_ == E::OK) {
    if (metadata_out_.version_ < min_ver_) {
      status_ = E::STALE;
    }
  }
  ld_info("key:%lu, status_:%s, min_ver:%s, metadata(%s)",
          log_id_.val_,
          error_name(status_),
          lsn_to_string(min_ver_).c_str(),
          metadata_out_.toString().c_str());
  cb_(status_,
      status_ == E::OK ? metadata_out_.snapshot_blob_ : "",
      RSMSnapshotStore::SnapshotAttributes(
          metadata_out_.version_, metadata_out_.update_time_));
}

void ReadRsmSnapshotStorageTask::onDropped() {
  ld_error("StorageTask for key:%lu dropped.", log_id_.val_);
  cb_(E::DROPPED,
      "",
      RSMSnapshotStore::SnapshotAttributes(
          LSN_INVALID, std::chrono::milliseconds(0)));
}

void LocalRSMSnapshotStoreImpl::getVersion(snapshot_ver_cb_t cb) {
  auto cb_read_storage_task =
      [this, cb](Status st,
                 std::string /* unused */,
                 RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
        cb(st, snapshot_attrs.base_version);
      };
  auto task = std::make_unique<ReadRsmSnapshotStorageTask>(
      logid_t(folly::to<logid_t::raw_type>(key_)),
      LSN_OLDEST,
      std::move(cb_read_storage_task));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_id_);
  task_queue->putTask(std::move(task));
}

ReplicationProperty LocalRSMSnapshotStoreImpl::getDurableReplicationProperty(
    ReplicationProperty rp_in,
    size_t cluster_size) {
  ReplicationProperty rp_out = rp_in;
  auto replicationFactors = rp_in.getDistinctReplicationFactors();
  auto rf_size = replicationFactors.size();
  if (rf_size == 0) {
    return rp_out;
  }

  auto r = replicationFactors[rf_size - 1].second;
  // Double the replication factor of delta log, but cap it to atmost half
  // of the cluster
  int desired_replication = r * 2;
  int max_replication = std::max(r, static_cast<int>(cluster_size / 2));
  rp_out.setReplication(
      NodeLocationScope::NODE, std::min(desired_replication, max_replication));
  return rp_out;
}

// Twice the ReplicationProperty of delta log is used to determine the number
// of nodes that should have some durable version of the delta log.
// All durable rsm versions in cluster are sorted in descending order, and
// we keep going down the version list until a vaild FailureDomainNodeset can
// be formed. The last version processed while doing the above iteration
// is returned as a trimmable version.
void LocalRSMSnapshotStoreImpl::getDurableVersion(snapshot_ver_cb_t cb) {
  if (!isWritable()) {
    cb(E::NOTSUPPORTED, LSN_INVALID);
    return;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  ServerProcessor* processor = worker->processor_;
  if (!worker->isAcceptingWork()) {
    cb(E::SHUTDOWN, LSN_INVALID);
    return;
  }

  auto delta_log_id = logid_t(folly::to<logid_t::raw_type>(key_));
  auto fd = processor->failure_detector_.get();
  if (!fd) {
    ld_error("FailureDetector not running, returning E::FAILED for log:%lu",
             delta_log_id.val_);
    cb(E::FAILED, LSN_INVALID);
    return;
  }

  ReplicationProperty rp;
  auto logsconfig = processor->config_->getLocalLogsConfig();
  auto status = logsconfig->getLogReplicationProperty(delta_log_id, rp);
  if (status != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to fetch ReplicationProperty for internal log:%lu",
                    delta_log_id.val_);
    cb(E::FAILED, LSN_INVALID);
    return;
  }

  // Double the replication property upto half the cluster nodes to determine
  // durability of locally stored snapshots
  const auto& nodes_cfg = worker->getNodesConfiguration();
  ReplicationProperty rp_double =
      getDurableReplicationProperty(rp, nodes_cfg->clusterSize());

  Status fstatus{E::DATALOSS};
  lsn_t durable_lsn{LSN_INVALID};
  StorageSet ss;
  int nodes_added{0};
  int min_nodes_needed = rp_double.getReplicationFactor();
  // Fetch all RSM versions in descending order
  std::multimap<lsn_t, node_index_t, std::greater<lsn_t>> sorted_rsm_versions;
  fd->getAllRSMVersionsInCluster(
      delta_log_id, RsmVersionType::DURABLE, sorted_rsm_versions);
  for (auto m : sorted_rsm_versions) {
    if (m.first == LSN_INVALID) {
      break;
    }
    ss.push_back(ShardID(m.second, 0));
    nodes_added++;
    if (nodes_added >= min_nodes_needed &&
        configuration::nodes::validStorageSet(*nodes_cfg, ss, rp_double)) {
      fstatus = E::OK;
      durable_lsn = m.first;
      break;
    }
  }

  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "%s a durable version(%s) for log:%lu, nodes checked:%d, "
                 " min_nodes_needed:%d, cluster_size:%zu",
                 fstatus == E::OK ? "Found" : "Didn't find",
                 lsn_to_string(durable_lsn).c_str(),
                 delta_log_id.val_,
                 nodes_added,
                 min_nodes_needed,
                 nodes_cfg->clusterSize());
  cb(fstatus, durable_lsn);
}

void LocalRSMSnapshotStoreImpl::getSnapshot(lsn_t min_ver, snapshot_cb_t cb) {
  if (read_in_flight_) {
    cb(E::INPROGRESS,
       "",
       SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
    return;
  }

  read_in_flight_ = true;
  read_cb_ = std::move(cb);
  auto cb_read_storage_task =
      [this, min_ver](Status st,
                      std::string snapshot_blob_out,
                      RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
        read_cb_(st, std::move(snapshot_blob_out), snapshot_attrs);
        read_in_flight_ = false;
      };
  auto task = std::make_unique<ReadRsmSnapshotStorageTask>(
      logid_t(folly::to<logid_t::raw_type>(key_)),
      min_ver,
      std::move(cb_read_storage_task));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_id_);
  task_queue->putTask(std::move(task));
}

void LocalRSMSnapshotStoreImpl::writeSnapshot(lsn_t snapshot_ver,
                                              std::string snapshot_blob,
                                              completion_cb_t cb) {
  if (!isWritable()) {
    cb(E::NOTSUPPORTED, LSN_INVALID);
    return;
  }

  if (write_in_flight_) {
    cb(E::INPROGRESS, LSN_INVALID);
    return;
  }
  ld_info("key:%s, ver:%s, payload size:%lu",
          key_.c_str(),
          lsn_to_string(snapshot_ver).c_str(),
          snapshot_blob.size());

  write_in_flight_ = true;
  write_cb_ = std::move(cb);
  auto cb_write_storage_task = [this](Status st, lsn_t snapshot_ver_out) {
    write_cb_(st, snapshot_ver_out);
    write_in_flight_ = false;
  };
  auto task = std::make_unique<WriteRsmSnapshotStorageTask>(
      logid_t(folly::to<logid_t::raw_type>(key_)),
      snapshot_ver,
      std::move(snapshot_blob),
      std::move(cb_write_storage_task));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_id_);
  task_queue->putTask(std::move(task));
}

}} // namespace facebook::logdevice
