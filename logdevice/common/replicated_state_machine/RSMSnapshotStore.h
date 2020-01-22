/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

/**
 * @file Common interface to a RSM SnapshotStore.
 * Responsibilities include
 * a) storing and retrieving Snapshots
 * b) fetching in-memory or durable versions
 *
 * For Server: 'Log' based store or a 'Local' storage based store can be
 * different implementations of this class.
 *
 * For Clients or Sequencer only nodes: a Message based implementation will be
 * provided
 */

namespace facebook { namespace logdevice {

class RSMSnapshotStore {
 public:
  explicit RSMSnapshotStore(std::string key, bool writable)
      : key_(std::move(key)), writable_(writable) {}

  virtual ~RSMSnapshotStore() {}

  typedef std::function<void(Status st, lsn_t lsn)> completion_cb_t;
  // Asynchronously writes the 'snapshot_blob' corresponding to delta log
  // lsn('snapshot_ver') into the store.
  // Callback 'cb' is called upon completion with one of the following Status
  // E::OK,
  // E::FAILED
  // E::INPROGRESS,
  // E::UPTODATE,
  virtual void writeSnapshot(lsn_t snapshot_ver,
                             std::string snapshot_blob,
                             completion_cb_t cb) = 0;

  struct SnapshotAttributes {
    explicit SnapshotAttributes(lsn_t base_ver, std::chrono::milliseconds ts)
        : base_version(base_ver), timestamp(ts) {}

    // version associated with the actual snapshot, this
    // is agnostic to the underlying SnapshotStore. It is also
    // the delta log's lsn upto which snapshot is taken.
    lsn_t base_version;

    // Time when this snapshot was written
    std::chrono::milliseconds timestamp;
  };

  // Asynchronously return the snapshot stored in the underlying store and
  // calls 'cb' upon completion.
  //
  // 'st' returns the success/failure to the caller with one of the following
  // error codes:
  // E::OK
  // E::EMPTY
  // E::NOTFOUND
  // E::FAILED
  // E::TIMEDOUT
  // E::STALE
  // E::TOOBIG
  //
  // 'min_ver' is the minimum version that the caller is interested in
  //
  // 'snapshot_blob_out' will hold the actual snapshot, and will be
  // valid only if 'st'==E::OK
  //
  // 'snapshot_attrs_out' contains the actual version corresponding to
  // the snapshot blob returned
  //
  // Note that this does not need to be linearizable with writeSnapshot()
  typedef std::function<void(Status st,
                             std::string snapshot_blob_out,
                             SnapshotAttributes snapshot_attrs_out)>
      snapshot_cb_t;
  virtual void getSnapshot(lsn_t min_ver, snapshot_cb_t) = 0;

  // Return the base version for which the store currently stores
  // a snapshot.
  //
  // Callback 'cb' will be called with the following error codes
  // E::OK
  // E::NOTSUPPORTED
  // E::EMPTY
  // E::FAILED
  // E::SHUTDOWN

  // 'snapshot_ver_out' will hold the base version for the underlying
  // store's snapshot. It is valid only if st is E::OK
  typedef std::function<void(Status st, lsn_t snapshot_ver_out)>
      snapshot_ver_cb_t;
  virtual void getVersion(snapshot_ver_cb_t cb) = 0;

  // Durable version is the version upto which it is safe to trim delta log.
  //
  // Callback 'cb' will be called with the error codes similar to getVersion()
  // In addition, the following error codes may be returned
  // E::OK
  // E::FAILED
  // E::SHUTDOWN
  // E::DATALOSS (for cases when not enough nodes have a durable snapshot, such
  //              that trimming delta log is risky)
  //
  // 'snapshot_ver_out' will hold the lsn upto which it is safe to trim
  // delta log. It is valid only if st is E::OK
  virtual void getDurableVersion(snapshot_ver_cb_t) = 0;

  // Tells if this store is writable, some stores are read only e.g. on most
  // clients or sequencer only nodes
  virtual bool isWritable() {
    return writable_;
  }

 protected:
  // 'key_' distinguishes different types of RSMs(LogsConfig, EventLog,
  // ClusterMaintenanceManager). It also helps in routing GET RSM requests
  // from clients to the correct state machine
  // Example:
  // For LogsConfig the key would be LogsConfig's delta log_id
  // For store implementations outside of Logdevice, the key can be
  // "cluster name":"log_id"
  std::string key_;
  bool writable_{false};
};

}} // namespace facebook::logdevice
