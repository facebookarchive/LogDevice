/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"
#include "logdevice/include/LogTailAttributes.h"

/**
 * @file Log based implementation of RSMSnapshotStore.
 */

namespace facebook { namespace logdevice {
class LogBasedRSMSnapshotStore : public RSMSnapshotStore {
 public:
  explicit LogBasedRSMSnapshotStore(std::string key,
                                    logid_t snapshot_log,
                                    Processor* p,
                                    bool allow_snapshotting)
      : RSMSnapshotStore(key, allow_snapshotting),
        processor_(p),
        snapshot_log_id_(snapshot_log) {}

  bool isWritable() const override;

  void writeSnapshot(lsn_t snapshot_ver,
                     std::string snapshot_blob,
                     completion_cb_t) override;

  void getVersion(snapshot_ver_cb_t) override;

  void getDurableVersion(snapshot_ver_cb_t) override;

  void getSnapshot(lsn_t min_ver, snapshot_cb_t) override;

 private:
  Processor* processor_{nullptr};
  const logid_t snapshot_log_id_;
  lsn_t last_released_real_lsn_{LSN_INVALID};
  SnapshotAttributes last_snapshot_attrs_{LSN_INVALID,
                                          std::chrono::milliseconds(0)};
  std::string latest_snapshot_blob_{""};
  std::chrono::milliseconds snapshot_append_timeout_{std::chrono::seconds{5}};
  // Id of the read streams for reading the snapshot log.
  read_stream_id_t snapshot_log_rsid_{READ_STREAM_ID_INVALID};
  uint32_t used_cache_{0};

  // Find lsn of snapshot log's most recently released record.
  // It will be used to extract base version from the snapshot record.
  virtual int
  getLastReleasedLsn(std::function<void(Status st, lsn_t last_release_lsn)> cb);

  void onGotLastReleased(Status st,
                         lsn_t last_released_real_lsn,
                         lsn_t min_ver,
                         snapshot_cb_t snapshot_cb);

  virtual int extractVersion(const std::string& payload_str,
                             RSMSnapshotHeader& header_out) const;

  virtual int postRequestWithRetrying(std::unique_ptr<Request>& rq) const;

  virtual void postAppendRequest(lsn_t snapshot_ver,
                                 std::string payload,
                                 completion_cb_t cb);

  bool onSnapshotRecord(lsn_t min_ver,
                        snapshot_cb_t cb,
                        Status st,
                        lsn_t read_upto,
                        const std::unique_ptr<DataRecord>& record);

  void setCachedItems(lsn_t last_released_real_lsn,
                      std::string snapshot_blob,
                      SnapshotAttributes snapshot_attrs);

  virtual read_stream_id_t
  createBasicReadStream(lsn_t start_lsn,
                        lsn_t until_lsn,
                        ClientReadStreamDependencies::record_cb_t on_record,
                        ClientReadStreamDependencies::gap_cb_t on_gap,
                        ClientReadStreamDependencies::health_cb_t health_cb);

  friend class MockLogBasedStore;
};
}} // namespace facebook::logdevice
