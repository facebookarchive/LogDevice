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

#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"
#include "logdevice/include/LogTailAttributes.h"

/**
 * @file Message based implementation of RSMSnapshotStore.
 */

namespace facebook { namespace logdevice {
class MessageBasedRSMSnapshotStore : public RSMSnapshotStore {
 public:
  explicit MessageBasedRSMSnapshotStore(std::string key)
      : RSMSnapshotStore(key, false /* allow snapshotting */) {}

  void getSnapshot(lsn_t min_ver, snapshot_cb_t) override;

  void writeSnapshot(lsn_t snapshot_ver,
                     std::string snapshot_blob,
                     completion_cb_t) override;

  void getVersion(snapshot_ver_cb_t) override;

  void getDurableVersion(snapshot_ver_cb_t) override;

 private:
  virtual int postRequestWithRetrying(std::unique_ptr<Request>& rq);
};
}} // namespace facebook::logdevice
