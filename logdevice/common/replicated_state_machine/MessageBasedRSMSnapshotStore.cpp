/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/replicated_state_machine/MessageBasedRSMSnapshotStore.h"

#include "logdevice/common/GetRsmSnapshotRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

int MessageBasedRSMSnapshotStore::postRequestWithRetrying(
    std::unique_ptr<Request>& rq) {
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  return processor->postWithRetrying(rq);
}

void MessageBasedRSMSnapshotStore::getSnapshot(lsn_t min_ver,
                                               snapshot_cb_t cb) {
  Worker* w = Worker::onThisThread(false);
  auto thread_affinity = w ? w->idx_ : worker_id_t(-1);
  auto worker_type = w ? w->worker_type_ : WorkerType::GENERAL;
  std::unique_ptr<Request> req = std::make_unique<GetRsmSnapshotRequest>(
      thread_affinity.val(), worker_type, key_, min_ver, cb);
  if (postRequestWithRetrying(req) != 0) {
    cb(E::FAILED,
       "",
       SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
    return;
  }
}

void MessageBasedRSMSnapshotStore::writeSnapshot(lsn_t /* unused */,
                                                 std::string /* unused */,
                                                 completion_cb_t cb) {
  cb(E::NOTSUPPORTED, LSN_INVALID);
}

void MessageBasedRSMSnapshotStore::getVersion(snapshot_ver_cb_t cb) {
  cb(E::NOTSUPPORTED, LSN_INVALID);
}

void MessageBasedRSMSnapshotStore::getDurableVersion(snapshot_ver_cb_t cb) {
  getVersion(std::move(cb));
}

}} // namespace facebook::logdevice
