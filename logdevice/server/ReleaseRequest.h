/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RecordID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice {

enum class ReleaseType : uint8_t;

/**
 * @file While processing a RELEASE message from a sequencer, a worker posts
 * this request to all other workers subscribed to this log, to let them know
 * about the new record.
 *
 * The worker receiving this request reads the new record from the local log
 * store and sends it to clients reading from the log.
 */

class ReleaseRequest : public Request {
 public:
  /**
   * @param target        worker thread that should process this request
   * @param rid           record that was released
   * @param force         if set, worker will attempt to read more data even if
   *                      rid was already delivered; this is used when last
   *                      released LSN and trim point are recovered (see
   *                      AllServerReadStreams::onRelease)
   */
  explicit ReleaseRequest(worker_id_t target,
                          RecordID rid,
                          shard_index_t shard,
                          bool force)
      : Request(RequestType::RELEASE),
        target_(target),
        rid_(rid),
        shard_(shard),
        force_(force) {}

  int getThreadAffinity(int /*nthreads*/) override {
    // ReleaseRequest gets targeted at a specific worker.  Multiple instances
    // are created if different workers need to know about the RELEASE.
    return target_.val_;
  }

  Request::Execution execute() override;

  /**
   * A helper function to post a new ReleaseRequest on all workers for
   * which filter functor returns true.
   *
   * @param processor     Processor object used to post a new request
   * @param rid           record part of the ReleaseRequest
   * @param filter        function worker_id_t -> bool used to determine to
   *                      which worker threads to send the request to
   * @param force         see ReleaseRequest constructor
   */
  template <typename Func>
  static void broadcastReleaseRequest(ServerProcessor* processor,
                                      RecordID const& rid,
                                      shard_index_t shard,
                                      Func&& filter,
                                      bool force = false) {
    processor->applyToWorkerIdxs(
        [&](worker_id_t idx, WorkerType /*unused*/) {
          if (!filter(idx)) {
            return;
          }

          std::unique_ptr<Request> req =
              std::make_unique<ReleaseRequest>(idx, rid, shard, force);
          if (processor->postRequest(req) != 0) {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            5,
                            "Could not propagate RELEASE %s to worker #%d.  "
                            "postRequest() failed "
                            "with error %s",
                            rid.toString().c_str(),
                            idx.val_,
                            error_description(err));
            retry(processor, rid.logid, shard, idx, force);
          }
        },
        Processor::Order::FORWARD,
        WorkerType::GENERAL);
  }

  static void
  retry(ServerProcessor*, logid_t, shard_index_t, worker_id_t, bool force);

 private:
  worker_id_t target_;
  RecordID rid_;
  shard_index_t shard_;
  bool force_;
};

}} // namespace facebook::logdevice
