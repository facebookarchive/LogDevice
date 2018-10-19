/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <unordered_map>

#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file SequencerContactor is a simple utility that sends GetSeqState
 *       messages to sequencers of given logs. The purpose is to ensure
 *       1) sequencers for these logs are active and running; and 2) get
 *       results (e.g., tail lsn) from sequencers.
 */

class SequencerContactor {
 public:
  // see SyncSequencerRequest::Callback
  struct Result {
    Status status;
    NodeID seq;
    lsn_t next_lsn;
    std::unique_ptr<LogTailAttributes> tail_attributes;
    std::shared_ptr<const EpochMetaDataMap> metadata_map;
  };

  using Results = std::unordered_map<logid_t, Result, logid_t::Hash>;
  using Callback = std::function<void(Results)>;

  SequencerContactor(std::shared_ptr<Client> client,
                     std::set<logid_t> logs,
                     size_t max_in_flight,
                     Callback cb);

  ~SequencerContactor();

  void setContactFlags(SyncSequencerRequest::flags_t flags) {
    flags_ = flags;
  }

  // the default timeout is 5s, set to 0 means no timeout
  void setContactTimeoutForEachLog(std::chrono::milliseconds timeout) {
    timeout_ = timeout;
  }

  void start();
  void onJobComplete(logid_t logid, Result result);

 private:
  std::shared_ptr<Client> client_;
  ResourceBudget jobs_budget_;
  size_t num_logs_;
  std::queue<logid_t> logs_;
  Callback cb_;
  Results results_;
  SyncSequencerRequest::flags_t flags_{0};
  std::chrono::milliseconds timeout_{5000};
  size_t job_scheduled_{0};
  size_t job_completed_{0};

  // Used to ensure we have a callback called on this Worker thread when the
  // SyncSequencerRequest completes.
  WorkerCallbackHelper<SequencerContactor> callbackHelper_;
  Worker* started_on_{nullptr};

  void contactSequencer(logid_t logid);

  void schedule();
  void checkIfDone();
  void finalize();
};

}} // namespace facebook::logdevice
