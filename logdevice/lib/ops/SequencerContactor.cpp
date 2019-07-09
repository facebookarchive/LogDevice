/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/ops/SequencerContactor.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/request_util.h"
#include "logdevice/lib/ClientImpl.h"

namespace facebook { namespace logdevice {

SequencerContactor::SequencerContactor(std::shared_ptr<Client> client,
                                       std::set<logid_t> logs,
                                       size_t max_in_flight,
                                       Callback cb)
    : client_(std::move(client)),
      jobs_budget_(max_in_flight),
      num_logs_(logs.size()),
      cb_(std::move(cb)),
      callbackHelper_(this) {
  ld_check(client_ != nullptr);
  ld_check(max_in_flight > 0);
  ld_check(num_logs_ > 0);
  ld_check(cb_ != nullptr);
  ld_check(!logs.empty());

  for (const logid_t log : logs) {
    logs_.push(log);
  }
}

void SequencerContactor::contactSequencer(logid_t logid) {
  ld_check(Worker::onThisThread() == started_on_);
  auto callback_ticket = callbackHelper_.ticket();
  auto cb_wrapper = [logid, callback_ticket](
                        Status st,
                        NodeID seq,
                        lsn_t next_lsn,
                        std::unique_ptr<LogTailAttributes> tail,
                        std::shared_ptr<const EpochMetaDataMap> md_map,
                        std::shared_ptr<TailRecord> /*tail_record*/,
                        folly::Optional<bool> /*is_log_empty*/) {
    folly::Optional<LogTailAttributes> tail_in;
    if (tail) {
      tail_in = *tail;
    }
    callback_ticket.postCallbackRequest([=](SequencerContactor* sc) {
      if (!sc) {
        ld_warning("SequencerContactor destroyed on job finish for log %lu.",
                   logid.val_);
        return;
      }
      sc->onJobComplete(
          logid,
          {st,
           seq,
           next_lsn,
           (tail_in.hasValue()
                ? std::make_unique<LogTailAttributes>(tail_in.value())
                : nullptr),
           md_map});
    });
  };

  ++job_scheduled_;

  RATELIMIT_INFO(std::chrono::seconds{1},
                 1,
                 "%lu/%lu complete",
                 job_completed_,
                 num_logs_);

  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      logid,
      flags_,
      std::move(cb_wrapper),
      GetSeqStateRequest::Context::SYNC_SEQUENCER,
      timeout_);

  ClientImpl* client = static_cast<ClientImpl*>(client_.get());
  client->getProcessor().postImportant(req);
}

void SequencerContactor::start() {
  run_on_worker_nonblocking(
      &static_cast<ClientImpl*>(client_.get())->getProcessor(),
      worker_id_t{-1},
      WorkerType::GENERAL,
      RequestType::MISC,
      [this]() {
        started_on_ = Worker::onThisThread();
        ld_info("Started execution for %lu logs.", logs_.size());
        schedule();
      },
      true);
}

void SequencerContactor::schedule() {
  ld_check(Worker::onThisThread() == started_on_);
  while (!logs_.empty()) {
    bool success = jobs_budget_.acquire();
    if (!success) {
      break;
    }
    const logid_t log = logs_.front();
    logs_.pop();
    contactSequencer(log);
  }

  // we are safe here because contactSequencer never synchronously complete
  checkIfDone();
}

void SequencerContactor::onJobComplete(logid_t logid, Result result) {
  ld_check(Worker::onThisThread() == started_on_);
  results_[logid] = std::move(result);
  ++job_completed_;
  jobs_budget_.release();
  schedule();
}

void SequencerContactor::checkIfDone() {
  ld_check(Worker::onThisThread() == started_on_);
  if (logs_.empty() && jobs_budget_.available() == jobs_budget_.getLimit()) {
    finalize();
  }
}

void SequencerContactor::finalize() {
  ld_check(Worker::onThisThread() == started_on_);
  ld_info("Finished execution. Logs completed %lu. Job scheduled %lu.",
          job_completed_,
          job_scheduled_);
  // `this' might get deleted
  cb_(std::move(results_));
}

SequencerContactor::~SequencerContactor() {}

}} // namespace facebook::logdevice
