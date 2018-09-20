/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/ops/IsLogEmptyBatchRunner.h"

#include "logdevice/common/IsLogEmptyRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/lib/ClientImpl.h"

namespace facebook { namespace logdevice {

void IsLogEmptyBatchRunner::complete(logid_t logid, Status status, bool empty) {
  results_[logid] = Result{status, empty};
  ld_check(in_flight_ > 0);
  --in_flight_;
  scheduleMore();
}

void IsLogEmptyBatchRunner::scheduleMore() {
  while (!logs_.empty() && in_flight_ < max_in_flight_) {
    const logid_t log = logs_.front();
    logs_.pop_front();
    scheduleForLog(log);
  }

  if (logs_.empty() && in_flight_ == 0) {
    cb_(std::move(results_));
    return;
  }
}

void IsLogEmptyBatchRunner::scheduleForLog(logid_t logid) {
  ++in_flight_;

  RATELIMIT_INFO(
      std::chrono::seconds{1}, 1, "%lu/%lu complete", results_.size(), count_);

  auto cb = [logid, this](
                Status status, bool empty) { complete(logid, status, empty); };

  std::unique_ptr<IsLogEmptyRequest> is_log_empty_req(
      new IsLogEmptyRequest(logid, std::chrono::seconds{10}, cb));
  is_log_empty_req->setWorkerThread(Worker::onThisThread()->idx_);
  std::unique_ptr<Request> request = std::move(is_log_empty_req);
  ClientImpl* client = static_cast<ClientImpl*>(client_.get());
  client->getProcessor().postImportant(request);
}

IsLogEmptyBatchRunner::IsLogEmptyBatchRunner(std::shared_ptr<Client> client,
                                             std::vector<logid_t> logs,
                                             Callback cb)
    : client_(std::move(client)),
      logs_(logs.begin(), logs.end()),
      count_(logs_.size()),
      cb_(cb) {}

IsLogEmptyBatchRunner::~IsLogEmptyBatchRunner() {}

class StartIsLogEmptyBatchRunnerRequest : public Request {
 public:
  StartIsLogEmptyBatchRunnerRequest(IsLogEmptyBatchRunner* runner)
      : Request(RequestType::MISC), runner_(runner) {}
  Request::Execution execute() override {
    runner_->scheduleMore();
    return Execution::COMPLETE;
  }

 private:
  IsLogEmptyBatchRunner* runner_;
};

void IsLogEmptyBatchRunner::start() {
  std::unique_ptr<Request> request =
      std::make_unique<StartIsLogEmptyBatchRunnerRequest>(this);
  ClientImpl* client = static_cast<ClientImpl*>(client_.get());
  client->getProcessor().postImportant(request);
}

}} // namespace facebook::logdevice
