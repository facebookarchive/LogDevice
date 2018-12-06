/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManagerDependencies.h"

#include <folly/Conv.h>
#include <folly/json.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/debug.h"

using namespace facebook::logdevice::membership;

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes { namespace ncm {

//////// REQUESTS ////////
Request::Execution NCMRequest::execute() {
  // Check if the state machine has been destroyed
  auto ncm_ptr = ncm_.lock();
  if (!ncm_ptr) {
    return Execution::COMPLETE;
  }

  // If weak ref is valid, NCM won't be destroyed until we finish execution
  // since we should be executed in the same context (on the same worker).
  if (folly::kIsDebug) {
    Dependencies* deps = ncm_ptr->deps();
    ld_assert(deps);
    deps->dcheckOnNCM();
  }

  return executeOnNCM(std::move(ncm_ptr));
}

Request::Execution Dependencies::InitRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  ld_check(ncm_ptr);
  ncm_ptr->initOnNCM();
  return Execution::COMPLETE;
}

Request::Execution NewConfigRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  ld_check(ncm_ptr);
  ncm_ptr->onNewConfig(std::move(new_config_));

  return Execution::COMPLETE;
}

Request::Execution ProcessingFinishedRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> FOLLY_NONNULL ncm_ptr) {
  ncm_ptr->onProcessingFinished(std::move(config_));
  return Execution::COMPLETE;
}

Dependencies::Dependencies(Processor* processor,
                           std::unique_ptr<NodesConfigurationStore> store)
    : processor_(processor), store_(std::move(store)) {
  ld_check(processor_);
  ld_check(store_);
  // Pick a random worker to pin the StateMachine on, use background worker if
  // available.
  worker_type_ = (processor_->getWorkerCount(WorkerType::BACKGROUND) > 0)
      ? WorkerType::BACKGROUND
      : WorkerType::GENERAL;
  worker_id_ = worker_id_t{static_cast<int>(
      folly::Random::rand32(processor_->getWorkerCount(worker_type_)))};
}

void Dependencies::dcheckOnNCM() const {
  if (!folly::kIsDebug) {
    return;
  }
  auto current_worker = Worker::onThisThread(/* enforce_worker = */ false);
  ld_assert(current_worker);
  ld_assert_eq(current_worker->idx_, worker_id_);
  ld_assert(current_worker->worker_type_ == worker_type_);
}

/* static */ constexpr const char* Dependencies::kConfigKey;

void Dependencies::init(NCMWeakPtr ncm) {
  ld_assert(ncm.lock());
  ncm_ = ncm;

  auto req = makeNCMRequest<InitRequest>();
  processor_->postWithRetrying(req);
}

void Dependencies::readFromStoreAndActivateTimer() {
  dcheckOnNCM();
  ld_assert(store_);
  int rc = store_->getConfig(
      kConfigKey, [ncm = ncm_](Status status, std::string value) {
        // May not be on the NCM thread
        auto ncm_ptr = ncm.lock();
        if (!ncm_ptr) {
          return;
        }
        if (status == Status::OK) {
          auto deps = ncm_ptr->deps();
          ld_assert(deps);
          auto req = deps->makeNCMRequest<NewConfigRequest>(std::move(value));
          deps->processor_->postWithRetrying(req);
        } else {
          RATELIMIT_ERROR(
              std::chrono::seconds(10),
              5,
              "Reading from NodesConfigurationStore failed with error %s",
              error_name(status));
        }
      });
  if (rc != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Reading from NodesConfigurationStore failed with error %s",
                    error_name(err));
  }

  if (!timer_) {
    timer_ = std::make_unique<Timer>([ncm = ncm_]() {
      // Currently timer callbacks are executed on the same thread as the state
      // machine, but this assumption will change.
      auto ncm_ptr = ncm.lock();
      if (!ncm_ptr) {
        return;
      }
      ncm_ptr->deps()->readFromStoreAndActivateTimer();
    });
  }

  // TODO: make this interval configurable
  timer_->activate(std::chrono::seconds(3));
}

}}}}} // namespace facebook::logdevice::configuration::nodes::ncm
