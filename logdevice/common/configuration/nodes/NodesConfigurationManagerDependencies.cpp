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

#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
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
  ncm_ptr->initOnNCM();
  return Execution::COMPLETE;
}

Request::Execution NewConfigRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  if (serialized_) {
    ld_assert_eq(nullptr, new_config_ptr_);
    ncm_ptr->onNewConfig(std::move(serialized_new_config_));
  } else {
    ld_assert_eq("", serialized_new_config_);
    ncm_ptr->onNewConfig(std::move(new_config_ptr_));
  }
  return Execution::COMPLETE;
}

Request::Execution ProcessingFinishedRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  ncm_ptr->onProcessingFinished(std::move(config_));
  return Execution::COMPLETE;
}

Request::Execution UpdateRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  ncm_ptr->onUpdateRequest(std::move(updates_), std::move(callback_));
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

void Dependencies::overwrite(std::shared_ptr<const NodesConfiguration> config,
                             NodesConfigurationAPI::CompletionCb callback) {
  // may be on a different thread; keep ncm alive while we use store_
  auto ncm_ptr = ncm_.lock();
  if (!ncm_ptr) {
    callback(E::SHUTDOWN, nullptr);
    return;
  }

  std::string serialized_initial_config =
      NodesConfigurationCodecFlatBuffers::serialize(*config);
  if (serialized_initial_config.empty()) {
    // Even an empty NC would have a non empty serialization due to fields in
    // the header
    callback(err, nullptr);
    return;
  }

  store_->getConfig([ncm = ncm_,
                     callback = std::move(callback),
                     config = std::move(config),
                     serialized_initial_config =
                         std::move(serialized_initial_config)](
                        Status status, std::string current_serialized) mutable {
    if (status != E::OK) {
      callback(status, nullptr);
      return;
    }

    // may be on a different thread
    auto ncm_ptr = ncm.lock();
    if (!ncm_ptr) {
      callback(E::SHUTDOWN, nullptr);
      return;
    }

    auto current_version_opt =
        NodesConfigurationCodecFlatBuffers::extractConfigVersion(
            current_serialized);
    if (current_version_opt.hasValue()) {
      auto current_version = current_version_opt.value();
      if (current_version >= config->getVersion()) {
        auto current_config = NodesConfigurationCodecFlatBuffers::deserialize(
            std::move(current_serialized));
        callback(E::VERSION_MISMATCH, std::move(current_config));
        return;
      }
    } else {
      // If current_version_opt == folly::none but the current_serialized config
      // is not empty, the stored value in NCS is corrupt / we cannot interpret
      // the value (which may be the reason we needed to call overwrite() in the
      // first place). Emergency tooling now has little choice other than to
      // discard what's in NCS and blindly update the config (i.e., using
      // folly::none as base_version).
      RATELIMIT_CRITICAL(std::chrono::seconds(5),
                         10,
                         "Cannot extract version from config in NCS, "
                         "overwriting with version %lu",
                         config->getVersion().val());
    }

    ncm_ptr->deps()->store_->updateConfig(
        std::move(serialized_initial_config),
        /* base_version = */ current_version_opt,
        [config = std::move(config), callback = std::move(callback), ncm = ncm](
            Status update_status,
            NodesConfigurationStore::version_t version,
            std::string value) mutable {
          std::shared_ptr<const NodesConfiguration> ret_config = nullptr;
          if (update_status == Status::OK) {
            ld_assert_eq(version, config->getVersion());
            ret_config = std::move(config);
          }
          if (update_status == E::VERSION_MISMATCH && !value.empty()) {
            ret_config = NodesConfigurationCodecFlatBuffers::deserialize(
                std::move(value));
            ld_assert(ret_config);
            ld_assert_eq(version, ret_config->getVersion());
          }

          callback(update_status, std::move(ret_config));
        }); // updateConfig
  });       // getConfig
}

void Dependencies::init(NCMWeakPtr ncm) {
  ld_assert(ncm.lock());
  ncm_ = ncm;

  auto req = makeNCMRequest<InitRequest>();
  processor_->postWithRetrying(req);
}

void Dependencies::postNewConfigRequest(std::string serialized_new_config) {
  // TODO: move all deserialization to a background worker thread
  auto req = makeNCMRequest<NewConfigRequest>(std::move(serialized_new_config));
  processor_->postWithRetrying(req);
}

void Dependencies::postNewConfigRequest(
    std::shared_ptr<const NodesConfiguration> new_config) {
  auto req = makeNCMRequest<NewConfigRequest>(std::move(new_config));
  processor_->postWithRetrying(req);
}

bool Dependencies::shouldDoConsistentConfigFetch() {
  auto ncm = ncm_.lock();
  ld_assert(ncm);
  return ncm->mode_.isStorageMember() && ncm->getConfig() == nullptr;
}

void Dependencies::readFromStoreAndActivateTimer() {
  dcheckOnNCM();
  ld_assert(store_);

  auto data_cb = [ncm = ncm_](Status status, std::string value) {
    // May not be on the NCM thread
    auto ncm_ptr = ncm.lock();
    if (!ncm_ptr) {
      return;
    }
    if (status == Status::OK) {
      auto deps = ncm_ptr->deps();
      ld_assert(deps);
      deps->postNewConfigRequest(std::move(value));
    } else {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          5,
          "Reading from NodesConfigurationStore failed with error %s",
          error_name(status));
    }
  };
  if (shouldDoConsistentConfigFetch()) {
    store_->getLatestConfig(std::move(data_cb));
  } else {
    store_->getConfig(std::move(data_cb));
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
