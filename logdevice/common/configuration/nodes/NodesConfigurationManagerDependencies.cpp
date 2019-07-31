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

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/ClientHistograms.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

using namespace facebook::logdevice::membership;

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes { namespace ncm {
//////// CONTEXTS ////////
void UpdateRequestData::onDestruction() {
  if (status_ != Status::OK) {
    // TODO: log errors separately
    return;
  }
  NodesConfigurationTracer::Sample sample;
  sample.nc_update_gen_ = [updates = std::move(update_)]() mutable {
    return logdevice::toString(std::move(updates));
  };
  // Note that nc is the published nc only if status is OK
  sample.published_nc_ = std::move(nc_);
  sample.source_ = NodesConfigurationTracer::Source::NCM_UPDATE;
  sample.timestamps_ = std::move(timestamps_);
  tracer_.trace(std::move(sample));
}

void OverwriteRequestData::onDestruction() {
  if (status_ != Status::OK) {
    // TODO: log errors separately
    return;
  }
  NodesConfigurationTracer::Sample sample;
  sample.nc_update_gen_ = [configuration = nc_]() mutable {
    // TODO: we don't have NodesConfiguration::toString(), so we use
    // the debug JSON string instead. This could likely be more
    // efficient.
    return NodesConfigurationCodec::debugJsonString(*configuration);
  };
  // Note that nc is the published nc only if status is OK
  sample.published_nc_ = std::move(nc_);
  sample.source_ = NodesConfigurationTracer::Source::NCM_OVERWRITE;
  sample.timestamps_ = std::move(timestamps_);
  tracer_.trace(std::move(sample));
}

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
  ncm_ptr->initOnNCM(std::move(init_nc_));
  return Execution::COMPLETE;
}

Request::Execution Dependencies::ShutdownRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  ncm_ptr->deps()->cancelTimer();
  ncm_ptr->shutdown_completed_.post();
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
  ncm_ptr->onUpdateRequest(std::move(ctx_), std::move(callback_));
  return Execution::COMPLETE;
}

Request::Execution ReportRequest::executeOnNCM(
    std::shared_ptr<NodesConfigurationManager> ncm_ptr) {
  auto holder = ncm_ptr->deps()->getStats();
  switch (type_) {
    case NCMReportType::NCS_READ_FAILED: {
      STAT_INCR(holder, nodes_configuration_store_read_failed);
      break;
    }
    case NCMReportType::ADVANCE_INTERMEDIARY_SHARD_STATES_FAILED: {
      STAT_INCR(holder,
                nodes_configuration_manager_advance_intermediary_state_failed);
      break;
    }
    default:
      break;
  }
  return Execution::COMPLETE;
}

Dependencies::Dependencies(Processor* processor,
                           std::unique_ptr<NodesConfigurationStore> store)
    : processor_(processor),
      store_(std::move(store)),
      tracer_(processor_->getTraceLogger()) {
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

bool Dependencies::isOnNCM() const {
  auto current_worker = Worker::onThisThread(/* enforce_worker = */ false);
  if (!current_worker) {
    return false;
  }

  return current_worker->idx_ == worker_id_ &&
      current_worker->worker_type_ == worker_type_;
}

void Dependencies::dcheckOnNCM() const {
  ld_assert(isOnNCM());
}

void Dependencies::dcheckNotOnNCM() const {
  ld_assert(!isOnNCM());
}

void Dependencies::dcheckNotOnProcessor() const {
  ld_assert(!Worker::onThisThread(/* enforce_worker = */ false));
}

void Dependencies::overwrite(OverwriteContext ctx,
                             NodesConfigurationAPI::CompletionCb callback) {
  // may be on a different thread; keep ncm alive while we use store_
  auto ncm_ptr = ncm_.lock();
  if (!ncm_ptr) {
    ctx.setStatus(E::SHUTDOWN);
    callback(E::SHUTDOWN, nullptr);
    return;
  }

  ctx.data_.serialized_nc_ = NodesConfigurationCodec::serialize(*ctx.data_.nc_);
  if (ctx.data_.serialized_nc_.empty()) {
    // Even an empty NC would have a non empty serialization due to fields in
    // the header
    ctx.setStatus(err);
    callback(err, nullptr);
    return;
  }

  store_->getConfig([ncm = ncm_,
                     callback = std::move(callback),
                     ctx = std::move(ctx)](
                        Status status, std::string current_serialized) mutable {
    ctx.addTimestamp("NCS read");
    // For overwrite, we handle the case where the initial NCS key is not
    // provisioned
    if (status != E::OK && status != E::NOTFOUND) {
      ctx.setStatus(status);
      callback(status, nullptr);
      return;
    }

    // may be on a different thread
    auto ncm_ptr = ncm.lock();
    if (!ncm_ptr) {
      ctx.setStatus(E::SHUTDOWN);
      callback(E::SHUTDOWN, nullptr);
      return;
    }

    auto current_version_opt =
        NodesConfigurationCodec::extractConfigVersion(current_serialized);
    if (current_version_opt.hasValue()) {
      auto current_version = current_version_opt.value();
      if (current_version >= ctx.data_.nc_->getVersion()) {
        auto current_config =
            NodesConfigurationCodec::deserialize(std::move(current_serialized));
        ctx.setStatus(E::VERSION_MISMATCH);
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
                         ctx.data_.nc_->getVersion().val());
    }

    auto serialized_initial_config = std::move(ctx.data_.serialized_nc_);
    ncm_ptr->deps()->store_->updateConfig(
        std::move(serialized_initial_config),
        /* base_version = */ current_version_opt,
        [callback = std::move(callback), ncm = ncm, ctx = std::move(ctx)](
            Status update_status,
            NodesConfigurationStore::version_t version,
            std::string value) mutable {
          ctx.addTimestamp("NCS updated");
          std::shared_ptr<const NodesConfiguration> ret_config = nullptr;
          if (update_status == Status::OK) {
            ld_assert_eq(version, ctx.data_.nc_->getVersion());
            ret_config = ctx.data_.nc_;
          }
          if (update_status == E::VERSION_MISMATCH && !value.empty()) {
            ret_config = NodesConfigurationCodec::deserialize(std::move(value));
            ld_assert(ret_config);
            ld_assert_eq(version, ret_config->getVersion());
          }

          ctx.setStatus(update_status);
          callback(update_status, std::move(ret_config));
        }); // updateConfig
  });       // getConfig
}

void Dependencies::init(NCMWeakPtr ncm,
                        std::shared_ptr<const NodesConfiguration> init_nc) {
  auto ncm_ptr = ncm.lock();
  ld_assert(ncm_ptr);
  ncm_ = ncm;
  processor_->setNodesConfigurationManager(ncm_ptr);

  auto req = makeNCMRequest<InitRequest>(std::move(init_nc));
  processor_->postWithRetrying(req);
}

void Dependencies::shutdown() {
  ld_info("NCM Dependencies shutting down...");
  shutdown_signaled_.store(true);
  store_->shutdown();
  auto req = makeNCMRequest<ShutdownRequest>();
  processor_->postWithRetrying(req);
}

bool Dependencies::shutdownSignaled() const {
  return shutdown_signaled_.load();
}

void Dependencies::cancelTimer() {
  dcheckOnNCM();
  if (timer_) {
    timer_->cancel();
  }
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

void Dependencies::readFromStore(bool should_do_consistent_config_fetch) {
  dcheckOnNCM();

  if (shutdownSignaled()) {
    return;
  }

  ld_assert(store_);
  auto data_cb = [ncm = ncm_](Status status, std::string value) {
    // May not be on the NCM thread
    auto ncm_ptr = ncm.lock();
    if (!ncm_ptr) {
      return;
    }

    auto deps = ncm_ptr->deps();
    ld_assert(deps);
    if (status != Status::OK) {
      if (status != Status::UPTODATE) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            5,
            "Reading from NodesConfigurationStore failed with error %s",
            error_name(status));
        deps->reportEvent(NCMReportType::NCS_READ_FAILED);
      }
      return;
    }

    deps->postNewConfigRequest(std::move(value));
  };
  if (should_do_consistent_config_fetch) {
    store_->getLatestConfig(std::move(data_cb));
  } else {
    store_->getConfig(std::move(data_cb));
  }
}

void Dependencies::scheduleHeartBeat() {
  dcheckOnNCM();

  if (shutdownSignaled()) {
    return;
  }

  if (!timer_) {
    timer_ = std::make_unique<Timer>([ncm = ncm_]() {
      // Currently timer callbacks are executed on the same thread as the state
      // machine, but this assumption will change.
      auto ncm_ptr = ncm.lock();
      if (!ncm_ptr) {
        return;
      }
      if (ncm_ptr->shutdownSignaled()) {
        return;
      }
      ncm_ptr->deps()->dcheckOnNCM();
      ncm_ptr->onHeartBeat();
      ncm_ptr->deps()->scheduleHeartBeat();
    });
  }

  auto polling_interval =
      processor_->settings()
          ->nodes_configuration_manager_store_polling_interval;
  auto jittered_timeout =
      std::chrono::duration_cast<std::chrono::milliseconds>(polling_interval) *
      (1 + folly::Random::randDouble(0, 0.1));
  timer_->activate(
      std::chrono::duration_cast<std::chrono::microseconds>(jittered_timeout));
}

StatsHolder* FOLLY_NULLABLE Dependencies::getStats() {
  if (Worker::onThisThread(false)) {
    return Worker::stats();
  } else {
    return nullptr;
  }
}

void Dependencies::reportPropagationLatency(
    const std::shared_ptr<const NodesConfiguration>& config) {
  auto propagation_latency = msec_since(config->getLastChangeTimestamp());
  if (processor_->settings()->server) {
    HISTOGRAM_ADD(getStats(),
                  nodes_configuration_manager_propagation_latency,
                  propagation_latency);
  } else {
    CLIENT_HISTOGRAM_ADD(getStats(),
                         nodes_configuration_manager_propagation_latency,
                         propagation_latency);
  }
}

void Dependencies::reportEvent(NCMReportType type) {
  // no need to be on NCM thread
  auto req = makeNCMRequest<ReportRequest>(type);
  processor_->postWithRetrying(req);
}

void Dependencies::checkAndReportConsistency() {
  const auto& server_nc =
      processor_->getNodesConfigurationFromServerConfigSource();
  const auto& ncm_nc = processor_->getNodesConfigurationFromNCMSource();

  auto diverged = !ncm_nc->equalWithTimestampAndVersionIgnored(*server_nc);
  auto same_version = server_nc->getVersion() == ncm_nc->getVersion();

  STAT_SET(
      getStats(), nodes_configuration_server_config_diverged, diverged ? 1 : 0);

  STAT_SET(getStats(),
           nodes_configuration_server_config_diverged_with_same_version,
           diverged && same_version ? 1 : 0);
}

}}}}} // namespace facebook::logdevice::configuration::nodes::ncm
