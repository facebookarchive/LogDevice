/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/LogsConfigManager.h"

#include <algorithm>

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigApiTracer.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_REPLY_Message.h"
#include "logdevice/common/stats/ServerHistograms.h"

using facebook::logdevice::configuration::LocalLogsConfig;
using facebook::logdevice::logsconfig::Delta;
using facebook::logdevice::logsconfig::DeltaOpType;
using facebook::logdevice::logsconfig::DirectoryNode;
using facebook::logdevice::logsconfig::LogGroupNode;
using facebook::logdevice::logsconfig::LogsConfigTree;
using facebook::logdevice::logsconfig::MkDirectoryDelta;
using namespace facebook::logdevice::logsconfig;

namespace facebook { namespace logdevice {

Request::Execution StartLogsConfigManagerRequest::execute() {
  Worker* w = Worker::onThisThread();
  ld_info("Starting LogsConfigManager on Worker %i in pool: %s",
          w->idx_.val_,
          workerTypeStr(w->worker_type_));
  // We will immediately start it if enabled in settings
  manager_->onSettingsUpdated();
  w->setLogsConfigManager(std::move(manager_));
  return Execution::COMPLETE;
}

int LogsConfigManager::getLogsConfigManagerWorkerIdx(int nthreads) {
  return configuration::InternalLogs::CONFIG_LOG_DELTAS.val_ % nthreads;
}

bool LogsConfigManager::createAndAttach(Processor& processor,
                                        bool is_writable) {
  std::unique_ptr<LogsConfigManager> manager;
  ld_debug("Creating LogsConfigManager");

  manager = std::make_unique<LogsConfigManager>(
      processor.updateableSettings(),
      processor.config_,
      LogsConfigManager::workerType(&processor),
      is_writable);
  std::unique_ptr<Request> req =
      std::make_unique<StartLogsConfigManagerRequest>(std::move(manager));

  const int rv = processor.postRequest(req);
  if (rv != 0) {
    ld_error("Cannot post request to start logs config manager: %s (%s)",
             error_name(err),
             error_description(err));
    // crash on debug
    ld_check(false);
    return false;
  }
  return true;
}

LogsConfigManager::LogsConfigManager(
    UpdateableSettings<Settings> updateable_settings,
    std::shared_ptr<UpdateableConfig> updateable_config,
    WorkerType lcm_worker_type,
    bool is_writable)
    : settings_(std::move(updateable_settings)),
      updateable_config_(std::move(updateable_config)),
      lcm_worker_type_(lcm_worker_type),
      is_writable_(is_writable) {}

void LogsConfigManager::onSettingsUpdated() {
  auto previous_grace_period = publish_grace_period_;
  publish_grace_period_ =
      Worker::onThisThread(true)->settings().logsconfig_manager_grace_period;

  if (isEnabledInSettings()) {
    if (publish_timer_.isActive()) {
      // LCM is already running and we have an active publish timer.
      ld_check(is_running_);

      // Let's see if the grace period has been updated, in this case we will
      // cancel and reactivate the timer.
      if (publish_grace_period_ != previous_grace_period) {
        ld_info(
            "grace period has been updated from %lums to %lums, refreshing LCM",
            previous_grace_period.count(),
            publish_grace_period_.count());
        cancelPublishTimer();
        activatePublishTimer();
      }
    } else {
      start();
    }
  } else {
    stop();
  }
}

void LogsConfigManager::start() {
  if (is_running_) {
    return;
  }
  ld_info("Starting LogsConfig Manager RSM");
  // immediately let's publish an empty (not fully loaded) config
  auto server_config = updateable_config_->getServerConfig();
  std::string delimiter = server_config->getNamespaceDelimiter();
  auto current_logsconfig = updateable_config_->updateableLogsConfig()->get();
  if (!current_logsconfig) {
    ld_info(
        "Publishing an empty config (not fully loaded) until RSM is replayed");
    std::shared_ptr<LocalLogsConfig> empty_config =
        std::make_shared<LocalLogsConfig>();
    // Setting the InternalLogs from ServerConfig
    empty_config->setInternalLogsConfig(server_config->getInternalLogsConfig());
    empty_config->setNamespaceDelimiter(delimiter);
    updateable_config_->updateableLogsConfig()->update(empty_config);
  }

  // Setting up the callback from the RSM
  auto cb = [this](const logsconfig::LogsConfigTree& tree,
                   const logsconfig::Delta* /* unused */,
                   lsn_t /* unused */) {
    if (!isEnabledInSettings()) {
      ld_info("LogsConfigManager has received an update from the RSM while "
              "being disabled in settings, ignoring the update. ");
      return;
    }
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Received update from LogsConfigStateMachine, version %s",
                   toString(tree.version()).c_str());
    STAT_INCR(getStats(), logsconfig_manager_received_update);
    activatePublishTimer();
  };
  auto updateable_server_config = updateable_config_->updateableServerConfig();
  state_machine_ = std::make_unique<LogsConfigStateMachine>(
      settings_, updateable_server_config, is_writable_);

  config_updates_handle_ = state_machine_->subscribe(cb);
  // Used to know whether the LogsConfig Manager is STARTED or not.
  ld_info("Starting LogsConfig Replicated State Machine");
  STAT_SET(getStats(), logsconfig_manager_started, 1);
  state_machine_->start();
  is_running_ = true;
}

void LogsConfigManager::cancelPublishTimer() {
  publish_timer_.cancel();
}

void LogsConfigManager::activatePublishTimer() {
  // If grace period is zero we publish the update immediately. We will also
  // publish immediately if this is the first update.
  if (publish_grace_period_ == std::chrono::milliseconds::zero() ||
      !is_fully_loaded_) {
    ld_check(!publish_timer_.isActive());
    updateLogsConfig(getStateMachine()->getState());
    return;
  }

  if (!publish_timer_.isActive()) {
    // Let's publish an initial version first before we activate the publish
    // timer.
    if (!publish_timer_.isAssigned()) {
      publish_timer_.assign([this] {
        // the tree may have been updated during grace_period, use
        // ReplicatedStateMachine::getState() to retrieve the current tree.
        updateLogsConfig(getStateMachine()->getState());
      });
    }
    publish_timer_.activate(publish_grace_period_);
  } else {
    // log that we are deferring the update because we have a grace period timer
    // set to publish_grace_perios_ value. rate limited PLEASE.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Not publishing immediately because a grace period is set "
                   "to %lums, deferring the LogsConfig update!",
                   publish_grace_period_.count());
  }
}

void LogsConfigManager::stop() {
  if (!is_running_) {
    return;
  }
  ld_info("Stopping LogsConfig Manager");
  config_updates_handle_.reset();
  cancelPublishTimer();
  state_machine_->stop();
  STAT_SET(getStats(), logsconfig_manager_started, 0);
  STAT_SET(getStats(), logsconfig_manager_tree_version, 0);
  is_running_ = false;
  is_fully_loaded_ = false;
}

void LogsConfigManagerRequest::postRequest(Status st,
                                           lsn_t config_version,
                                           std::string payload) {
  Worker* w = Worker::onThisThread();
  tracer_->setStatus(st);
  tracer_->setRequestType(request_header_.request_type);
  tracer_->setTreeVersion(config_version);
  auto sock_addr = Sender::sockaddrOrInvalid(to_);
  auto client_addr_str =
      sock_addr.valid() ? sock_addr.toStringNoPort() : std::string("UNKNOWN");
  tracer_->setClientAddress(std::move(client_addr_str));

  std::unique_ptr<Request> req =
      std::make_unique<LogsConfigManagerReply>(st,
                                               to_,
                                               config_version,
                                               payload,
                                               request_header_.client_rqid,
                                               respond_to_worker_,
                                               respond_to_worker_type_,
                                               request_header_.origin,
                                               std::move(tracer_));
  int rv = w->processor_->postRequest(req);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Cannot post a response to a LogsConfigManagerRequest: %s",
                    error_description(err));
  }
}

StatsHolder* FOLLY_NULLABLE LogsConfigManager::getStats() {
  if (Worker::onThisThread(false)) {
    return Worker::stats();
  } else {
    return nullptr;
  }
}

void LogsConfigManager::updateLogsConfig(
    const logsconfig::LogsConfigTree& tree) {
  auto server_config = updateable_config_->getServerConfig();
  std::shared_ptr<LocalLogsConfig> new_logs_config =
      std::make_shared<LocalLogsConfig>();
  // Setting the InternalLogs from ServerConfig
  new_logs_config->setInternalLogsConfig(
      server_config->getInternalLogsConfig());

  // We want the latest namespace delimiter to be set for this config.
  new_logs_config->setNamespaceDelimiter(
      server_config->getNamespaceDelimiter());

  // This is a fully loaded config, so it should be marked as one.
  new_logs_config->markAsFullyLoaded();
  auto clone_start_time = std::chrono::steady_clock::now();
  // Clone the LogsConfigTree to publish an immutable to
  // UpdateableLogsConfig while this tree object can be mutated by the
  // LogsConfigStateMachine
  new_logs_config->setLogsConfigTree(tree.copy());
  const int64_t clone_latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - clone_start_time)
          .count();
  updateable_config_->updateableLogsConfig()->update(new_logs_config);
  // updating the internal state
  is_fully_loaded_ = true;
  ld_info("Published new LogsConfig (fully loaded? %s) version (%lu) from "
          "LogsConfigManager",
          new_logs_config->isFullyLoaded() ? "yes" : "no",
          new_logs_config->getVersion());
  // increment the counter of number of published updates
  STAT_INCR(getStats(), logsconfig_manager_published_update);
  // Set the last published version to tree.version()
  STAT_SET(getStats(), logsconfig_manager_tree_version, tree.version());
  HISTOGRAM_ADD(
      Worker::stats(), logsconfig_manager_tree_clone_latency, clone_latency_us);
}

Request::Execution LogsConfigManagerRequest::execute() {
  Worker* w = Worker::onThisThread();
  std::shared_ptr<LocalLogsConfig> logs_config =
      w->getUpdateableConfig()->getLocalLogsConfig();

  if (!logs_config->isFullyLoaded()) {
    // We don't have a fully loaded config yet, we should not respond to any
    // request unless we load a config.
    postRequest(E::AGAIN);
    return Execution::COMPLETE;
  }

  STAT_INCR(w->stats(), logsconfig_api_requests_received);

  if (request_header_.request_type !=
      LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST) {
    // We don't need to hit the RSM with a delta unless we are asking for a
    // mutation
    // a get directory or get log group
    tracer_->setPath(blob_);
    return executeGetDirectoryOrLogGroup(
        logs_config->getLogsConfigTree(),
        w->getUpdateableConfig()->getServerConfig()->getMetaDataLogGroup());
  }
  LogsConfigManager* manager = w->getLogsConfigManager();
  if (!manager || !manager->isEnabledInSettings()) {
    // We can only accept mutation requests if LogsConfigManager is running on
    // this node. Otherwise, this means that this node is running the
    // LogsConfig of a file.
    STAT_INCR(w->stats(), logsconfig_api_requests_failed);
    postRequest(E::NOTSUPPORTED);
    return Execution::COMPLETE;
  }
  // the real stuff...
  // verify the payload
  std::string delimiter = w->getLogsConfig()->getNamespaceDelimiter();
  std::shared_ptr<Delta> delta =
      LogsConfigStateMachine::deserializeDelta(blob_, delimiter);
  if (delta == nullptr) {
    // we couldn't deserialize the delta, we fail and report to the user.
    STAT_INCR(w->stats(), logsconfig_api_requests_failed);
    postRequest(E::PROTO);
    return Execution::COMPLETE;
  }
  tracer_->setDeltaOpType(delta->type());
  if (delta->getPath()) {
    tracer_->setPath(*delta->getPath());
  }
  // add this internal request into the tracking map in Worker
  auto insert_result =
      Worker::onThisThread()->runningLogsConfigManagerRequests().map.insert(
          std::make_pair(id_, std::unique_ptr<LogsConfigManagerRequest>(this)));
  ld_check(insert_result.second);

  LogsConfigStateMachine* state_machine = manager->getStateMachine();
  ld_check(state_machine != nullptr);

  // Write delta callback
  auto cb = [=](Status st,
                lsn_t /* unused */,
                const std::string& failure_reason) {
    /*
     * We get the latest version from the tree directly, we don't use the
     * supplied version here.
     */

    std::string response_blob;
    const LogsConfigTree& tree = state_machine->getState();
    if (st == E::OK) {
      response_blob = calculateResponsePayload(*delta, tree);

      // Chunking does not support payloads bigger than uint32_t max
      if (response_blob.length() > std::numeric_limits<uint32_t>::max()) {
        STAT_INCR(w->stats(), logsconfig_api_response_toobig);
        STAT_INCR(w->stats(), logsconfig_api_requests_failed);
        postRequest(E::TOOBIG);
        deleteThis();
        return;
      }
    } else {
      response_blob = failure_reason;
    }
    STAT_INCR(Worker::stats(), logsconfig_api_requests_success);
    STAT_SET(
        w->stats(), logsconfig_api_reply_payload_size, response_blob.size());
    postRequest(st, tree.version(), response_blob);
    deleteThis();
  };
  state_machine->writeDelta(
      blob_, cb, LogsConfigStateMachine::WriteMode::CONFIRM_APPLIED);
  return Execution::CONTINUE;
}

void LogsConfigManagerRequest::deleteThis() {
  auto& rqmap = Worker::onThisThread()->runningLogsConfigManagerRequests().map;
  auto it = rqmap.find(id_);
  ld_check(it != rqmap.end());
  rqmap.erase(it); // destroys unique_ptr which owns this
}

std::string LogsConfigManagerRequest::serializeLogGroupInDirectory(
    const logsconfig::LogGroupWithParentPath& lid) {
  PayloadHolder payload =
      FBuffersLogsConfigCodec::serialize(lid, true /* flatten */);
  return payload.toString();
}

Request::Execution LogsConfigManagerRequest::executeGetDirectoryOrLogGroup(
    const LogsConfigTree& tree,
    const std::shared_ptr<LogsConfig::LogGroupNode>& metadata_log) {
  Worker* w = Worker::onThisThread();
  Status st = E::OK;
  std::string response_payload;

  switch (request_header_.request_type) {
    case LOGS_CONFIG_API_Header::Type::GET_DIRECTORY: {
      DirectoryNode* dir = tree.findDirectory(blob_);
      if (dir == nullptr) {
        st = E::NOTFOUND;
      } else {
        response_payload = LogsConfigStateMachine::serializeDirectory(*dir);
      }
      STAT_INCR(w->stats(), logsconfig_api_requests_success);
      break;
    }
    case LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_NAME: {
      std::shared_ptr<LogGroupNode> lg = tree.findLogGroup(blob_);
      if (lg == nullptr) {
        st = E::NOTFOUND;
      } else {
        response_payload = LogsConfigStateMachine::serializeLogGroup(*lg);
      }
      STAT_INCR(w->stats(), logsconfig_api_requests_success);
      break;
    }
    case LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID: {
      // For convenience, the log ID is passed as a string
      // The client sanitizes and generates these, so theoretically
      // string -> log id should never fail.
      logid_t log_id = logid_t(0);
      try {
        log_id = logid_t(std::stoull(blob_));
      } catch (const std::invalid_argument& e) {
        st = E::NOTFOUND;
        break;
      } catch (const std::out_of_range& e) {
        st = E::NOTFOUND;
        break;
      }
      if (log_id.val() == 0) {
        st = E::NOTFOUND;
      } else if (MetaDataLog::isMetaDataLog(log_id)) {
        // We do not check for the existence of the metadata log since
        // we might want to read metadata from deleted data logs
        LogGroupWithParentPath flat_lid{metadata_log, ""};
        response_payload = serializeLogGroupInDirectory(flat_lid);
      } else {
        const LogGroupInDirectory* lgid = tree.getLogGroupByID(log_id);
        if (lgid == nullptr) {
          st = E::NOTFOUND;
        } else {
          LogGroupWithParentPath flat_lid{
              lgid->log_group, lgid->parent->getFullyQualifiedName()};
          response_payload = serializeLogGroupInDirectory(flat_lid);
        }
        STAT_INCR(w->stats(), logsconfig_api_requests_success);
      }
      break;
    }
    default: {
      // We don't know what you are talking about!
      ld_error("Unsupported LOGS_CONFIG_API_Header::Type was supplied. "
               "The type passed was %i",
               static_cast<int>(request_header_.request_type));
      st = E::PROTO;
      STAT_INCR(w->stats(), logsconfig_api_requests_failed);
      break;
    }
  }
  STAT_SET(
      w->stats(), logsconfig_api_reply_payload_size, response_payload.size());
  postRequest(st, tree.version(), response_payload);
  return Execution::COMPLETE;
}

std::string LogsConfigManagerRequest::calculateResponsePayload(
    const Delta& delta,
    const LogsConfigTree& tree) const {
  if (delta.type() == DeltaOpType::MK_DIRECTORY) {
    const MkDirectoryDelta& mkdir =
        checked_downcast<const MkDirectoryDelta&>(delta);
    DirectoryNode* dir = tree.findDirectory(mkdir.path);
    if (dir == nullptr) {
      // This should never ever happened, this means that a successful creation
      // of directory resulted in a tree that doesn't have this directory.
      ld_error("Could not find directory '%s' in the tree after a successful "
               "insertion, this is fatal error!",
               mkdir.path.c_str());
      ld_check(false);
      return std::string();
    } else {
      return LogsConfigStateMachine::serializeDirectory(*dir);
    }
  } else if (delta.type() == DeltaOpType::MK_LOG_GROUP) {
    const MkLogGroupDelta& mk_log_group =
        checked_downcast<const MkLogGroupDelta&>(delta);
    std::shared_ptr<LogGroupNode> lg = tree.findLogGroup(mk_log_group.path);
    return LogsConfigStateMachine::serializeLogGroup(*lg);
  } else {
    // We don't need a payload in response of any other delta type.
    // setAttributes, etc. are either (fail/success). We don't need to prepare
    // a payload response for these.
    return std::string();
  }
}

LogsConfigManagerReply::LogsConfigManagerReply(
    Status st,
    const Address& to,
    uint64_t config_version,
    std::string response_blob,
    request_id_t client_rqid,
    int worker,
    WorkerType worker_type,
    LogsConfigRequestOrigin origin,
    std::unique_ptr<LogsConfigApiTracer> tracer)
    : Request(RequestType::LOGS_CONFIG_MANAGER_REPLY),
      status_(st),
      to_(to),
      config_version_(config_version),
      response_blob_(std::move(response_blob)),
      client_rqid_(client_rqid),
      worker_(worker),
      worker_type_(worker_type),
      tracer_(std::move(tracer)),
      response_offset_(0),
      origin_(origin) {}

LogsConfigManagerReply::SendResult
LogsConfigManagerReply::sendChunks(Worker* w) {
  // Chunks size, based on payload, length prefix, header
  // Living on the edge: maximum payload size we can get in one message
  const size_t CHUNK_SIZE = Message::MAX_LEN -
      LOGS_CONFIG_API_REPLY_Message::HEADER_SIZE -
      sizeof(LOGS_CONFIG_API_REPLY_Message::blob_size_t);

  size_t sent = 0;
  size_t payload_size = response_blob_.length();
  do {
    uint32_t chunk_size = std::min(payload_size - response_offset_, CHUNK_SIZE);

    // We use 0 as payload size if this is the 2nd fragment or more
    // Initial message contains the full payload length
    LOGS_CONFIG_API_REPLY_Header hdr{.client_rqid = client_rqid_,
                                     .config_version = config_version_,
                                     .status = status_,
                                     .total_payload_size = response_offset_ == 0
                                         ? static_cast<uint32_t>(payload_size)
                                         : 0,
                                     .origin = origin_};

    ld_debug("Chunked reply: trying to send offset %zu size %zu",
             response_offset_,
             payload_size);

    auto msg = std::make_unique<LOGS_CONFIG_API_REPLY_Message>(
        hdr, response_blob_.substr(response_offset_, chunk_size));
    int rv = w->sender().sendMessage(std::move(msg), to_);

    if (rv != 0) {
      if (err == E::NOBUFS) {
        ld_debug("Socket buffer full");

        // we should continue later
        return sent == 0 ? LogsConfigManagerReply::SendResult::SENT_NONE
                         : LogsConfigManagerReply::SendResult::SENT_SOME;
      } else {
        // we no longer want to work on this reply
        ld_error("Failed to sendMessage: %s", error_name(err));
        return LogsConfigManagerReply::SendResult::ERROR;
      }
    }

    sent++;
    response_offset_ += chunk_size;
  } while (payload_size > response_offset_);
  tracer_->setChunkCount(sent);
  tracer_->setResponseSizeBytes(payload_size);
  tracer_->trace();
  return LogsConfigManagerReply::SendResult::SENT_ALL; // we have sent all data
}

Request::Execution LogsConfigManagerReply::execute() {
  Worker* w = Worker::onThisThread();

  size_t payload_size = response_blob_.length();

  // Should have checked before posting the reply
  ld_check(payload_size <= std::numeric_limits<uint32_t>::max());

  auto send_result = sendChunks(w);
  if (send_result == LogsConfigManagerReply::SendResult::SENT_SOME ||
      send_result == LogsConfigManagerReply::SendResult::SENT_NONE) {
    ld_debug("Failed to put all chunks on the socket. Retrying later.");
    // We want to keep track of the requests since we'll be retrying later
    auto insert_result = w->runningLogsConfigManagerReplies().map.insert(
        std::make_pair(id_, std::unique_ptr<LogsConfigManagerReply>(this)));
    ld_check(insert_result.second);

    retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

        std::bind(&LogsConfigManagerReply::onRetry, this),
        std::chrono::milliseconds(10),
        std::chrono::milliseconds(100));

    retry_timer_->activate();
    return Execution::CONTINUE;
  } else {
    return Execution::COMPLETE;
  }
}

void LogsConfigManagerReply::onRetry() {
  retry_timer_->cancel();
  Worker* w = Worker::onThisThread();
  auto send_result = sendChunks(w);
  switch (send_result) {
    case LogsConfigManagerReply::SendResult::SENT_SOME:
      retry_timer_->reset(); // reset timer if we were able to send
      FOLLY_FALLTHROUGH;
    case LogsConfigManagerReply::SendResult::SENT_NONE:
      ld_debug("More chunked data to be sent. Restarting timer.");
      retry_timer_->activate();
      break;

    case LogsConfigManagerReply::SendResult::ERROR:
    case LogsConfigManagerReply::SendResult::SENT_ALL:
      // We are responsible for cleaning up after ourselves
      auto& rmap = w->runningLogsConfigManagerReplies().map;
      auto it = rmap.find(id_);
      ld_check(it != rmap.end());
      rmap.erase(it);
      break;
  }
}

}} // namespace facebook::logdevice
