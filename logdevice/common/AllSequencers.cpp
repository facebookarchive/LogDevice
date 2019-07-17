/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AllSequencers.h"

#include <chrono>
#include <memory>
#include <thread>

#include <folly/Memory.h>

#include "logdevice/common/AppenderBuffer.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RecipientSet.h"
#include "logdevice/common/SequencerBackgroundActivator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

namespace {
class CheckMetadataLogEmptyRequest : public FireAndForgetRequest {
 public:
  using Callback = std::function<void(Status, logid_t)>;
  CheckMetadataLogEmptyRequest(logid_t log_id, Callback cb)
      : FireAndForgetRequest(RequestType::CHECK_METADATA_LOG),
        cb_(std::move(cb)),
        log_id_(log_id) {}

  void executionBody() override {
    nodeset_finder_ = std::make_unique<NodeSetFinder>(
        log_id_,
        Worker::onThisThread()->settings().check_metadata_log_empty_timeout,
        [this](Status st) {
          cb_(st, log_id_);
          // destroy the request
          destroy();
        },
        NodeSetFinder::Source::METADATA_LOG);
    nodeset_finder_->checkMetadataLogEmptyMode();
    nodeset_finder_->start();
  }

 private:
  Callback cb_;
  std::unique_ptr<NodeSetFinder> nodeset_finder_;
  logid_t log_id_;
};
} // namespace

AllSequencers::AllSequencers(
    Processor* processor,
    const std::shared_ptr<UpdateableConfig>& updateable_config,
    UpdateableSettings<Settings> settings)
    : updateable_config_(updateable_config),
      settings_(settings),
      processor_(processor) {
  ld_check(settings_.get());

  if (updateable_config_) { // might not be set if not running sequencers
    server_config_subscription_ =
        updateable_config_->updateableServerConfig()->subscribeToUpdates(
            std::bind(&AllSequencers::noteConfigurationChanged, this));

    logs_config_subscription_ =
        updateable_config_->updateableLogsConfig()->subscribeToUpdates(
            std::bind(&AllSequencers::noteConfigurationChanged, this));

    nodes_configuration_subscription_ =
        updateable_config_->updateableNodesConfiguration()->subscribeToUpdates(
            std::bind(&AllSequencers::noteConfigurationChanged, this));
  }
}

std::shared_ptr<Sequencer> AllSequencers::findSequencer(logid_t logid) {
  if (MetaDataLog::isMetaDataLog(logid)) {
    return getMetaDataLogSequencer(logid);
  }

  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  // data log id
  auto it = map_.find(logid.val_);
  if (it == map_.end()) {
    err = E::NOSEQUENCER;
    return nullptr;
  }
  return it->second;
}

int AllSequencers::activateSequencer(
    logid_t logid,
    const std::string& reason,
    Sequencer::ActivationPred pred,
    folly::Optional<epoch_t> acceptable_activation_epoch,
    bool check_metadata_log_before_provisioning,
    std::shared_ptr<EpochMetaData> new_metadata) {
  // sequencer that we end up activating. This may be one in the map,
  // or one that we create.
  std::shared_ptr<Sequencer> seq;

  ld_check(logid != LOGID_INVALID);
  ld_check(updateable_config_);
  // metadata logs are never activated using epoch store
  ld_check(!MetaDataLog::isMetaDataLog(logid));

  std::shared_ptr<Configuration> cfg = updateable_config_->get();
  const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
      cfg->getLogGroupByIDShared(logid);
  if (!logcfg) {
    err = E::NOTFOUND;
    return -1;
  }

  // if node is isolated we should't activate a sequencer
  if (processor_->isNodeIsolated()) {
    err = E::ISOLATED;
    return -1;
  }

  folly::SharedMutex::UpgradeHolder map_lock(map_mutex_);
  auto it = map_.find(logid.val_);
  if (it != map_.end()) {
    // Already have a Sequencer for this log, check state. In order to
    // avoid shutdown crashes all threads that may run activateSequencer()
    // must stop before this AllSequencers object (a subobject of Processor)
    // is destroyed on shutdown.
    seq = it->second;
    map_lock.unlock();
  } else {
    // no Sequencer for logid in the map, create one and insert it in the map.
    // We shouldn't race with another thread that is also running
    // activateSequencer() since we there can only be one thread in upgrade mode
    // of the shared lock
    seq = createSequencer(logid, settings_);
    folly::SharedMutex::WriteHolder map_write_lock(std::move(map_lock));
    auto insertion_result = map_.insert(std::make_pair(logid.val(), seq));

    ld_check(insertion_result.second);
  }

  ld_check(seq);

  return seq->startActivation(
      // metadata function
      [this,
       reason,
       acceptable_activation_epoch,
       cfg,
       check_metadata_log_before_provisioning,
       new_metadata](logid_t data_logid) -> int {
        return getEpochMetaData(data_logid,
                                reason,
                                cfg,
                                updateable_config_->getNodesConfiguration(),
                                acceptable_activation_epoch,
                                check_metadata_log_before_provisioning,
                                new_metadata);
      },
      pred);
}

int AllSequencers::getEpochMetaData(
    logid_t logid,
    const std::string& activation_reason,
    std::shared_ptr<Configuration> cfg,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    folly::Optional<epoch_t> acceptable_activation_epoch,
    bool check_metadata_log_before_provisioning,
    std::shared_ptr<EpochMetaData> new_metadata) {
  ld_check(epoch_store_);
  MetaDataTracer tracer(processor_->getTraceLogger(),
                        logid,
                        MetaDataTracer::Action::SEQUENCER_ACTIVATION);

  auto cb = [this, activation_reason](
                Status st,
                logid_t _logid,
                std::unique_ptr<EpochMetaData> info,
                std::unique_ptr<EpochStoreMetaProperties> meta_properties) {
    onEpochMetaDataFromEpochStore(st,
                                  _logid,
                                  activation_reason,
                                  std::move(info),
                                  std::move(meta_properties));
  };

  // To verify metadata log being empty before provisioning the log, simply
  // prevent provisioning the log in the epoch store here. It will be triggered
  // without the flag later if the check is successful.
  // check_metadata_log_before_provisioning will only be false if we're
  // activating sequencers on startup, since it's not possible to read the
  // metadata log at that point.
  int rv = epoch_store_->createOrUpdateMetaData(
      logid,
      std::make_shared<EpochMetaDataUpdateToNextEpoch>(
          cfg,
          nodes_configuration,
          new_metadata,
          acceptable_activation_epoch,
          settings_->epoch_metadata_use_new_storage_set_format,
          /*provision_if_empty=*/!check_metadata_log_before_provisioning),
      cb,
      std::move(tracer),
      EpochStore::WriteNodeID::MY);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to request next epoch "
                    "number for log %lu from epoch store '%s': %s",
                    logid.val_,
                    epoch_store_->identify().c_str(),
                    error_description(err));
    ld_check(err != E::INVALID_PARAM);

    // error for getting epoch metadata from epoch store.
    // Sequencer::startActivation() will put the sequencer back to the original
    // state and activation will be attempted again by subsequent APPENDs or
    // GET_SEQ_STATEs.

    // Note that there may be Appenders buffered _after_ the sequencer was set
    // to ACTIVATING state. We do not drain them here, but rely on the future
    // activations to drain them in their completion callbacks.
    err = E::AGAIN;
    return -1;
  }

  return 0;
}

int AllSequencers::activateSequencerIfNotActive(
    logid_t logid,
    const std::string& reason,
    bool check_metadata_log_before_provisioning) {
  int rv =
      activateSequencer(logid,
                        reason,
                        [](const Sequencer& seq) {
                          return seq.getState() != Sequencer::State::ACTIVE;
                        },
                        folly::none,
                        check_metadata_log_before_provisioning);
  if (rv != 0 && err == E::ABORTED) {
    err = E::EXISTS;
  }
  return rv;
}

int AllSequencers::reactivateIf(logid_t logid,
                                const std::string& reason,
                                Sequencer::ActivationPred pred,
                                bool only_consecutive_epoch) {
  ld_check(!MetaDataLog::isMetaDataLog(logid));
  std::shared_ptr<Sequencer> seq = findSequencer(logid);

  if (!seq) {
    ld_check(err == E::NOSEQUENCER);
    return -1;
  }

  if (seq->getState() == Sequencer::State::ACTIVATING) {
    // Sequencer is being reactivated, no need to activate again
    return 0;
  }

  folly::Optional<epoch_t> acceptable_activation_epoch;
  if (only_consecutive_epoch) {
    epoch_t current_epoch = seq->getCurrentEpoch();
    // this check can fail if the sequencer was never initialized
    if (current_epoch != EPOCH_INVALID) {
      acceptable_activation_epoch = epoch_t(current_epoch.val() + 1);
    }
  }

  return activateSequencer(
      logid, reason, std::move(pred), acceptable_activation_epoch);
}

int AllSequencers::reactivateSequencer(logid_t logid,
                                       const std::string& reason) {
  // reactivate unconditionally
  return reactivateIf(logid, reason, [](const Sequencer&) { return true; });
}

int AllSequencers::activateAllSequencers(std::chrono::milliseconds timeout,
                                         const std::string& reason) {
  using std::chrono::steady_clock;

  std::shared_ptr<Configuration> cfg = updateable_config_->get();
  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      cfg->localLogsConfig();
  configuration::LocalLogsConfig::Iterator it;
  int n_logs = 0; // total number of logs in config

  for (it = logs_config->logsBegin(); it != logs_config->logsEnd(); ++it) {
    logid_t logid(it->first);
    n_logs++;

    // Can't verify with metadata log since this is done before listeners are
    // started; allow provisioning log to epoch store if it's found empty
    int rv = activateSequencerIfNotActive(
        logid, reason, /*check_metadata_log_before_provisioning=*/false);
    if (rv != 0) {
      switch (err) {
        case E::EXISTS:
        case E::INPROGRESS:
        case E::SYSLIMIT:
          ld_error("A sequencer for log %lu already exists.", logid.val_);
          return -1;
        case E::FAILED:
          ld_error("Could not activate a sequencer for log %lu because an "
                   "epoch store request failed.",
                   logid.val_);
          return -1;
        case E::NOBUFS:
          ld_error("Failed to activate a sequencer for log %lu because maximum "
                   "number of sequencers has been reached",
                   logid.val_);
          return -1;
        case E::ISOLATED:
          ld_error("Node is isolated, sequencer activation is suspended");
          return -1;
        default:
          ld_error(
              "Unexpected error %s from AllSequencers::activateSequencer() "
              "for log %lu",
              error_name(err),
              logid.val_);
          ld_check(false);
          err = E::INTERNAL;
          return -1;
      }
    } else {
      ld_debug("Activating a sequencer for log %lu", logid.val_);
    }
  }

  steady_clock::time_point tstart = steady_clock::now();
  const std::chrono::milliseconds interval(100);

  for (;;) {
    int n_initialized = 0;
    for (it = logs_config->logsBegin(); it != logs_config->logsEnd(); ++it) {
      logid_t logid(it->first);
      std::shared_ptr<Sequencer> seq = findSequencer(logid);
      ld_check(seq); // must have a sequencer because all got activated above
      if (seq->getState() != Sequencer::State::UNAVAILABLE &&
          seq->getState() != Sequencer::State::ACTIVATING) {
        n_initialized++;
      }
    }
    if (n_initialized == n_logs) {
      break;
    }
    if (steady_clock::now() - tstart >= timeout) {
      err = E::TIMEDOUT;
      return -1;
    }
    /* sleep override */
    std::this_thread::sleep_for(interval);
  }

  return 0;
}

using ActivationCompletionRequest =
    logdevice::CompletionRequestBase<std::function, logid_t>;

void AllSequencers::notifyWorkerActivationCompletion(logid_t logid, Status st) {
  Worker* worker = Worker::onThisThread();

  auto completion_callback = [](Status c_st, logid_t c_logid) {
    Worker* w = Worker::onThisThread();
    if (c_st == E::OK) {
      ld_debug("Received requests of clearing buffer.");
      w->appenderBuffer().processQueue(
          c_logid, AppenderBuffer::processBufferedAppender);

      if (!MetaDataLog::isMetaDataLog(c_logid)) {
        w->appenderBuffer().processQueue(
            MetaDataLog::metaDataLogID(c_logid),
            AppenderBuffer::processBufferedAppender);
      }
    } else {
      // Sequencer activation failed becaused of an error from epoch store or
      // the log is no longer in config or there is a permanent error with
      // the Sequencer, clear any pending Appender objects on the sequencer's
      // queue for this logid by sending appropriate error.
      c_st = E::NOSEQUENCER;
      w->appenderBuffer().bufferedAppenderSendError(c_logid, c_st);
      if (!MetaDataLog::isMetaDataLog(c_logid)) {
        w->appenderBuffer().bufferedAppenderSendError(
            MetaDataLog::metaDataLogID(c_logid), c_st);
      }
    }
  };

  // Post an ActivationCompletionRequest to all workers
  for (worker_id_t worker_idx{0}; worker_idx.val_ <
       worker->processor_->getWorkerCount(WorkerType::GENERAL);
       ++worker_idx.val_) {
    std::unique_ptr<Request> rq = std::make_unique<ActivationCompletionRequest>(
        completion_callback, worker_idx, st, logid);

    int rv = worker->processor_->postWithRetrying(rq);
    if (rv != 0 && err != E::SHUTDOWN) {
      ld_error("Got unexpected err %s for Processor::postWithRetrying() "
               "with log %lu",
               error_name(err),
               logid.val_);
      ld_check(false);
    }
  }

  // Notify SequencerBackgroundActivator.
  SequencerBackgroundActivator::requestNotifyCompletion(
      worker->processor_, logid, st);
}

void AllSequencers::onEpochMetaDataFromEpochStore(
    Status st,
    logid_t logid,
    const std::string& activation_reason,
    std::unique_ptr<EpochMetaData> info,
    std::unique_ptr<EpochStoreMetaProperties> meta_props) {
  if (info && st == E::OK) {
    // `info` is the EpochMetaData that was written to epoch store.
    // `info->epoch` is the _next_ epoch to be assigned.
    // The epoch of this newly activated sequencer is less by one.
    // Let's decrement it.
    // (EpochMetaDataUpdateToNextEpoch made sure that it's at least 2.)
    ld_check(info->h.epoch > EPOCH_MIN);
    --info->h.epoch.val_;
  }

  std::shared_ptr<Configuration> cfg = updateable_config_->get();
  ld_check(cfg != nullptr);

  auto settings = settings_.get();
  ld_check(settings != nullptr);

  std::shared_ptr<Sequencer> seq = findSequencer(logid);
  // Sequencers are never removed from Processor.allSequencers() and this
  // function can only be called if a Sequencer for logid was once in the map
  ld_check(seq);

  epoch_t epoch = info ? info->h.epoch : EPOCH_INVALID;
  bool permanent = false; // on failure, whether it is permanent
  ActivateResult result = ActivateResult::FAILED;
  std::string metadata_str;

  STAT_INCR(getStats(), sequencer_activations);
  switch (st) {
    case E::OK:
      ld_check(info != nullptr);
      ld_check(info->isValid());
      metadata_str = info->toString();

      // It is possible that the config on the sequencer node is stale and
      // not consistent with metadata got from the epochstore. In such case,
      // instead of failing the activation and leaving the sequencer in
      // transient error state, it proceeds with activation with a critial error
      // message. It's likely that the sequencer will not be able to generate
      // copyset with the metadataa. The reason why we still activate the
      // sequencer is to prevent incoming appends from keep reactivating the
      // sequencer with still invalid metadata.
      //
      // The situation will get resolved once the sequencer gets an updated
      // configuration or updated epoch metadata from epoch store.
      //
      // TODO: handle this better. possibly changing activation sequence so that
      //       sequencer can continue writing to the old epoch in such case?
      if (!info->validWithConfig(
              logid, cfg, updateable_config_->getNodesConfiguration())) {
        ld_critical(
            "Activating sequencer for log %lu. However, metadata got "
            "from the epochstore is not compatible with the current "
            "configuration. MetaData: %s. The sequencer may not be able "
            "to perform writes!",
            logid.val_,
            metadata_str.c_str());

        STAT_INCR(getStats(), sequencer_activations_incompatible_metadata);
        // still continue the activation
      }

      // update the sequencer with metadata fetched from the epoch store:
      // 1) epoch; 2) nodeset; 3) replication property;
      result = seq->completeActivationWithMetaData(epoch, cfg, std::move(info));
      if (result != ActivateResult::FAILED) {
        ld_info("Activated a sequencer for log %lu (reason: %s) with epoch %u, "
                "metadata: %s. Activation Result: %s.",
                logid.val_,
                activation_reason.c_str(),
                epoch.val_,
                metadata_str.c_str(),
                Sequencer::activateResultToString(result));
      }

      finalizeActivation(result, seq.get(), epoch, settings->bypass_recovery);
      return;

    case E::NOTFOUND:
    case E::EMPTY:
      if (!cfg->serverConfig()->sequencersProvisionEpochStore()) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            100,
            "Attempt to activate a sequencer for log %lu (reason: %s) failed "
            "because that log id is not provisioned%s in the epoch store.",
            logid.val_,
            activation_reason.c_str(),
            st == E::EMPTY ? " (empty metadata)" : "");
        break;
      }

      // Epoch store is empty for this log. This means one of two things:
      //  1. No sequencer was ever activated for this log.
      //     In this case we should just activate sequencer in epoch 1.
      //  2. (unlikely) Epoch store was wiped or switched by mistake. In this
      //     case it's important to *not* activate the sequencer. Storage nodes
      //     likely already have records for this log, and writing new records
      //     in epoch 1 would lead to LSN collisions and other forms of data
      //     corruption.
      // To distinguish the two cases, we check whether metadata log is
      // empty. It's not a perfect test (some data records may be stored before
      // metadata record is appended), but plenty good enough for our purposes.
      // If metadata log is empty, we'll proceed with sequencer activation in
      // epoch 1.
      startMetadataLogEmptyCheck(logid, activation_reason);
      return;

    case E::ACCESS:
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          1,
          "Attempt to activate a sequencer for log %lu (reason: %s) failed "
          "because epoch store denied access",
          logid.val_,
          activation_reason.c_str());
      break;

    case E::CONNFAILED:
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Attempt to activate a sequencer for log %lu (reason: %s) failed "
          "because connection to epoch store was lost or "
          "timeout expired",
          logid.val_,
          activation_reason.c_str());
      break;

    case E::SHUTDOWN:
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Attempt to activate a sequencer for log %lu (reason: %s) failed "
          "because connection to epoch store was closed",
          logid.val_,
          activation_reason.c_str());
      break;

    case E::AGAIN:
      ld_warning("Attempt to activate a sequencer for log %lu (reason: %s) "
                 "failed because "
                 "some other logdeviced simultaneously tried to increment "
                 "epoch for that log and we lost the race.",
                 logid.val_,
                 activation_reason.c_str());
      break;

    case E::BADMSG:
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          100,
          "Epoch store record for log %lu is corrupted. Log cannot be "
          "used until the record is fixed.",
          logid.val_);
      break;

    case E::DISABLED:
      // the epoch metadata is marked as disabled. This happens when the log
      // is considered not to exist but not yet removed from the config
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      100,
                      "Cannot activate sequencer for log %lu (reason: %s): "
                      "metadata received from "
                      "the epoch store indicates that the log is disabled, run "
                      "metadata-utility provision again!",
                      logid.val_,
                      activation_reason.c_str());
      break;

    case E::EXISTS:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      100,
                      "Epoch store entry for log %lu already exists.",
                      logid.val_);
      break;

    case E::ABORTED:
      ld_check(info != nullptr);
      notePreemption(logid,
                     info->h.epoch,
                     meta_props.get(),
                     seq.get(),
                     "reactivating sequencer");
      break;
    case E::INTERNAL:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Internal error in epoch store interface while attempting"
                      " to activate a sequencer for log %lu",
                      logid.val_);
      permanent = true;
      break;

    case E::TOOBIG:
      ld_critical("Log %lu can no longer be written to because epoch numbers "
                  "for that log have been exhausted.",
                  logid.val_);
      permanent = true;
      break;

    case E::FAILED:
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Nodeset selector was unable to generate nodeset for "
                         "log %lu or the epoch store content is invalid. or if "
                         "there is an internal error with the epoch store!",
                         logid.val_);
      break;

    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Unexpected status code %s in EpochStore::nextEpoch() "
                      "completion function for log %lu.",
                      error_name(st),
                      logid.val_);
      ld_check(false);
  }

  onActivationFailed(logid, st, seq.get(), permanent);
}

void AllSequencers::notePreemption(logid_t logid,
                                   epoch_t preemption_epoch,
                                   const EpochStore::MetaProperties* meta_props,
                                   Sequencer* seq,
                                   const char* context) {
  ld_check(seq != nullptr);
  if (!meta_props || !meta_props->last_writer_node_id.hasValue()) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "Epoch store has higher epoch than our current sequencer for log %lu, "
        "but doesn't have new sequencer's node ID. Ignoring the preemption. "
        "Context: %s",
        logid.val(),
        context);
    return;
  }
  if (meta_props->last_writer_node_id.value() == processor_->getMyNodeID()) {
    // Sequencer was reactivated on this node.
    return;
  }

  seq->notePreempted(preemption_epoch, meta_props->last_writer_node_id.value());

  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "Preempting for log %lu with preemption epoch %u "
                 "after detecting a "
                 "newer sequencer exists from the epoch store. Context: %s",
                 logid.val(),
                 preemption_epoch.val(),
                 context);
}

void AllSequencers::onMetadataLogEmptyCheckResult(
    Status st,
    logid_t logid,
    const std::string& activation_reason) {
  switch (st) {
    case E::NOTFOUND:
      // We already knew that the epoch store was empty for this log; now we
      // know that the metadata log is, too. Make a new attempt to increment
      // the log's epoch in the epoch store, this time allowing the provision
      // of an empty log.
      {
        int rv =
            getEpochMetaData(logid,
                             activation_reason,
                             updateable_config_->get(),
                             updateable_config_->getNodesConfiguration(),
                             epoch_t(1),
                             /*check_metadata_log_before_provisioning=*/false);
        if (rv == 0) {
          return;
        }
      }
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Sequencer activation for log %lu found both epoch "
                      "store and metadata log to be empty, but subsequent "
                      "update to epoch store failed with error %s",
                      logid.val_,
                      error_name(st));
      break;
    case E::NOTEMPTY:
      // Epoch store is empty for this log, but metadata log is not!
      // Probable cause: corruption or accidental change of epoch store
      STAT_INCR(getStats(), sequencer_activation_failed_metadata_inconsistency);
      st = E::AGAIN;

      // We saw empty epoch store, then we saw nonempty metadata log.
      // This means one of two rare situations:
      //  1. Benign race condition: another node activated a sequencer after
      //     we checked epoch store but before we checked metadata log.
      //  2. Bad misconfiguration or bug: the epoch store was wiped or switched.
      // In both cases we shouldn't activate the sequencer. In the seconds case
      // we should also raise an alarm. To distinguish the two cases, let's
      // re-check the epoch store; if it's still empty, we're in situation 2.

      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Sequencer activation for log %lu found epoch store "
                     "to be empty, but metadata log is not. Probably the "
                     "sequencer was activated on a different node. "
                     "Canceling our sequencer activation.",
                     logid.val_);

      startEpochStoreNonemptyCheck(logid);
      break;
    case E::INVALID_PARAM:
      // Log not yet in config; probably a race between config update and
      // sequencer activation
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Sequencer activation failed to read metadata log as "
                        "log %lu not in config",
                        logid.val_);
      st = E::AGAIN;
      break;
    case E::TIMEDOUT:
      // Failed to read metadata log within the time limit
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Sequencer activation for log %lu timed out reading "
                        "metadata log",
                        logid.val_);
      STAT_INCR(getStats(), sequencer_metadata_log_check_timeouts);
      st = E::AGAIN;
      break;
    case E::ACCESS:
      // Permission denied
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Sequencer activation for log %lu with empty epoch "
                        "store failed; denied access to read metadata log",
                        logid.val_);
      break;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Sequencer activation unexpectedly got error %s on "
                      "attempt to read metadata log",
                      error_name(st));
      ld_check(false);
      // Try to activate again in hopes that it'll clear
      st = E::AGAIN;
  }

  std::shared_ptr<Sequencer> seq = findSequencer(logid);
  // Sequencers are never removed from Processor.allSequencers() and this
  // function can only be called if a Sequencer for logid was once in the map
  ld_check(seq);
  onActivationFailed(logid, st, seq.get(), /*permanent=*/false);
}

void AllSequencers::startEpochStoreNonemptyCheck(logid_t logid) {
  // Do a fire-and-forget request to epoch store. Callback captures everything
  // by value so that we don't need to worry about `this` getting destroyed
  // before the request completes.
  auto cb = [](Status st,
               logid_t _logid,
               std::unique_ptr<EpochMetaData>,
               std::unique_ptr<EpochStoreMetaProperties>) {
    if (st == E::NOTFOUND || st == E::EMPTY) {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          100,
          "Sequencer activation for log %lu found epoch store to be empty, but "
          "metadata log is NOT! Looks like epoch store was unexpectedly wiped. "
          "Can't activate sequencers for this log since we don't know the "
          "epoch number.",
          _logid.val());
    } else if (st != E::DISABLED && st != E::OK) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Failed to check if epoch store for log %lu is empty: %s",
                      _logid.val(),
                      error_name(st));
    } else {
      // The mismatch between epoch store and metadata log was transient, phew.
    }
  };

  int rv = epoch_store_->readMetaData(logid, cb);
  if (rv != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Failed to initiate a check if epoch store for log %lu is empty: %s.",
        logid.val(),
        error_name(err));
  }
}

void AllSequencers::onActivationFailed(logid_t logid,
                                       Status st,
                                       Sequencer* seq,
                                       bool permanent) {
  if (permanent) {
    seq->onPermanentError();
  } else {
    // notifyWorkerActivationCompletion() below will make sure on transient
    // error for config-change activations it enqueues a background reacitvation
    // to schedule the activation later
    seq->onActivationFailed();
  }

  // Sequencer encountered an activation error, notify the workers to clear its
  // buffered appenders
  notifyWorkerActivationCompletion(logid, st);
  STAT_INCR(getStats(), sequencer_activation_failures);
}

void AllSequencers::startMetadataLogEmptyCheck(
    logid_t logid,
    const std::string& activation_reason) {
  std::unique_ptr<Request> rq = std::make_unique<CheckMetadataLogEmptyRequest>(
      logid, [this, activation_reason](Status st, logid_t _logid) {
        onMetadataLogEmptyCheckResult(st, _logid, activation_reason);
      });
  processor_->postImportant(rq);
}

void AllSequencers::finalizeActivation(ActivateResult result,
                                       Sequencer* seq,
                                       epoch_t epoch,
                                       bool bypass_recovery) {
  ld_check(seq != nullptr);
  const logid_t logid = seq->getLogID();
  bool success = false;
  switch (result) {
    case ActivateResult::RECOVERY:
      if (bypass_recovery) {
        ld_warning("Bypassing recovery of log %lu next_epoch %u according "
                   "to test options.",
                   logid.val_,
                   epoch.val_);
      } else {
        int rv = seq->startRecovery();
        STAT_INCR(getStats(), recovery_scheduled);
        if (rv != 0) {
          ld_error("Failed to start log recovery for log %lu: %s",
                   logid.val_,
                   error_description(err));

          // currently this only happens on SHUTDOWN, do nothing here
          ld_check(err == E::SHUTDOWN);
          return;
        }
      }
      success = true;
      break;
    case ActivateResult::GRACEFUL_DRAINING:
      // graceful reactivation is in progress, its completion will be deferred
      // until the last appender of the previous epoch is reaped (i.e., epoch is
      // drained)
      STAT_INCR(getStats(), graceful_reactivation_result_deferred);
      ld_debug("Completion of graceful reactivation to epoch %u is deferred "
               "for log %lu, waiting for appenders in the old epoch to be "
               "reaped",
               epoch.val_,
               logid.val_);
      success = true;
      break;
    case ActivateResult::FAILED:
      success = false;
      break;
  }

  if (success) {
    // this Sequencer is reactivated, inform each worker to process
    // pending Appenders in their buffer
    notifyWorkerActivationCompletion(logid, E::OK);
    notifyMetaDataLogWriterOnActivation(seq, epoch, bypass_recovery);
  }
}

void AllSequencers::notifyMetaDataLogWriterOnActivation(Sequencer* seq,
                                                        epoch_t epoch,
                                                        bool bypass_recovery) {
  ld_check(seq != nullptr);
  const logid_t logid = seq->getLogID();
  auto meta_writer = seq->getMetaDataLogWriter();
  // must be a data log sequencer
  ld_check(meta_writer);
  meta_writer->onDataSequencerReactivated(epoch);

  if (!Worker::onThisThread()->appenderBuffer().hasBufferedAppenders(
          MetaDataLog::metaDataLogID(logid))) {
    /**
     * Run recovery for metadata log in order to:
     * (1) recover the last released lsn for the metadata log; and
     * (2) make sure data log recovery can eventually read the sequencer
     *     metadata from the metadata log
     *
     * Note that we do not need to recover the metadata log if there are
     * already buffered appenders for the metadata log because of the
     * sequencer activation. These buffered appenders will be process later
     * and WriteMetaDataRecord state machine will take care of the recovery
     * for the metadata log.
     */
    if (!bypass_recovery) {
      meta_writer->recoverMetaDataLog();
    }
  }
}

void AllSequencers::noteConfigurationChanged() {
  if (!processor_->hasMyNodeID()) {
    // not a server node
    ld_check(map_.empty());
    return;
  }
  std::vector<logid_t> log_ids;
  {
    folly::stop_watch<std::chrono::milliseconds> watch;
    folly::SharedMutex::ReadHolder map_lock(map_mutex_);
    uint64_t lock_ms = watch.lap().count();
    for (auto const& x : map_) {
      log_ids.push_back(logid_t(x.first));
    }
    ld_info("Acquiring lock for sequencer map took %lums", lock_ms);
  }
  if (!log_ids.empty()) {
    SequencerBackgroundActivator::requestSchedule(
        processor_, std::move(log_ids));
  }
}

void AllSequencers::shutdown() {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  for (auto const& x : map_) {
    x.second->shutdown();
  }
}

AllSequencers::Accessor::Accessor(AllSequencers* owner)
    : owner_(owner), map_lock_(owner_->map_mutex_) {}

AllSequencers::Accessor AllSequencers::accessAll() {
  return Accessor(this);
}
AllSequencers::Accessor::Iterator AllSequencers::Accessor::begin() {
  return Iterator(owner_->map_.begin());
}
AllSequencers::Accessor::Iterator AllSequencers::Accessor::end() {
  return Iterator(owner_->map_.end());
}

std::vector<std::shared_ptr<Sequencer>> AllSequencers::getAll() {
  std::vector<std::shared_ptr<Sequencer>> out;
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  for (auto& p : map_) {
    out.push_back(p.second);
  }
  return out;
}

std::shared_ptr<Sequencer>
AllSequencers::createSequencer(logid_t logid,
                               UpdateableSettings<Settings> settings) {
  return std::make_shared<Sequencer>(logid, settings, getStats(), this);
}

StatsHolder* AllSequencers::getStats() const {
  return processor_->stats_;
}

std::shared_ptr<Sequencer>
AllSequencers::getMetaDataLogSequencer(logid_t datalog_id) {
  const Worker* worker = Worker::onThisThread(false /* enforce_worker */);
  if (!worker) {
    // may happen in tests
    err = E::NOSEQUENCER;
    return nullptr;
  }
  auto& writes_map = worker->runningWriteMetaDataRecords().map;
  auto it = writes_map.find(MetaDataLog::metaDataLogID(datalog_id));
  if (it == writes_map.end()) {
    err = E::NOSEQUENCER;
    return nullptr;
  }
  ld_check(it->second);
  return it->second->getMetaSequencer();
}

void AllSequencers::disableAllSequencersDueToIsolation() {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  for (const auto& x : map_) {
    x.second->onNodeIsolated();
  }
}
}} // namespace facebook::logdevice
