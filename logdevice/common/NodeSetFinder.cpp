/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeSetFinder.h"

#include <folly/Memory.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

constexpr std::chrono::milliseconds
    NodeSetFinder::RETRY_READ_METADATALOG_INITIAL_DELAY;

constexpr std::chrono::milliseconds
    NodeSetFinder::RETRY_READ_METADATALOG_MAX_DELAY;

NodeSetFinder::NodeSetFinder(logid_t log_id,
                             std::chrono::milliseconds timeout,
                             std::function<void(Status status)> callback,
                             Source source)
    : log_id_(log_id),
      callback_(callback),
      timeout_(timeout),
      source_(source),
      state_(State::NOT_STARTED),
      callback_helper_(this) {
  // Metadata logs are not supported for NodesetFinder at this time
  ld_check(!MetaDataLog::isMetaDataLog(log_id));
}

NodeSetFinder::~NodeSetFinder() {
  if (state_ != State::FINISHED) {
    // NodeSetFinder did not complete.
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "NodeSetFinder destroyed while still processing "
                      "for log %lu",
                      log_id_.val_);
    metadata_log_read_job_timer_.reset();
    metadata_log_retry_timer_.reset();
    stopReadingMetaDataLog();
  }
}

void NodeSetFinder::start() {
  ld_check(state_ == State::NOT_STARTED);

  State target_state = State::NOT_STARTED;
  switch (source_) {
    case Source::METADATA_LOG:
      target_state = State::READ_METADATALOG;
      break;
    case Source::SEQUENCER:
    case Source::BOTH:
      target_state = State::READ_FROM_SEQUENCER;
      break;
  }

  metadata_log_read_job_timer_ =
      createJobTimer([this] { onMetaDataLogReadTimeout(); });
  ld_check(metadata_log_read_job_timer_);
  std::chrono::milliseconds stage_timeout =
      getStageTimeout(source_, target_state, timeout_);

  state_ = target_state;
  switch (target_state) {
    case State::READ_FROM_SEQUENCER: {
      // here we do not activate the job timer but pass the stage timeout to the
      // SyncSequencerRequest and let it control the timeout
      readFromSequencer(stage_timeout);
    } break;
    case State::READ_METADATALOG: {
      metadata_log_read_job_timer_->activate(stage_timeout);
      readFromMetaDataLog();
    } break;
    default:
      ld_check(false);
  }
}

void NodeSetFinder::readFromSequencer(std::chrono::milliseconds stage_timeout) {
  // makes sure this is executed from a worker thread
  ld_check(Worker::onThisThread(false));
  ld_check(source_ == Source::SEQUENCER || source_ == Source::BOTH);
  ld_check(state_ == State::READ_FROM_SEQUENCER);

  auto ticket = callback_helper_.ticket();
  auto cb_wrapper = [ticket](Status st,
                             NodeID seq_node,
                             lsn_t /*next_lsn*/,
                             std::unique_ptr<LogTailAttributes> /*unused*/,
                             std::shared_ptr<const EpochMetaDataMap> metadata,
                             std::shared_ptr<TailRecord> /*unused*/,
                             folly::Optional<bool> /*unused*/) {
    ticket.postCallbackRequest([=](NodeSetFinder* finder) {
      if (finder && finder->getState() != State::FINISHED) {
        finder->onMetaDataFromSequencer(st, seq_node, std::move(metadata));
      }
    });
  };

  auto ssr = std::unique_ptr<SyncSequencerRequest>(new SyncSequencerRequest(
      log_id_,
      SyncSequencerRequest::INCLUDE_HISTORICAL_METADATA,
      std::move(cb_wrapper),
      GetSeqStateRequest::Context::HISTORICAL_METADATA,
      stage_timeout));

  std::unique_ptr<Request> req(std::move(ssr));
  Worker::onThisThread()->processor_->postImportant(req);
}

void NodeSetFinder::onMetaDataFromSequencer(
    Status status,
    NodeID seq_node,
    std::shared_ptr<const EpochMetaDataMap> metadata) {
  ld_check(state_ == State::READ_FROM_SEQUENCER);
  ld_check(source_ == Source::SEQUENCER || source_ == Source::BOTH);
  if (status != E::OK) {
    if (source_ == Source::SEQUENCER) {
      // finalize with error
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Cannot retrieve metadata for log %lu from sequencer on "
                      "%s: %s. Finalizing.",
                      log_id_.val_,
                      seq_node.toString().c_str(),
                      error_description(status));
      // convert all other failure status except TIMEDOUT to FAILED
      finalize(status == E::TIMEDOUT ? status : E::FAILED);
      return;
    } else {
      ld_check(source_ == Source::BOTH);
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Cannot retrieve metadata for log %lu from sequencer on "
                      "%s: %s. Retry reading from metadata log instead.",
                      log_id_.val_,
                      seq_node.toString().c_str(),
                      error_description(status));
      state_ = State::READ_METADATALOG;
      ld_check(!metadata_log_read_job_timer_->isActive());
      metadata_log_read_job_timer_->activate(
          getStageTimeout(source_, state_, timeout_));
      WORKER_STAT_INCR(nodeset_finder_fallback_to_metadata_log);
      readFromMetaDataLog();
      return;
    }
  }

  // historical metadata successfully read from the sequencer
  ld_check(metadata != nullptr);
  result_ = std::move(metadata);
  WORKER_STAT_INCR(nodeset_finder_read_from_sequencer);
  finalize(E::OK);
}

void NodeSetFinder::readFromMetaDataLog() {
  // makes sure this is executed from a worker thread
  ld_check(Worker::onThisThread(false));
  ld_check(source_ == Source::METADATA_LOG || source_ == Source::BOTH);
  ld_check(state_ == State::READ_METADATALOG);

  // TODO(T23729975): this code is needed because MetaDataLogReader will
  // complete with E::NOTFOUND instead of E::NOTINCONFIG if you try and read the
  // metadata log of a non existent log.
  auto cfg = Worker::getConfig();
  auto ticket = callback_helper_.ticket();
  cfg->getLogGroupByIDAsync(
      log_id_, [ticket](std::shared_ptr<LogsConfig::LogGroupNode> logcfg) {
        if (!logcfg) {
          ticket.postCallbackRequest([=](NodeSetFinder* finder) {
            if (finder && finder->getState() != State::FINISHED) {
              finder->finalize(E::INVALID_PARAM);
            }
          });
        }
      });

  auto cb_wrapper = [this](Status st, MetaDataLogReader::Result result) {
    onMetaDataLogRecord(st, std::move(result));
  };
  meta_reader_ = std::make_unique<MetaDataLogReader>(
      log_id_, EPOCH_MIN, EPOCH_MAX, cb_wrapper);
  if (metadata_log_empty_mode_) {
    meta_reader_->dontWarnIfNotFound();
  }
  meta_reader_->start();
}

void NodeSetFinder::onMetaDataLogRecord(Status st,
                                        MetaDataLogReader::Result result) {
  ld_check(state_ == State::READ_METADATALOG);

  if (metadata_log_empty_mode_) {
    stopReadingMetaDataLog();
    if (st == E::OK) {
      finalize(Status::NOTEMPTY);
      return;
    } else if (st == E::NOTFOUND) {
      finalize(Status::NOTFOUND);
      return;
    }
  }

  if (st != E::OK) {
    if (st == E::NOTFOUND) {
      // if metadata is not found, it's possible that the log has not been fully
      // provisioned yet. Start a backoff timer to retry.
      onMetadataLogNotFound();
      return;
    }
    // otherwise we fail immediately
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Cannot retrieve metadata for log %lu: %s.",
                    log_id_.val_,
                    error_description(st));
    if (st != E::ACCESS) {
      st = E::FAILED;
    }
    finalize(st);
    return;
  }

  ld_check(result.metadata != nullptr);
  const auto& metadata = *result.metadata;

  // collect the result gotten so far by inserting each metadata entry gotten
  // into the result map
  auto res = metadata_recv_.insert(
      std::make_pair(metadata.h.effective_since, metadata));

  if (!res.second) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Duplicate (same effective since) EpochMetaData "
                       "delivered by MetaDataLogReader for log %lu, "
                       "metadata: %s.! This shouldn't happen for correctly "
                       "written metadata log records!",
                       log_id_.val_,
                       metadata.toString().c_str());
    return;
  }

  auto extras_res = metadata_extras_.insert(
      std::make_pair(metadata.h.effective_since,
                     MetaDataExtras{result.lsn, result.timestamp}));

  ld_check(extras_res.second);

  if (result.epoch_until == EPOCH_MAX) {
    // This was the last metadata record.
    onMetaDataLogFullyRead();
  }
}

void NodeSetFinder::onMetadataLogNotFound() {
  ld_check(state_ == State::READ_METADATALOG);

  if (metadata_log_retry_timer_ == nullptr) {
    metadata_log_retry_timer_ =
        createMetaDataLogRetryTimer([this]() { readFromMetaDataLog(); });
  }
  metadata_log_retry_timer_->activate();

  RATELIMIT_ERROR(std::chrono::seconds(10),
                  10,
                  "Could not read metadata log for log %lu, no metadata "
                  "records were found, "
                  "will retry in %lums",
                  log_id_.val_,
                  metadata_log_retry_timer_->getNextDelay().count());
}

void NodeSetFinder::onMetaDataLogFullyRead() {
  ld_check(state_ == State::READ_METADATALOG);
  stopReadingMetaDataLog();

  // we must have received at least one metadata log entries
  ld_check(!metadata_recv_.empty());

  // For metadata log, the effective until of the historical metadata is the
  // effective_since of the very last entry. This is the best we can get by just
  // reading the metadata log. The user can leverage extra information they have
  // (e.g., last released) to extend the effective until range if needed.
  const epoch_t last_effective_since =
      metadata_recv_.crbegin()->second.h.effective_since;

  result_ = EpochMetaDataMap::create(
      std::make_shared<const EpochMetaDataMap::Map>(std::move(metadata_recv_)),
      /*effective_until=*/last_effective_since);

  if (result_ == nullptr) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "EpochMetaData received by MetaDataLogReader is not "
                       "valid to construct EpochMetaDataMap for log %lu! "
                       "Error: %s. This shouldn't happen!",
                       log_id_.val_,
                       error_description(err));
    ld_check(false);
    finalize(E::INTERNAL);
    return;
  }

  // successfully got the result
  WORKER_STAT_INCR(nodeset_finder_read_from_metadata_log);
  finalize(E::OK);
}

void NodeSetFinder::onMetaDataLogReadTimeout() {
  // currently this timer can only fire in READ_METADATALOG state
  ld_check(state_ == State::READ_METADATALOG);
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  10,
                  "Timed out (%ld ms) reading metadata log of log %lu.",
                  getStageTimeout(source_, state_, timeout_).count(),
                  log_id_.val());
  finalize(E::TIMEDOUT);
}

void NodeSetFinder::finalize(Status status) {
  ld_check(state_ != State::FINISHED);
  ld_check_in(status,
              ({E::OK,
                E::TIMEDOUT,
                E::INVALID_PARAM,
                E::ACCESS,
                E::FAILED,
                E::NOTFOUND,
                E::NOTEMPTY}));
  metadata_log_read_job_timer_.reset();
  metadata_log_retry_timer_.reset();
  stopReadingMetaDataLog();

  if (status == E::OK) {
    // must have a valid result for now.
    if (result_ == nullptr) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "NodeSetFinder for log %lu finished successfully "
                         "but did not obtain a valid result!",
                         log_id_.val_);
      ld_check(false);
      status = E::INTERNAL;
    }
  }

  state_ = State::FINISHED;
  callback_(status);
}

StorageSet NodeSetFinder::getUnionStorageSet(
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  // check that the NodeSetFinder completed its processing. if that is not true,
  // the behavior is undefined
  if (state_ != State::FINISHED) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Called on a NodeSetFinder for log %lu that has not "
                       "successfully finished execution and executed the "
                       "callback!",
                       log_id_.val_);
    ld_check(false);
  }

  ld_check(result_ != nullptr);
  auto storage_set = result_->getUnionStorageSet(nodes_configuration);
  // we should always get the storage set as the effective_until is EPOCH_MAX
  ld_check(storage_set != nullptr);
  return *storage_set;
}

ReplicationProperty NodeSetFinder::getNarrowestReplication() {
  // check that the NodeSetFinder completed its processing. if that is not true,
  // the behavior is undefined
  if (state_ != State::FINISHED) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Called on a NodeSetFinder for log %lu that has not "
                       "successfully finished execution and executed the "
                       "callback!",
                       log_id_.val_);
    ld_check(false);
  }

  ld_check(result_ != nullptr);
  epoch_t effective_until = result_->getEffectiveUntil();
  auto replication =
      result_->getNarrowestReplication(EPOCH_MIN, effective_until);
  // we should always get the replication as the effective_until is EPOCH_MAX
  ld_check(replication != nullptr);
  return *replication;
}

const EpochMetaDataMap::Map& NodeSetFinder::getAllEpochsMetaData() const {
  return *result_->getMetaDataMap();
}

void NodeSetFinder::setResultAndFinalize(
    Status status,
    std::shared_ptr<const EpochMetaDataMap> result) {
  result_ = std::move(result);
  finalize(status);
}

/* static */
std::chrono::milliseconds
NodeSetFinder::getStageTimeout(Source source,
                               State target_state,
                               std::chrono::milliseconds total_timeout) {
  ld_check(target_state == State::READ_FROM_SEQUENCER ||
           target_state == State::READ_METADATALOG);
  switch (source) {
    case Source::METADATA_LOG:
    case Source::SEQUENCER:
      return total_timeout;
    case Source::BOTH:
      // use 1/3 of the total timeout for attempting to read from sequencer,
      // and 2/3 of the total timeout for reading metadata logs
      return (target_state == State::READ_FROM_SEQUENCER
                  ? (total_timeout / 3)
                  : (total_timeout - (total_timeout / 3)));
  }
  ld_check(false);
  return total_timeout;
}

std::unique_ptr<Timer>
NodeSetFinder::createJobTimer(std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

std::unique_ptr<BackoffTimer>
NodeSetFinder::createMetaDataLogRetryTimer(std::function<void()> callback) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(

      callback,
      RETRY_READ_METADATALOG_INITIAL_DELAY,
      RETRY_READ_METADATALOG_MAX_DELAY);

  return std::move(timer);
}

void NodeSetFinder::stopReadingMetaDataLog() {
  if (meta_reader_) {
    Worker::onThisThread()->disposeOfMetaReader(std::move(meta_reader_));
  }
}
}} // namespace facebook::logdevice
