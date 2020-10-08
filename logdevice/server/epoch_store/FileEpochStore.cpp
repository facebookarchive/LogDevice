/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/epoch_store/FileEpochStore.h"

#include <cstdio>
#include <cstdlib>
#include <unistd.h>

#include <folly/FileUtil.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/experimental/TestUtil.h>
#include <sys/file.h>

#include "logdevice/common/CompletionRequest.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/epoch_store/EpochMetaDataZRQ.h"
#include "logdevice/server/epoch_store/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/server/epoch_store/GetLastCleanEpochZRQ.h"
#include "logdevice/server/epoch_store/SetLastCleanEpochZRQ.h"

namespace facebook { namespace logdevice {

FileEpochStore::FileEpochStore(
    std::string path,
    RequestExecutor request_executor,
    folly::Optional<NodeID> my_node_id,
    std::shared_ptr<UpdateableNodesConfiguration> config)
    : path_(std::move(path)),
      request_executor_(std::move(request_executor)),
      my_node_id_(std::move(my_node_id)),
      config_(std::move(config)) {
  ld_check(!path_.empty());
}

int FileEpochStore::getLastCleanEpoch(logid_t log_id,
                                      EpochStore::CompletionLCE cf) {
  auto log_metadata = LogMetaData::forNewLog(log_id);
  auto zrq = std::unique_ptr<ZookeeperEpochStoreRequest>(
      new GetLastCleanEpochZRQ(log_id, cf));
  int rv = updateEpochStore(zrq, log_metadata);
  zrq->postCompletion(
      rv == 0 ? E::OK : err, std::move(log_metadata), request_executor_);
  return 0;
}

int FileEpochStore::setLastCleanEpoch(logid_t log_id,
                                      epoch_t lce,
                                      const TailRecord& tail_record,
                                      EpochStore::CompletionLCE cf) {
  if (!tail_record.isValid() || tail_record.containOffsetWithinEpoch()) {
    RATELIMIT_CRITICAL(std::chrono::seconds(5),
                       5,
                       "INTERNAL ERROR: attempting to update LCE with invalid "
                       "tail record! log %lu, lce %u, tail record flags: %u",
                       log_id.val_,
                       lce.val_,
                       tail_record.header.flags);
    err = E::INVALID_PARAM;
    ld_check(false);
    return -1;
  }

  auto log_metadata = LogMetaData::forNewLog(log_id);
  auto zrq = std::unique_ptr<ZookeeperEpochStoreRequest>(
      new SetLastCleanEpochZRQ(log_id, lce, tail_record, cf));
  int rv = updateEpochStore(zrq, log_metadata);
  zrq->postCompletion(
      rv == 0 ? E::OK : err, std::move(log_metadata), request_executor_);
  return 0;
}

int FileEpochStore::createOrUpdateMetaData(
    logid_t log_id,
    std::shared_ptr<EpochMetaData::Updater> updater,
    EpochStore::CompletionMetaData cf,
    MetaDataTracer tracer,
    WriteNodeID write_node_id) {
  if (log_id <= LOGID_INVALID || log_id > LOGID_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  auto log_metadata = LogMetaData::forNewLog(log_id);
  auto zrq = std::unique_ptr<ZookeeperEpochStoreRequest>(
      new EpochMetaDataZRQ(log_id,
                           cf,
                           std::move(updater),
                           std::move(tracer),
                           write_node_id,
                           config_->get(),
                           my_node_id_));
  int rv = updateEpochStore(zrq, log_metadata);
  zrq->postCompletion(
      rv == 0 ? E::OK : err, std::move(log_metadata), request_executor_);
  return 0;
}

int FileEpochStore::provisionMetaDataLog(
    logid_t log_id,
    std::shared_ptr<EpochMetaData::Updater> provisioner) {
  if (log_id <= LOGID_INVALID || log_id > LOGID_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  auto log_metadata = LogMetaData::forNewLog(log_id);
  auto zrq = std::unique_ptr<ZookeeperEpochStoreRequest>(
      new EpochMetaDataZRQ(log_id,
                           [](auto, auto, auto, auto) {},
                           std::move(provisioner),
                           MetaDataTracer(),
                           WriteNodeID::NO,
                           config_->get(),
                           folly::none));

  int rv = updateEpochStore(zrq, log_metadata);
  if (rv != 0 && err != E::UPTODATE) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Failed to provision initial metadata log for "
                    "log %lu: error code %s",
                    log_id.val_,
                    error_name(err));
  }
  return rv;
}

int FileEpochStore::provisionMetaDataLogs(
    std::shared_ptr<EpochMetaData::Updater> provisioner,
    std::shared_ptr<Configuration> config) {
  const auto& logs_config = config->localLogsConfig();
  for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd(); ++it) {
    logid_t log_id(it->first);
    int rv = provisionMetaDataLog(log_id, provisioner);
    if (rv != 0 && err != E::UPTODATE) {
      return -1;
    }
  }
  return 0;
}

int FileEpochStore::updateEpochStore(
    std::unique_ptr<ZookeeperEpochStoreRequest>& zrq,
    LogMetaData& log_metadata) {
  logid_t logid = zrq->logid_;

  boost::filesystem::path log_path =
      folly::sformat("{}/{}", path_, MetaDataLog::dataLogID(logid).val());
  boost::filesystem::path lock_path = log_path.string() + ".lock";

  int lock_fd = open(lock_path.c_str(),
                     O_RDWR | O_CREAT,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
  if (lock_fd < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error opening file `%s': %d (%s)",
                    lock_path.c_str(),
                    errno,
                    strerror(errno));
    err = E::NOTFOUND;
    return -1;
  }

  {
    std::unique_lock<std::mutex> lk(paused_mutex_);
    paused_cv_.wait(lk, [&] { return !paused_; });
  }

  if (flock(lock_fd, LOCK_EX) < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "flock() failed on `%s': %d (%s)",
                    lock_path.c_str(),
                    errno,
                    strerror(errno));

    close(lock_fd);
    err = E::NOTFOUND;
    return -1;
  }

  SCOPE_EXIT {
    flock(lock_fd, LOCK_UN);
    close(lock_fd);
  };

  std::string data;
  auto read_success = folly::readFile(log_path.c_str(), data);

  if (read_success) {
    auto deserialization_st = zrq->deserializeLogMetaData(data, log_metadata);

    if (deserialization_st != E::OK) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Failed to deserialize log metadata for log %lu",
                      zrq->logid_.val_);
      err = deserialization_st;
      return -1;
    }
  }

  auto next_step = zrq->applyChanges(log_metadata, read_success);

  switch (next_step) {
    case ZookeeperEpochStoreRequest::NextStep::PROVISION:
    case ZookeeperEpochStoreRequest::NextStep::MODIFY:
      break;
    case ZookeeperEpochStoreRequest::NextStep::STOP:
      ld_check(
          (dynamic_cast<GetLastCleanEpochZRQ*>(zrq.get()) && err == E::OK) ||
          (dynamic_cast<EpochMetaDataZRQ*>(zrq.get()) && err == E::UPTODATE));
      return err == E::OK ? 0 : -1;
    case ZookeeperEpochStoreRequest::NextStep::FAILED:
      ld_check(err == E::FAILED || err == E::BADMSG || err == E::NOTFOUND ||
               err == E::EMPTY || err == E::EXISTS || err == E::DISABLED ||
               err == E::TOOBIG ||
               ((err == E::INVALID_PARAM || err == E::ABORTED) &&
                dynamic_cast<EpochMetaDataZRQ*>(zrq.get())) ||
               (err == E::STALE &&
                (dynamic_cast<EpochMetaDataZRQ*>(zrq.get()) ||
                 dynamic_cast<SetLastCleanEpochZRQ*>(zrq.get()))));
      return -1;
  }

  // Increment version and timestamp of log metadata.
  log_metadata.touch();

  {
    // This is still needed as it can modify the LogMetaData. Can be removed
    // when ZookeeperEpochStore is fully migrated. We no longer care about its
    // return value though.
    char znode_value[FILE_LEN_MAX];
    zrq->composeZnodeValue(log_metadata, znode_value, sizeof(znode_value));
  }

  auto serialized_log_metadata = zrq->serializeLogMetaData(log_metadata);

  using folly::test::TemporaryFile;
  TemporaryFile tmp(
      folly::sformat("{}.tmp", MetaDataLog::dataLogID(logid).val()),
      path_,
      TemporaryFile::Scope::PERMANENT);
  int rv = folly::writeFull(
      tmp.fd(), serialized_log_metadata.data(), serialized_log_metadata.size());
  if (rv < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Writing to `%s' failed: %d (%s)",
                    tmp.path().c_str(),
                    errno,
                    strerror(errno));
    err = E::FAILED;
    return -1;
  }
  rv = rename(tmp.path().c_str(), log_path.c_str());

  if (rv < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error renaming `%s' to '%s': %d (%s)",
                    tmp.path().c_str(),
                    log_path.c_str(),
                    errno,
                    strerror(errno));
    err = E::FAILED;
    return -1;
  }
  return 0;
}

bool FileEpochStore::pause() {
  bool was_paused;
  {
    std::lock_guard<std::mutex> lk(paused_mutex_);
    was_paused = paused_;
    paused_ = true;
  }

  // Return true if the store wasn't already paused.
  return was_paused == false;
}

bool FileEpochStore::unpause() {
  bool was_paused;
  {
    std::lock_guard<std::mutex> lk(paused_mutex_);
    was_paused = paused_;
    paused_ = false;
  }
  paused_cv_.notify_all();

  // Return true if the store was paused.
  return was_paused == true;
}
}} // namespace facebook::logdevice
