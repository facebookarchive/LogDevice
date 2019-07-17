/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FileEpochStore.h"

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
#include "logdevice/common/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

FileEpochStore::FileEpochStore(
    std::string path,
    Processor* processor,
    std::shared_ptr<UpdateableNodesConfiguration> config)
    : path_(std::move(path)),
      processor_(processor),
      config_(std::move(config)) {
  ld_check(!path_.empty());
}

// a subclass of FileEpochStore::FileUpdater used to get/set last clean epoch
// value in file epoch store
class FileEpochStore::LCEUpdater : public FileEpochStore::FileUpdater {
 public:
  LCEUpdater(epoch_func_t func, logid_t log_id, epoch_t initial_epoch)
      : func_(std::move(func)), log_id_(log_id), initial_epoch_(initial_epoch) {
    ld_check(func_ != nullptr);
  }

  int update(const char* buf, size_t len, char* out, size_t out_len) override {
    ld_check(buf && out);
    if (len > FileEpochStore::FILE_LEN_MAX) {
      err = E::BADMSG;
      return -1;
    }
    epoch_t epoch = initial_epoch_;
    TailRecord tail_record;

    int rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
        buf, len, log_id_, &epoch, &tail_record);

    if (rv != 0) {
      err = E::BADMSG;
      return -1;
    }

    epoch_t cur_epoch(epoch), next(EPOCH_INVALID);
    TailRecord next_tail;
    func_(cur_epoch, &next, &next_tail);

    if (next > cur_epoch) {
      ld_check(next <= EPOCH_MAX && "Invalid epoch number");
      // return the updated lce epoch
      epoch_out_ = next;
      tail_record_out_ = next_tail;
      return EpochStoreLastCleanEpochFormat::toLinearBuffer(
          out, out_len, epoch_out_, tail_record_out_);
    }

    if (next != EPOCH_INVALID) {
      // set operation
      err = E::STALE;
      epoch_out_ = cur_epoch;
      tail_record_out_ = tail_record;
      return -1;
    }

    epoch_out_ = cur_epoch;
    tail_record_out_ = tail_record;
    // get only, no record written
    err = E::OK;
    return 0;
  }

 private:
  epoch_func_t func_;
  const logid_t log_id_;
  epoch_t initial_epoch_;
};

// a subclass of FileEpochStore::FileUpdater used to access and update
// epoch metadata in file epoch store
class FileEpochStore::MetaDataUpdater : public FileEpochStore::FileUpdater {
 public:
  MetaDataUpdater(
      logid_t log_id,
      std::shared_ptr<EpochMetaData::Updater> updater,
      MetaDataTracer* tracer,
      std::shared_ptr<const NodesConfiguration> nodes_configuration,
      folly::Optional<NodeID> my_node_id = folly::none,
      EpochStore::WriteNodeID write_node_id = EpochStore::WriteNodeID::NO,
      bool return_fetched_value = false)
      : log_id_(log_id),
        meta_updater_(std::move(updater)),
        tracer_(tracer),
        nodes_configuration_(std::move(nodes_configuration)),
        my_node_id_(std::move(my_node_id)),
        write_node_id_(write_node_id),
        return_fetched_value_(return_fetched_value) {
    // If we should write our NodeID, it must have a value.
    ld_assert(write_node_id_ != EpochStore::WriteNodeID::MY ||
              my_node_id_.hasValue());
  }

  int update(const char* buf, size_t len, char* out, size_t out_len) override {
    ld_check(buf && out);
    if (len > FileEpochStore::FILE_LEN_MAX) {
      err = E::BADMSG;
      return -1;
    }

    std::unique_ptr<EpochMetaData> metadata_out;
    if (len > 0) {
      if (len <= sizeof(NodeID)) {
        err = E::BADMSG;
        return -1;
      }
      NodeID node_id;
      memcpy(&node_id, buf, sizeof(NodeID));
      buf += sizeof(NodeID);
      len -= sizeof(NodeID);
      if (node_id.isNodeID()) {
        if (!meta_props_out_) {
          meta_props_out_ = std::make_unique<EpochStoreMetaProperties>();
        }
        meta_props_out_->last_writer_node_id.assign(node_id);
      }
      metadata_out = std::make_unique<EpochMetaData>();
      if (metadata_out->fromPayload(
              Payload(buf, len), log_id_, *nodes_configuration_)) {
        // err set in fromPayload
        return -1;
      }
    } else if (return_fetched_value_) {
      err = E::NOTFOUND;
      return -1;
    }

    int rv = 0;
    // if asked to return the original fetched result, perform update
    // on a copy to keep the original
    std::unique_ptr<EpochMetaData> info_copy;
    std::unique_ptr<EpochMetaData>* to_update = &metadata_out;
    if (return_fetched_value_) {
      ld_check(metadata_out);
      ld_check(metadata_out->isValid());
      info_copy = std::make_unique<EpochMetaData>(*metadata_out);
      to_update = &info_copy;
    }

    EpochMetaData::UpdateResult result =
        (*meta_updater_)(log_id_, *to_update, tracer_);
    switch (result) {
      case EpochMetaData::UpdateResult::UNCHANGED:
        // no need to change metadata
        err = E::UPTODATE;
        break;
      case EpochMetaData::UpdateResult::FAILED:
        rv = -1;
        break;
      case EpochMetaData::UpdateResult::ONLY_NODESET_PARAMS_CHANGED:
      case EpochMetaData::UpdateResult::NONSUBSTANTIAL_RECONFIGURATION:
      case EpochMetaData::UpdateResult::CREATED:
      case EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION: {
        ld_check((*to_update)->isValid());
        // write to the epoch store file
        if (out_len < sizeof(NodeID)) {
          return -1;
        }
        NodeID node_id_to_write;
        switch (write_node_id_) {
          case EpochStore::WriteNodeID::MY:
            node_id_to_write = my_node_id_.value();
            break;
          case EpochStore::WriteNodeID::KEEP_LAST:
            if (meta_props_out_ &&
                meta_props_out_->last_writer_node_id.hasValue()) {
              node_id_to_write = meta_props_out_->last_writer_node_id.value();
            }
            break;
          case EpochStore::WriteNodeID::NO:
            break;
        }
        memcpy(out, &node_id_to_write, sizeof(node_id_to_write));
        rv = (*to_update)
                 ->toPayload(out + sizeof(NodeID), out_len - sizeof(NodeID));
        if (rv < 0) {
          return rv;
        }
        // expect positive bytes copied
        ld_check(rv > 0);
        rv += sizeof(NodeID);
        break;
      }
    };

    if (metadata_out) {
      epoch_out_ = metadata_out->h.epoch;
      metadata_out_ = std::move(metadata_out);
    }
    return rv;
  }

 private:
  logid_t log_id_;
  std::shared_ptr<EpochMetaData::Updater> meta_updater_;
  MetaDataTracer* tracer_;
  std::shared_ptr<const NodesConfiguration> nodes_configuration_;
  folly::Optional<NodeID> my_node_id_;
  EpochStore::WriteNodeID write_node_id_;
  // if true, metadata_out_ is assigned with the metadata fetched from the epoch
  // store (i.e., value before update). Otherwise, metadata_out_ is assigned
  // with the updated value.
  bool return_fetched_value_;
};

int FileEpochStore::getLastCleanEpoch(logid_t log_id,
                                      EpochStore::CompletionLCE cf) {
  LCEUpdater file_updater([](epoch_t /*epoch*/,
                             epoch_t* /*out_epoch*/,
                             TailRecord* /*out_tail_record*/) {},
                          log_id,
                          EPOCH_INVALID);

  int rv = updateEpochStore(log_id, "lce", file_updater);
  postCompletionLCE(cf,
                    (rv == 0 ? E::OK : err),
                    log_id,
                    file_updater.epoch_out_,
                    file_updater.tail_record_out_);
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

  LCEUpdater file_updater(
      [lce, tail_record](
          epoch_t /*epoch*/, epoch_t* out_epoch, TailRecord* out_tail_record) {
        *out_epoch = lce;
        *out_tail_record = tail_record;
      },
      log_id,
      EPOCH_INVALID);

  int rv = updateEpochStore(log_id, "lce", file_updater);
  postCompletionLCE(cf,
                    (rv == 0 ? E::OK : err),
                    log_id,
                    file_updater.epoch_out_,
                    file_updater.tail_record_out_);
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

  MetaDataUpdater file_updater(
      log_id,
      updater,
      &tracer,
      config_->get(),
      processor_ == nullptr ? folly::none : processor_->getOptionalMyNodeID(),
      write_node_id);

  int rv = updateEpochStore(log_id, "seq", file_updater);
  tracer.trace(rv == 0 ? E::OK : err);
  postCompletionMetaData(std::move(cf),
                         (rv == 0 ? E::OK : err),
                         log_id,
                         std::move(file_updater.metadata_out_),
                         std::move(file_updater.meta_props_out_));
  return 0;
}

int FileEpochStore::provisionMetaDataLog(
    logid_t log_id,
    std::shared_ptr<EpochMetaData::Updater> provisioner) {
  if (log_id <= LOGID_INVALID || log_id > LOGID_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  MetaDataUpdater file_updater(log_id,
                               std::move(provisioner),
                               /*MetaDataTracer*/ nullptr,
                               config_->get());
  int rv = updateEpochStore(log_id, "seq", file_updater);
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

void FileEpochStore::postCompletionLCE(EpochStore::CompletionLCE cf,
                                       Status status,
                                       logid_t log_id,
                                       epoch_t epoch,
                                       TailRecord tail_record) {
  ld_debug("Setting tail record for log %lu, epoch %u: %s.",
           log_id.val_,
           epoch.val_,
           tail_record.toString().c_str());

  // If called from a worker thread, invoke `cf' on the same thread. Otherwise,
  // use any thread.
  worker_id_t worker_idx = Worker::onThisThread(false /* enforce_worker */)
      ? Worker::onThisThread()->idx_
      : worker_id_t(-1);

  std::unique_ptr<Request> rq =
      std::make_unique<EpochStore::CompletionLCERequest>(
          cf, worker_idx, status, log_id, epoch, std::move(tail_record));

  int rv = processor_->postWithRetrying(rq);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got an unexpected status code %s from "
                    "Processor::postWithRetrying(), "
                    "dropping request for log %lu",
                    error_name(err),
                    log_id.val_);

    ld_check(false &&
             "Posting a request via Processor::postWithRetrying() failed");
  }
}

void FileEpochStore::postCompletionMetaData(
    EpochStore::CompletionMetaData cf,
    Status status,
    logid_t log_id,
    std::unique_ptr<EpochMetaData> metadata,
    std::unique_ptr<EpochStoreMetaProperties> meta_properties) {
  // If called from a worker thread, invoke `cf' on the same thread. Otherwise,
  // use any thread.
  worker_id_t worker_idx = Worker::onThisThread(false /* enforce_worker */)
      ? Worker::onThisThread()->idx_
      : worker_id_t(-1);

  std::unique_ptr<Request> rq =
      std::make_unique<EpochStore::CompletionMetaDataRequest>(
          cf,
          worker_idx,
          status,
          log_id,
          std::move(metadata),
          std::move(meta_properties));

  int rv = processor_->postWithRetrying(rq);
  if (rv != 0 && err != E::SHUTDOWN) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Got an unexpected status code %s from "
                    "Processor::postWithRetrying(), "
                    "dropping request for log %lu",
                    error_name(err),
                    log_id.val_);

    ld_check(false &&
             "Posting a request via Processor::postWithRetrying() failed");
  }
}

int FileEpochStore::updateEpochStore(logid_t log_id,
                                     std::string prefix,
                                     FileUpdater& file_updater) {
  char lock_path[1024];
  snprintf(lock_path,
           sizeof(lock_path),
           "%s/%s_%lu.lock",
           path_.c_str(),
           prefix.c_str(),
           log_id.val_);

  char store_path[1024];
  snprintf(store_path,
           sizeof(store_path),
           "%s/%s_%lu",
           path_.c_str(),
           prefix.c_str(),
           log_id.val_);

  int lock_fd = open(lock_path,
                     O_RDWR | O_CREAT,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
  if (lock_fd < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error opening file `%s': %d (%s)",
                    lock_path,
                    errno,
                    strerror(errno));
    err = E::NOTFOUND;
    return -1;
  }

  folly::SharedMutex::ReadHolder lock(flock_mutex_);

  if (flock(lock_fd, LOCK_EX) < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "flock() failed on `%s': %d (%s)",
                    lock_path,
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

  int store_fd = open(store_path,
                      O_RDWR | O_CREAT,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
  if (store_fd < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error opening file `%s': %d (%s)",
                    store_path,
                    errno,
                    strerror(errno));
    err = E::NOTFOUND;
    return -1;
  }
  SCOPE_EXIT {
    close(store_fd);
  };

  char buf[FILE_LEN_MAX];
  int bytes_read = read(store_fd, buf, sizeof(buf) - 1);
  if (bytes_read < 0) {
    err = E::FAILED; // permission or I/O error
    return -1;
  }

  int bytes_written = file_updater.update(buf, bytes_read, buf, sizeof(buf));
  if (bytes_written <= 0) {
    // no record needs to be written, err set by update (could be E::OK)
    ld_check((bytes_written < 0 && err != E::OK) ||
             (bytes_written == 0 && (err == E::OK || err == E::UPTODATE)));
    return -1;
  }

  using folly::test::TemporaryFile;
  TemporaryFile tmp("epoch_store_new_contents",
                    boost::filesystem::path(store_path).parent_path(),
                    TemporaryFile::Scope::PERMANENT);
  int rv = folly::writeFull(tmp.fd(), buf, bytes_written);
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
  rv = rename(tmp.path().c_str(), store_path);

  if (rv < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error renaming `%s' to '%s': %d (%s)",
                    tmp.path().c_str(),
                    store_path,
                    errno,
                    strerror(errno));
    err = E::FAILED;
    return -1;
  }

  return 0;
}

bool FileEpochStore::pause() {
  flock_mutex_.lock();
  return true;
}

bool FileEpochStore::unpause() {
  if (flock_mutex_.try_lock()) {
    flock_mutex_.unlock();
    return false;
  }
  flock_mutex_.unlock();
  return true;
}

}} // namespace facebook::logdevice
