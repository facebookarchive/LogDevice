/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <boost/noncopyable.hpp>

#include "logdevice/common/EpochStore.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/Processor.h"

/**
 * @file  FileEpochStore is an implementation of the EpochStore interface
 *        which reads (and atomically increments) epoch numbers from files.
 */

namespace facebook { namespace logdevice {

class Processor;
class Configuration;

class FileEpochStore : public EpochStore, boost::noncopyable {
 public:
  /**
   * @param path      root directory storing one file for each log
   * @param processor parent processor for current FileEpochStore
   */
  FileEpochStore(std::string path,
                 Processor* processor,
                 std::shared_ptr<UpdateableNodesConfiguration> config);

  ~FileEpochStore() override {}

  int getLastCleanEpoch(logid_t logid, EpochStore::CompletionLCE cf) override;
  int setLastCleanEpoch(logid_t logid,
                        epoch_t lce,
                        const TailRecord& tail_record,
                        EpochStore::CompletionLCE cf) override;
  int createOrUpdateMetaData(
      logid_t logid,
      std::shared_ptr<EpochMetaData::Updater> updater,
      CompletionMetaData cf,
      MetaDataTracer tracer,
      WriteNodeID write_node_id = WriteNodeID::NO) override;

  std::string identify() const override {
    return std::string("file://") + path_;
  }

  // provision metadata for a single log using the given provision_updater
  int provisionMetaDataLog(
      logid_t log_id,
      std::shared_ptr<EpochMetaData::Updater> provision_updater);

  // provision metadata for logs in config
  int provisionMetaDataLogs(
      std::shared_ptr<EpochMetaData::Updater> provision_updater,
      std::shared_ptr<Configuration> config);

  // pause() waits for all file locks to be released and prevents flock() calls
  // until unpause() is called.
  // See PauseOrUnpauseFileEpochStore admin command for explanation.
  // Return false if the store was already paused/unpaused.
  bool pause();
  bool unpause();

  // maximum length of the file contents
  // TODO: calculate it based on MAX_NODESET_SIZE
  static const int FILE_LEN_MAX = 4096;

 protected:
  /**
   * Helper method used to post a request that will invoke a provided completion
   * function.
   */
  virtual void postCompletionLCE(EpochStore::CompletionLCE cf,
                                 Status status,
                                 logid_t log_id,
                                 epoch_t epoch,
                                 TailRecord tail_record);

  virtual void postCompletionMetaData(
      EpochStore::CompletionMetaData cf,
      Status status,
      logid_t log_id,
      std::unique_ptr<EpochMetaData> metadata,
      std::unique_ptr<EpochStoreMetaProperties> meta_properties);

 private:
  using epoch_func_t = std::function<
      void(epoch_t epoch, epoch_t* out_epoch, TailRecord* out_tail_record)>;

  // FileUpdater encapsulates operations to parse and update the epochstore
  // record file, as well as the data returned as the result of the update.
  class FileUpdater {
   public:
    /**
     * Read the file content from @buf and write the updated content
     * to @out buffer.
     *
     * @return: >0  size of data written
     *           0  no need to change the record (e.g., get only request)
     *          -1  error occured, err is set accordingly
     */
    virtual int update(const char* buf,
                       size_t len,
                       char* out,
                       size_t out_len) = 0;
    virtual ~FileUpdater() {}

    std::unique_ptr<EpochMetaData> metadata_out_{nullptr};
    std::unique_ptr<EpochStoreMetaProperties> meta_props_out_{nullptr};
    epoch_t epoch_out_{EPOCH_INVALID};
    TailRecord tail_record_out_;
  };

  class LCEUpdater;
  class MetaDataUpdater;

  /**
   * Atomically reads the content of epochstore file for (log_id, prefix) and
   * updates it to a newer value using the file_updater@ provided.
   *
   * @param log_id       log for which to perform an update
   * @param prefix       file prefix (e.g. "lce"), used in conjunction with
   *                     log_id to determine which file to read from
   * @param file_updater FileUpdater object to perform the update and store the
   *                     results.
   *
   * @return           0 on file successfully updated
   *                  -1 if no update was performed, with err set to
   *                   OK             get-only requeset and it is successful
   *                   UPTODATE       value already up-to-date, no change needed
   *                   NOTFOUND       the file for (log_id, prefix) doesn't
   *                                  exist or can't be created
   *                   INVALID_PARAM  invalid value read from the epoch file
   *                   FAILED         updating the epoch file failed
   *                   TOOBIG         epoch number exhausted
   *                   BADMSG         malformed record format
   */
  int updateEpochStore(logid_t log_id,
                       std::string prefix,
                       FileUpdater& file_updater);

  std::string path_;
  Processor* processor_;

  // Cluster config.
  std::shared_ptr<UpdateableNodesConfiguration> config_;

  // Locked in shared mode while a file lock is held. Locked in exclusive mode
  // between pause() and unpause() calls.
  folly::SharedMutex flock_mutex_;
};

}} // namespace facebook::logdevice
