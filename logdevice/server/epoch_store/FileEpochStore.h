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
#include "logdevice/common/RequestExecutor.h"
#include "logdevice/server/epoch_store/ZookeeperEpochStoreRequest.h"

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
                 RequestExecutor request_executor,
                 folly::Optional<NodeID> my_node_id,
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

 private:
  /**
   * Atomically execute the passed epoch store request.
   *
   * @param zrq          The EpochStoreRequest to execute.
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
  int updateEpochStore(std::unique_ptr<ZookeeperEpochStoreRequest>& zrq);

  std::string path_;

  RequestExecutor request_executor_;
  folly::Optional<NodeID> my_node_id_;

  // Cluster config.
  std::shared_ptr<UpdateableNodesConfiguration> config_;

  // A condition_variable used to pause/unpause epoch store via admin commands.
  std::mutex paused_mutex_;
  std::condition_variable paused_cv_;
  bool paused_{false};
};

}} // namespace facebook::logdevice
