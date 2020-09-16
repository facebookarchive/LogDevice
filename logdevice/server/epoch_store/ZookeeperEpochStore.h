/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <boost/noncopyable.hpp>
#include <folly/Optional.h>
#include <folly/concurrency/AtomicSharedPtr.h>
#include <zookeeper/zookeeper.h>

#include "logdevice/common/EpochStore.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/ZookeeperClientBase.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"
#include "logdevice/server/epoch_store/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/server/epoch_store/LogMetaData.h"

/**
 * @file ZookeeperEpochStore implements an EpochStore interface to a
 *       Zookeeper-based epoch store.
 */

namespace facebook { namespace logdevice {

class Processor;
class ZookeeperEpochStoreRequest;

class ZookeeperEpochStore : public EpochStore, boost::noncopyable {
 public:
  /**
   * @param   cluster_name  name of LD cluster this epoch store services
   * @param   processor     parent processor for current ZookeeperEpochStore
   * @param   zkclient      Zookeeper client
   * @param   config        Zookeeper config, we only use this to get the id
   *                        of the node we are running on
   * @param   settings      settings, used to get zk-create-root-znodes
   *
   */
  ZookeeperEpochStore(
      std::string cluster_name,
      RequestExecutor request_executor,
      std::shared_ptr<ZookeeperClientBase> zkclient,
      const std::shared_ptr<UpdateableNodesConfiguration>& nodes_configuration,
      UpdateableSettings<Settings> settings,
      folly::Optional<NodeID> my_node_id,
      StatsHolder* stats);

  ~ZookeeperEpochStore() override;

  // see EpochStore.h
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

  std::string identify() const override;

  /**
   * Returns the path to the root znode for the logdevice cluster that this
   * EpochStore is for (`cluster_name_`)
   */
  std::string rootPath() const {
    return "/logdevice/" + cluster_name_ + "/logs";
  }

  /**
   * Returns the path to the root of subtree for log @param logid and LogDevice
   * cluster `cluster_name_`
   */
  std::string znodePathForLog(logid_t logid) const;

  // maximum length of any znode value to be written in bytes
  static const int ZNODE_VALUE_WRITE_LEN_MAX =
      EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX;

  // maximum length of any znode value to be read in bytes
  static const int ZNODE_VALUE_READ_LEN_MAX =
      EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX;

  const UpdateableSettings<Settings>& getSettings() {
    return settings_;
  }

  Status completionStatus(int rc, logid_t logid);

 private:
  // RequestExecutor to post requests on
  RequestExecutor request_executor_;

  // wraps the zhandle_t over which we talk to Zookeeper.
  std::shared_ptr<ZookeeperClientBase> zkclient_;

  // name of LD cluster serviced by this epoch store. Used as a
  // component of the path to epoch znodes
  std::string cluster_name_;

  // Cluster config, used to figure out NodeID
  std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_;

  // Settings
  UpdateableSettings<Settings> settings_;

  folly::Optional<NodeID> my_node_id_;

  StatsHolder* stats_;

  // This bit changes when the epoch store is being destroyed. This is needed
  // to control whether the EpochStore is being destroyed in callbacks that get
  // a ZCLOSING status - even if they run after the ZookeperEpochStore instance
  // had been destroyed
  std::atomic<bool> shutting_down_;

  /**
   * Run a zoo_aget() on a znode, optionally followed by a modify and a
   * version-conditional zoo_aset() of a new value into the same znode.
   *
   * @param  zrq   controls the path to znode, znode value (de)serialization,
   *               and whether a new value must be written back.
   *
   * @return 0 if the request was successfully submitted to Zookeeper, -1
   *         on failure. Sets err to INTERNAL, NOTCONN, ACCESS, SYSLIMIT as
   *         defined for EpochStore::nextEpoch().
   */
  int runRequest(std::unique_ptr<ZookeeperEpochStoreRequest> zrq);

  /**
   * Schedules a request on the Processor after a Zookeeper modification
   * completes.
   */
  void postRequestCompletion(Status st,
                             std::unique_ptr<ZookeeperEpochStoreRequest> zrq,
                             LogMetaData&& log_metadata);

  /**
   * The callback executed when a znode has been fetched.
   */
  void onGetZnodeComplete(int rc,
                          std::string value,
                          const zk::Stat& stat,
                          std::unique_ptr<ZookeeperEpochStoreRequest> zrq);

  /**
   * Provisions znodes for a log that a particular zrq runs on. Executes
   * a zookeeper multiOp.
   */
  void provisionLogZnodes(std::unique_ptr<ZookeeperEpochStoreRequest> zrq,
                          LogMetaData&& log_metadata,
                          std::string znode_value);
};

}} // namespace facebook::logdevice
