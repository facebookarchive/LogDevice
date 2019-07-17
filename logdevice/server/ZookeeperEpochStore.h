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
#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"
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
   * @param   zkFactory     factory used to create ZookeeperClient
   *
   */
  ZookeeperEpochStore(
      std::string cluster_name,
      Processor* processor,
      const std::shared_ptr<UpdateableZookeeperConfig>& zk_config,
      const std::shared_ptr<UpdateableNodesConfiguration>& nodes_configuration,
      UpdateableSettings<Settings> settings,
      std::shared_ptr<ZookeeperClientFactory> zkFactory);

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
   * Attempts to post an completion requests containing the result of
   * an async request to the epoch store. On transient failures (e.g., a
   * Worker pipe overflow), queues up the completion request for redelivery.
   *
   * @param completion    completion request to post
   */
  virtual void
  postCompletion(std::unique_ptr<EpochStore::CompletionLCERequest>&&) const;
  virtual void postCompletion(
      std::unique_ptr<EpochStore::CompletionMetaDataRequest>&&) const;

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

  // This method should be used cautiously. It returns a shared pointer to the
  // zkclient created by ZookeeperEpochStore. Once ZookeeperEpochStore is
  // destroyed, no new calls should be made using this zkclient pointer. The
  // behaviour is currently undefined
  std::shared_ptr<ZookeeperClientBase> getZookeeperClient() const {
    return zkclient_.load();
  }

  std::shared_ptr<std::atomic<bool>> getShuttingDownPtr() const {
    ld_check(shutting_down_);
    return shutting_down_;
  }

  const UpdateableSettings<Settings>& getSettings() {
    return settings_;
  }

  Status completionStatus(int rc, logid_t logid);

 private:
  // parent processor for current ZookeeperEpochStore
  Processor* processor_;

  // wraps the zhandle_t over which we talk to Zookeeper.
  folly::atomic_shared_ptr<ZookeeperClientBase> zkclient_;

  // name of LD cluster serviced by this epoch store. Used as a
  // component of the path to epoch znodes
  std::string cluster_name_;

  std::shared_ptr<UpdateableZookeeperConfig> zk_config_;

  // Cluster config, used to figure out NodeID
  std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_;

  // Settings
  UpdateableSettings<Settings> settings_;

  // This bit changes when the epoch store is being destroyed. This is needed
  // to control whether the EpochStore is being destroyed in callbacks that get
  // a ZCLOSING status - even if they run after the ZookeperEpochStore instance
  // had been destroyed
  std::shared_ptr<std::atomic<bool>> shutting_down_;

  // Used to detect changes in Zookeeper quorum
  folly::Optional<ConfigSubscriptionHandle> config_subscription_;

  // ZookeeperClientFactory to create ZookeeperClient
  std::shared_ptr<ZookeeperClientFactory> zkFactory_;

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

  // Callback invoked when the config has changed.  Checks if the Zookeper
  // quorum changed; if so, creates a new ZookeperClient.
  void onConfigUpdate();

  /**
   * Schedules a request on the Processor after a Zookeeper modification
   * completes.
   */
  void postRequestCompletion(int rc,
                             std::unique_ptr<ZookeeperEpochStoreRequest> zrq);

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
                          std::string znode_value);
};

}} // namespace facebook::logdevice
