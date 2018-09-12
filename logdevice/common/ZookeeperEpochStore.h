/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>
#include <memory>

#include <boost/noncopyable.hpp>

#include <folly/Optional.h>

#include <zookeeper/zookeeper.h>

#include "logdevice/common/EpochStore.h"
#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/common/MetaDataTracer.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/ZookeeperClientBase.h"
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
  class MultiOpState;
  class CreateRootsState;

  /**
   * @param   cluster_name  name of LD cluster this epoch store services
   * @param   processor     parent processor for current ZookeeperEpochStore
   * @param   zkclient      Zookeeper client
   * @param   config        cluster config, we only use this to get the id
   *                        of the node we are running on
   * @param   settings      settings, used to get zk-create-root-znodes
   * @param   zkFactory     factory used to create ZookeeperClient
   *
   */
  ZookeeperEpochStore(std::string cluster_name,
                      Processor* processor,
                      const std::shared_ptr<UpdateableServerConfig>& config,
                      UpdateableSettings<Settings> settings,
                      ZKFactory zkFactory);

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
   * @return  id of this node in config_, or an invalid NodeID if this node
   *          does not appear in config.
   */
  NodeID getMyNodeID() const;

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

  std::shared_ptr<ZookeeperClientBase> getZookeeperClient() const {
    return zkclient_.get();
  }

  std::shared_ptr<std::atomic<bool>> getShuttingDownPtr() const {
    ld_check(shutting_down_);
    return shutting_down_;
  }

  /**
   * Completion function for the  state machine that creates root znodes (its
   * state is stored in CreateRootsState). Public since it's called from
   * CreateRootsState
   */
  static void createRootZnodesCF(std::unique_ptr<CreateRootsState> state,
                                 int rc);

  Status zkOpStatus(int rc, logid_t logid, const char* op) const;

 private:
  // parent processor for current ZookeeperEpochStore
  Processor* processor_;

  // wraps the zhandle_t over which we talk to Zookeeper.
  UpdateableSharedPtr<ZookeeperClientBase> zkclient_;

  // name of LD cluster serviced by this epoch store. Used as a
  // component of the path to epoch znodes
  std::string cluster_name_;

  // Cluster config.
  std::shared_ptr<UpdateableServerConfig> config_;

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
  ZKFactory zkFactory_;

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
   * The Zookeeper completion function for the zoo_aset() call. ZK
   * client will call it on Zookeeper client's own thread when a reply
   * for the set() arrives, or when the session times out. See
   * Zookeeper docs for stat_completion_t.
   */
  static void zkSetCF(int rc, const struct ::Stat* stat, const void* data);

  /**
   * The Zookeeper completion function for the zoo_amulti() call that is called
   * to create multiple znodes for a particular log. ZK client will call it on
   * Zookeeper client's own thread when a reply for the multi-op arrives, or
   * when the session times out. See Zookeeper docs for void_completion_t.
   */
  static void zkLogMultiCreateCF(int rc, const void* data);

  /**
   * The Zookeeper completion function for the zoo_aget() call. See Zookeeper
   * docs for data_completion_t.
   */
  static void zkGetCF(int rc,
                      const char* value,
                      int value_len,
                      const struct ::Stat* stat,
                      const void* data);

  /**
   * Provisions znodes for a log that a particular zrq runs on. Calls
   * zoo_amulti() which will call back into zkMultiCF() on completion.
   */
  Status provisionLogZnodes(std::unique_ptr<ZookeeperEpochStoreRequest>& zrq,
                            const char* sequencer_znode_value,
                            int sequencer_znode_value_size);

  /**
   * Creates root znodes if creation of znodes for an individual log_id
   * (the operation in the supplied argument) failed with ZNONODE.
   * Retries the original multi-op after the creation of parent znodes has
   * been completed.
   */
  void createRootZnodes(std::unique_ptr<MultiOpState> multi_op_state);
};

}} // namespace facebook::logdevice
