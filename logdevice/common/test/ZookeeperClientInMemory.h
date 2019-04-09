/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <google/dense_hash_map>
#include <zookeeper/zookeeper.h>

#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"

namespace facebook { namespace logdevice {

class ZookeeperClientInMemory : public ZookeeperClientBase {
 public:
  /*
   * The (simplified) in-memory representation of the data in Zookeeper
   *   key - full path of node
   *   value - pair of value stored in the node and the stat of the node
   */
  using state_map_t =
      std::unordered_map<std::string, std::pair<std::string, zk::Stat>>;

  /**
   * ZookeeperClientInMemory emulates zookeeper using in-memory map
   * @param quorum           zookeeper quorum.
   *                         for testing any not null value could be used.
   * @param map              initial state of zookeeper.
   */
  ZookeeperClientInMemory(std::string quorum, state_map_t map);

  int state() override;

  int setData(const char* znode_path,
              const char* znode_value,
              int znode_value_size,
              int version,
              stat_completion_t completion,
              const void* data) override;

  int getData(const char* znode_path,
              data_completion_t completion,
              const void* data) override;

  int exists(const char* znode_path,
             stat_completion_t completion,
             const void* data);

  int multiOp(int count,
              const zoo_op_t* ops,
              zoo_op_result_t* results,
              void_completion_t completion,
              const void* data) override;

  ~ZookeeperClientInMemory() override;

 private:
  bool parentsExist(const std::lock_guard<std::mutex>& lock,
                    const char* znode_path);

  int reconnect(zhandle_t* prev) override;
  std::shared_ptr<std::atomic<bool>> alive_;
  // Mutex protects `map_' and `callbacksGetData_'
  std::mutex mutex_;
  state_map_t map_;
  std::vector<std::thread> callbacksGetData_;

  //////// New API ////////
 public:
  void getData(std::string path, data_callback_t cb) override;
  void exists(std::string path, stat_callback_t cb) override;
  void setData(std::string path,
               std::string data,
               stat_callback_t cb,
               zk::version_t base_version = -1) override;
  void create(std::string path,
              std::string data,
              create_callback_t cb,
              std::vector<zk::ACL> acl = zk::openACL_UNSAFE(),
              int32_t flags = 0) override;
  void multiOp(std::vector<zk::Op> ops, multi_op_callback_t cb) override;
  void sync(sync_callback_t cb) override;

  void createWithAncestors(std::string path,
                           std::string data,
                           create_callback_t cb,
                           std::vector<zk::ACL> acl = zk::openACL_UNSAFE(),
                           int32_t flags = 0) override;

 private:
  static Stat toCStat(const zk::Stat& stat);
  int mockSync(const char* znode_path,
               string_completion_t completion,
               const void* context);
};

class ZookeeperClientInMemoryFactory : public ZookeeperClientFactory {
 public:
  explicit ZookeeperClientInMemoryFactory(
      ZookeeperClientInMemory::state_map_t znodes)
      : znodes_(znodes) {}

  std::string identifier() const override {
    return "in-memory-zk";
  }

  std::string displayName() const override {
    return "In Memory ZK";
  }

  std::unique_ptr<ZookeeperClientBase>
  getClient(const configuration::ZookeeperConfig& config) override {
    return std::make_unique<ZookeeperClientInMemory>(
        config.getQuorumString(), znodes_);
  }

 private:
  ZookeeperClientInMemory::state_map_t znodes_;
};

}} // namespace facebook::logdevice
