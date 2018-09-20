/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/noncopyable.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <zookeeper/zookeeper.h>
#include "logdevice/common/debug.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/UpdateableSharedPtr.h"

namespace facebook { namespace logdevice {

/**
 * @file Facade Interface which abstracts zookeeper API.
 *       Used for mocking zookeeper in tests.
 */
class ZookeeperClientBase : boost::noncopyable {
 public:
  explicit ZookeeperClientBase(std::string quorum) : quorum_(quorum) {
    ld_check(!quorum.empty());
  };

  virtual std::string getQuorum() {
    return quorum_;
  }

  virtual int state() = 0;

  virtual int setData(const char* znode_path,
                      const char* znode_value,
                      int znode_value_size,
                      int version,
                      stat_completion_t completion,
                      const void* data) = 0;

  virtual int getData(const char* znode_path,
                      data_completion_t completion,
                      const void* data) = 0;

  virtual int multiOp(int count,
                      const zoo_op_t* ops,
                      zoo_op_result_t* results,
                      void_completion_t,
                      const void* data) = 0;

  virtual ~ZookeeperClientBase(){};

 protected:
  const std::string quorum_; // see @param quorum in the constructor

 private:
  virtual int reconnect(zhandle_t* prev) = 0;
};

// Factory type used to create ZookeeperClient instances utilizing ServerConfig
// Decouples creation from usage, so gives possibility to
// use mock object in testing or easily switch implementations
using ZKFactory = std::function<std::unique_ptr<ZookeeperClientBase>(
    const ServerConfig& config)>;

}} // namespace facebook::logdevice
