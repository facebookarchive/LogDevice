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

#include <folly/Function.h>
#include <zookeeper/zookeeper.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/UpdateableSharedPtr.h"

namespace facebook { namespace logdevice {

namespace zk {
using version_t = int32_t;

struct Stat {
  // version -1 has special meaning in Zookeeper
  version_t version_ = -0xbad;
};

struct Id {
  std::string scheme_;
  std::string id_;
};

struct ACL {
  int32_t perms_;
  Id id_;
};

inline std::vector<ACL> openACL_UNSAFE() {
  std::vector<ACL> rval(1);
  rval.at(0).perms_ = ZOO_PERM_ALL;
  rval.at(0).id_.scheme_ = "world";
  rval.at(0).id_.id_ = "anyone";
  return rval;
}

class CreateOp {
 public:
  explicit CreateOp(std::string path,
                    std::string data,
                    int32_t flags = 0,
                    std::vector<ACL> acl = openACL_UNSAFE())
      : path_(std::move(path)),
        data_(std::move(data)),
        acl_(std::move(acl)),
        flags_(flags) {}

 protected:
  std::string path_;
  std::string data_;
  std::vector<ACL> acl_;
  int32_t flags_;
};

class DeleteOp {
 public:
  explicit DeleteOp(std::string path, version_t version = -1)
      : path_(std::string(path)), version_(version) {}

 protected:
  std::string path_;
  version_t version_;
};

class SetOp {
 public:
  explicit SetOp(std::string path, std::string data, version_t version = -1)
      : path_(path), data_(data), version_(version) {}

 protected:
  std::string path_;
  std::string data_;
  version_t version_;
};

class CheckOp {
 public:
  explicit CheckOp(std::string path, version_t version = -1)
      : path_(path), version_(version) {}

 protected:
  std::string path_;
  version_t version_;
};

enum class OpType {
  CREATE,
  DELETE,
  SET,
  CHECK,
};

struct Op {
  OpType type_;
  union {
    CreateOp create_;
    DeleteOp delete_;
    SetOp set_;
    CheckOp check_;
  };
};

struct OpResponse {
  int rc_;
  std::string value_;
  Stat stat_;
};

} // namespace zk

/**
 * @file Facade Interface which abstracts Zookeeper API.
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

  //////// New API ////////
 public:
  // callbacks will only be called when the return code (first param of the
  // callback) matches the Zookeeper C API specification.
  using data_callback_t = folly::Function<void(int, std::string, zk::Stat)>;
  using stat_callback_t = folly::Function<void(int, zk::Stat)>;
  using create_callback_t = folly::Function<void(int, std::string)>;
  using multi_op_callback_t =
      folly::Function<void(int, std::vector<zk::OpResponse>)>;

  explicit ZookeeperClientBase() : quorum_() {}

  virtual int getData(std::string path, data_callback_t cb) = 0;
  virtual int setData(std::string path,
                      std::string data,
                      stat_callback_t cb,
                      zk::version_t base_version = -1) = 0;

  virtual int multiOp(std::vector<zk::Op> ops, multi_op_callback_t cb) = 0;
};

// Factory type used to create ZookeeperClient instances utilizing ServerConfig
// Decouples creation from usage, so gives possibility to
// use mock object in testing or easily switch implementations
using ZKFactory = std::function<std::unique_ptr<ZookeeperClientBase>(
    const ServerConfig& config)>;

}} // namespace facebook::logdevice
