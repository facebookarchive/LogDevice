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
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include <boost/noncopyable.hpp>
#include <boost/variant.hpp>
#include <folly/Function.h>
#include <zookeeper/zookeeper.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {
namespace configuration {
class ZookeeperConfig;
}

namespace zk {
using version_t = int32_t;

struct Stat {
  // version -1 has special meaning in Zookeeper
  version_t version_ = -0xbad;
  // last modified timestamp of the znode from Zookeeper
  SystemTimestamp mtime_{std::chrono::milliseconds{0xbad}};
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

namespace detail {
// Use the ZookeeperClientBase::makeXXXOp helper functions below instead of
// directly constructing indificual Ops.
struct CreateOp {
  explicit CreateOp(std::string path,
                    std::string data,
                    int32_t flags,
                    std::vector<ACL> acl)
      : path_(std::move(path)),
        data_(std::move(data)),
        acl_(std::move(acl)),
        flags_(flags) {}

  std::string path_;
  std::string data_;
  std::vector<ACL> acl_;
  // TODO: instead of exposing flags directly, add isEphemeral and isSequential
  int32_t flags_;
};

class DeleteOp {
 public:
  explicit DeleteOp(std::string path, version_t version)
      : path_(std::string(path)), version_(version) {}

  std::string path_;
  version_t version_;
};

class SetOp {
 public:
  explicit SetOp(std::string path, std::string data, version_t version)
      : path_(path), data_(data), version_(version) {}

  std::string path_;
  std::string data_;
  version_t version_;
};

class CheckOp {
 public:
  explicit CheckOp(std::string path, version_t version)
      : path_(path), version_(version) {}

  std::string path_;
  version_t version_;
};

} // namespace detail

struct Op {
  enum class Type { NONE = 0, CREATE = 1, DELETE = 2, SET = 3, CHECK = 4 };
  Type getType() const {
    return static_cast<Type>(op_.which());
  }

  // TODO: replace with std::variant
  boost::variant<char,
                 detail::CreateOp,
                 detail::DeleteOp,
                 detail::SetOp,
                 detail::CheckOp>
      op_;
};

struct OpResponse {
  int rc_{-0xbad};
  std::string value_{};
  Stat stat_{};
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

  virtual ~ZookeeperClientBase() {}

 protected:
  const std::string quorum_; // see @param quorum in the constructor

 private:
  virtual int reconnect(zhandle_t* prev) = 0;

  //////// New API ////////
 public:
  // callbacks will only be called when the return code (first param of the
  // callback) matches the Zookeeper C API specification. It is valid to
  // pass in an empty function as a callback (to send a fire-and-forget type of
  // request).
  using data_callback_t = folly::Function<void(int, std::string, zk::Stat)>;
  using stat_callback_t = folly::Function<void(int, zk::Stat)>;
  using create_callback_t = folly::Function<void(int, std::string)>;
  using multi_op_callback_t =
      folly::Function<void(int, std::vector<zk::OpResponse>)>;
  using sync_callback_t = folly::Function<void(int)>;

  // Converts a Zookeeper return code to LogDevice status
  static Status toStatus(int zk_rc);
  // Helper functions for multiOp
  static zk::Op makeCreateOp(std::string path,
                             std::string data,
                             int32_t flags = 0,
                             std::vector<zk::ACL> acl = zk::openACL_UNSAFE());
  static zk::Op makeDeleteOp(std::string path, zk::version_t version = -1);
  static zk::Op makeSetOp(std::string path,
                          std::string data,
                          zk::version_t version = -1);
  static zk::Op makeCheckOp(std::string path, zk::version_t version = -1);

  explicit ZookeeperClientBase() : quorum_() {}

  virtual void getData(std::string path, data_callback_t cb) = 0;
  virtual void exists(std::string path, stat_callback_t cb) = 0;
  virtual void setData(std::string path,
                       std::string data,
                       stat_callback_t cb,
                       zk::version_t base_version = -1) = 0;
  virtual void create(std::string path,
                      std::string data,
                      create_callback_t cb,
                      std::vector<zk::ACL> acl = zk::openACL_UNSAFE(),
                      int32_t flags = 0) = 0;

  // Transactionally (atomically) perform multiple create / delete / set / check
  // operations, mostly used for read-modify-write. Unlikely to be what you
  // want.
  virtual void multiOp(std::vector<zk::Op> ops, multi_op_callback_t cb) = 0;

  // Flush leader channel, used before a read to force linearizability; should
  // only be used sparingly.
  //
  // If you do not know what this API does, you probably do not need it.
  virtual void sync(sync_callback_t cb) = 0;

  virtual void close() {}

  //////// RECIPES ////////
  // The following methods are implemented on top of the Zookeeper primitives
  // and may consist of multiple ZK operations / RPCs / transactions and thus
  // may not obey the ordering guarantees one normally expects from Zookeeper.

  // createWithAncestors creates the parent nodes as needed. E.g.,
  // createWithAncestors("/a/b/c", ...) will create znodes "/a" and "/a/b" if
  // they did not exist. The callback will be invoked with the created path of
  // the leaf znode ("/a/b/c").
  //
  // The ancestor znodes will be created using the same ACL as the leaf node.
  // The ancestors will NOT be created with any flags.
  //
  // The create callback is guaranteed to be invoked. The user must guarantee
  // that the ZookeeperClientBase object is alive until then (e.g., by capturing
  // a shared pointer in the callback).
  virtual void
  createWithAncestors(std::string path,
                      std::string data,
                      create_callback_t cb,
                      std::vector<zk::ACL> acl = zk::openACL_UNSAFE(),
                      int32_t flags = 0) = 0;
};

// Factory type used to create ZookeeperClient instances utilizing
// ZookeeperConfig. Decouples creation from usage, so gives possibility to
// use mock object in testing or easily switch implementations
using ZKFactory = std::function<std::unique_ptr<ZookeeperClientBase>(
    const facebook::logdevice::configuration::ZookeeperConfig& config)>;

}} // namespace facebook::logdevice
