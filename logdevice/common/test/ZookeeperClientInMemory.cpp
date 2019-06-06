/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/ZookeeperClientInMemory.h"

#include <iostream>
#include <set>
#include <vector>

#include <boost/filesystem.hpp>
#include <folly/futures/Future.h>

#include "logdevice/common/debug.h"

namespace fs = boost::filesystem;

namespace {
std::vector<std::string> getParents(const std::string& str_path) {
  std::vector<std::string> res;
  fs::path path(str_path);
  while (!path.parent_path().empty() && path.parent_path().string() != "/") {
    path = path.parent_path();
    res.push_back(path.string());
  }
  // ld_info("Output: %s", facebook::logdevice::toString(res).c_str());
  return res;
}

bool mapContainsParents(
    const facebook::logdevice::ZookeeperClientInMemory::state_map_t& map,
    const char* znode_path) {
  auto parents = getParents(znode_path);
  for (const auto& parent : parents) {
    if (map.find(parent) == map.end()) {
      ld_spew("Missing parent %s for path %s", parent.c_str(), znode_path);
      return false;
    }
  }
  return true;
}
} // namespace

namespace facebook { namespace logdevice {
ZookeeperClientInMemory::ZookeeperClientInMemory(std::string quorum,
                                                 state_map_t map)
    : ZookeeperClientBase(quorum), map_(std::move(map)) {
  auto mtime = SystemTimestamp::now();
  // The root znode always exists
  map_.emplace(
      "/", std::make_pair("", zk::Stat{.version_ = 0, .mtime_ = mtime}));
  // Creating parents for all the nodes in the map
  std::set<std::string> parent_nodes;
  for (const auto& node : map_) {
    auto parents = getParents(node.first);
    parent_nodes.insert(parents.begin(), parents.end());
  }
  for (const auto& parent : parent_nodes) {
    // if this path already exists, does nothing
    map_.emplace(
        parent, std::make_pair("", zk::Stat{.version_ = 0, .mtime_ = mtime}));
  }

  alive_ = std::make_shared<std::atomic<bool>>(true);
}

ZookeeperClientInMemory::~ZookeeperClientInMemory() {
  *alive_ = false;
  for (auto& thread : callbacksGetData_) {
    thread.join();
  }
}

int ZookeeperClientInMemory::reconnect(zhandle_t*) {
  return ZOK;
}

int ZookeeperClientInMemory::state() {
  return ZOK;
}

int ZookeeperClientInMemory::setData(const char* znode_path,
                                     const char* znode_value,
                                     int znode_value_size,
                                     int version,
                                     stat_completion_t completion,
                                     const void* data) {
  Stat stat;
  auto locked_operations = [&]() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (!mapContainsParents(map_, znode_path)) {
      return ZNONODE;
    }

    auto it = map_.find(znode_path);
    if (it == map_.end()) {
      return ZNONODE;
    }

    auto old_version = it->second.second.version_;
    if (old_version != version && version != -1) {
      // conditional update
      return ZBADVERSION;
    }

    it->second.first = std::string(znode_value, znode_value_size);
    it->second.second =
        zk::Stat{.version_ = old_version + 1, .mtime_ = SystemTimestamp::now()};
    stat = toCStat(it->second.second);
    return ZOK;
  };
  int rv = locked_operations();

  // Calling the completion function without holding the lock
  completion(rv, &stat, data);
  return ZOK;
}

int ZookeeperClientInMemory::getData(const char* znode_path,
                                     data_completion_t completion,
                                     const void* data) {
  std::lock_guard<std::mutex> guard(mutex_);

  int rc;
  std::string value;
  zk::Stat zk_stat{};
  auto it = map_.find(znode_path);
  if (it == map_.end()) {
    rc = ZNONODE;
    // zk_stat should be garbage data
  } else {
    rc = ZOK;
    value = it->second.first;
    zk_stat = it->second.second;
  }

  std::shared_ptr<std::atomic<bool>> alive = alive_;

  std::thread callback([rc, value, zk_stat, data, completion, alive]() {
    Stat stat = toCStat(zk_stat);
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (alive.get()->load()) {
      completion(rc, value.data(), value.size(), &stat, data);
    } else {
      completion(ZCLOSING, nullptr, 0, &stat, data);
    }
  });
  callbacksGetData_.push_back(std::move(callback));
  return ZOK;
}

int ZookeeperClientInMemory::exists(const char* znode_path,
                                    stat_completion_t completion,
                                    const void* data) {
  std::lock_guard<std::mutex> guard(mutex_);

  int rc;
  zk::Stat zk_stat{};
  auto it = map_.find(znode_path);
  if (it == map_.end()) {
    rc = ZNONODE;
  } else {
    rc = ZOK;
    zk_stat = it->second.second;
  }

  std::shared_ptr<std::atomic<bool>> alive = alive_;

  std::thread callback([rc, zk_stat, data, completion, alive]() {
    Stat stat = toCStat(zk_stat);
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (alive.get()->load()) {
      completion(rc, &stat, data);
    } else {
      completion(ZCLOSING, &stat, data);
    }
  });
  callbacksGetData_.push_back(std::move(callback));
  return ZOK;
}

int ZookeeperClientInMemory::multiOp(int count,
                                     const zoo_op_t* ops,
                                     zoo_op_result_t* results,
                                     void_completion_t completion,
                                     const void* data) {
  auto fill_result = [&](int rv) {
    for (int i = 0; i < count; ++i) {
      results[i].err = rv;
      results[i].value = nullptr;
      results[i].valuelen = 0;
      results[i].stat = nullptr;
    }
    return rv;
  };

  auto locked_operations = [&]() {
    std::lock_guard<std::mutex> guard(mutex_);

    // This map contains the copy of the entire epoch store in order to
    // easily validate whether a given operation dependent on a preceding
    // operation within the same batch succeeds or not
    state_map_t new_map = map_;
    auto mtime = SystemTimestamp::now();

    for (int i = 0; i < count; ++i) {
      if (ops[i].type == ZOO_CREATE_OP) {
        // Checking the input and verifying that the node exist
        const auto& op = ops[i].create_op;
        if (!mapContainsParents(new_map, op.path)) {
          return fill_result(ZNONODE);
        }
        if (new_map.find(op.path) != new_map.end()) {
          return fill_result(ZNODEEXISTS);
        }
        new_map[op.path] =
            std::make_pair(std::string(op.data, op.datalen),
                           zk::Stat{.version_ = 0, .mtime_ = mtime});

      } else if (ops[i].type == ZOO_DELETE_OP) {
        const auto& op = ops[i].delete_op;
        if (!mapContainsParents(new_map, op.path)) {
          return fill_result(ZNONODE);
        }
        if (new_map.find(op.path) != new_map.end()) {
          new_map.erase(op.path);
        } else {
          return fill_result(ZNONODE);
        }
      } else {
        // no other ops supported currently
        ld_critical("Only create/delete operations supported in multi-ops");
        ld_check(false);
        return -1;
      }
    }

    std::swap(map_, new_map);
    return fill_result(ZOK);
  };
  int rv = locked_operations();

  // Calling the completion without holding the lock
  completion(rv, data);
  return ZOK;
}

void ZookeeperClientInMemory::getData(std::string path, data_callback_t cb) {
  // Use the callback function object as context, which must be freed in
  // completion. The callback could also be empty.
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<data_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = getData(path.data(), &ZookeeperClient::getDataCompletion, context);
  if (rc != ZOK) {
    ZookeeperClient::getDataCompletion(rc,
                                       /* value = */ nullptr,
                                       /* value_len = */ 0,
                                       /* stat = */ nullptr,
                                       context);
  }
}

void ZookeeperClientInMemory::exists(std::string path, stat_callback_t cb) {
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<stat_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = exists(path.data(), &ZookeeperClient::existsCompletion, context);
  if (rc != ZOK) {
    ZookeeperClient::existsCompletion(rc,
                                      /* stat = */ nullptr,
                                      context);
  }
}

void ZookeeperClientInMemory::setData(std::string path,
                                      std::string data,
                                      stat_callback_t cb,
                                      zk::version_t base_version) {
  // Use the callback function object as context, which must be freed in
  // completion. The callback could also be empty.
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<stat_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = setData(path.data(),
                   data.data(),
                   data.size(),
                   base_version,
                   &ZookeeperClient::setDataCompletion,
                   context);
  if (rc != ZOK) {
    ZookeeperClient::setDataCompletion(rc, /* stat = */ nullptr, context);
  }
}

void ZookeeperClientInMemory::create(std::string path,
                                     std::string data,
                                     create_callback_t cb,
                                     std::vector<zk::ACL> acl,
                                     int32_t flags) {
  auto op = ZookeeperClientBase::makeCreateOp(
      std::move(path), std::move(data), flags, std::move(acl));
  multiOp({std::move(op)},
          [cb = std::move(cb)](
              int rc, std::vector<zk::OpResponse> responses) mutable {
            ld_check_eq(1, responses.size());
            if (!cb) {
              return;
            }
            if (rc == ZOK) {
              cb(rc, std::move(responses.front().value_));
            } else {
              cb(rc, "");
            }
          });
}

void ZookeeperClientInMemory::multiOp(std::vector<zk::Op> ops,
                                      multi_op_callback_t cb) {
  int count = ops.size();
  if (count == 0) {
    cb(ZOK, {});
    return;
  }

  auto p = std::make_unique<ZookeeperClient::MultiOpContext>(
      std::move(ops), std::move(cb));
  ZookeeperClient::MultiOpContext* context = p.release();
  int rc = multiOp(count,
                   context->c_ops_.data(),
                   context->c_results_.data(),
                   &ZookeeperClient::multiOpCompletion,
                   static_cast<const void*>(context));
  if (rc != ZOK) {
    ZookeeperClient::multiOpCompletion(rc, static_cast<const void*>(context));
  }
}

/* static */ Stat ZookeeperClientInMemory::toCStat(const zk::Stat& stat) {
  Stat ret;
  ret.version = stat.version_;
  ret.mtime = stat.mtime_.toMilliseconds().count();
  return ret;
}

int ZookeeperClientInMemory::mockSync(const char* /* unused */,
                                      string_completion_t completion,
                                      const void* context) {
  // Pretend to take the lock to get linearizability
  std::lock_guard<std::mutex> guard(mutex_);

  int rc = ZOK;
  std::shared_ptr<std::atomic<bool>> alive = alive_;

  std::thread callback([rc, context, completion, alive]() {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (alive.get()->load()) {
      completion(rc, nullptr, context);
    } else {
      completion(ZCLOSING, nullptr, context);
    }
  });
  // reuse the callbacks for getData
  callbacksGetData_.push_back(std::move(callback));
  return ZOK;
}

void ZookeeperClientInMemory::sync(sync_callback_t cb) {
  // sync() should be a no-op for the in memory implementation, but to get test
  // coverage on ZookeeperClient::syncCompletion, we still pretend to do an RPC.

  // Use the callback function object as context, which must be freed in
  // completion. The callback could also be empty.
  const void* context = nullptr;
  if (cb) {
    auto p = std::make_unique<sync_callback_t>(std::move(cb));
    context = p.release();
  }
  int rc = mockSync("/", &ZookeeperClient::syncCompletion, context);
  if (rc != ZOK) {
    ZookeeperClient::syncCompletion(rc, /* value = */ nullptr, context);
  }
}

void ZookeeperClientInMemory::createWithAncestors(std::string path,
                                                  std::string data,
                                                  create_callback_t cb,
                                                  std::vector<zk::ACL> acl,
                                                  int32_t flags) {
  auto ancestor_paths = ZookeeperClient::getAncestorPaths(path);

  std::vector<folly::SemiFuture<ZookeeperClient::CreateResult>> futures;
  futures.reserve(ancestor_paths.size());
  std::transform(
      ancestor_paths.rbegin(),
      ancestor_paths.rend(),
      std::back_inserter(futures),
      [this, acl](std::string& ancestor_path) {
        auto promise =
            std::make_unique<folly::Promise<ZookeeperClient::CreateResult>>();
        auto fut = promise->getSemiFuture();
        this->create(
            ancestor_path,
            /* data = */ "",
            [promise = std::move(promise)](int rc, std::string resulting_path) {
              ZookeeperClient::CreateResult res;
              res.rc_ = rc;
              if (rc == ZOK) {
                res.path_ = std::move(resulting_path);
              }
              promise->setValue(std::move(res));
            },
            acl,
            /* flags = */ 0);
        return fut;
      });

  folly::collectAllSemiFuture(std::move(futures))
      .toUnsafeFuture()
      .thenTry([this,
                path = std::move(path),
                data = std::move(data),
                cb = std::move(cb),
                acl = std::move(acl),
                flags](auto&& t) mutable {
        int rc = ZookeeperClient::aggregateCreateAncestorResults(std::move(t));
        if (rc != ZOK) {
          if (cb) {
            cb(rc, /* path = */ "");
          }
          return;
        }

        this->create(std::move(path),
                     std::move(data),
                     std::move(cb),
                     std::move(acl),
                     flags);
      });
}
}} // namespace facebook::logdevice
