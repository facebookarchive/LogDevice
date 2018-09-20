/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ZookeeperClientInMemory.h"

#include "logdevice/common/debug.h"

#include <boost/filesystem.hpp>
#include <iostream>

namespace fs = boost::filesystem;

namespace {
std::vector<std::string> getParents(const std::string& str_path) {
  std::vector<std::string> res;
  fs::path path(str_path);
  while (!path.parent_path().empty() && path.parent_path().string() != "/") {
    path = path.parent_path();
    res.push_back(path.string());
  }
  ld_info("Output: %s", facebook::logdevice::toString(res).c_str());
  return res;
}

bool mapContainsParents(const std::map<std::string, std::string>& map,
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
ZookeeperClientInMemory::ZookeeperClientInMemory(
    std::string quorum,
    std::map<std::string, std::string> map)
    : ZookeeperClientBase(quorum), map_(std::move(map)) {
  // Creating parents for all the nodes in the map
  std::set<std::string> parent_nodes;
  for (const auto& node : map_) {
    auto parents = getParents(node.first);
    parent_nodes.insert(parents.begin(), parents.end());
  }
  for (const auto& parent : parent_nodes) {
    // if this path already exists, does nothing
    map_.emplace(parent, "");
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
                                     int,
                                     stat_completion_t completion,
                                     const void* data) {
  Stat stat;
  auto locked_operations = [&]() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (!mapContainsParents(map_, znode_path)) {
      return ZNONODE;
    }

    map_[znode_path].assign(znode_value, znode_value_size);
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
  if (map_.count(znode_path) == 0) {
    rc = ZNONODE;
  } else {
    rc = ZOK;
    value = map_[znode_path];
  }

  std::shared_ptr<std::atomic<bool>> alive = alive_;

  std::thread callback([rc, value, data, completion, alive]() {
    Stat stat;
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
    std::map<std::string, std::string> new_map = map_;

    // Checking the input and verifying that none of the nodes exist
    for (int i = 0; i < count; ++i) {
      if (ops[i].type != ZOO_CREATE_OP) {
        // no other ops supported currently
        ld_critical("Only create operations supported in multi-ops");
        ld_check(false);
        return -1;
      }
      const auto& op = ops[i].create_op;
      if (!mapContainsParents(new_map, op.path)) {
        return fill_result(ZNONODE);
      }
      if (new_map.find(op.path) != new_map.end()) {
        return fill_result(ZNODEEXISTS);
      }
      new_map[op.path].assign(op.data, op.datalen);
    }

    std::swap(map_, new_map);
    return fill_result(ZOK);
  };
  int rv = locked_operations();

  // Calling the completion without holding the lock
  completion(rv, data);
  return ZOK;
}

}} // namespace facebook::logdevice
