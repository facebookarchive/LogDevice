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
  // Creating parents for all the nodes in the map
  std::set<std::string> parent_nodes;
  for (const auto& node : map_) {
    auto parents = getParents(node.first);
    parent_nodes.insert(parents.begin(), parents.end());
  }
  for (const auto& parent : parent_nodes) {
    // if this path already exists, does nothing
    map_.emplace(parent, std::make_pair("", zk::Stat{.version_ = 0}));
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

    stat.version = old_version + 1;
    it->second.first = std::string(znode_value, znode_value_size);
    it->second.second = zk::Stat{.version_ = stat.version};
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
    Stat stat;
    stat.version = zk_stat.version_;
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
    state_map_t new_map = map_;

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
      new_map[op.path] = std::make_pair(
          std::string(op.data, op.datalen), zk::Stat{.version_ = 0});
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

}} // namespace facebook::logdevice
