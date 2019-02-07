/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <folly/IntrusiveList.h>
#include <folly/Memory.h>
#include <folly/hash/Hash.h>
#include <zookeeper/zookeeper.h>

#include "logdevice/common/ConfigSource.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"

/**
 * @file Config source that gets configs from Zookeeper.  It can talk to
 * multiple Zookeeper clusters; each path needs to include the quorum.
 */

namespace facebook { namespace logdevice {

class ZookeeperClientBase;

class ZookeeperConfigSource : public ConfigSource {
 public:
  std::string getName() override {
    return "Zookeeper";
  }
  std::vector<std::string> getSchemes() override {
    return {"zookeeper", "zk"};
  }

  static std::chrono::milliseconds defaultPollingInterval() {
    return std::chrono::seconds(1);
  }

  Status getConfig(const std::string& quorum_path, Output* out) override;

  explicit ZookeeperConfigSource(
      std::chrono::milliseconds retry_delay,
      std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory,
      std::string uri_scheme = configuration::ZookeeperConfig::URI_SCHEME_IP);
  explicit ZookeeperConfigSource(
      std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory,
      std::string uri_scheme = configuration::ZookeeperConfig::URI_SCHEME_IP);
  ZookeeperConfigSource() = delete;
  ~ZookeeperConfigSource() override;

 private:
  // Protects all data structures, ensures we only run one callback at a
  // time in case we're running with multithreaded Zookeeper.
  std::mutex mutex_;
  // Owns the Zookeeper connections (keyed by quorum string)
  std::unordered_map<std::string, std::unique_ptr<ZookeeperClientBase>>
      zkclients_;
  // Background thread for polling and retrying.
  const std::chrono::milliseconds polling_delay_;
  class BackgroundThread;
  std::unique_ptr<BackgroundThread> bg_thread_;

  // Keyed by quorum + path, used by polling to determine when a znode has
  // changed and the contents should be refetched
  std::unordered_map<std::string, int64_t> delivered_versions_;

  std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory_;

  // URI Scheme, default value is "ip"
  std::string uri_scheme_;
  const int ZK_SESSION_TIMEOUT_SEC = 10;

  // Track all inflight requests, to delete any outstanding ones in the
  // destructor.
  struct RequestContext {
    ZookeeperConfigSource* parent;
    std::string quorum;
    std::string path;
    bool with_data;
    folly::IntrusiveListHook list_hook;

    RequestContext(ZookeeperConfigSource* parent_in,
                   std::string quorum_in,
                   std::string path_in,
                   bool with_data_in)
        : parent(parent_in),
          quorum(std::move(quorum_in)),
          path(std::move(path_in)),
          with_data(with_data_in) {}

    std::unique_ptr<RequestContext> clone() const {
      return std::make_unique<RequestContext>(parent, quorum, path, with_data);
    }
  };
  folly::IntrusiveList<RequestContext, &RequestContext::list_hook>
      requests_in_flight_;

  ZookeeperClientBase* getClient(const std::string& quorum);
  // Kicks off a zoo_aget() or zoo_aexists() (depending on `with_data')
  // request for the specified quorum+path
  int requestZnode(const std::string& quorum,
                   const std::string& path,
                   bool with_data);
  static void dataCompletionCallback(int rc,
                                     const char* value,
                                     int value_len,
                                     const struct ::Stat* stat,
                                     const void* context_void);
  static void statCompletionCallback(int rc,
                                     const struct ::Stat* stat,
                                     const void* context_void);

  // Accessor for `bg_thread_' with lazy initialization.  Mutex should not
  // be held.
  BackgroundThread& bgThread();
};

}} // namespace facebook::logdevice
