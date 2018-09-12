/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include <folly/Synchronized.h>

#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/lib/shadow/Shadow.h"

/**
 * @file Handles creation and use of Client objects used for appending to
 *       shadow clusters. The factory maintains a map of destination URLs to
 *       their associated client objects, and provides a method for creating the
 *       client object asynchronously if it doesn't yet exist.
 */

namespace facebook { namespace logdevice {

class Shadow;
class ShadowClient;

class ShadowClientFactory {
 public:
  ShadowClientFactory(std::string origin_name, StatsHolder* stats);

  virtual ~ShadowClientFactory();

  /**
   * Starts the background thread for initializing client objects.
   * @param client_timeout will be passed to each Client::create().
   */
  virtual void start(std::chrono::milliseconds client_timeout);

  virtual void shutdown();

  /**
   * Retrieves a ShadowClient object for the given shadow destination cluster.
   * There will always be at most one ShadowClient object for each destination.
   * Will return null if the client hasn't yet been created with createAsync.
   */
  virtual std::shared_ptr<ShadowClient>
  get(const std::string& destination) const;

  /**
   * Creates shadow client on background thread for given destination cluster,
   * only if not already existing or being created.
   *
   * @return 0 on success, -1 on failure with err set
   */
  virtual int createAsync(const Shadow::Attrs& attrs);

  /**
   * Called when shadowing disabled, to free resources. Will first shutdown if
   * background thread is currently running.
   */
  void reset();

 private:
  using Mutex = std::mutex;

  void clientInitThreadMain();

  const std::string origin_name_;
  StatsHolder* const stats_;
  std::chrono::milliseconds client_timeout_{0};

  // Stores clients used for shadowing, key is shadow destination
  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<ShadowClient>>>
      client_map_;

  // Handles initializing clients on background thread
  std::thread client_init_thread_;
  std::queue<Shadow::Attrs> client_init_queue_;
  Mutex client_init_mutex_;
  std::condition_variable client_init_cv_;
  bool shutdown_ = false;
};

class ShadowClient {
 public:
  /**
   * Creates a shadow client object backed by a Client object used for shadow
   * appends. Runs on current thread, so should only be used by the factory.
   */
  static std::shared_ptr<ShadowClient> create(const std::string& origin_name,
                                              const Shadow::Attrs& attrs,
                                              std::chrono::milliseconds timeout,
                                              StatsHolder* stats);

  ~ShadowClient();

  int append(logid_t logid,
             const Payload& payload,
             AppendAttributes attrs,
             bool buffered_writer_blob) noexcept;

 private:
  ShadowClient(std::shared_ptr<Client> client,
               const Shadow::Attrs& attrs,
               StatsHolder* stats);

  void appendCallback(Status status, const DataRecord& record);

  std::shared_ptr<Client> client_;
  Shadow::Attrs shadow_attrs_;
  StatsHolder* stats_;
  std::chrono::milliseconds client_timeout_{0};
};

}} // namespace facebook::logdevice
