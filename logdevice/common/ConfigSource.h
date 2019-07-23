/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file Interface for sources of config data.  A `ConfigSource' instance
 * interacts with a class of storage (e.g. files, Zookeeper etc) to fetch
 * config contents as needed.
 *
 * This is used in `TextConfigUpdater` for fetching ServerConfig and in
 * `NodesConfigurationInit` to parse server seed.
 */
class ConfigSource {
 public:
  /**
   * Name for this config source, used for logging.
   */
  virtual std::string getName() = 0;

  /**
   * Schemes this source should be invoked for.
   *
   * For example, if this returns "file", `TextConfigUpdater' will use
   * this config source to fetch any config locations starting with "file:".
   */
  virtual std::vector<std::string> getSchemes() = 0;

  /**
   * Output of a ConfigSource; the text contents of a config with a bit of
   * metadata on top.
   */
  struct Output {
    std::string contents;
    std::string hash;
    std::chrono::milliseconds mtime; // Since Unix epoch
  };

  /**
   * Invoked to request config to be fetched. The `path' parameter does not
   * include the scheme. The fetched data can be in any format depending on the
   * content of the path and the usecase. For example, for `TextConfigUpdater`
   * usecase it's usually a plain JSON or a gzipped JSON.
   *
   * If possible, the source should start tracking the path for changes on a
   * separate thread, and invoke `parent_->onAsyncGet()' whenever the config
   * contents change.
   *
   * If the config is immediately available, place its contents into
   * `out' and return E::OK.  If the source wishes to provide a
   * specific hash (e.g. a source control hash) for the config, it can also
   * populate `hash_out'.
   *
   * If the source wishes to asynchronously fetch the config contents, this
   * should return E::NOTREADY and subsequently call
   * `parent_->onAsyncGet()' on *another* thread.
   *
   * On error, return an appropriate error status.
   */
  virtual Status getConfig(const std::string& path, Output* out) = 0;

  class AsyncCallback {
   public:
    virtual void onAsyncGet(ConfigSource* source,
                            const std::string& path,
                            Status status,
                            Output output) = 0;
    virtual ~AsyncCallback() {}
  };

  void setAsyncCallback(AsyncCallback* async_cb) {
    async_cb_ = async_cb;
  }

  virtual ~ConfigSource() {}

 protected:
  AsyncCallback* async_cb_;
};

}} // namespace facebook::logdevice
