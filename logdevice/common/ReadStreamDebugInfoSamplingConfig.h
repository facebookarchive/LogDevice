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
#include <string>

#include <folly/Function.h>
#include <folly/Synchronized.h>

#include "logdevice/common/ConfigSource.h"
#include "logdevice/common/configuration/if/gen-cpp2/AllReadStreamsDebugConfig_types.h"
#include "logdevice/common/plugin/ConfigSourceFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"

namespace facebook { namespace logdevice {

/**
 * Global configs for all read streams debug info sampling. Fetches configs
 * every time they are updated and caches it locally. Read streams can check
 * whether they are allowed to sample their debug info or not. Permission of
 * sampling is given by searching the Client Session ID(CSID) in the allowed
 * list if CSIDs. Each config has also its own expiration deadline in UTC
 * timestamp
 */
class ReadStreamDebugInfoSamplingConfig {
 public:
  using Callback =
      folly::Function<void(const configuration::all_read_streams_debug_config::
                               thrift::AllReadStreamsDebugConfigs&)>;

  ReadStreamDebugInfoSamplingConfig(
      std::shared_ptr<PluginRegistry>,
      const std::string& all_read_streams_debug_config_path);

  ~ReadStreamDebugInfoSamplingConfig() {
    unsetUpdateCallback();
  }

  bool
  isReadStreamDebugInfoSamplingAllowed(const std::string& csid,
                                       std::chrono::seconds current_time) const;

  bool isReadStreamDebugInfoSamplingAllowed(const std::string& csid) const;

  /*
   * Sets callback to be notified whenever configuration changes.
   * If the config is already available when this function is called,
   * the callback is immediatelly called, otherwise callback is called
   * from the appropriate ConfigSource's thread.
   * If there is an update being processed by the previous callback,
   * this call blocks till that operation is done.
   */
  void setUpdateCallback(Callback&& callback);
  void unsetUpdateCallback();

 private:
  void updateCallback(Status, ConfigSource::Output);

 private:
  std::unique_ptr<ConfigSource::AsyncCallback> all_read_streams_debug_cb_;
  std::unique_ptr<ConfigSource> source_;
  folly::Synchronized<
      std::unique_ptr<configuration::all_read_streams_debug_config::thrift::
                          AllReadStreamsDebugConfigs>>
      configs_;

  Callback updateCallback_ = [](auto&&...) {};
};

}} // namespace facebook::logdevice
