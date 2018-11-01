/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ConfigSource.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {

class Processor;
class UpdateableConfig;

/**
 * @file Config source that gets configs from LogDevice servers. Can accept
 * multiple seed hosts in the config path. The config path should be of the
 * form "server:<host1>,<host2>,<host3>...".
 */

class ServerConfigSource : public ConfigSource {
 public:
  explicit ServerConfigSource() {}
  ~ServerConfigSource() override {
    // The local processor needs to shutdown its workers first, before anything
    // else gets destroyed
    processor_.reset();
  }

  std::string getName() override {
    return "server";
  }
  std::vector<std::string> getSchemes() override {
    return {"server"};
  }

  Status getConfig(const std::string& path, Output* out) override;

 private:
  std::shared_ptr<Processor> processor_;
  std::shared_ptr<UpdateableConfig> config_;
  ConfigSubscriptionHandle server_config_subscription_;

  void init(const std::string& path, const std::vector<std::string>& hosts);
  bool fetch(const std::string& host);
};

}} // namespace facebook::logdevice
