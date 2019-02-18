/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ConfigSource.h"

namespace facebook { namespace logdevice {

// Allows passing the config directly in the path string.
// For example: "data:{...}".
class DataConfigSource : public ConfigSource {
 public:
  virtual ~DataConfigSource() override = default;

  virtual std::string getName() override {
    return "data";
  }
  virtual std::vector<std::string> getSchemes() override {
    return {"data"};
  }

  virtual Status getConfig(const std::string& path,
                           ConfigSource::Output* out) override {
    out->contents = path;
    return Status::OK;
  }
};

}} // namespace facebook::logdevice
