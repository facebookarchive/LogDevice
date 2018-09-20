/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PluginPack.h"

namespace facebook { namespace logdevice {

class ClientPluginPack : public virtual PluginPack {
 public:
  virtual const char* description() const override {
    return "built-in client plugin";
  }

  virtual std::string getMyLocation() const {
    return "";
  }
};

// Internal: attempts to dynamically load the plugin.  Returns a
// default Plugin instance if the "logdevice_client_plugin" symbol is not
// found.
//
// If `logstr_out' is null (default), the function logs the outcome
// of plugin loading using the standard LogDevice logging framework.
// If non-null, the debug info is saved into the string instead (
// useful because this function can be called very early on, before
// the logging framework is initialised.)
std::unique_ptr<ClientPluginPack>
load_client_plugin(std::string* logstr_out = nullptr);

}} // namespace facebook::logdevice
