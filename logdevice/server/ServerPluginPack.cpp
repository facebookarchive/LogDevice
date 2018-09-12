/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerPluginPack.h"

#include <dlfcn.h>

namespace facebook { namespace logdevice {

std::unique_ptr<ServerPluginPack> load_server_plugin(std::string* logstr_out) {
  std::string logstr;
  void* ptr = dlsym(RTLD_DEFAULT, "logdevice_server_plugin");
  std::unique_ptr<ServerPluginPack> rv;
  if (ptr != nullptr) {
    auto fnptr = reinterpret_cast<ServerPluginPack* (*)()>(ptr);
    rv.reset(fnptr());
    logstr = std::string("Server plugin loaded: ") + rv->description();
  } else {
    rv.reset(new ServerPluginPack());
    logstr = "No client plugin found";
  }
  if (logstr_out != nullptr) {
    *logstr_out = std::move(logstr);
  } else {
    ld_info("%s", logstr.c_str());
  }
  return rv;
}

std::unique_ptr<AdminServer>
ServerPluginPack::createAdminServer(Server* /* unused */) {
  ld_info("No AdminAPI server will be created!");
  return nullptr;
}

}} // namespace facebook::logdevice
