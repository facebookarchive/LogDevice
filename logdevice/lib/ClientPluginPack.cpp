/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/ClientPluginPack.h"

namespace facebook { namespace logdevice {

std::unique_ptr<ClientPluginPack> load_client_plugin(std::string* logstr_out) {
  return load_plugin_pack<ClientPluginPack>(
      "Client", "logdevice_client_plugin", logstr_out);
}

}} // namespace facebook::logdevice
