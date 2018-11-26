/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/SocketTypes.h"

namespace facebook { namespace logdevice {

const char* socketTypeToString(SocketType sock_type) {
  switch (sock_type) {
    case SocketType::DATA:
      return "DATA";
    case SocketType::GOSSIP:
      return "GOSSIP";
  }
  return "";
}

const char* connectionTypeToString(ConnectionType conn_type) {
  switch (conn_type) {
    case ConnectionType::NONE:
      return "NONE";
    case ConnectionType::PLAIN:
      return "PLAIN";
    case ConnectionType::SSL:
      return "SSL";
  }
  return "";
}
}} // namespace facebook::logdevice
