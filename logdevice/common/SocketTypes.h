/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

namespace facebook { namespace logdevice {

enum class SocketType : uint8_t { DATA, GOSSIP };
enum class ConnectionType : uint8_t { NONE, PLAIN, SSL };

const char* socketTypeToString(SocketType sock_type);
const char* connectionTypeToString(ConnectionType conn_type);

}} // namespace facebook::logdevice
