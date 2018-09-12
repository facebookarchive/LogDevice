/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice {

enum class SocketType : uint8_t { DATA, SSL, GOSSIP };

class Socket;

}} // namespace facebook::logdevice
