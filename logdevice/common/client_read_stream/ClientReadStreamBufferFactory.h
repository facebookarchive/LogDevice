/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/client_read_stream/ClientReadStreamCircularBuffer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamOrderedMapBuffer.h"

namespace facebook { namespace logdevice {

/**
 * @file  create a ClientReadStreamBuffer with the specified type
 */

enum class ClientReadStreamBufferType : uint8_t {
  CIRCULAR = 0,
  ORDERED_MAP,
};

class ClientReadStreamBufferFactory {
 public:
  static std::unique_ptr<ClientReadStreamBuffer>
  create(ClientReadStreamBufferType type, size_t capacity, lsn_t buffer_head) {
    ld_check(capacity > 0);
    switch (type) {
      case ClientReadStreamBufferType::CIRCULAR:
        return std::make_unique<ClientReadStreamCircularBuffer>(
            capacity, buffer_head);
      case ClientReadStreamBufferType::ORDERED_MAP:
        return std::make_unique<ClientReadStreamOrderedMapBuffer>(
            capacity, buffer_head);
    }

    ld_check(false);
    return nullptr;
  }
};

}} // namespace facebook::logdevice
