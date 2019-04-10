/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook { namespace logdevice {

/**
 * A class to convert any thrift structures to/from strings using a certain
 * Serializer. The serializer can, for example, be
 * apache::thrift::BinarySerializer or apache::thrift::SimpleJSONSerializer.
 */
class ThriftCodec {
 public:
  template <class Serializer, class T>
  static std::string serialize(T thrift) {
    return Serializer::template serialize<std::string>(thrift);
  }

  template <class Serializer, class T>
  static std::shared_ptr<T> deserialize(Slice binary) {
    std::shared_ptr<T> thrift_ptr{nullptr};
    try {
      auto thrift = Serializer::template deserialize<T>(
          folly::StringPiece(binary.ptr(), binary.size));
      thrift_ptr = std::make_shared<T>(std::move(thrift));
    } catch (const std::exception& exception) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Failed to deserialize thrift as %s: %s",
                      typeid(T).name(),
                      exception.what());
    }
    return thrift_ptr;
  }
};

}} // namespace facebook::logdevice
