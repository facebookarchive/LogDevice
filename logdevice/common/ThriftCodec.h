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
#include "logdevice/include/Err.h"
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
  static std::string serialize(const T& thrift) {
    return Serializer::template serialize<std::string>(thrift);
  }

  template <class Serializer, class T, class... Args>
  static void serialize(const T& thrift, Args&&... args) {
    Serializer::template serialize(thrift, std::forward<Args>(args)...);
  }

  template <class Serializer, class T>
  static std::unique_ptr<T> deserialize(const Slice& binary) {
    T thrift;
    size_t consumed = deserialize<Serializer>(binary, thrift);
    if (consumed == 0) {
      return nullptr;
    } else {
      return std::make_unique<T>(std::move(thrift));
    }
  }

  /**
   * Deserializes object from binary representation.
   * Returns number of bytes consumed or 0 in case of error.
   */
  template <class Serializer, class T, class... Args>
  static size_t deserialize(const Slice& binary, T& thrift, Args&&... args) {
    try {
      return Serializer::template deserialize<T>(
          folly::StringPiece(binary.ptr(), binary.size),
          thrift,
          std::forward<Args>(args)...);
    } catch (const std::exception& exception) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Failed to deserialize thrift as %s: %s",
                      folly::demangle(typeid(T).name()).c_str(),
                      exception.what());
      err = E::BADMSG;
    }
    return 0;
  }
};

}} // namespace facebook::logdevice
