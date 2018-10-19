/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/configuration/logs/CodecType.h"

namespace facebook { namespace logdevice { namespace logsconfig {

using logsconfig_codec_version_t = uint8_t;

/*
 * This class has a bunch of serialize and deserialize methods that can convert
 * the different data structures in the LogsConfigTree into its corresponding
 * flatbuffers representations and vice versa. The declaration of the templated
 * functions allow to extend this easily to deserialize new sub-types and by
 * recursively calling other deserializers when witnessing other types we end up
 * deserializing the whole tree.
 *
 * The Codec is just an interface, two independent files see
 * FBuffersLogsConfig.h for implementation example(s).
 */

template <CodecType CODEC_TYPE>
class LogsConfigCodec {
 public:
  /**
   * takes an object and serialize it to payload
   * the codec_type picks the correct encoding algorithm at compile-time
   *
   * In is the type you want to serialize to, the availability of
   * whether there exists a serializer for your type or not depends on whether
   * you have a specialization for this template for your type or not.
   *
   * @param flatten will force serializers to serialize all inherited
   *                LogAttributes.
   *
   */
  template <typename In>
  static facebook::logdevice::PayloadHolder serialize(const In& in,
                                                      bool flatten);

  /**
   * Deserialize a Payload (without owning its memory life-cycle) into an object
   * Out is the desination type you want to deserialize to, the availability of
   * whether there exists a deserializer for your type or not depends on whether
   * you have a specialization for this template for your type or not.
   */
  template <typename Out>
  static std::unique_ptr<Out>
  deserialize(const facebook::logdevice::Payload& payload,
              const std::string& delimiter);
};

}}} // namespace facebook::logdevice::logsconfig
