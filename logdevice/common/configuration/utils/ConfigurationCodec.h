/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <zstd.h>

#include <folly/Range.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/configuration/utils/gen-cpp2/ConfigurationCodec_types.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/include/Err.h"
#include "thrift/lib/cpp2/protocol/BinaryProtocol.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook { namespace logdevice {

class ProtocolWriter;
class ProtocolReader;

namespace configuration {

/**
 * @file ConfigurationCodec is the template class that provides a thrift-based
 * serialiation wrapper for a configuration type with existing thrift
 * serialization definitions. It provides the additional features:
 *  1) support compression (currently zstd is supported);
 *  2) support extracting config version without fully deserializting the whole
 * config; 3) TODO: full protocol version support for implementing compatibility
 * that thrift is not able to handle (e.g., adding enum values)
 *
 * Type parameters:
 *   ConfigurationType: type of the configuration object. Must have a
 *                      getVersion() method that returns a version of
 *                      ConfigurationVersionType.
 *  ConfigurationVersionType: type of the config version. Must be 64 bit.
 *  ThriftConverter:    class that handles serde of the configuration from/to
 *                      thrift. must define the following method/alias:
 *                      1. define ThriftConfigType alias for the thrift type of
 *                         the configuraiton
 *                      2. ThriftConfigType toThrift(const ConfigurationType&);
 *                      3. std::shared_ptr<ConfigurationType> fromThrift(
 *                           const ThriftConfigType& obj);
 */

template <typename ConfigurationType,
          typename ConfigurationVersionType,
          typename ThriftConverter,
          uint32_t CURRENT_PROTO = 1>
class ConfigurationCodec {
 public:
  using ProtocolVersion = uint32_t;

  // Will be prepended to the serialized configuration for forward and
  // backward compatibility;
  //
  // Note: normally backward and forward compatibility should be handled
  // by thrift itself. This version is only needed when extra compatibility
  // handling (e.g., adding a new enum value of an existing enum class) is
  // needed.

  static constexpr ProtocolVersion CURRENT_PROTO_VERSION = CURRENT_PROTO;

  struct SerializeOptions {
    // use zstd to compress the configuration data blob
    bool compression;
  };

  /**
   * Serializes the object into the buffer handled by ProtocoWriter.
   *
   * @return  nothing is returned. But if there is an error on serialization,
   *          @param writer should enter error state (i.e., writer.error()
   *          == true).
   */
  static void serialize(const ConfigurationType& config,
                        ProtocolWriter& writer,
                        SerializeOptions options = {true});

  // convenience wrappers for serialization / deserialization with linear buffer
  // such as strings. If a serialization error occurs, returns an empty string.
  static std::string serialize(const ConfigurationType& config,
                               SerializeOptions options = {true});

  static std::string debugJsonString(const ConfigurationType& config);

  static std::shared_ptr<const ConfigurationType> deserialize(Slice buf);
  static std::shared_ptr<const ConfigurationType>
  deserialize(folly::StringPiece buf);

  // extract the configuration version from the thrift wrapper header without
  // deserialize the entire config
  static folly::Optional<ConfigurationVersionType>
  extractConfigVersion(folly::StringPiece serialized_data);

  static_assert(sizeof(ConfigurationVersionType) == 8,
                "ConfigurationVersionType must be 64 bit");

  static_assert(sizeof(thrift::ConfigurationCodecHeader::config_version) ==
                    sizeof(ConfigurationVersionType),
                "");
  static_assert(sizeof(thrift::ConfigurationCodecHeader::proto_version) ==
                    sizeof(ProtocolVersion),
                "");
};

} // namespace configuration
}} // namespace facebook::logdevice

#define LOGDEVICE_CONFIGURATION_CODEC_H_
#include "logdevice/common/configuration/utils/ConfigurationCodec-inl.h"
#undef LOGDEVICE_CONFIGURATION_CODEC_H_
