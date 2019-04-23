/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Range.h>

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/configuration/nodes/gen-cpp2/NodesConfiguration_types.h"

namespace facebook { namespace logdevice {

class ProtocolWriter;
class ProtocolReader;

namespace configuration { namespace nodes {

class NodesConfigurationCodec {
 public:
  using ProtocolVersion = uint32_t;

  // Will be prepended to the serialized nodes configuration for forward and
  // backward compatibility;
  //
  // Note: normally backward and forward compatibility should be handled
  // by flatbuffers itself. This version is only needed when extra compatibility
  // handling (e.g., adding a new enum value of an existing enum class) is
  // needed.

  static constexpr ProtocolVersion CURRENT_PROTO_VERSION = 1;

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
  static void serialize(const NodesConfiguration& nodes_config,
                        ProtocolWriter& writer,
                        SerializeOptions options = {true});

  // convenience wrappers for serialization / deserialization with linear buffer
  // such as strings. If a serialization error occurs, returns an empty string.
  static std::string serialize(const NodesConfiguration& nodes_config,
                               SerializeOptions options = {true});

  static std::string debugJsonString(const NodesConfiguration& nodes_config);

  static std::shared_ptr<const NodesConfiguration> deserialize(Slice buf);
  static std::shared_ptr<const NodesConfiguration>
  deserialize(folly::StringPiece buf);

  // try to extract the nodes configuration version from a data blob.
  static folly::Optional<membership::MembershipVersion::Type>
  extractConfigVersion(folly::StringPiece serialized_data);
};

class NodesConfigurationThriftConverter {
#define GEN_SERIALIZATION_CONFIG(_Config)                 \
  static thrift::_Config toThrift(const _Config& config); \
  static std::shared_ptr<_Config> fromThrift(             \
      const thrift::_Config& flat_buffer_config);

#define GEN_SERIALIZATION_OBJECT(_Object)                 \
  static thrift::_Object toThrift(const _Object& object); \
  static int fromThrift(const thrift::_Object& obj, _Object* out);

 public:
  GEN_SERIALIZATION_CONFIG(ServiceDiscoveryConfig)
  GEN_SERIALIZATION_CONFIG(SequencerAttributeConfig)
  GEN_SERIALIZATION_CONFIG(StorageAttributeConfig)
  GEN_SERIALIZATION_CONFIG(SequencerConfig);
  GEN_SERIALIZATION_CONFIG(StorageConfig);
  GEN_SERIALIZATION_CONFIG(MetaDataLogsReplication);
  GEN_SERIALIZATION_CONFIG(NodesConfiguration);

 private:
  GEN_SERIALIZATION_OBJECT(NodeServiceDiscovery)
  GEN_SERIALIZATION_OBJECT(SequencerNodeAttribute)
  GEN_SERIALIZATION_OBJECT(StorageNodeAttribute)

#undef GEN_SERIALIZATION_CONFIG
#undef GEN_SERIALIZATION_OBJECT
};

}} // namespace configuration::nodes
}} // namespace facebook::logdevice
