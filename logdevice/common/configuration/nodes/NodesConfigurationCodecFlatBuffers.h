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
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec_generated.h"

namespace facebook { namespace logdevice {

class ProtocolWriter;
class ProtocolReader;

namespace configuration { namespace nodes {

class NodesConfigurationCodecFlatBuffers {
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

#define GEN_SERIALIZATION_CONFIG(_Config)                           \
  static flatbuffers::Offset<flat_buffer_codec::_Config> serialize( \
      flatbuffers::FlatBufferBuilder& b, const _Config& config);    \
  static std::shared_ptr<_Config> deserialize(                      \
      const flat_buffer_codec::_Config* flat_buffer_config);

#define GEN_SERIALIZATION_OBJECT(_Object)                           \
  static flatbuffers::Offset<flat_buffer_codec::_Object> serialize( \
      flatbuffers::FlatBufferBuilder& b, const _Object& object);    \
  static int deserialize(const flat_buffer_codec::_Object* obj, _Object* out);

  GEN_SERIALIZATION_CONFIG(ServiceDiscoveryConfig)
  GEN_SERIALIZATION_CONFIG(SequencerAttributeConfig)
  GEN_SERIALIZATION_CONFIG(StorageAttributeConfig)
  GEN_SERIALIZATION_CONFIG(SequencerConfig);
  GEN_SERIALIZATION_CONFIG(StorageConfig);
  GEN_SERIALIZATION_CONFIG(MetaDataLogsReplication);
  GEN_SERIALIZATION_CONFIG(NodesConfiguration);

  ////////// serialization to linear buffer ///////////

  struct SerializeOptions {
    // use zstd to compress the configuration data blob
    bool compression;
  };

  /**
   * Serializes the object into the buffer handled by ProtocoWriter.
   *
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

  static std::shared_ptr<const NodesConfiguration> deserialize(Slice buf);
  static std::shared_ptr<const NodesConfiguration>
  deserialize(folly::StringPiece buf);

  // try to extract the nodes configuration version from a data blob. An empty
  // string is considered equivalent to a (default-constructed)
  // NodesConfiguration with EMPTY_VERSION.
  static folly::Optional<membership::MembershipVersion::Type>
  extractConfigVersion(folly::StringPiece serialized_data);

  // Uses the verifer to verify that the passed Slice is correctly formatted and
  // returns a pointer to the root table of type T.
  // Returns a nullptr if the verification fails.
  template <class T>
  static const T* verifyAndGetRoot(Slice data_blob);

 private:
  GEN_SERIALIZATION_OBJECT(NodeServiceDiscovery)
  GEN_SERIALIZATION_OBJECT(SequencerNodeAttribute)
  GEN_SERIALIZATION_OBJECT(StorageNodeAttribute)

#undef GEN_SERIALIZATION_CONFIG
#undef GEN_SERIALIZATION_OBJECT
};

}} // namespace configuration::nodes
}} // namespace facebook::logdevice
