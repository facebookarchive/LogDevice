/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec_generated.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

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

 private:
  GEN_SERIALIZATION_OBJECT(NodeServiceDiscovery)
  GEN_SERIALIZATION_OBJECT(SequencerNodeAttribute)
  GEN_SERIALIZATION_OBJECT(StorageNodeAttribute)

#undef GEN_SERIALIZATION_CONFIG
#undef GEN_SERIALIZATION_OBJECT
};

}}}} // namespace facebook::logdevice::configuration::nodes
