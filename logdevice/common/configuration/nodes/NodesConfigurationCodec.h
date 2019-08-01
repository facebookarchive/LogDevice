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
#include "logdevice/common/configuration/utils/ConfigurationCodec.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class NodesConfigurationThriftConverter {
#define GEN_SERIALIZATION_CONFIG(_Config)                 \
  static thrift::_Config toThrift(const _Config& config); \
  static std::shared_ptr<_Config> fromThrift(             \
      const thrift::_Config& thrift_config);

#define GEN_SERIALIZATION_OBJECT(_Object)                 \
  static thrift::_Object toThrift(const _Object& object); \
  static int fromThrift(const thrift::_Object& obj, _Object* out);

 public:
  using ThriftConfigType = thrift::NodesConfiguration;

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

using NodesConfigurationCodec =
    ConfigurationCodec<NodesConfiguration,
                       membership::MembershipVersion::Type,
                       NodesConfigurationThriftConverter,
                       /*CURRENT_PROTO_VERSION*/ 1>;

}}}} // namespace facebook::logdevice::configuration::nodes
