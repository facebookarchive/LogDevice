/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/configuration/utils/ConfigurationCodec.h"
#include "logdevice/server/epoch_store/LogMetaData.h"
#include "logdevice/server/epoch_store/if/gen-cpp2/EpochStore_types.h"

namespace facebook { namespace logdevice {

class LogMetaDataThriftConverter {
 public:
  using ThriftConfigType = epoch_store::thrift::LogMetaData;

  static epoch_store::thrift::LogMetaData toThrift(const LogMetaData&);
  static std::shared_ptr<LogMetaData>
  fromThrift(const epoch_store::thrift::LogMetaData&);

 private:
  // EpochMetadta (de)serialization
  static EpochMetaData
  epochMetaDataFromThrift(const epoch_store::thrift::LogMetaData&);

  static void epochMetaDataToThrift(const EpochMetaData&,
                                    epoch_store::thrift::LogMetaData&);

  // EpochStoreMetaProperties (de)serialization
  static EpochStoreMetaProperties
  metaPropertiesFromThrift(const epoch_store::thrift::LogMetaData&);
  static void metaPropertiesToThrift(const EpochStoreMetaProperties&,
                                     epoch_store::thrift::LogMetaData&);

  // TailRecord (de)serialization
  static TailRecord
  tailRecordFromThrift(const epoch_store::thrift::TailRecord&);
  static epoch_store::thrift::TailRecord tailRecordToThrift(const TailRecord&);

  // StorageSet (de)serialization
  static StorageSet
  storageSetFromThrift(const epoch_store::thrift::StorageSet&);
  static epoch_store::thrift::StorageSet storageSetToThrift(const StorageSet&);

  // NodeSetParams (de)serialization
  static EpochMetaData::NodeSetParams
  nodesetParamsFromThrift(const epoch_store::thrift::NodeSetParams&);
  static epoch_store::thrift::NodeSetParams
  nodesetParamsToThrift(const EpochMetaData::NodeSetParams&);
};

using LogMetaDataCodec =
    configuration::ConfigurationCodec<LogMetaData,
                                      LogMetaData::Version,
                                      LogMetaDataThriftConverter,
                                      /*CURRENT_PROTO_VERSION*/ 1>;

}} // namespace facebook::logdevice
