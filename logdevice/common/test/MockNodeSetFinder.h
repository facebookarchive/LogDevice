/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <functional>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class MockNodeSetFinder : public NodeSetFinder {
 public:
  MockNodeSetFinder(StorageSet storage_set,
                    ReplicationProperty replication,
                    std::function<void(Status status)> callback)
      : NodeSetFinder(logid_t(1), std::chrono::milliseconds::max(), callback),
        storage_set_(storage_set),
        replication_(replication) {}

  virtual void start() override {
    EpochMetaDataMap::Map map;
    map[EPOCH_MIN] = EpochMetaData(storage_set_, replication_);
    auto result = EpochMetaDataMap::create(
        std::make_shared<const EpochMetaDataMap::Map>(std::move(map)),
        EPOCH_MAX);
    setResultAndFinalize(E::OK, std::move(result));
  }

 private:
  StorageSet storage_set_;
  ReplicationProperty replication_;
};

}} // namespace facebook::logdevice
