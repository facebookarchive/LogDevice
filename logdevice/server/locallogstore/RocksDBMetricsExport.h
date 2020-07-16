/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {
class Processor;
class ShardedRocksDBLocalLogStore;

/**
 * @file
 * `RocksDBMetricsExport` defines interface for exporting RocksDB metrics to
 * external monitoring and alerting system.
 */
class RocksDBMetricsExport : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::ROCKSDB_METRICS_EXPORT;
  }
  /**
   * Registers internal RocksDB metrics from given store to be continiously
   * exported to external monitoring system.
   *
   * This method should be called once on start-up and it is safe to call it in
   * binary running without storage role.
   */
  virtual void operator()(ShardedRocksDBLocalLogStore* store,
                          Processor* processor) = 0;
};
}} // namespace facebook::logdevice
