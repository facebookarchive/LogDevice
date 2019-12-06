/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

#include "logdevice/common/checks.h"
#include "logdevice/common/toString.h"

namespace facebook { namespace logdevice { namespace ldbench {

static WorkerFactoryMap& getWorkerFactoryMapImpl() {
  static WorkerFactoryMap m;
  return m;
}

const WorkerFactoryMap& getWorkerFactoryMap() {
  registerReadWorker();
  registerWriteWorker();
  registerBackfillWorker();
  registerReadYourWriteLatencyWorker();
  registerWriteSaturationWorker();
  registerIsLogEmptyWorker();
  registerFindTimeWorker();

  return getWorkerFactoryMapImpl();
}

// Worker registration works as follows. The first call to getWorkerFactoryMap()
// calls all register*Worker() functions. Each of these functions calls
// registerWorkerImpl(), which adds the worker type to the map.

/**
 * Registers a worker.
 */
void registerWorkerImpl(std::string name,
                        WorkerTypeInfo::FactoryFunc factory,
                        OptionsRestrictions accepted_options) {
  WorkerTypeInfo info;
  info.factory = factory;
  ld_check(!accepted_options.allowed_partitioning.empty());
  if (accepted_options.allowed_partitioning.count(PartitioningMode::DEFAULT)) {
    ld_check_eq(accepted_options.allowed_partitioning.size(), 1);
  }
  info.accepted_options = std::move(accepted_options);
  getWorkerFactoryMapImpl().emplace(std::move(name), std::move(info));
}

}}} // namespace facebook::logdevice::ldbench
