/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>

#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice {

class Client;

namespace ldbench {

// To get the ist of benchmark worker types, use getWorkerFactoryMap().
// To add a new worker type, implement a subclass of Worker, add a
// register[Type]Worker() function below, implement it and call it from
// getWorkerFactoryMap().

class Worker;

/**
 * For a particular worker type, this struct holds a factory function to produce
 * workers of this type, and the set options this worker type accepts.
 */
struct WorkerTypeInfo {
  using FactoryFunc = std::function<std::unique_ptr<Worker>()>;
  FactoryFunc factory;
  OptionsRestrictions accepted_options;
};

using WorkerFactoryMap = std::map<std::string, WorkerTypeInfo>;

/**
 * Map of (benchmark name, worker factory) entries. Initialized lazily on first
 * call.
 */
const WorkerFactoryMap& getWorkerFactoryMap();

// Worker registration works as follows. The first call to getWorkerFactoryMap()
// calls all register*Worker() functions. Each of these functions calls
// registerWorkerImpl(), which adds the worker type to the map.

/**
 * Registers a worker.
 */
void registerWorkerImpl(std::string name,
                        WorkerTypeInfo::FactoryFunc factory,
                        OptionsRestrictions accepted_options);

// Each of these functions is defined in its own file, together with the
// implementation of the corresponding worker type. The function implementation
// normally consists of just a single call to registerWorkerImpl().
// If you're wondering why not call registerWorkerImpl() from constructors of
// static variables in the corresponding .cpp files: this doesn't work with
// bucks cpp_python_extension targets for some reason.
void registerReadWorker();
void registerWriteWorker();
void registerBackfillWorker();
void registerReadYourWriteLatencyWorker();
void registerWriteSaturationWorker();
void registerIsLogEmptyWorker();
void registerFindTimeWorker();

} // namespace ldbench
}} // namespace facebook::logdevice
