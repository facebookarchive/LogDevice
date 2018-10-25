/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file Allows to get a backtrace or kernel stack trace on demand. Used to
 * help investigate worker thread stalls.
 */

class BacktraceRunner : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::BACKTRACE_RUNNER;
  }

  // Called by watchdog thread for each stalled worker thread, if
  // Settings::watchdog_print_bt_on_stall is true.  Called with the thread id of
  // the worker, as returned by gettid(2), which is different than
  // pthread_self().  Intended for printing the stack trace of the given thread.
  virtual void printBacktraceOnStall(int pid) = 0;

  // Called by watchdog thread once, if Settings::watchdog_print_bt_on_stall is
  // true.  Intended to call 'kernelctl walker' which outputs stack traces of
  // threads in UNINTERRUPTIBLE state.
  virtual void printKernelStacktrace() = 0;
};

}} // namespace facebook::logdevice
