/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>

namespace facebook { namespace logdevice { namespace dbg {

enum class Level : unsigned {
  NONE,
  CRITICAL,
  ERROR,
  WARNING,
  NOTIFY,
  INFO,
  DEBUG,
  SPEW
};

/**
 * log errors and debug messages at this level or worse. Default is
 * INFO. NONE is not a valid debug level and should not be used. The
 * same value is used for all logdevice::Client objects. Applications
 * can change this variable at any time.
 */
extern std::atomic<Level> currentLevel;

/**
 * Maximum size of the background logger queue. Used if a custom log function is
 * provided. The background thread queue will approximately hold this many
 * elements. if the custom logging function cannot sustain the rate of log
 * messages, the queue may fill up and new messages will be dropped.
 */
extern std::atomic<size_t> maxBufferedLogMsg;

/**
 * This function directs all error and debug output to the specified
 * fd. -1 or any other negative value turns off all logging. Until
 * this function is called, all LogDevice error and debug output will
 * go to stderr (fd 2).
 *
 * @return the previous value of debug log fd
 */
int useFD(int fd);

/**
 * Require the LogDevice library to call this callback instead of logging to
 * stderr or the last file descriptor passed to `useFD`.
 */
typedef void (*logging_fn_t)(const char*,
                             const char*,
                             const char*,
                             const int,
                             Level,
                             struct timeval created,
                             pid_t tid,
                             const char* thread_name,
                             const char* msg);
void useCallback(logging_fn_t fn);

}}} // namespace facebook::logdevice::dbg
