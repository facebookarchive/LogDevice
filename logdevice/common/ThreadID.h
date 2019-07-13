/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <pthread.h>
#include <unistd.h>

#include <folly/Range.h>
#include <sys/syscall.h>
#include <sys/types.h>

/**
 * @file A class for identifying a thread
 */

namespace facebook { namespace logdevice {
class ThreadID {
 public:
  enum Type {
    UNKNOWN = 0,        // Default type that will be returned if nothing was set
    SERVER_WORKER,      // Worker threads (server context)
    CLIENT_WORKER,      // Worker threads (client context)
    CPU_EXEC,           // Threads belonging to CPUExecThreadPool
    UNKNOWN_WORKER,     // Worker threads (unknown context, only in tests)
    UNKNOWN_EVENT_LOOP, // EventLoop threads that are not Worker's
                        // (only in tests and tools)
    STORAGE,            // Storage threads
    LOGSDB,             // PartitionedRocksDBStore background threads
    ROCKSDB,            // RocksDB background threads
    UTILITY,            // LogDevice utility threads
    WHEEL_TIMER,        // WheelTimer class
  };

  /**
   * Call this from a thread to register its type and number, as well as set
   * the thread name for debugging purposes.
   * The name must be no longer than 15 characters.
   * If name is empty, thrad name is left unchanged.
   */
  static void set(Type type, folly::StringPiece name);

  static bool isWorker() {
    return type_ == Type::SERVER_WORKER || type_ == Type::CLIENT_WORKER ||
        type_ == Type::UNKNOWN_WORKER || type_ == CPU_EXEC;
  }
  static bool isEventLoop() {
    return isWorker() || type_ == Type::UNKNOWN_EVENT_LOOP;
  }
  static Type getType() {
    return type_;
  }

  // Always inline because this is called during logging, we don't want to have
  // to page in code if the system is under stress.
  static const char* getName() __attribute__((__always_inline__)) {
    return type_ == Type::UNKNOWN ? "not-set" : name_.data();
  }

  // Always inline because this is called during logging, we don't want to have
  // to page in code if the system is under stress.
  static pid_t getId() __attribute__((__always_inline__)) {
    // Declaring tid as a local static, rather than a static field, allows
    // us to put the default value inline, and produces much tighter code.
    static __thread pid_t tid{0}; /* library-local */
    if (UNLIKELY(tid == 0)) {
      tid = syscall(SYS_gettid);
    }
    return tid;
  }

 private:
  static __thread Type type_;
  static __thread std::array<char, 16> name_;
};
}} // namespace facebook::logdevice
