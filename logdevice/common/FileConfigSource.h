/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/synchronization/CallOnce.h>

#include "logdevice/common/ConfigSource.h"
#include "logdevice/common/plugin/BuiltinConfigSourceFactory.h"

namespace facebook { namespace logdevice {

class FileConfigSourceThread;

class FileConfigSource : public ConfigSource {
 public:
  explicit FileConfigSource(std::chrono::milliseconds polling_interval);
  explicit FileConfigSource(
      UpdateableSettings<BuiltinConfigSourceFactory::Settings>);
  FileConfigSource()
      : FileConfigSource(FileConfigSource::defaultPollingInterval()) {}
  ~FileConfigSource() override;

  static std::chrono::milliseconds defaultPollingInterval() {
    return std::chrono::milliseconds(10000);
  }

  std::string getName() override {
    return "file";
  }
  std::vector<std::string> getSchemes() override {
    // The empty scheme will make this source the default if the scheme isn't
    // specified
    return {"file", ""};
  }

  Status getConfig(const std::string& path, Output* out) override;

  /**
   * Lazily initializes the polling thread and returns a pointer to it (or
   * nullptr if initialization failed).
   *
   * Exposed for tests.
   */
  FileConfigSourceThread* thread();

  /**
   * Periodically invoked on a dedicated thread to see if any tracked files
   * have changed.
   */
  void checkForUpdates();

 private:
  UpdateableSettings<BuiltinConfigSourceFactory::Settings> settings_;
  // Type of modification timestamp
  using mtime_t = std::chrono::nanoseconds;
  // Thread that periodically checks for updates (lazily initialized)
  folly::once_flag thread_init_flag_;
  std::unique_ptr<FileConfigSourceThread> thread_;
  // Protects `mtimes_'
  mutable std::mutex mutex_;
  // All currently monitored files
  std::unordered_map<std::string, mtime_t> mtimes_;

  /**
   * Attempts to stat() a file and extract a (high-precision) modification
   * timestamp.
   *
   * @return Same as stat(): 0 on success, -1 on error with errno set
   */
  static int stat_mtime(const char* path, mtime_t* time_out);
};

}} // namespace facebook::logdevice
