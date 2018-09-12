/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "FileConfigSource.h"

#include <errno.h>
#include <memory>
#include <sys/stat.h>

#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/FileConfigSourceThread.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

FileConfigSource::FileConfigSource(std::chrono::milliseconds polling_interval)
    : polling_interval_(polling_interval) {}

FileConfigSource::~FileConfigSource() {
  // The FileConfigSourceThread that accesses this FileConfigSource should be
  // shut down before anything else is destroyed
  thread_.reset();
}

Status FileConfigSource::getConfig(const std::string& path, Output* out) {
  // Stat before reading the file - it's better for the timestamp to be a bit
  // stale.  (If it's fresher than the file contents, we might skip a config
  // update.)
  mtime_t mtime;
  if (stat_mtime(path.c_str(), &mtime) != 0) {
    return E::FILE_OPEN;
  }

  out->contents = configuration::parser::readFileIntoString(path.c_str());
  if (out->contents.empty()) {
    return err;
  }

  {
    std::lock_guard<std::mutex> guard(mutex_);
    mtimes_[path] = mtime;
  }
  out->mtime = std::chrono::duration_cast<std::chrono::milliseconds>(mtime);

  FileConfigSourceThread* th = thread();
  return th != nullptr ? E::OK : err;
}

void FileConfigSource::checkForUpdates() {
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto& entry : mtimes_) {
    const std::string& path = entry.first;
    mtime_t mtime;
    int rv = FileConfigSource::stat_mtime(path.c_str(), &mtime);
    if (rv != 0 || mtime == entry.second) {
      continue;
    }
    entry.second = mtime;
    ld_info("Change detected in config file %s, mtime = %ld",
            path.c_str(),
            mtime.count());
    std::string contents =
        configuration::parser::readFileIntoString(path.c_str());
    if (!contents.empty()) {
      Output output;
      output.contents = std::move(contents);
      output.mtime =
          std::chrono::duration_cast<std::chrono::milliseconds>(mtime);
      async_cb_->onAsyncGet(this, path, E::OK, std::move(output));
    }
  }
}

FileConfigSourceThread* FileConfigSource::thread() {
  folly::call_once(thread_init_flag_, [&]() {
    try {
      thread_ =
          std::make_unique<FileConfigSourceThread>(this, polling_interval_);
    } catch (const ConstructorFailed&) {
      // `thread_' remains null, err set by the constructor
    }
  });
  return thread_.get();
}

int FileConfigSource::stat_mtime(const char* path, mtime_t* time_out) {
  ld_check(time_out != nullptr);

  struct stat st;
  int rv = stat(path, &st);
  if (rv == 0) {
    std::chrono::nanoseconds ns(
        uint64_t(st.st_mtime) * 1000000000 +
        // assumes _POSIX_SOURCE >= 200809L || _XOPEN_SOURCE >= 700
        st.st_mtim.tv_nsec);
    *time_out = std::chrono::duration_cast<mtime_t>(ns);
  } else {
    ld_error("stat() on config file \"%s\" failed. errno=%d (%s)",
             path,
             errno,
             strerror(errno));
  }
  return rv;
}

}} // namespace facebook::logdevice
