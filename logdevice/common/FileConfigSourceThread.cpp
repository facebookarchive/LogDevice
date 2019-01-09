/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FileConfigSourceThread.h"

#include <thread>

#include <folly/Singleton.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

FileConfigSourceThread::FileConfigSourceThread(
    FileConfigSource* parent,
    UpdateableSettings<BuiltinConfigSourceFactory::Settings> settings)
    : parent_(parent), settings_(settings) {
  pollingInterval_.store(settings_->file_config_update_interval);
  auto cb = [this]() {
    if (pollingInterval_.load() != settings_->file_config_update_interval) {
      pollingInterval_.store(settings_->file_config_update_interval);
      // Note that changing the setting will cause the updater to re-read the
      // config right away
      advisePollingIteration();
    }
  };
  settings_sub_handle_ = settings_.callAndSubscribeToUpdates(std::move(cb));

  int rv = pthread_create(&mainLoopThread_,
                          nullptr,
                          FileConfigSourceThread::threadEntryPoint,
                          (void*)this);
  if (rv != 0) {
    ld_error("pthread_create() failed to start config monitoring thread, "
             "returned %d (%s)",
             rv,
             strerror(rv));
    err = E::SYSLIMIT;
    throw ConstructorFailed();
  }
}

FileConfigSourceThread::~FileConfigSourceThread() {
  settings_sub_handle_.unsubscribe();
  {
    std::lock_guard<std::mutex> lock(mainLoopWaitMutex_);
    mainLoopStop_.store(true);
  }
  mainLoopWaitCondition_.notify_all();
  pthread_join(mainLoopThread_, nullptr);
}

void FileConfigSourceThread::advisePollingIteration() {
  mainLoopWaitCondition_.notify_all();
}

void* FileConfigSourceThread::threadEntryPoint(void* arg) {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:conf");
  FileConfigSourceThread* self = static_cast<FileConfigSourceThread*>(arg);
  ld_check(self);
  self->mainLoop();
  return nullptr;
}

void FileConfigSourceThread::mainLoop() {
  while (!mainLoopStop_.load()) {
    std::unique_lock<std::mutex> cv_lock_(mainLoopWaitMutex_);
    mainLoopWaitCondition_.wait_for(cv_lock_, pollingInterval_.load());
    if (mainLoopStop_.load()) {
      break;
    }
    parent_->checkForUpdates();
    ++main_loop_iterations_;
  }
}

}} // namespace facebook::logdevice
