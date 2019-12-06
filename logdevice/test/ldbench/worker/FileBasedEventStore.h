/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fstream>
#include <string>
#include <thread>

#include <folly/MPMCQueue.h>
#include <folly/json.h>

#include "logdevice/test/ldbench/worker/EventStore.h"

namespace facebook { namespace logdevice { namespace ldbench {

class BufferDrainerThread {
 public:
  BufferDrainerThread(std::fstream* event_writer,
                      folly::MPMCQueue<folly::dynamic>* buff)
      : event_writer_(event_writer),
        buff_(buff),
        is_draining_(true),
        thread_(std::bind(&BufferDrainerThread::drainBuffLoop, this)) {}

  ~BufferDrainerThread() {
    thread_.join();
    // we close the file here to make sure all events are persisted
    event_writer_->flush();
    event_writer_->close();
  }

  bool isDraining() {
    return is_draining_;
  }

  void drainBuffLoop() {
    folly::dynamic event_obj;
    while (true) {
      buff_->blockingRead(event_obj);
      if (event_obj.empty()) {
        // we use empty object to stop draining process
        break;
      }
      folly::json::serialization_opts opts;
      std::string write_line = folly::json::serialize(event_obj, opts);
      *event_writer_ << write_line << std::endl;
    }
    is_draining_ = false;
    return;
  }

 private:
  std::fstream* event_writer_;
  folly::MPMCQueue<folly::dynamic>* buff_;
  std::atomic<bool> is_draining_;
  std::thread thread_;
};
/* *
 * write events to files
 */
class FileBasedEventStore : public EventStore {
 public:
  FileBasedEventStore(std::string dir_name)
      : buff_(10240),
        event_writer_(dir_name, std::fstream::app),
        drain_thread_(&event_writer_, &buff_) {}

  ~FileBasedEventStore() {
    // write an empty obj to tell drainerThread to stop
    folly::dynamic empty_obj;
    buff_.blockingWrite(std::move(empty_obj));
  }

  bool isReady() override {
    return event_writer_.is_open() && drain_thread_.isDraining();
  }

  void logEvent(folly::dynamic event_obj) override {
    // The sampled events will be dropped if the buffer is full.
    // The filled-up buffer means the operation is every intensive.
    // It should be fine to skip some sampled events since
    // they may have every similar values.
    buff_.write(std::move(event_obj));
    return;
  }

 private:
  // Currently, we empirically allocate 400KB for buffer
  // TODO: decide the buffer size based on by the sample ratio and write-rate
  folly::MPMCQueue<folly::dynamic> buff_;
  std::fstream event_writer_;
  BufferDrainerThread drain_thread_;
};

}}} // namespace facebook::logdevice::ldbench
