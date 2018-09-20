/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdarg>

#include <event2/buffer.h>

#include "logdevice/common/libevent/compat.h"

/**
 * @file Simple utility wrapper for writing text into an evbuffer.  Helps
 * reduce the number of direct calls to evbuffer_add_* scattered throughout
 * our codebase.
 */

namespace facebook { namespace logdevice {

class EvbufferTextOutput {
 public:
  explicit EvbufferTextOutput(struct evbuffer* out) : out_(out) {}
  EvbufferTextOutput() : out_(nullptr) {}

  void printf(const char* fmt, ...)
      __attribute__((__format__(__printf__, 2, 3))) {
    va_list args;
    va_start(args, fmt);
    LD_EV(evbuffer_add_vprintf)(out_, fmt, args);
    va_end(args);
  }

  void write(const void* buf, size_t n) {
    LD_EV(evbuffer_add)(out_, buf, n);
  }

  void write(const char* str) {
    this->write(str, strlen(str));
  }

  void write(const std::string& str) {
    this->write(str.data(), str.size());
  }

 private:
  struct evbuffer* out_;
};

}} // namespace facebook::logdevice
